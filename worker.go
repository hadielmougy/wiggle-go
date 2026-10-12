package wiggle

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hadielmougy/wiggle-go/criteria"
)

// activityHandler is the internal, pre-wrapped step handler: it returns the value to report
// (the complete next context for a task, a bool for a predicate, nil for an effect).
type activityHandler func(*StepRun, Context) (any, error)

type claim struct{ workflow, step, kind string }

// WorkerOption tunes a Worker.
type WorkerOption func(*Worker)

// WithConcurrency sets the maximum steps in flight (default: NumCPU).
func WithConcurrency(n int) WorkerOption { return func(w *Worker) { w.concurrency = n } }

// WithLease sets how long a claimed step may run before the server reclaims it (default 30s).
func WithLease(d time.Duration) WorkerOption {
	return func(w *Worker) { w.leaseMillis = d.Milliseconds() }
}

// WithQueues restricts the worker to the given queues (else it serves every queue it learns about).
func WithQueues(names ...string) WorkerOption {
	return func(w *Worker) {
		w.explicitQueues = map[string]bool{}
		for _, n := range names {
			w.explicitQueues[n] = true
		}
	}
}

// WithCriteria serves each criteria's queue, claiming only the tasks its predicate holds for (see
// package criteria), instead of the queues of the steps the worker binds. It replaces WithQueues.
func WithCriteria(cs ...criteria.QueueCriteria) WorkerOption {
	return func(w *Worker) { w.criteria = append(w.criteria, cs...) }
}

// WithAwaitRegistration makes Start wait up to d for a Handle-bound workflow's graph to appear before
// failing (rides out a registration race). Zero fails fast.
func WithAwaitRegistration(d time.Duration) WorkerOption {
	return func(w *Worker) { w.awaitRegistration = d }
}

// Worker pulls tasks it has capacity for, runs the matching handler, and reports the result. It holds
// no durable state: a crash loses at most the in-flight step, which the server re-leases. A worker
// implements steps by name (Handle); topology is registered separately (Client.Register of a FlowSpec
// from Define, or the wiggle CLI).
type Worker struct {
	client *Client
	id     string

	handlers     map[string]activityHandler
	itemHandlers map[string]ItemActivity
	queues       map[string]bool
	claims       []claim
	handlerSets  []handlerSet

	concurrency       int
	leaseMillis       int64
	waitMillis        int64
	idleBackoff       time.Duration
	errorBackoff      time.Duration
	awaitRegistration time.Duration
	explicitQueues    map[string]bool
	criteria          []criteria.QueueCriteria

	branchSteps  map[string]map[string]bool // workflow -> canonical names of steps only created branches run
	createdKinds map[string]string          // activity -> the kind a branch step's handler was bound as
	lazy         sync.Map                   // activity -> activityHandler bound on a created node's first dispatch
	graphsMu     sync.Mutex
	graphs       map[string]map[string]any // "workflow:version" -> registered graph

	running atomic.Bool
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewWorker creates a worker bound to a client.
func NewWorker(client *Client, id string, opts ...WorkerOption) *Worker {
	w := &Worker{
		client: client, id: id,
		handlers:     map[string]activityHandler{},
		itemHandlers: map[string]ItemActivity{},
		branchSteps:  map[string]map[string]bool{},
		createdKinds: map[string]string{},
		graphs:       map[string]map[string]any{},
		queues:       map[string]bool{},
		concurrency:  runtime.NumCPU(),
		leaseMillis:  30_000,
		waitMillis:   10_000,
		idleBackoff:  200 * time.Millisecond,
		errorBackoff: 2 * time.Second,
	}
	for _, o := range opts {
		o(w)
	}
	return w
}

// Handle binds a task handler to one step of an already-registered workflow, by name. On Start the
// worker reconciles the binding against the server graph.
func (w *Worker) Handle(workflow, step string, fn Activity) *Worker {
	return w.bind(workflow, step, "TASK", taskHandler(fn))
}

// HandleStep binds a task handler that also receives the running StepRun, to read its attempt or
// emit events. See Handle.
func (w *Worker) HandleStep(workflow, step string, fn StepActivity) *Worker {
	return w.bind(workflow, step, "TASK", stepHandler(fn))
}

// HandleGate binds a predicate step (gate / choose guard / do-while condition) by name.
func (w *Worker) HandleGate(workflow, step string, test Predicate) *Worker {
	return w.bind(workflow, step, "PREDICATE", func(_ *StepRun, ctx Context) (any, error) {
		return test(ctx)
	})
}

// HandleEffect binds a side-effect step by name; the context is left unchanged.
func (w *Worker) HandleEffect(workflow, step string, fn SideEffect) *Worker {
	return w.bind(workflow, step, "TASK", func(_ *StepRun, ctx Context) (any, error) {
		return nil, fn(ctx)
	})
}

// HandleCombine binds the mandatory merge step that follows a fork's join. fn receives the context
// with each branch's result staged under the branch's name, and must return the COMPLETE post-join
// context: it is sent verbatim (no diff, no implicit fold) and the engine REPLACES the context with
// it -- keys fn omits do not survive the join.
func (w *Worker) HandleCombine(workflow, step string, fn Activity) *Worker {
	return w.bind(workflow, step, "COMBINE", combineHandler(withoutStep(fn)))
}

// HandleCombineStep is HandleCombine for a combine that also receives the running StepRun: a
// spawning step's combine reads its branches' results through StepRun.Results, and may Create the
// next round.
func (w *Worker) HandleCombineStep(workflow, step string, fn StepActivity) *Worker {
	return w.bind(workflow, step, "COMBINE", combineHandler(fn))
}

// HandleCompensation binds the undo of a compensable step (one declared Compensate in the
// topology). fn runs in the reverse pass after the instance fails, receiving the step's
// input/result snapshots. Start refuses a worker that serves a compensable step's forward handler
// without also binding its compensator — the undo task is minted on the same queue, and a worker
// that can do but not undo would strand the reverse pass.
func (w *Worker) HandleCompensation(workflow, step string, fn Compensator) *Worker {
	activity := workflow + "#" + step + "#compensate"
	if _, dup := w.handlers[activity]; dup {
		panic(fmt.Sprintf("duplicate compensator for activity %q", activity))
	}
	w.handlers[activity] = compensationHandler(fn)
	w.claims = append(w.claims, claim{workflow, step, "COMPENSATE"})
	return w
}

// HandleItem binds one step of a forEach body: fn receives the frozen pre-forEach context (base,
// read-only) and the item's current value, and its return replaces that value (nil = untouched).
func (w *Worker) HandleItem(workflow, step string, fn ItemActivity) *Worker {
	activity := workflow + "#" + step
	if _, dup := w.itemHandlers[activity]; dup {
		panic(fmt.Sprintf("duplicate handler for activity %q", activity))
	}
	w.itemHandlers[activity] = fn
	w.claims = append(w.claims, claim{workflow, step, "TASK"})
	return w
}

// BranchSteps declares steps of workflow that only branches created at run time run
// (s.Create(...).ThenApply(step)), so they are in no registered graph. Their handlers -- bound with
// Handle and its siblings, or methods of a RegisterHandler struct -- are then not matched against
// the graph on Start, but bound when such a step is first dispatched. A branch step on a queue no
// graph mentions runs only on a worker that polls that queue (Queues or WithCriteria).
func (w *Worker) BranchSteps(workflow string, steps ...string) *Worker {
	if workflow == "" || len(steps) == 0 {
		panic("BranchSteps needs a workflow and at least one step")
	}
	set := w.branchSteps[workflow]
	if set == nil {
		set = map[string]bool{}
		w.branchSteps[workflow] = set
	}
	for _, s := range steps {
		set[canonicalName(s)] = true
	}
	return w
}

func (w *Worker) bind(workflow, step, kind string, h activityHandler) *Worker {
	activity := workflow + "#" + step
	if _, dup := w.handlers[activity]; dup {
		panic(fmt.Sprintf("duplicate handler for activity %q", activity))
	}
	w.handlers[activity] = h
	w.claims = append(w.claims, claim{workflow, step, kind})
	return w
}

func (w *Worker) servedQueues() []string {
	if len(w.criteria) > 0 {
		out := make([]string, 0, len(w.criteria))
		for _, c := range w.criteria {
			out = append(out, c.Queue)
		}
		sort.Strings(out)
		return out
	}
	src := w.queues
	if w.explicitQueues != nil {
		src = w.explicitQueues
	}
	out := make([]string, 0, len(src))
	for q := range src {
		out = append(out, q)
	}
	sort.Strings(out)
	return out
}

// Start reconciles the Handle-bound claims against the registered graph, then begins polling.
func (w *Worker) Start(ctx context.Context) error {
	if !w.running.CompareAndSwap(false, true) {
		return nil
	}
	if err := w.checkCriteria(); err != nil {
		w.running.Store(false)
		return err
	}
	if len(w.claims) > 0 || len(w.handlerSets) > 0 {
		if err := w.reconcile(ctx); err != nil {
			w.running.Store(false)
			return err
		}
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.wg.Add(1)
	go w.pollLoop()
	log.Printf("wiggle: worker %s polling queues %v (concurrency %d)", w.id, w.servedQueues(), w.concurrency)
	return nil
}

// Stop halts polling and waits for in-flight steps to finish.
func (w *Worker) Stop() {
	if !w.running.CompareAndSwap(true, false) {
		return
	}
	w.cancel()
	w.wg.Wait()
}

// reconcile checks every Handle-bound claim against the registered graph and learns the queue each
// claimed step polls. It fails fast on a mistyped step name or a kind mismatch.
func (w *Worker) reconcile(ctx context.Context) error {
	byWorkflow := map[string][]claim{}
	for _, c := range w.claims {
		byWorkflow[c.workflow] = append(byWorkflow[c.workflow], c)
	}
	for wf, claims := range byWorkflow {
		graph, err := w.fetchGraph(ctx, wf)
		if err != nil {
			return err
		}
		nodes := map[string]map[string]any{} // activity -> node
		for _, n := range asList(graph["nodes"]) {
			node, _ := n.(map[string]any)
			kind, _ := node["kind"].(string)
			act, _ := node["activity"].(string)
			if act != "" && (kind == "TASK" || kind == "PREDICATE") {
				nodes[act] = node
			}
		}
		branch := w.branchSteps[wf]
		for _, c := range claims {
			activity := c.workflow + "#" + c.step
			node, ok := nodes[activity]
			if !ok && branch[canonicalName(c.step)] {
				if c.kind == "COMPENSATE" {
					w.createdKinds[activity+"#compensate"] = c.kind
				} else {
					w.createdKinds[activity] = c.kind
				}
				continue
			}
			if !ok {
				return fmt.Errorf("no step %q in registered workflow %q (available: %v)",
					c.step, wf, availableSteps(nodes))
			}
			k, _ := node["kind"].(string)
			combine := isCombine(node)
			compensable, _ := node["compensable"].(bool)
			switch {
			case c.kind == "COMPENSATE" && (k != "TASK" || combine || !compensable):
				return fmt.Errorf("step %q of workflow %q is not declared Compensate in the "+
					"topology; HandleCompensation binds only compensable steps", c.step, wf)
			case c.kind == "COMPENSATE":
				// ok: a compensator on a compensable step
			case c.kind == "COMBINE" && !combine:
				return fmt.Errorf("activity %q is not a fork combine; bind it with Handle()", activity)
			case c.kind == "COMBINE":
				// ok: a combine claim on a combine node
			case combine:
				return fmt.Errorf("activity %q is a fork combine; bind it with HandleCombine() -- "+
					"its return is the complete post-join context (no implicit fold)", activity)
			case k != c.kind:
				verb := "Handle"
				if k == "PREDICATE" {
					verb = "HandleGate"
				}
				return fmt.Errorf("activity %q is a %s in the graph but bound as %s; use %s()",
					activity, k, c.kind, verb)
			}
			queue, _ := node["queue"].(string)
			if queue == "" {
				queue = wf
			}
			w.queues[queue] = true
		}
		// Pairing: a forward handler on a compensable step requires its compensator here too —
		// the undo task lands on the same queue this worker polls, so "can do but not undo"
		// would strand the reverse pass at claim time.
		compensated := map[string]bool{}
		for _, c := range claims {
			if c.kind == "COMPENSATE" {
				compensated[c.step] = true
			}
		}
		for _, c := range claims {
			if c.kind != "TASK" || compensated[c.step] {
				continue
			}
			if node, ok := nodes[c.workflow+"#"+c.step]; ok {
				if compensable, _ := node["compensable"].(bool); compensable {
					return fmt.Errorf("step %q of workflow %q is declared Compensate but this "+
						"worker binds no compensator; add HandleCompensation(%q, %q, ...)",
						c.step, wf, wf, c.step)
				}
			}
		}
	}
	for _, set := range w.handlerSets {
		if err := w.matchHandlerSet(ctx, set); err != nil {
			return err
		}
	}
	return nil
}

func (w *Worker) fetchGraph(ctx context.Context, workflow string) (map[string]any, error) {
	deadline := time.Now().Add(w.awaitRegistration)
	for {
		g, err := w.client.GetWorkflow(ctx, workflow)
		if err == nil {
			if v, ok := g["version"].(float64); ok {
				w.graphsMu.Lock()
				w.graphs[fmt.Sprintf("%s:%d", workflow, int64(v))] = g
				w.graphsMu.Unlock()
			}
			return g, nil
		}
		if time.Now().Before(deadline) {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		return nil, fmt.Errorf("workflow %q is not registered; register its graph before starting a "+
			"worker that binds handlers to it (or set WithAwaitRegistration): %w", workflow, err)
	}
}

func availableSteps(nodes map[string]map[string]any) []string {
	out := make([]string, 0, len(nodes))
	for act := range nodes {
		if i := indexHash(act); i >= 0 {
			out = append(out, act[i+1:])
		}
	}
	sort.Strings(out)
	return out
}

func indexHash(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] == '#' {
			return i
		}
	}
	return -1
}

// ---- poll / execute ----

func (w *Worker) pollLoop() {
	defer w.wg.Done()
	sem := make(chan struct{}, w.concurrency)
	var inflight sync.WaitGroup
	for w.running.Load() {
		free := w.concurrency - len(sem)
		if free <= 0 {
			w.sleep(w.idleBackoff)
			continue
		}
		var res *PollResult
		var err error
		if len(w.criteria) > 0 {
			res, err = w.client.PollCriteria(w.ctx, w.id, w.criteria, free, w.leaseMillis, w.waitMillis)
		} else {
			res, err = w.client.Poll(w.ctx, w.id, w.servedQueues(), free, w.leaseMillis, w.waitMillis)
		}
		if err != nil {
			if !w.running.Load() {
				break
			}
			log.Printf("wiggle: poll failed: %v", err)
			w.sleep(w.errorBackoff)
			continue
		}
		if len(res.Tasks) == 0 {
			backoff := w.idleBackoff
			if res.RetryAfterMillis > 0 {
				backoff = time.Duration(res.RetryAfterMillis) * time.Millisecond
			}
			w.sleep(backoff)
			continue
		}
		for _, t := range res.Tasks {
			sem <- struct{}{}
			inflight.Add(1)
			go func(task *Task) {
				defer func() { <-sem; inflight.Done() }()
				w.execute(task)
			}(t)
		}
	}
	inflight.Wait()
}

func (w *Worker) execute(task *Task) {
	ctx := context.Background()
	created := isCreatedNode(task.NodeID)
	if task.IsItem {
		if _, item := w.itemHandlers[task.Activity]; item || !created {
			w.executeItem(ctx, task)
			return
		}
	}
	if _, item := w.itemHandlers[task.Activity]; item {
		_ = w.client.Fail(ctx, task.TaskID, task.LeaseOwner,
			fmt.Sprintf("activity %q was bound with HandleItem but is not a forEach item step; use Handle()", task.Activity), false)
		return
	}
	handler, err := w.handlerFor(task)
	if err != nil {
		_ = w.client.Fail(ctx, task.TaskID, task.LeaseOwner, err.Error(), false)
		return
	}
	if handler == nil {
		_ = w.client.Fail(ctx, task.TaskID, task.LeaseOwner,
			fmt.Sprintf("no handler registered for activity %q", task.Activity), false)
		return
	}
	if task.IsItem {
		if _, object := task.RawContext.(map[string]any); !object {
			_ = w.client.Fail(ctx, task.TaskID, task.LeaseOwner, fmt.Sprintf("step %q runs on a %T, not a "+
				"JSON object; bind it with HandleItem(base, item)", task.StepName, task.RawContext), false)
			return
		}
	}
	stop := w.startHeartbeat(task)
	defer close(stop)

	input, base, results, combine, err := unstage(task)
	if err != nil {
		_ = w.client.Fail(ctx, task.TaskID, task.LeaseOwner, err.Error(), false)
		return
	}
	step := newStep(task)
	step.creates = w.createsBranches(task)
	step.results, step.hasResults = results, combine
	switch {
	case task.IsItem:
		step.base, step.hasBase = task.BaseContext, true
	case combine:
		step.base, step.hasBase = base, true
	}
	started := time.Now()
	result, err := handler(step, input)
	if err != nil {
		w.fail(ctx, task, err)
		return
	}
	events, branches, err := step.drain()
	if err != nil {
		_ = w.client.Fail(ctx, task.TaskID, task.LeaseOwner, err.Error(), false)
		return
	}
	report := StepReport{NodeID: task.NodeID, StartedAt: started.UnixMilli(),
		FinishedAt: started.UnixMilli() + time.Since(started).Milliseconds(), Events: events, Branches: branches}
	if task.Kind == "PREDICATE" {
		b, isBool := result.(bool)
		if !isBool {
			_ = w.client.Fail(ctx, task.TaskID, task.LeaseOwner,
				fmt.Sprintf("predicate %q returned %T", task.StepName, result), false)
			return
		}
		report.Predicate = &b
	} else {
		report.Merge = result
	}
	w.report(ctx, task, report)
}

func isCreatedNode(nodeID string) bool { return strings.HasPrefix(nodeID, "~") }

// handlerFor is the handler for task. A step on a created node is in no graph, so it binds by name
// on first dispatch -- from a Handle binding of a declared branch step, or a method of a
// RegisterHandler struct -- and so does its undo, which may run on a worker that never ran the
// step. It fails when the handler does not fit the dispatched step.
func (w *Worker) handlerFor(task *Task) (activityHandler, error) {
	h := w.handlers[task.Activity]
	if !isCreatedNode(task.NodeID) {
		return h, nil
	}
	undo := strings.HasSuffix(task.Activity, "#compensate")
	combine := task.CollectKey != "" || len(task.ArmNames) > 0
	kind := task.Kind
	switch {
	case undo:
		kind = "COMPENSATE"
	case combine:
		kind = "COMBINE"
	}
	if h != nil {
		if claimed, ok := w.createdKinds[task.Activity]; ok && claimed != kind {
			return nil, fmt.Errorf("step %q runs as a %s but its handler was bound as a %s",
				task.StepName, kind, claimed)
		}
		return h, nil
	}
	if bound, ok := w.lazy.Load(task.Activity); ok {
		return bound.(activityHandler), nil
	}
	i := indexHash(task.Activity)
	if i < 0 {
		return nil, nil
	}
	workflow, name := task.Activity[:i], strings.TrimSuffix(task.Activity[i+1:], "#compensate")
	for _, set := range w.handlerSets {
		if set.workflow != workflow {
			continue
		}
		bound, err := bindCreated(set, name, task.Kind, combine && !undo, undo)
		if err != nil {
			return nil, err
		}
		if bound != nil {
			actual, _ := w.lazy.LoadOrStore(task.Activity, bound)
			return actual.(activityHandler), nil
		}
	}
	return nil, nil
}

// createsBranches reports whether task may call Create: a spawning step or its combine. A node this
// worker does not hold -- a created one, or another version's -- is left to the server.
func (w *Worker) createsBranches(task *Task) bool {
	if strings.HasPrefix(task.CollectKey, spawnScratchPrefix) || isCreatedNode(task.NodeID) {
		return true
	}
	w.graphsMu.Lock()
	graph := w.graphs[fmt.Sprintf("%s:%d", task.Workflow, task.Version)]
	w.graphsMu.Unlock()
	if graph == nil {
		return true
	}
	return spawns(graph, task.NodeID)
}

// checkCriteria refuses criteria beside Queues, or two criteria for one queue.
func (w *Worker) checkCriteria() error {
	if len(w.criteria) == 0 {
		return nil
	}
	if w.explicitQueues != nil {
		return fmt.Errorf("worker %s: set Queues or WithCriteria, not both", w.id)
	}
	seen := map[string]bool{}
	for _, c := range w.criteria {
		if seen[c.Queue] {
			return fmt.Errorf("worker %s: queue %q has more than one criteria", w.id, c.Queue)
		}
		seen[c.Queue] = true
	}
	return nil
}

// executeItem runs a forEach body step: the handler gets (base, item) and its return replaces the
// item's value. A step bound with plain Handle cannot serve an item (the context is the raw item
// value, possibly a scalar) -- fail fast with a pointer to HandleItem.
func (w *Worker) executeItem(ctx context.Context, task *Task) {
	fn, ok := w.itemHandlers[task.Activity]
	if !ok {
		hint := "no handler registered for activity %q"
		if _, plain := w.handlers[task.Activity]; plain {
			hint = "activity %q is a forEach item step; bind it with HandleItem(base, item)"
		}
		_ = w.client.Fail(ctx, task.TaskID, task.LeaseOwner, fmt.Sprintf(hint, task.Activity), false)
		return
	}
	stop := w.startHeartbeat(task)
	defer close(stop)
	started := time.Now()
	result, err := fn(task.BaseContext, task.RawContext)
	if err != nil {
		w.fail(ctx, task, err)
		return
	}
	w.report(ctx, task, StepReport{NodeID: task.NodeID, Merge: result, StartedAt: started.UnixMilli(),
		FinishedAt: started.UnixMilli() + time.Since(started).Milliseconds()})
}

// report settles a finished step. A refusal means the lease was lost; the task re-runs elsewhere.
func (w *Worker) report(ctx context.Context, task *Task, step StepReport) {
	if _, err := w.client.ReportSteps(ctx, task.TaskID, task.LeaseOwner, []StepReport{step}, true); err != nil {
		log.Printf("wiggle: report of step %s of %s failed: %v", task.StepName, task.InstanceID, err)
	}
}

func (w *Worker) fail(ctx context.Context, task *Task, err error) {
	var perm *PermanentError
	if errors.As(err, &perm) {
		_ = w.client.Fail(ctx, task.TaskID, task.LeaseOwner, perm.Error(), false)
	} else {
		_ = w.client.Fail(ctx, task.TaskID, task.LeaseOwner, err.Error(), true)
	}
}

// startHeartbeat extends the lease periodically while a slow step runs; close the channel to stop.
func (w *Worker) startHeartbeat(task *Task) chan struct{} {
	stop := make(chan struct{})
	interval := time.Duration(w.leaseMillis/2) * time.Millisecond
	if interval < time.Second {
		interval = time.Second
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				if _, err := w.client.Heartbeat(context.Background(), task.TaskID, task.LeaseOwner, w.leaseMillis); err != nil {
					return
				}
			}
		}
	}()
	return stop
}

func (w *Worker) sleep(d time.Duration) {
	select {
	case <-w.ctx.Done():
	case <-time.After(d):
	}
}
