package wiggle

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// activityHandler is the internal, pre-wrapped step handler: it returns the value to report
// (a shallow-diff map for a task, a bool for a predicate, nil for an effect).
type activityHandler func(Context) (any, error)

type claim struct{ workflow, step, kind string }

// WorkerOption tunes a Worker.
type WorkerOption func(*Worker)

// Concurrency sets the maximum steps in flight (default: NumCPU).
func Concurrency(n int) WorkerOption { return func(w *Worker) { w.concurrency = n } }

// Lease sets how long a claimed step may run before the server reclaims it (default 30s).
func Lease(d time.Duration) WorkerOption { return func(w *Worker) { w.leaseMillis = d.Milliseconds() } }

// Queues restricts the worker to the given queues (else it serves every queue it learns about).
func Queues(names ...string) WorkerOption {
	return func(w *Worker) {
		w.explicitQueues = map[string]bool{}
		for _, n := range names {
			w.explicitQueues[n] = true
		}
	}
}

// AwaitRegistration makes Start wait up to d for a Handle-bound workflow's graph to appear before
// failing (rides out a registration race). Zero fails fast.
func AwaitRegistration(d time.Duration) WorkerOption {
	return func(w *Worker) { w.awaitRegistration = d }
}

// Worker pulls tasks it has capacity for, runs the matching handler, and reports the result. It holds
// no durable state: a crash loses at most the in-flight step, which the server re-leases. A worker
// implements steps by name (Handle); topology is registered separately (Client.Register of a compiled
// Graph, or the wiggle CLI).
type Worker struct {
	client *Client
	id     string

	handlers    map[string]activityHandler
	queues      map[string]bool
	claims      []claim
	handlerSets []handlerSet

	concurrency       int
	leaseMillis       int64
	waitMillis        int64
	idleBackoff       time.Duration
	errorBackoff      time.Duration
	awaitRegistration time.Duration
	explicitQueues    map[string]bool

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

// HandleGate binds a predicate step (gate / choose guard / do-while condition) by name.
func (w *Worker) HandleGate(workflow, step string, test Predicate) *Worker {
	return w.bind(workflow, step, "PREDICATE", func(ctx Context) (any, error) {
		return test(ctx)
	})
}

// HandleEffect binds a side-effect step by name; the context is left unchanged.
func (w *Worker) HandleEffect(workflow, step string, fn SideEffect) *Worker {
	return w.bind(workflow, step, "TASK", func(ctx Context) (any, error) {
		return nil, fn(ctx)
	})
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
		for _, c := range claims {
			activity := c.workflow + "#" + c.step
			node, ok := nodes[activity]
			if !ok {
				return fmt.Errorf("no step %q in registered workflow %q (available: %v)",
					c.step, wf, availableSteps(nodes))
			}
			if k, _ := node["kind"].(string); k != c.kind {
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
			return g, nil
		}
		if time.Now().Before(deadline) {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		return nil, fmt.Errorf("workflow %q is not registered; register its graph before starting a "+
			"worker that binds handlers to it (or set AwaitRegistration): %w", workflow, err)
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
		res, err := w.client.Poll(w.ctx, w.id, w.servedQueues(), free, w.leaseMillis, w.waitMillis)
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
	handler, ok := w.handlers[task.Activity]
	if !ok {
		_ = w.client.Fail(ctx, task.TaskID, task.LeaseOwner,
			fmt.Sprintf("no handler registered for activity %q", task.Activity), false)
		return
	}
	stop := w.startHeartbeat(task)
	defer close(stop)

	result, err := handler(task.Context)
	if err != nil {
		var perm *PermanentError
		if errors.As(err, &perm) {
			_ = w.client.Fail(ctx, task.TaskID, task.LeaseOwner, perm.Error(), false)
		} else {
			_ = w.client.Fail(ctx, task.TaskID, task.LeaseOwner, err.Error(), true)
		}
		return
	}
	if task.Kind == "PREDICATE" {
		b, _ := result.(bool)
		_ = w.client.Complete(ctx, task.TaskID, task.LeaseOwner, Context{"value": b})
		return
	}
	_ = w.client.Complete(ctx, task.TaskID, task.LeaseOwner, result)
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
