package wiggle

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// Blueprint is a compiled workflow: the graph sent to the server, plus the worker-side handlers.
type Blueprint struct {
	Name       string
	Version    int
	Definition map[string]any
	Queues     []string
	handlers   map[string]activityHandler
}

// Retry is a per-step retry policy.
type Retry struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	Multiplier     float64
	MaxBackoff     time.Duration
	Jitter         float64
}

// RetryExponential retries up to max times with exponential backoff and jitter.
func RetryExponential(max int, initial time.Duration) Retry {
	return Retry{MaxAttempts: max, InitialBackoff: initial, Multiplier: 2, MaxBackoff: 5 * time.Minute, Jitter: 0.2}
}

// RetryFixed retries up to max times with a constant backoff.
func RetryFixed(max int, backoff time.Duration) Retry {
	return Retry{MaxAttempts: max, InitialBackoff: backoff, Multiplier: 1, MaxBackoff: backoff}
}

// RetryNone disables retries (a single attempt).
func RetryNone() Retry { return Retry{MaxAttempts: 1, Multiplier: 1} }

// RetryForever retries indefinitely.
func RetryForever() Retry {
	return Retry{MaxAttempts: 1<<31 - 1, InitialBackoff: time.Second, Multiplier: 1, MaxBackoff: time.Minute}
}

func (r Retry) toJSON() map[string]any {
	mult := r.Multiplier
	if mult < 1 {
		mult = 1
	}
	return map[string]any{
		"maxAttempts":          r.MaxAttempts,
		"initialBackoffMillis": r.InitialBackoff.Milliseconds(),
		"multiplier":           mult,
		"maxBackoffMillis":     r.MaxBackoff.Milliseconds(),
		"jitter":               r.Jitter,
	}
}

// Branch is one parallel arm of a Fork.
type Branch struct {
	Name string
	Body func(*Workflow)
}

// BranchOf builds a named fork branch.
func BranchOf(name string, body func(*Workflow)) Branch { return Branch{name, body} }

// Case is one arm of a Choose; a nil Guard marks the default (Otherwise) arm.
type Case struct {
	Name  string
	Guard Predicate
	Body  func(*Workflow)
}

// When builds a guarded choose arm.
func When(name string, guard Predicate, body func(*Workflow)) Case {
	return Case{name, guard, body}
}

// Otherwise builds the default choose arm; it must be the last case.
func Otherwise(name string, body func(*Workflow)) Case { return Case{name, nil, body} }

// ---- graph store ----

type graph struct {
	name         string
	defaultQueue string
	nodes        map[string]map[string]any
	handlers     map[string]activityHandler
	queues       map[string]bool
	reserved     map[string]bool
	startNode    string
	counter      int
}

func (g *graph) nid(prefix string) string {
	g.counter++
	return fmt.Sprintf("%s%d", prefix, g.counter)
}

func (g *graph) reserve(name string) {
	if g.reserved[name] {
		panic(fmt.Sprintf("duplicate step name %q", name))
	}
	g.reserved[name] = true
}

func (g *graph) addWorker(kind, name string, h activityHandler, queue string, retry *Retry) string {
	g.reserve(name)
	id := g.nid("n")
	q := queue
	if q == "" {
		q = g.defaultQueue
	}
	g.queues[q] = true
	activity := g.name + "#" + name
	g.handlers[activity] = h
	node := map[string]any{"id": id, "kind": kind, "name": name, "activity": activity, "queue": q}
	r := RetryForever()
	if retry != nil {
		r = *retry
	}
	node["retry"] = r.toJSON()
	g.nodes[id] = node
	return id
}

func (g *graph) addTimer(kind, name string, millis int64, reserve bool) string {
	if reserve {
		g.reserve(name)
	}
	id := g.nid("n")
	node := map[string]any{"id": id, "kind": kind, "name": name}
	if millis > 0 {
		node["sleepMillis"] = millis
	}
	g.nodes[id] = node
	return id
}

func (g *graph) addFork() string {
	id := g.nid("fork")
	g.nodes[id] = map[string]any{"id": id, "kind": "FORK", "name": id}
	return id
}

func (g *graph) addSubWorkflow(name, child string) string {
	g.reserve(name)
	id := g.nid("sub")
	g.nodes[id] = map[string]any{"id": id, "kind": "SUB_WORKFLOW", "name": name, "activity": child}
	return id
}

func (g *graph) addDynFork(name, itemsKey, itemKey string) string {
	g.reserve(name)
	id := g.nid("dynfork")
	g.nodes[id] = map[string]any{"id": id, "kind": "DYN_FORK", "name": name, "itemsKey": itemsKey, "itemKey": itemKey}
	return id
}

func (g *graph) addJoin(expected int) string {
	id := g.nid("join")
	node := map[string]any{"id": id, "kind": "JOIN", "name": id}
	if expected > 0 {
		node["expected"] = expected
	}
	g.nodes[id] = node
	return id
}

func (g *graph) addEnd(reason string) string {
	id := g.nid("end")
	node := map[string]any{"id": id, "kind": "END", "success": true}
	if reason != "" {
		node["reason"] = reason
	}
	g.nodes[id] = node
	return id
}

func (g *graph) wire(nodeID, edge, target string) {
	key := "next"
	if edge != "next" {
		key = "altNext"
	}
	g.nodes[nodeID][key] = target
}

func (g *graph) setBranches(forkID string, starts []string) {
	g.nodes[forkID]["branches"] = starts
}

// ---- fluent builder ----

type openEnd struct{ nodeID, edge string }

// Workflow is the fluent builder. Every operator returns the builder so calls chain.
type Workflow struct {
	name          string
	version       int
	g             *graph
	enclosingJoin string
	open          []openEnd
	start         string
	isRoot        bool
}

// Define starts a workflow; the default queue is the workflow name.
func Define(name string) *Workflow {
	if name == "" {
		panic("workflow name is required")
	}
	g := &graph{
		name: name, defaultQueue: name,
		nodes: map[string]map[string]any{}, handlers: map[string]activityHandler{},
		queues: map[string]bool{}, reserved: map[string]bool{},
	}
	return &Workflow{name: name, g: g, isRoot: true}
}

// Version pins an explicit version instead of the content hash.
func (w *Workflow) Version(v int) *Workflow { w.version = v; return w }

// DefaultQueue sets the queue for steps that don't specify their own.
func (w *Workflow) DefaultQueue(q string) *Workflow { w.g.defaultQueue = q; return w }

func (w *Workflow) sub(enclosingJoin string) *Workflow {
	return &Workflow{name: w.name, g: w.g, enclosingJoin: enclosingJoin}
}

func (w *Workflow) attach(nodeID string) {
	if len(w.open) > 0 {
		for _, e := range w.open {
			w.g.wire(e.nodeID, e.edge, nodeID)
		}
		w.open = nil
	} else if w.start == "" {
		w.start = nodeID
		if w.isRoot {
			w.g.startNode = nodeID
		}
	}
}

func (w *Workflow) chain(nodeID string) *Workflow {
	w.attach(nodeID)
	w.open = []openEnd{{nodeID, "next"}}
	return w
}

func (w *Workflow) wireOpenTo(target string) {
	for _, e := range w.open {
		w.g.wire(e.nodeID, e.edge, target)
	}
	w.open = nil
}

func retryPtr(r []Retry) *Retry {
	if len(r) > 0 {
		return &r[0]
	}
	return nil
}

// Step runs fn on a worker; its returned context is diffed and merged back.
func (w *Workflow) Step(name string, fn Activity, opts ...StepOpt) *Workflow {
	o := stepOpts(opts)
	return w.chain(w.g.addWorker("TASK", name, taskHandler(fn), o.queue, o.retry))
}

// Then is an alias for Step that reads well when sequencing.
func (w *Workflow) Then(name string, fn Activity, opts ...StepOpt) *Workflow {
	return w.Step(name, fn, opts...)
}

// Effect runs fn for its side effect only; the context is unchanged.
func (w *Workflow) Effect(name string, fn SideEffect, opts ...StepOpt) *Workflow {
	o := stepOpts(opts)
	h := func(ctx Context) (any, error) { return nil, fn(ctx) }
	return w.chain(w.g.addWorker("TASK", name, h, o.queue, o.retry))
}

// Gate continues only while test holds; a false result ends the instance as gated:<name> (inside a
// branch it short-circuits to the enclosing join).
func (w *Workflow) Gate(name string, test Predicate, opts ...StepOpt) *Workflow {
	o := stepOpts(opts)
	h := func(ctx Context) (any, error) { return test(ctx) }
	id := w.g.addWorker("PREDICATE", name, h, o.queue, o.retry)
	w.attach(id)
	target := w.enclosingJoin
	if target == "" {
		target = w.g.addEnd("gated:" + name)
	}
	w.g.wire(id, "alt", target)
	w.open = []openEnd{{id, "next"}}
	return w
}

// Sleep waits on a server-side timer; no worker is held.
func (w *Workflow) Sleep(name string, d time.Duration) *Workflow {
	return w.chain(w.g.addTimer("SLEEP", name, d.Milliseconds(), false))
}

// AwaitSignal waits for a named external signal (delivered via Client.Signal). With escalation set,
// a timeout runs the escalation branch and rejoins instead of failing.
func (w *Workflow) AwaitSignal(name string, timeout time.Duration, escalation func(*Workflow)) *Workflow {
	id := w.g.addTimer("SIGNAL", name, timeout.Milliseconds(), true)
	w.attach(id)
	if escalation == nil {
		w.open = []openEnd{{id, "next"}}
		return w
	}
	if timeout <= 0 {
		panic("AwaitSignal escalation needs a positive timeout")
	}
	esc := w.sub(w.enclosingJoin)
	escalation(esc)
	if esc.start == "" {
		panic(fmt.Sprintf("escalation branch of %q defines no steps", name))
	}
	w.g.wire(id, "alt", esc.start)
	w.open = append([]openEnd{{id, "next"}}, esc.open...)
	return w
}

// SubWorkflow runs another registered workflow as a child; its result merges back.
func (w *Workflow) SubWorkflow(name, child string) *Workflow {
	return w.chain(w.g.addSubWorkflow(name, child))
}

// Fork fans out into parallel branches and waits for all of them (needs >= 2).
func (w *Workflow) Fork(branches ...Branch) *Workflow {
	if len(branches) < 2 {
		panic("fork needs at least two branches")
	}
	forkID := w.g.addFork()
	w.attach(forkID)
	joinID := w.g.addJoin(len(branches))
	starts := make([]string, 0, len(branches))
	for _, b := range branches {
		starts = append(starts, w.buildBranch(b, joinID))
	}
	w.g.setBranches(forkID, starts)
	w.open = []openEnd{{joinID, "next"}}
	return w
}

// ForkEach fans out one branch per element of the list at itemsKey, injecting each element under
// itemKey (and its index under itemKey+"Index").
func (w *Workflow) ForkEach(name, itemsKey, itemKey string, body func(*Workflow)) *Workflow {
	forkID := w.g.addDynFork(name, itemsKey, itemKey)
	w.attach(forkID)
	joinID := w.g.addJoin(0)
	template := w.buildBranch(Branch{name, body}, joinID)
	w.g.setBranches(forkID, []string{template})
	w.g.wire(forkID, "next", joinID) // empty-list skip
	w.open = []openEnd{{joinID, "next"}}
	return w
}

// DoWhile runs body once, then repeats while cond holds (body runs at least once).
func (w *Workflow) DoWhile(condName string, cond Predicate, body func(*Workflow)) *Workflow {
	sub := w.sub(w.enclosingJoin)
	body(sub)
	if sub.start == "" {
		panic("doWhile body defines no steps")
	}
	h := func(ctx Context) (any, error) { return cond(ctx) }
	condID := w.g.addWorker("PREDICATE", condName, h, "", nil)
	w.attach(sub.start)
	sub.wireOpenTo(condID)
	w.g.wire(condID, "next", sub.start) // true: loop back
	w.open = []openEnd{{condID, "alt"}} // false: continue
	return w
}

// Choose runs the first matching guard's branch; the rest are skipped. An Otherwise arm (last)
// handles no match.
func (w *Workflow) Choose(cases ...Case) *Workflow {
	if len(cases) == 0 {
		panic("choose needs at least one case")
	}
	hasDefault := cases[len(cases)-1].Guard == nil
	for _, c := range cases[:len(cases)-1] {
		if c.Guard == nil {
			panic("otherwise() must be the last case")
		}
	}
	if hasDefault && len(cases) == 1 {
		panic("choose needs at least one guarded case")
	}
	nGuards := len(cases)
	if hasDefault {
		nGuards--
	}
	guardIDs := make([]string, nGuards)
	for i := 0; i < nGuards; i++ {
		c := cases[i]
		h := func(ctx Context) (any, error) { return c.Guard(ctx) }
		guardIDs[i] = w.g.addWorker("PREDICATE", c.Name, h, "", nil)
	}
	w.attach(guardIDs[0])
	w.open = nil
	for i := 0; i < nGuards-1; i++ {
		w.g.wire(guardIDs[i], "alt", guardIDs[i+1])
	}
	for i := 0; i < nGuards; i++ {
		w.collectCase(cases[i], guardIDs[i], "next")
	}
	last := guardIDs[nGuards-1]
	if hasDefault {
		w.collectCase(cases[len(cases)-1], last, "alt")
	} else {
		w.open = append(w.open, openEnd{last, "alt"})
	}
	return w
}

func (w *Workflow) buildBranch(b Branch, joinID string) string {
	sub := w.sub(joinID)
	b.Body(sub)
	if sub.start == "" {
		panic(fmt.Sprintf("branch %q defines no steps", b.Name))
	}
	sub.wireOpenTo(joinID)
	return sub.start
}

func (w *Workflow) collectCase(c Case, guardID, edge string) {
	sub := w.sub(w.enclosingJoin)
	c.Body(sub)
	if sub.start == "" {
		panic(fmt.Sprintf("case %q defines no steps", c.Name))
	}
	w.g.wire(guardID, edge, sub.start)
	w.open = append(w.open, sub.open...)
}

// Build compiles the workflow into a Blueprint (topology + handlers).
func (w *Workflow) Build() *Blueprint {
	endID := w.g.addEnd("")
	w.wireOpenTo(endID)
	if w.g.startNode == "" {
		w.g.startNode = endID
	}
	queues := make([]string, 0, len(w.g.queues))
	for q := range w.g.queues {
		queues = append(queues, q)
	}
	sort.Strings(queues)

	nodes := make([]any, 0, len(w.g.nodes))
	ids := make([]string, 0, len(w.g.nodes))
	for id := range w.g.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		nodes = append(nodes, w.g.nodes[id])
	}

	def := map[string]any{
		"name":          w.name,
		"startNode":     w.g.startNode,
		"nodes":         nodes,
		"queues":        toAnySlice(queues),
		"executionMode": "SERVER",
	}
	version := w.version
	if version == 0 {
		version = contentVersion(def)
	}
	def["version"] = version

	return &Blueprint{
		Name: w.name, Version: version, Definition: def,
		Queues: queues, handlers: w.g.handlers,
	}
}

// contentVersion is a deterministic positive 31-bit hash of the graph (name, nodes, edges, mode),
// stable across re-registration. It is the Go client's own hash and need not equal the Java/Python
// version -- cross-language interop is by activity name.
func contentVersion(def map[string]any) int {
	material := map[string]any{
		"name":          def["name"],
		"startNode":     def["startNode"],
		"nodes":         def["nodes"],
		"executionMode": def["executionMode"],
	}
	b, _ := json.Marshal(material) // encoding/json sorts map keys -> deterministic
	sum := sha256.Sum256(b)
	v := (int(sum[0]&0x7f) << 24) | (int(sum[1]) << 16) | (int(sum[2]) << 8) | int(sum[3])
	if v == 0 {
		return 1
	}
	return v
}

func toAnySlice(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

// ---- per-step options ----

// StepOpt customizes a step (queue / retry).
type StepOpt func(*stepOptions)

type stepOptions struct {
	queue string
	retry *Retry
}

// Queue routes a step to a named queue.
func WithQueue(q string) StepOpt { return func(o *stepOptions) { o.queue = q } }

// WithRetry sets a step's retry policy.
func WithRetry(r Retry) StepOpt { return func(o *stepOptions) { o.retry = &r } }

func stepOpts(opts []StepOpt) stepOptions {
	var o stepOptions
	for _, f := range opts {
		f(&o)
	}
	return o
}
