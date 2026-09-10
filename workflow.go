package wiggle

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

// Blueprint is a compiled workflow topology: the graph sent to the server. Step handlers are NOT
// part of it -- they are bound by name on a worker (see Worker.Handle).
type Blueprint struct {
	Name       string
	Version    int
	Definition map[string]any
	Queues     []string
}

// Retry is a per-step retry policy. The zero value means "use the default".
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

// ---- declarative topology ----
//
// A workflow is described as data, not a fluent chain: a Graph with an ordered list of Nodes. Each
// Node is one of the concrete types below (Task, Gate, Fork, ...). Compile() turns it into a
// Blueprint. Handlers are implemented separately and bound by name (Worker.Handle).

// Graph is the declarative description of a workflow's topology.
type Graph struct {
	Name         string
	Version      int    // 0 = a content-hash version
	DefaultQueue string // "" = the workflow name
	Steps        []Node
}

// Node is one step in a workflow. The interface is sealed: only the types in this package
// (Task, Effect, Gate, Sleep, AwaitSignal, SubWorkflow, Fork, ForkEach, Choose, DoWhile) are Nodes.
type Node interface{ isNode() }

// Step is a unit of work run on a worker; its handler returns the new context.
//
// Compensate declares the step compensable: if the instance later fails, the engine runs the
// step's compensator (bound with Worker.HandleCompensation or a Compensate<Step> method) in the
// reverse pass, handing it the input/result context snapshots captured at this step's completion.
type Step struct {
	Name       string
	Queue      string // "" = the default queue
	Retry      Retry  // zero = default
	Compensate bool   // declare an undo; a compensator MUST be bound for this step
}

// Effect is a step run for its side effect only; the context is unchanged. (Topologically identical
// to a Step -- the difference is only in the bound handler.) Compensate declares an undo exactly
// as on a Step (for an effect the two snapshots are the same context).
type Effect struct {
	Name       string
	Queue      string
	Retry      Retry
	Compensate bool
}

// Gate continues only while its predicate holds; false ends the instance as gated:<name> (or, inside
// a branch, short-circuits to that fork's join).
type Gate struct {
	Name  string
	Queue string
	Retry Retry
}

// Sleep waits on a server-side timer; no worker is held. Name is optional.
type Sleep struct {
	Name string
	For  time.Duration
}

// AwaitSignal waits for a named external signal (delivered via Client.Signal). With Escalation set, a
// timeout runs the escalation branch and rejoins instead of failing.
type AwaitSignal struct {
	Name       string
	Timeout    time.Duration
	Escalation []Node
}

// SubWorkflow runs another registered workflow as a child; its result merges back. Name is the node
// name (defaults to Workflow); Workflow is the child's name.
type SubWorkflow struct {
	Name     string
	Workflow string
}

// Branch is one arm of a Fork.
type Branch struct {
	Name  string
	Steps []Node
}

// Fork fans out into parallel branches and waits for all of them (needs >= 2). Combine names the
// MANDATORY merge step that runs after the join: its handler receives the context with each
// branch's result staged under the branch's name, and must return the COMPLETE post-join context
// (the engine replaces the context with it -- there is no implicit fold of the arms, and keys the
// handler omits do not survive the join). Bind it with Worker.HandleCombine or a RegisterHandlers
// method matched by name.
type Fork struct {
	Branches []Branch
	Combine  string
}

// ForEach fans out one ISOLATED branch per element of the collection at Over (a list or a map).
// The element IS each item's context: body steps are bound with HandleItem (or an
// ItemActivity-shaped method) — they receive the item's current value and their return replaces
// it; the frozen base and the element's index/source key ride on the activation. Combine names the
// MANDATORY merge step: its handler receives the context with every item's FINAL VALUE collected
// under the forEach's name — a list ordered by item index for a list input, a map keyed like the
// input for a map input — and must return the COMPLETE post-join context. An empty collection
// skips the body and the combine.
type ForEach struct {
	Name    string // label; the collected results are staged under this key for the combine
	Over    string // itemsKey
	Body    []Node
	Combine string
}

// Case is one arm of a Choose. When == "" marks the default (otherwise) arm, which must be last.
type Case struct {
	When string // predicate name; "" = otherwise
	Then []Node
}

// Choose runs the first matching guard's branch; the rest are skipped.
type Choose struct {
	Cases []Case
}

// DoWhile runs Body once, then repeats while the While predicate holds (Body runs at least once).
//
// Every loop is budgeted: the While guard may evaluate true at most MaxIterations times, after
// which the instance FAILS with a clear error — an unbounded loop with a buggy condition would
// hot-spin workers and the database. Zero means the engine default (WIGGLE_LOOP_MAX_ITERATIONS,
// 10,000); set it explicitly when a loop legitimately needs more.
type DoWhile struct {
	While         string
	MaxIterations int
	Body          []Node
}

func (Step) isNode()        {}
func (Effect) isNode()      {}
func (Gate) isNode()        {}
func (Sleep) isNode()       {}
func (AwaitSignal) isNode() {}
func (SubWorkflow) isNode() {}
func (Fork) isNode()        {}
func (ForEach) isNode()     {}
func (Choose) isNode()      {}
func (DoWhile) isNode()     {}

// ---- graph store ----

type graph struct {
	name         string
	defaultQueue string
	nodes        map[string]map[string]any
	queues       map[string]bool
	reserved     map[string]bool
	startNode    string
	counter      int
}

func newGraph(name, defaultQueue string) *graph {
	return &graph{
		name: name, defaultQueue: defaultQueue,
		nodes: map[string]map[string]any{}, queues: map[string]bool{}, reserved: map[string]bool{},
	}
}

func (g *graph) nid(prefix string) string {
	g.counter++
	return fmt.Sprintf("%s%d", prefix, g.counter)
}

func (g *graph) reserve(name string) {
	if name == "" {
		fail("a step is missing a name")
	}
	if g.reserved[name] {
		fail("duplicate step name %q", name)
	}
	g.reserved[name] = true
}

func (g *graph) addWorker(kind, name, queue string, retry Retry) string {
	g.reserve(name)
	id := g.nid("n")
	q := queue
	if q == "" {
		q = g.defaultQueue
	}
	g.queues[q] = true
	r := RetryForever()
	if retry.MaxAttempts > 0 {
		r = retry
	}
	g.nodes[id] = map[string]any{
		"id": id, "kind": kind, "name": name, "activity": g.name + "#" + name, "queue": q,
		"retry": r.toJSON(),
	}
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

// addCombine emits the mandatory merge node after a fork's join: a TASK bound by name like any
// step, carrying the fork's arm names (a JSON array) on its itemsKey so the engine can stage each
// isolated branch's result under its name for the handler, and strip those keys afterward.
func (g *graph) addCombine(name string, arms []string) string {
	id := g.addWorker("TASK", name, "", Retry{})
	names, _ := json.Marshal(arms)
	g.nodes[id]["itemsKey"] = string(names)
	return id
}

// addForEachCombine emits the mandatory merge node after a forEach's join: a TASK bound by name,
// whose itemsKey is a JSON STRING (the scratch key the engine stages the collected item results
// under) — versus a fork combine's arm-name array.
func (g *graph) addForEachCombine(name, scratchKey string) string {
	id := g.addWorker("TASK", name, "", Retry{})
	key, _ := json.Marshal(scratchKey)
	g.nodes[id]["itemsKey"] = string(key)
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

// ---- compiler (open-ends wiring over the declarative structs) ----

type openEnd struct{ nodeID, edge string }

type builder struct {
	g             *graph
	enclosingJoin string
	open          []openEnd
	start         string
	isRoot        bool
}

func (b *builder) sub(enclosingJoin string) *builder {
	return &builder{g: b.g, enclosingJoin: enclosingJoin}
}

func (b *builder) attach(nodeID string) {
	if len(b.open) > 0 {
		for _, e := range b.open {
			b.g.wire(e.nodeID, e.edge, nodeID)
		}
		b.open = nil
	} else if b.start == "" {
		b.start = nodeID
		if b.isRoot {
			b.g.startNode = nodeID
		}
	}
}

func (b *builder) chain(nodeID string) {
	b.attach(nodeID)
	b.open = []openEnd{{nodeID, "next"}}
}

func (b *builder) wireOpenTo(target string) {
	for _, e := range b.open {
		b.g.wire(e.nodeID, e.edge, target)
	}
	b.open = nil
}

func (b *builder) appendNodes(nodes []Node) {
	for _, n := range nodes {
		b.appendNode(n)
	}
}

func (b *builder) appendNode(n Node) {
	switch node := n.(type) {
	case Step:
		id := b.g.addWorker("TASK", node.Name, node.Queue, node.Retry)
		if node.Compensate {
			b.g.nodes[id]["compensable"] = true
		}
		b.chain(id)
	case Effect:
		id := b.g.addWorker("TASK", node.Name, node.Queue, node.Retry)
		if node.Compensate {
			b.g.nodes[id]["compensable"] = true
		}
		b.chain(id)
	case Gate:
		id := b.g.addWorker("PREDICATE", node.Name, node.Queue, node.Retry)
		b.attach(id)
		target := b.enclosingJoin
		if target == "" {
			target = b.g.addEnd("gated:" + node.Name)
		}
		b.g.wire(id, "alt", target)
		b.open = []openEnd{{id, "next"}}
	case Sleep:
		name := node.Name
		if name == "" {
			name = fmt.Sprintf("sleep-%dms", node.For.Milliseconds())
		}
		b.chain(b.g.addTimer("SLEEP", name, node.For.Milliseconds(), false))
	case AwaitSignal:
		id := b.g.addTimer("SIGNAL", node.Name, node.Timeout.Milliseconds(), true)
		b.attach(id)
		if len(node.Escalation) == 0 {
			b.open = []openEnd{{id, "next"}}
			return
		}
		if node.Timeout <= 0 {
			fail("await_signal %q escalation needs a positive timeout", node.Name)
		}
		esc := b.sub(b.enclosingJoin)
		esc.appendNodes(node.Escalation)
		if esc.start == "" {
			fail("escalation branch of %q defines no steps", node.Name)
		}
		b.g.wire(id, "alt", esc.start)
		b.open = append([]openEnd{{id, "next"}}, esc.open...)
	case SubWorkflow:
		child := node.Workflow
		if child == "" {
			fail("sub_workflow is missing its child workflow name")
		}
		name := node.Name
		if name == "" {
			name = child
		}
		b.chain(b.g.addSubWorkflow(name, child))
	case Fork:
		if len(node.Branches) < 2 {
			fail("fork needs at least two branches")
		}
		if node.Combine == "" {
			fail("fork needs a combine step name (Fork.Combine): branches rejoin at an explicit merge handler")
		}
		forkID := b.g.addFork()
		b.attach(forkID)
		joinID := b.g.addJoin(len(node.Branches))
		starts := make([]string, 0, len(node.Branches))
		arms := make([]string, 0, len(node.Branches))
		for _, br := range node.Branches {
			starts = append(starts, b.buildBranch(br, joinID))
			arms = append(arms, br.Name)
		}
		b.g.setBranches(forkID, starts)
		combineID := b.g.addCombine(node.Combine, arms)
		b.g.wire(joinID, "next", combineID)
		b.open = []openEnd{{combineID, "next"}}
	case ForEach:
		if len(node.Body) == 0 {
			fail("for_each %q body defines no steps", node.Name)
		}
		if node.Combine == "" {
			fail("for_each needs a combine step name (ForEach.Combine): item results rejoin at an explicit merge handler")
		}
		name := node.Name
		if name == "" {
			name = node.Over
		}
		forkID := b.g.addDynFork(name, node.Over, node.Over)
		b.attach(forkID)
		joinID := b.g.addJoin(0)
		template := b.buildBranch(Branch{Name: name, Steps: node.Body}, joinID)
		b.g.setBranches(forkID, []string{template})
		b.g.wire(forkID, "next", joinID) // empty-collection skip (past the combine too, engine-side)
		combineID := b.g.addForEachCombine(node.Combine, name)
		b.g.wire(joinID, "next", combineID)
		b.open = []openEnd{{combineID, "next"}}
	case Choose:
		b.appendChoose(node)
	case DoWhile:
		sub := b.sub(b.enclosingJoin)
		sub.appendNodes(node.Body)
		if sub.start == "" {
			fail("do_while body defines no steps")
		}
		if node.MaxIterations < 0 {
			fail("do_while %q: MaxIterations must be positive (0 = engine default)", node.While)
		}
		condID := b.g.addWorker("PREDICATE", node.While, "", Retry{})
		budget := node.MaxIterations
		if budget == 0 {
			budget = -1 // engine default — mirrors the Java client's two-arg doWhile
		}
		b.g.nodes[condID]["loopBudget"] = budget
		b.attach(sub.start)
		sub.wireOpenTo(condID)
		b.g.wire(condID, "next", sub.start) // true: loop back
		b.open = []openEnd{{condID, "alt"}} // false: continue
	default:
		fail("unknown node type %T", n)
	}
}

func (b *builder) appendChoose(c Choose) {
	if len(c.Cases) == 0 {
		fail("choose needs at least one case")
	}
	hasDefault := c.Cases[len(c.Cases)-1].When == ""
	for _, cs := range c.Cases[:len(c.Cases)-1] {
		if cs.When == "" {
			fail("the otherwise (default) case must be last")
		}
	}
	if hasDefault && len(c.Cases) == 1 {
		fail("choose needs at least one guarded case")
	}
	nGuards := len(c.Cases)
	if hasDefault {
		nGuards--
	}
	guardIDs := make([]string, nGuards)
	for i := 0; i < nGuards; i++ {
		guardIDs[i] = b.g.addWorker("PREDICATE", c.Cases[i].When, "", Retry{})
	}
	b.attach(guardIDs[0])
	b.open = nil
	for i := 0; i < nGuards-1; i++ {
		b.g.wire(guardIDs[i], "alt", guardIDs[i+1])
	}
	for i := 0; i < nGuards; i++ {
		b.collectCase(c.Cases[i].Then, guardIDs[i], "next")
	}
	last := guardIDs[nGuards-1]
	if hasDefault {
		b.collectCase(c.Cases[len(c.Cases)-1].Then, last, "alt")
	} else {
		b.open = append(b.open, openEnd{last, "alt"})
	}
}

func (b *builder) buildBranch(br Branch, joinID string) string {
	sub := b.sub(joinID)
	sub.appendNodes(br.Steps)
	if sub.start == "" {
		fail("branch %q defines no steps", br.Name)
	}
	sub.wireOpenTo(joinID)
	return sub.start
}

func (b *builder) collectCase(then []Node, guardID, edge string) {
	sub := b.sub(b.enclosingJoin)
	sub.appendNodes(then)
	if sub.start == "" {
		fail("a choose case defines no steps")
	}
	b.g.wire(guardID, edge, sub.start)
	b.open = append(b.open, sub.open...)
}

// ---- compile ----

type compileError string

func (e compileError) Error() string { return string(e) }

func fail(format string, args ...any) { panic(compileError(fmt.Sprintf(format, args...))) }

// Compile turns the declarative Graph into a Blueprint (topology only). It returns an error -- never
// panics to the caller -- for an invalid topology (duplicate name, fork with < 2 branches, an empty
// branch/body, a bad choose, escalation without a timeout).
func (g Graph) Compile() (bp *Blueprint, err error) {
	if g.Name == "" {
		return nil, errors.New("workflow name is required")
	}
	if len(g.Steps) == 0 {
		return nil, errors.New("workflow has no steps")
	}
	defer func() {
		if r := recover(); r != nil {
			if ce, ok := r.(compileError); ok {
				bp, err = nil, ce
				return
			}
			panic(r)
		}
	}()

	dq := g.DefaultQueue
	if dq == "" {
		dq = g.Name
	}
	gr := newGraph(g.Name, dq)
	root := &builder{g: gr, isRoot: true}
	root.appendNodes(g.Steps)
	endID := gr.addEnd("")
	root.wireOpenTo(endID)
	if gr.startNode == "" {
		gr.startNode = endID
	}

	queues := make([]string, 0, len(gr.queues))
	for q := range gr.queues {
		queues = append(queues, q)
	}
	sort.Strings(queues)

	ids := make([]string, 0, len(gr.nodes))
	for id := range gr.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	nodes := make([]any, 0, len(ids))
	for _, id := range ids {
		nodes = append(nodes, gr.nodes[id])
	}

	def := map[string]any{
		"name":          g.Name,
		"startNode":     gr.startNode,
		"nodes":         nodes,
		"queues":        toAnySlice(queues),
		"executionMode": "SERVER",
	}
	version := g.Version
	if version == 0 {
		version = contentVersion(def)
	}
	def["version"] = version

	return &Blueprint{Name: g.Name, Version: version, Definition: def, Queues: queues}, nil
}

// MustCompile is Compile that panics on error -- convenient for a topology defined as a literal that
// is known-good at build time.
func (g Graph) MustCompile() *Blueprint {
	bp, err := g.Compile()
	if err != nil {
		panic(err)
	}
	return bp
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
