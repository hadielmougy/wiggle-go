package wiggle

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// FlowSpec is a compiled workflow topology: the graph sent to the server. Step handlers are NOT
// part of it -- they are bound by name on a worker (see Worker.Handle).
type FlowSpec struct {
	Name       string
	Version    int
	Definition map[string]any
	Queues     []string
}

// RetryPolicy is a per-step retry policy. The zero value means "use the default".
type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	Multiplier     float64
	MaxBackoff     time.Duration
	Jitter         float64
}

// RetryExponential retries up to max times with exponential backoff and jitter.
func RetryExponential(max int, initial time.Duration) RetryPolicy {
	return RetryPolicy{MaxAttempts: max, InitialBackoff: initial, Multiplier: 2, MaxBackoff: 5 * time.Minute, Jitter: 0.2}
}

// RetryFixed retries up to max times with a constant backoff.
func RetryFixed(max int, backoff time.Duration) RetryPolicy {
	return RetryPolicy{MaxAttempts: max, InitialBackoff: backoff, Multiplier: 1, MaxBackoff: backoff}
}

// RetryNone disables retries (a single attempt).
func RetryNone() RetryPolicy { return RetryPolicy{MaxAttempts: 1, Multiplier: 1} }

// RetryForever retries indefinitely.
func RetryForever() RetryPolicy {
	return RetryPolicy{MaxAttempts: 1<<31 - 1, InitialBackoff: time.Second, Multiplier: 1, MaxBackoff: time.Minute}
}

func (r RetryPolicy) toJSON() map[string]any {
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

// dispatchAttribute is a value the workflow declares (Flow.Attribute) for queue criteria to test:
// Path is a dotted path into the context a step receives, read as Type -- STRING, LONG, DECIMAL or
// BOOLEAN. The server extracts it each time a step becomes ready; a value that is missing or does
// not fit the type is absent.
type dispatchAttribute struct {
	Name string
	Type string
	Path string
}

// MaxAttributes is the most attributes one workflow declares.
const MaxAttributes = 16

var attributeName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

func (a dispatchAttribute) validate() error {
	if !attributeName.MatchString(a.Name) {
		return fmt.Errorf("attribute name %q must match %s", a.Name, attributeName)
	}
	switch a.Type {
	case "STRING", "LONG", "DECIMAL", "BOOLEAN":
	default:
		return fmt.Errorf("attribute %q type must be STRING, LONG, DECIMAL or BOOLEAN, got %q", a.Name, a.Type)
	}
	if a.Path == "" || strings.Contains("."+a.Path+".", "..") {
		return fmt.Errorf("attribute %q needs a dotted path, got %q", a.Name, a.Path)
	}
	return nil
}

// ---- recorded topology ----
//
// A Flow records the nodes below as its chain is built; compile wires them into a FlowSpec.

type node interface{ isNode() }

// taskNode is a task or a side effect (the two differ only in the bound handler). A combine makes it
// a spawning step; compensate declares its undo.
type taskNode struct {
	name       string
	queue      string
	retry      RetryPolicy
	compensate bool
	combine    string
}

type gateNode struct {
	name  string
	queue string
	retry RetryPolicy
}

type sleepNode struct {
	name string
	d    time.Duration
}

type signalNode struct {
	name       string
	timeout    time.Duration
	escalation []node
}

type subFlowNode struct {
	name     string
	workflow string
}

type armNode struct {
	name  string
	steps []node
}

type forkNode struct {
	arms    []armNode
	combine string
}

type forEachNode struct {
	name     string
	itemsKey string
	body     []node
	combine  string
}

type caseNode struct {
	when string // "" = otherwise
	then []node
}

type chooseNode struct{ cases []caseNode }

type loopNode struct {
	while         string
	maxIterations int
	body          []node
}

func (taskNode) isNode()    {}
func (gateNode) isNode()    {}
func (sleepNode) isNode()   {}
func (signalNode) isNode()  {}
func (subFlowNode) isNode() {}
func (forkNode) isNode()    {}
func (forEachNode) isNode() {}
func (chooseNode) isNode()  {}
func (loopNode) isNode()    {}

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

func (g *graph) addWorker(kind, name, queue string, retry RetryPolicy) string {
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
// step, carrying the fork's armNames so the engine can stage each isolated branch's result under
// its name for the handler, and strip those keys afterward.
func (g *graph) addCombine(name string, arms []string) string {
	id := g.addWorker("TASK", name, "", RetryPolicy{})
	g.nodes[id]["armNames"] = toAnySlice(arms)
	return id
}

// addForEachCombine emits the mandatory merge node after a forEach's join: a TASK bound by name,
// whose collectKey is the scratch key the engine stages the collected item results under.
func (g *graph) addForEachCombine(name, forEachName string) string {
	id := g.addWorker("TASK", name, "", RetryPolicy{})
	g.nodes[id]["collectKey"] = forEachScratchPrefix + forEachName
	return id
}

// addSpawnCombine emits the combine after a spawning step: a TASK bound by name whose collectKey
// stages the results of the branches stepName created.
func (g *graph) addSpawnCombine(name, stepName string) string {
	id := g.addWorker("TASK", name, "", RetryPolicy{})
	g.nodes[id]["collectKey"] = spawnScratchPrefix + stepName
	return id
}

// isCombine reports whether a registered graph node is a fork or forEach combine: a TASK with
// armNames or a collectKey, or -- in a graph written before those fields -- an itemsKey.
func isCombine(node map[string]any) bool {
	if k, _ := node["kind"].(string); k != "TASK" {
		return false
	}
	arms, _ := node["armNames"].([]any)
	collect, _ := node["collectKey"].(string)
	legacy, _ := node["itemsKey"].(string)
	return len(arms) > 0 || collect != "" || legacy != ""
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
	g.nodes[forkID]["branches"] = toAnySlice(starts)
}

// ---- compiler (open-ends wiring over the declarative structs) ----

type openEnd struct{ nodeID, edge string }

// appendTask chains a task; with a combine it is a spawning step, followed by a dynamic join and
// the combine collecting the branches it creates.
func (b *builder) appendTask(name, queue string, retry RetryPolicy, compensate bool, combine string) {
	id := b.g.addWorker("TASK", name, queue, retry)
	if compensate {
		b.g.nodes[id]["compensable"] = true
	}
	b.chain(id)
	if combine != "" {
		b.chain(b.g.addJoin(0))
		b.chain(b.g.addSpawnCombine(combine, name))
	}
}

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

func (b *builder) appendNodes(nodes []node) {
	for _, n := range nodes {
		b.appendNode(n)
	}
}

func (b *builder) appendNode(n node) {
	switch node := n.(type) {
	case taskNode:
		b.appendTask(node.name, node.queue, node.retry, node.compensate, node.combine)
	case gateNode:
		id := b.g.addWorker("PREDICATE", node.name, node.queue, node.retry)
		b.attach(id)
		target := b.enclosingJoin
		if target == "" {
			target = b.g.addEnd("gated:" + node.name)
		}
		b.g.wire(id, "alt", target)
		b.open = []openEnd{{id, "next"}}
	case sleepNode:
		name := node.name
		if name == "" {
			name = fmt.Sprintf("sleep-%dms", node.d.Milliseconds())
		}
		b.chain(b.g.addTimer("SLEEP", name, node.d.Milliseconds(), false))
	case signalNode:
		id := b.g.addTimer("SIGNAL", node.name, node.timeout.Milliseconds(), true)
		b.attach(id)
		if len(node.escalation) == 0 {
			b.open = []openEnd{{id, "next"}}
			return
		}
		if node.timeout <= 0 {
			fail("await_signal %q escalation needs a positive timeout", node.name)
		}
		esc := b.sub(b.enclosingJoin)
		esc.appendNodes(node.escalation)
		if esc.start == "" {
			fail("escalation branch of %q defines no steps", node.name)
		}
		b.g.wire(id, "alt", esc.start)
		b.open = append([]openEnd{{id, "next"}}, esc.open...)
	case subFlowNode:
		child := node.workflow
		if child == "" {
			fail("sub_workflow is missing its child workflow name")
		}
		name := node.name
		if name == "" {
			name = child
		}
		b.chain(b.g.addSubWorkflow(name, child))
	case forkNode:
		if len(node.arms) < 2 {
			fail("fork needs at least two branches")
		}
		if node.combine == "" {
			fail("fork needs a combine step name (Fork.Combine): branches rejoin at an explicit merge handler")
		}
		forkID := b.g.addFork()
		b.attach(forkID)
		joinID := b.g.addJoin(len(node.arms))
		starts := make([]string, 0, len(node.arms))
		arms := make([]string, 0, len(node.arms))
		for _, br := range node.arms {
			starts = append(starts, b.buildBranch(br, joinID))
			arms = append(arms, br.name)
		}
		b.g.setBranches(forkID, starts)
		combineID := b.g.addCombine(node.combine, arms)
		b.g.wire(joinID, "next", combineID)
		b.open = []openEnd{{combineID, "next"}}
	case forEachNode:
		if len(node.body) == 0 {
			fail("for_each %q body defines no steps", node.name)
		}
		if node.combine == "" {
			fail("for_each needs a combine step name (ForEach.Combine): item results rejoin at an explicit merge handler")
		}
		name := node.name
		if name == "" {
			name = node.itemsKey
		}
		forkID := b.g.addDynFork(name, node.itemsKey, node.itemsKey)
		b.attach(forkID)
		joinID := b.g.addJoin(0)
		template := b.buildBranch(armNode{name: name, steps: node.body}, joinID)
		b.g.setBranches(forkID, []string{template})
		b.g.wire(forkID, "next", joinID) // empty-collection skip (past the combine too, engine-side)
		combineID := b.g.addForEachCombine(node.combine, name)
		b.g.wire(joinID, "next", combineID)
		b.open = []openEnd{{combineID, "next"}}
	case chooseNode:
		b.appendChoose(node)
	case loopNode:
		sub := b.sub(b.enclosingJoin)
		sub.appendNodes(node.body)
		if sub.start == "" {
			fail("do_while body defines no steps")
		}
		if node.maxIterations < 0 {
			fail("do_while %q: MaxIterations must be positive (0 = engine default)", node.while)
		}
		condID := b.g.addWorker("PREDICATE", node.while, "", RetryPolicy{})
		budget := node.maxIterations
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

func (b *builder) appendChoose(c chooseNode) {
	if len(c.cases) == 0 {
		fail("choose needs at least one case")
	}
	hasDefault := c.cases[len(c.cases)-1].when == ""
	for _, cs := range c.cases[:len(c.cases)-1] {
		if cs.when == "" {
			fail("the otherwise (default) case must be last")
		}
	}
	if hasDefault && len(c.cases) == 1 {
		fail("choose needs at least one guarded case")
	}
	nGuards := len(c.cases)
	if hasDefault {
		nGuards--
	}
	guardIDs := make([]string, nGuards)
	for i := 0; i < nGuards; i++ {
		guardIDs[i] = b.g.addWorker("PREDICATE", c.cases[i].when, "", RetryPolicy{})
	}
	b.attach(guardIDs[0])
	b.open = nil
	for i := 0; i < nGuards-1; i++ {
		b.g.wire(guardIDs[i], "alt", guardIDs[i+1])
	}
	for i := 0; i < nGuards; i++ {
		b.collectCase(c.cases[i].then, guardIDs[i], "next")
	}
	last := guardIDs[nGuards-1]
	if hasDefault {
		b.collectCase(c.cases[len(c.cases)-1].then, last, "alt")
	} else {
		b.open = append(b.open, openEnd{last, "alt"})
	}
}

func (b *builder) buildBranch(br armNode, joinID string) string {
	sub := b.sub(joinID)
	sub.appendNodes(br.steps)
	if sub.start == "" {
		fail("branch %q defines no steps", br.name)
	}
	sub.wireOpenTo(joinID)
	return sub.start
}

func (b *builder) collectCase(then []node, guardID, edge string) {
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

// compile wires a recorded flow into a FlowSpec (topology only). It returns an error -- never
// panics to the caller -- for an invalid topology (duplicate name, fork with < 2 branches, an empty
// branch/body, a bad choose, escalation without a timeout).
func compile(f *Flow) (bp *FlowSpec, err error) {
	if f.name == "" {
		return nil, errors.New("workflow name is required")
	}
	if len(f.steps) == 0 {
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

	dq := f.defaultQueue
	if dq == "" {
		dq = f.name
	}
	gr := newGraph(f.name, dq)
	root := &builder{g: gr, isRoot: true}
	root.appendNodes(f.steps)
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
		"name":          f.name,
		"startNode":     gr.startNode,
		"nodes":         nodes,
		"queues":        toAnySlice(queues),
		"executionMode": "SERVER",
	}
	if len(f.attributes) > MaxAttributes {
		return nil, fmt.Errorf("a workflow declares at most %d attributes, got %d", MaxAttributes, len(f.attributes))
	}
	if len(f.attributes) > 0 {
		attrs := make([]any, 0, len(f.attributes))
		seen := map[string]bool{}
		for _, a := range f.attributes {
			if err := a.validate(); err != nil {
				return nil, err
			}
			if seen[a.Name] {
				return nil, fmt.Errorf("attribute %q is declared twice", a.Name)
			}
			seen[a.Name] = true
			attrs = append(attrs, map[string]any{"name": a.Name, "type": a.Type, "path": a.Path})
		}
		def["attributes"] = attrs
	}
	version := f.version
	if version == 0 {
		version = contentVersion(def)
	}
	def["version"] = version

	return &FlowSpec{Name: f.name, Version: version, Definition: def, Queues: queues}, nil
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
	if attrs, ok := def["attributes"]; ok {
		material["attributes"] = attrs
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
