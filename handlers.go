package wiggle

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unicode"
)

// A struct passed to Worker.RegisterHandler implements a workflow's steps as methods. Each exported
// method shaped like a handler is matched to a step by name; its Go signature picks the kind:
//
//	func(Context) (Context, error)  -> a task    (returns the complete next context)
//	func(*StepRun, Context) (Context, error) -> a task that also reads its StepRun or emits events
//	func(Context) (bool, error)     -> a gate     (predicate: gate / choose guard / do-while condition)
//	func(Context) error             -> an effect  (side effect; the context is left unchanged)
//
// Method names match step names regardless of case style: InStock, and a step named "in-stock", both
// fold to the same key. Methods with any other shape are ignored, so helpers can live on the struct.
//
// A step declared Compensate in the topology pairs with a compensator method named
// Compensate<Step> with the shape
//
//	func(Compensation) error
//
// (CompensateCharge undoes the step "charge"). The pairing is checked both ways at bind time: a
// compensable step served without a compensator, or a Compensate<X> method whose X is not a
// compensable step, refuses to bind.

// reflection types compared against method signatures (Context is an alias for map[string]any).
var (
	reflectCtxType  = reflect.TypeOf(Context{})
	reflectAnyType  = reflect.TypeOf((*any)(nil)).Elem()
	reflectErrType  = reflect.TypeOf((*error)(nil)).Elem()
	reflectBoolType = reflect.TypeOf(false)
	reflectStepType = reflect.TypeOf(&StepRun{})
)

type handlerCandidate struct {
	name    string          // the exported method name, for error messages
	kind    string          // the graph node kind this handler expects: "TASK" or "PREDICATE"
	handler activityHandler // pre-wrapped, ready to bind (nil for item candidates)
	raw     StepActivity    // the unwrapped task (nil for gates/effects); a combine binds this verbatim
	item    ItemActivity    // a forEach body step: func(base Context, item any) (any, error)
}

type handlerSet struct {
	workflow     string
	candidates   map[string]handlerCandidate // keyed by canonical (case-folded) name
	compensators map[string]compCandidate    // keyed by canonical TARGET step name
	branch       map[string]bool             // canonical names of steps only created branches run
}

// BranchStepper is implemented by a RegisterHandler struct whose methods include steps only
// branches created at run time run; it declares them as Worker.BranchSteps does.
type BranchStepper interface {
	BranchSteps() []string
}

type compCandidate struct {
	name string // the exported method name, for error messages
	fn   Compensator
}

// canonicalName folds a name to a case/style-independent key: its lowercase alphanumerics, in order.
// So "in-stock", "in_stock", "inStock", "InStock", and "instock" all yield "instock".
func canonicalName(s string) string {
	var b strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}

// RegisterHandler binds every handler-shaped method of handlers as a step of workflow, matched by
// name on Start (so a method InStock implements a step named "in-stock"), the graph confirming the
// exact name and kind. It panics if two method names collide under case-folding (ambiguous), or if
// handlers exposes no handler method. Pass a pointer if your methods use pointer receivers.
func (w *Worker) RegisterHandler(workflow string, handlers any) *Worker {
	if workflow == "" {
		panic("RegisterHandler: workflow is required")
	}
	if handlers == nil {
		panic("RegisterHandler: handlers is nil")
	}
	set := handlerSet{workflow: workflow,
		candidates:   map[string]handlerCandidate{},
		compensators: map[string]compCandidate{}}
	v := reflect.ValueOf(handlers)
	t := v.Type()
	for i := 0; i < t.NumMethod(); i++ {
		name := t.Method(i).Name
		if target, fn, isComp := asCompensatorCandidate(name, v.Method(i)); isComp {
			canon := canonicalName(target)
			if prev, dup := set.compensators[canon]; dup {
				panic(fmt.Sprintf("RegisterHandler: compensators %q and %q both target step %q",
					prev.name, name, canon))
			}
			set.compensators[canon] = compCandidate{name, fn}
			continue
		}
		cand, ok := asHandlerCandidate(name, v.Method(i))
		if !ok {
			cand, ok = asItemCandidate(name, v.Method(i))
		}
		if !ok {
			continue
		}
		canon := canonicalName(name)
		if canon == "" {
			continue
		}
		if prev, dup := set.candidates[canon]; dup {
			panic(fmt.Sprintf("RegisterHandler: methods %q and %q map to the same step name %q; "+
				"names differing only in case/style are ambiguous", prev.name, name, canon))
		}
		set.candidates[canon] = cand
	}
	if len(set.candidates) == 0 && len(set.compensators) == 0 {
		panic(fmt.Sprintf("RegisterHandler: %T exposes no handler methods (want a method shaped like "+
			"func(Context)(Context,error), func(Context)(bool,error), or func(Context)error)", handlers))
	}
	if bs, ok := handlers.(BranchStepper); ok {
		w.BranchSteps(workflow, bs.BranchSteps()...)
	}
	w.handlerSets = append(w.handlerSets, set)
	return w
}

// asHandlerCandidate inspects a bound method value and, if its signature matches one of the handler
// shapes, returns the pre-wrapped candidate.
func asHandlerCandidate(name string, m reflect.Value) (handlerCandidate, bool) {
	mt := m.Type()
	if mt.NumIn() == 2 && mt.In(0) == reflectStepType && mt.In(1) == reflectCtxType &&
		mt.NumOut() == 2 && mt.Out(0) == reflectCtxType && mt.Out(1) == reflectErrType {
		fn := StepActivity(m.Interface().(func(*StepRun, Context) (Context, error)))
		return handlerCandidate{name, "TASK", stepHandler(fn), fn, nil}, true
	}
	if mt.NumIn() != 1 || mt.In(0) != reflectCtxType {
		return handlerCandidate{}, false
	}
	switch {
	case mt.NumOut() == 2 && mt.Out(0) == reflectCtxType && mt.Out(1) == reflectErrType:
		fn := m.Interface().(func(Context) (Context, error))
		return handlerCandidate{name, "TASK", taskHandler(fn), withoutStep(fn), nil}, true
	case mt.NumOut() == 2 && mt.Out(0) == reflectBoolType && mt.Out(1) == reflectErrType:
		fn := m.Interface().(func(Context) (bool, error))
		return handlerCandidate{name, "PREDICATE", func(_ *StepRun, ctx Context) (any, error) { return fn(ctx) }, nil, nil}, true
	case mt.NumOut() == 1 && mt.Out(0) == reflectErrType:
		fn := m.Interface().(func(Context) error)
		return handlerCandidate{name, "TASK", func(_ *StepRun, ctx Context) (any, error) { return nil, fn(ctx) }, nil, nil}, true
	}
	return handlerCandidate{}, false
}

// asItemCandidate matches the forEach body shape: func(base Context, item any) (any, error).
func asItemCandidate(name string, m reflect.Value) (handlerCandidate, bool) {
	mt := m.Type()
	if mt.NumIn() != 2 || mt.In(0) != reflectCtxType || mt.In(1) != reflectAnyType {
		return handlerCandidate{}, false
	}
	if mt.NumOut() != 2 || mt.Out(0) != reflectAnyType || mt.Out(1) != reflectErrType {
		return handlerCandidate{}, false
	}
	fn := m.Interface().(func(Context, any) (any, error))
	return handlerCandidate{name: name, kind: "TASK", item: fn}, true
}

// asCompensatorCandidate matches the compensator convention: a method named Compensate<Step> with
// the shape func(Compensation) error. Compensation-shaped methods under any other name are ignored
// like helpers — a forgotten binding is still caught by the both-ways pairing check at bind time.
func asCompensatorCandidate(name string, m reflect.Value) (string, Compensator, bool) {
	const prefix = "Compensate"
	if !strings.HasPrefix(name, prefix) || len(name) == len(prefix) {
		return "", nil, false
	}
	mt := m.Type()
	if mt.NumIn() != 1 || mt.In(0) != reflect.TypeOf(Compensation{}) {
		return "", nil, false
	}
	if mt.NumOut() != 1 || mt.Out(0) != reflectErrType {
		return "", nil, false
	}
	return name[len(prefix):], m.Interface().(func(Compensation) error), true
}

// compensationHandler adapts a Compensator to the internal handler shape: the engine stages the
// two snapshots as the activation context {"input": ..., "result": ...}; split them out. The
// return is nil — an undo never changes the (already doomed) instance context.
func compensationHandler(fn Compensator) activityHandler {
	return func(_ *StepRun, ctx Context) (any, error) {
		input, _ := ctx["input"].(map[string]any)
		result, _ := ctx["result"].(map[string]any)
		return nil, fn(Compensation{Input: input, Result: result})
	}
}

// taskHandler wraps a user Activity into the internal handler. The return is the step's COMPLETE
// next context: it is sent whole and REPLACES the previous value server-side (no diff, no merge).
// A nil return leaves the context untouched.
func taskHandler(fn Activity) activityHandler {
	return stepHandler(withoutStep(fn))
}

// stepHandler wraps a StepActivity the same way as taskHandler.
func stepHandler(fn StepActivity) activityHandler {
	return func(s *StepRun, ctx Context) (any, error) {
		out, err := fn(s, ctx)
		if err != nil {
			return nil, err
		}
		if out == nil {
			return nil, nil
		}
		return out, nil
	}
}

// combineHandler wraps a combine step's Activity: unlike taskHandler there is NO diff -- the
// return is the COMPLETE post-join context and is sent verbatim, because the engine REPLACES the
// context with it (keys the handler omits do not survive the join). A nil return leaves the
// context untouched.
func combineHandler(fn StepActivity) activityHandler {
	return func(s *StepRun, ctx Context) (any, error) {
		out, err := fn(s, ctx)
		if err != nil {
			return nil, err
		}
		if out == nil {
			return nil, nil
		}
		return out, nil
	}
}

// withoutStep adapts an Activity to a StepActivity that ignores its StepRun.
func withoutStep(fn Activity) StepActivity {
	return func(_ *StepRun, ctx Context) (Context, error) { return fn(ctx) }
}

// binding is one resolved handler: where it plugs into the worker, and exactly one of
// handler/item set (a forEach body step binds an ItemActivity; everything else an activityHandler).
type binding struct {
	activity string
	step     string
	queue    string
	handler  activityHandler
	item     ItemActivity
}

// matchHandlerSet resolves a RegisterHandler set against the registered graph (fetched here --
// the binder itself is pure), then installs the resulting bindings.
func (w *Worker) matchHandlerSet(ctx context.Context, set handlerSet) error {
	graph, err := w.fetchGraph(ctx, set.workflow)
	if err != nil {
		return err
	}
	set.branch = w.branchSteps[set.workflow]
	bindings, err := bindHandlerSet(graph, set)
	if err != nil {
		return err
	}
	return w.applyBindings(bindings)
}

// applyBindings installs resolved bindings into the worker's registries (the only stateful part).
func (w *Worker) applyBindings(bindings []binding) error {
	for _, b := range bindings {
		if _, dup := w.handlers[b.activity]; dup {
			return fmt.Errorf("duplicate handler for activity %q", b.activity)
		}
		if b.item != nil {
			w.itemHandlers[b.activity] = b.item
		} else {
			w.handlers[b.activity] = b.handler
		}
		w.queues[b.queue] = true
	}
	return nil
}

// bindHandlerSet matches a set's candidates to the graph's steps by canonical name, validates each
// candidate's shape against its node kind, and builds the invocation wrappers. PURE: graph in,
// bindings out -- no I/O, no worker state -- so every rule here is testable offline against a
// compiled Graph's Definition (mirrors the Java HandlerBinder / Python wiggle._binder).
func bindHandlerSet(graph map[string]any, set handlerSet) ([]binding, error) {
	nodeByCanon := map[string]map[string]any{} // canonical step name -> node
	stepByCanon := map[string]string{}         // canonical step name -> real step name
	for _, n := range asList(graph["nodes"]) {
		node, _ := n.(map[string]any)
		kind, _ := node["kind"].(string)
		name, _ := node["name"].(string)
		if name == "" || (kind != "TASK" && kind != "PREDICATE") {
			continue
		}
		c := canonicalName(name)
		if _, seen := nodeByCanon[c]; !seen {
			nodeByCanon[c] = node
			stepByCanon[c] = name
		}
	}
	var bindings []binding
	for _, canon := range sortedKeys(set.candidates) { // deterministic order for stable errors
		cand := set.candidates[canon]
		node, ok := nodeByCanon[canon]
		if !ok && set.branch[canon] {
			continue // bound by bindCreated on its first dispatch
		}
		if !ok {
			return nil, fmt.Errorf("handler %q matches no step in workflow %q (available: %v)",
				cand.name, set.workflow, sortedValues(stepByCanon))
		}
		step := stepByCanon[canon]
		activity := set.workflow + "#" + step
		if k, _ := node["kind"].(string); k != cand.kind {
			return nil, fmt.Errorf("activity %q is a %s in the graph but handler %q is a %s",
				activity, k, cand.name, cand.kind)
		}
		queue, _ := node["queue"].(string)
		if queue == "" {
			queue = set.workflow
		}
		if cand.item != nil {
			// A forEach body step: dispatched by activation shape at execution time.
			bindings = append(bindings, binding{activity: activity, step: step, queue: queue, item: cand.item})
			continue
		}
		handler := cand.handler
		if isCombine(node) {
			// A combine node: its return is the COMPLETE post-join context, sent verbatim (no
			// diff) -- the engine replaces the context with it. Only an Activity shape can serve it.
			if cand.raw == nil {
				return nil, fmt.Errorf("step %q of workflow %q is a fork combine; handler %q must be "+
					"func(Context) (Context, error) or func(*StepRun, Context) (Context, error) returning the complete post-join context",
					step, set.workflow, cand.name)
			}
			handler = combineHandler(cand.raw)
		}
		bindings = append(bindings, binding{activity: activity, step: step, queue: queue, handler: handler})
	}
	// Compensators bind under "<activity>#compensate"; the pairing is checked BOTH ways — a
	// Compensate<X> targeting a non-compensable (or missing) step is a lie, and a compensable
	// step served forward without its undo would strand the engine's reverse pass.
	for _, canon := range sortedCompKeys(set.compensators) {
		comp := set.compensators[canon]
		node, ok := nodeByCanon[canon]
		if !ok && set.branch[canon] {
			continue
		}
		if !ok {
			return nil, fmt.Errorf("compensator %q targets no step in workflow %q (available: %v)",
				comp.name, set.workflow, sortedValues(stepByCanon))
		}
		step := stepByCanon[canon]
		kind, _ := node["kind"].(string)
		compensable, _ := node["compensable"].(bool)
		if kind != "TASK" || isCombine(node) || !compensable {
			return nil, fmt.Errorf("compensator %q targets step %q of workflow %q, which is not "+
				"declared Compensate in the topology", comp.name, step, set.workflow)
		}
		queue, _ := node["queue"].(string)
		if queue == "" {
			queue = set.workflow
		}
		bindings = append(bindings, binding{activity: set.workflow + "#" + step + "#compensate",
			step: step, queue: queue, handler: compensationHandler(comp.fn)})
	}
	for _, canon := range sortedKeys(set.candidates) {
		node := nodeByCanon[canon]
		if node == nil {
			continue // unmatched candidates already errored above
		}
		compensable, _ := node["compensable"].(bool)
		if kind, _ := node["kind"].(string); compensable && kind == "TASK" {
			if _, has := set.compensators[canon]; !has {
				return nil, fmt.Errorf("step %q of workflow %q is declared Compensate but the "+
					"handler struct has no Compensate%s method (func(Compensation) error)",
					stepByCanon[canon], set.workflow, set.candidates[canon].name)
			}
		}
	}
	return bindings, nil
}

// bindCreated is the handler for a step a created branch runs -- in no graph, so matched by name
// on dispatch -- or nil when no method of set implements it. kind and combine describe the
// dispatched task; undo asks for the step's compensator. It fails when the method's shape does not
// fit the step, as bindHandlerSet would.
func bindCreated(set handlerSet, step, kind string, combine, undo bool) (activityHandler, error) {
	canon := canonicalName(step)
	if undo {
		if comp, ok := set.compensators[canon]; ok {
			return compensationHandler(comp.fn), nil
		}
		return nil, nil
	}
	cand, ok := set.candidates[canon]
	if !ok {
		return nil, nil
	}
	switch {
	case cand.item != nil:
		return nil, fmt.Errorf("step %q of workflow %q is a created branch's step; handler %q must take "+
			"the branch's context, not (base, item)", step, set.workflow, cand.name)
	case combine && cand.raw == nil:
		return nil, fmt.Errorf("step %q of workflow %q is a combine; handler %q must return the "+
			"complete post-join context", step, set.workflow, cand.name)
	case combine:
		return combineHandler(cand.raw), nil
	case cand.kind != kind:
		return nil, fmt.Errorf("step %q of workflow %q is a %s but handler %q is a %s",
			step, set.workflow, kind, cand.name, cand.kind)
	}
	return cand.handler, nil
}

func sortedCompKeys(m map[string]compCandidate) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedKeys(m map[string]handlerCandidate) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedValues(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}
