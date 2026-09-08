package wiggle

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unicode"
)

// A struct passed to Worker.RegisterHandlers implements a workflow's steps as methods. Each exported
// method shaped like a handler is matched to a step by name; its Go signature picks the kind:
//
//	func(Context) (Context, error)  -> a task    (returns the new context; only the diff is sent back)
//	func(Context) (bool, error)     -> a gate     (predicate: gate / choose guard / do-while condition)
//	func(Context) error             -> an effect  (side effect; the context is left unchanged)
//
// Method names match step names regardless of case style: InStock, and a step named "in-stock", both
// fold to the same key. Methods with any other shape are ignored, so helpers can live on the struct.

// reflection types compared against method signatures (Context is an alias for map[string]any).
var (
	reflectCtxType  = reflect.TypeOf(Context{})
	reflectAnyType  = reflect.TypeOf((*any)(nil)).Elem()
	reflectErrType  = reflect.TypeOf((*error)(nil)).Elem()
	reflectBoolType = reflect.TypeOf(false)
)

type handlerCandidate struct {
	name    string          // the exported method name, for error messages
	kind    string          // the graph node kind this handler expects: "TASK" or "PREDICATE"
	handler activityHandler // pre-wrapped, ready to bind (nil for item candidates)
	raw     Activity        // the unwrapped Activity (nil for gates/effects); a combine binds this verbatim
	item    ItemActivity    // a forEach body step: func(base Context, item any) (any, error)
}

type handlerSet struct {
	workflow   string
	candidates map[string]handlerCandidate // keyed by canonical (case-folded) name
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

// RegisterHandlers binds every handler-shaped method of handlers as a step of workflow, matched by
// name on Start (so a method InStock implements a step named "in-stock"), the graph confirming the
// exact name and kind. It panics if two method names collide under case-folding (ambiguous), or if
// handlers exposes no handler method. Pass a pointer if your methods use pointer receivers.
func (w *Worker) RegisterHandlers(workflow string, handlers any) *Worker {
	if workflow == "" {
		panic("RegisterHandlers: workflow is required")
	}
	if handlers == nil {
		panic("RegisterHandlers: handlers is nil")
	}
	set := handlerSet{workflow: workflow, candidates: map[string]handlerCandidate{}}
	v := reflect.ValueOf(handlers)
	t := v.Type()
	for i := 0; i < t.NumMethod(); i++ {
		name := t.Method(i).Name
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
			panic(fmt.Sprintf("RegisterHandlers: methods %q and %q map to the same step name %q; "+
				"names differing only in case/style are ambiguous", prev.name, name, canon))
		}
		set.candidates[canon] = cand
	}
	if len(set.candidates) == 0 {
		panic(fmt.Sprintf("RegisterHandlers: %T exposes no handler methods (want a method shaped like "+
			"func(Context)(Context,error), func(Context)(bool,error), or func(Context)error)", handlers))
	}
	w.handlerSets = append(w.handlerSets, set)
	return w
}

// asHandlerCandidate inspects a bound method value and, if its signature matches one of the handler
// shapes, returns the pre-wrapped candidate.
func asHandlerCandidate(name string, m reflect.Value) (handlerCandidate, bool) {
	mt := m.Type()
	if mt.NumIn() != 1 || mt.In(0) != reflectCtxType {
		return handlerCandidate{}, false
	}
	switch {
	case mt.NumOut() == 2 && mt.Out(0) == reflectCtxType && mt.Out(1) == reflectErrType:
		fn := m.Interface().(func(Context) (Context, error))
		return handlerCandidate{name, "TASK", taskHandler(fn), fn, nil}, true
	case mt.NumOut() == 2 && mt.Out(0) == reflectBoolType && mt.Out(1) == reflectErrType:
		fn := m.Interface().(func(Context) (bool, error))
		return handlerCandidate{name, "PREDICATE", func(ctx Context) (any, error) { return fn(ctx) }, nil, nil}, true
	case mt.NumOut() == 1 && mt.Out(0) == reflectErrType:
		fn := m.Interface().(func(Context) error)
		return handlerCandidate{name, "TASK", func(ctx Context) (any, error) { return nil, fn(ctx) }, nil, nil}, true
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

// taskHandler wraps a user Activity into the internal handler. The return is the step's COMPLETE
// next context: it is sent whole and REPLACES the previous value server-side (no diff, no merge).
// A nil return leaves the context untouched.
func taskHandler(fn Activity) activityHandler {
	return func(ctx Context) (any, error) {
		out, err := fn(ctx)
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
func combineHandler(fn Activity) activityHandler {
	return func(ctx Context) (any, error) {
		out, err := fn(ctx)
		if err != nil {
			return nil, err
		}
		if out == nil {
			return nil, nil
		}
		return out, nil
	}
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

// matchHandlerSet resolves a RegisterHandlers set against the registered graph (fetched here --
// the binder itself is pure), then installs the resulting bindings.
func (w *Worker) matchHandlerSet(ctx context.Context, set handlerSet) error {
	graph, err := w.fetchGraph(ctx, set.workflow)
	if err != nil {
		return err
	}
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
		if itemsKey, _ := node["itemsKey"].(string); itemsKey != "" {
			// A combine node: its return is the COMPLETE post-join context, sent verbatim (no
			// diff) -- the engine replaces the context with it. Only an Activity shape can serve it.
			if cand.raw == nil {
				return nil, fmt.Errorf("step %q of workflow %q is a fork combine; handler %q must be "+
					"func(Context) (Context, error) returning the complete post-join context",
					step, set.workflow, cand.name)
			}
			handler = combineHandler(cand.raw)
		}
		bindings = append(bindings, binding{activity: activity, step: step, queue: queue, handler: handler})
	}
	return bindings, nil
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
