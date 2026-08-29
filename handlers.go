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
	reflectErrType  = reflect.TypeOf((*error)(nil)).Elem()
	reflectBoolType = reflect.TypeOf(false)
)

type handlerCandidate struct {
	name    string          // the exported method name, for error messages
	kind    string          // the graph node kind this handler expects: "TASK" or "PREDICATE"
	handler activityHandler // pre-wrapped, ready to bind
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
		return handlerCandidate{name, "TASK", taskHandler(fn)}, true
	case mt.NumOut() == 2 && mt.Out(0) == reflectBoolType && mt.Out(1) == reflectErrType:
		fn := m.Interface().(func(Context) (bool, error))
		return handlerCandidate{name, "PREDICATE", func(ctx Context) (any, error) { return fn(ctx) }}, true
	case mt.NumOut() == 1 && mt.Out(0) == reflectErrType:
		fn := m.Interface().(func(Context) error)
		return handlerCandidate{name, "TASK", func(ctx Context) (any, error) { return nil, fn(ctx) }}, true
	}
	return handlerCandidate{}, false
}

// matchHandlerSet resolves a RegisterHandlers set against the registered graph, then binds each method.
func (w *Worker) matchHandlerSet(ctx context.Context, set handlerSet) error {
	graph, err := w.fetchGraph(ctx, set.workflow)
	if err != nil {
		return err
	}
	return w.bindHandlerSet(graph, set)
}

// bindHandlerSet matches a set's candidates to the graph's steps by canonical name and binds them.
// Split out from matchHandlerSet so it can be exercised offline against a compiled Graph's Definition.
func (w *Worker) bindHandlerSet(graph map[string]any, set handlerSet) error {
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
	for _, canon := range sortedKeys(set.candidates) { // deterministic order for stable errors
		cand := set.candidates[canon]
		node, ok := nodeByCanon[canon]
		if !ok {
			return fmt.Errorf("handler %q matches no step in workflow %q (available: %v)",
				cand.name, set.workflow, sortedValues(stepByCanon))
		}
		step := stepByCanon[canon]
		activity := set.workflow + "#" + step
		if k, _ := node["kind"].(string); k != cand.kind {
			return fmt.Errorf("activity %q is a %s in the graph but handler %q is a %s",
				activity, k, cand.name, cand.kind)
		}
		if _, dup := w.handlers[activity]; dup {
			return fmt.Errorf("duplicate handler for activity %q", activity)
		}
		w.handlers[activity] = cand.handler
		queue, _ := node["queue"].(string)
		if queue == "" {
			queue = set.workflow
		}
		w.queues[queue] = true
	}
	return nil
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