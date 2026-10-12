package wiggle

import (
	"fmt"
	"strings"
	"testing"
)

func TestCanonicalNameFoldsCaseStyles(t *testing.T) {
	for _, name := range []string{"in-stock", "in_stock", "inStock", "InStock", "instock", "IN_STOCK", "in stock"} {
		if got := canonicalName(name); got != "instock" {
			t.Fatalf("canonicalName(%q) = %q, want instock", name, got)
		}
	}
	if canonicalName("autoApprove") != canonicalName("auto-approve") {
		t.Fatalf("autoApprove and auto-approve should fold the same")
	}
}

// orderHandlers implements the order steps as methods; names are in mixed styles and the Go signature
// picks the kind. The graph step names are validate / in-stock / charge / notify.
type orderHandlers struct{}

func (orderHandlers) Validate(o Context) (Context, error) { o["status"] = "VALIDATED"; return o, nil }
func (orderHandlers) InStock(o Context) (bool, error)     { return o["qty"].(float64) > 0, nil }
func (orderHandlers) Charge(o Context) (Context, error)   { o["paid"] = true; return o, nil }
func (orderHandlers) Notify(o Context) error              { return nil }
func (orderHandlers) helper() string                      { return "ignored (unexported)" } //nolint:unused

func orderGraph() map[string]any {
	return MustDefine("order", 0, func(f *Flow) {
		f.ThenApply("validate").ThenFilter("in-stock").ThenApply("charge", OnQueue("payments")).ThenAccept("notify")
	}).Definition
}

func TestRegisterHandlersMatchesByNameAndKind(t *testing.T) {
	w := NewWorker(nil, "w")
	w.RegisterHandler("order", orderHandlers{})
	bindings, err := bindHandlerSet(orderGraph(), w.handlerSets[0])
	if err == nil {
		err = w.applyBindings(bindings)
	}
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	for _, act := range []string{"order#validate", "order#in-stock", "order#charge", "order#notify"} {
		if _, ok := w.handlers[act]; !ok {
			t.Fatalf("missing handler for %q (have %v)", act, keys(w.handlers))
		}
	}
	// gate wrapper returns the bool
	got, err := w.handlers["order#in-stock"](nil, Context{"qty": 2.0})
	if err != nil || got != true {
		t.Fatalf("gate = %v, %v", got, err)
	}
	// task wrapper returns the WHOLE next context (it replaces server-side)
	res, _ := w.handlers["order#charge"](nil, Context{"orderId": "o1", "qty": 1.0})
	m := res.(Context)
	if m["paid"] != true || m["orderId"] != "o1" {
		t.Fatalf("charge result = %v (want the complete context incl. paid)", m)
	}
	// effect wrapper returns nil
	if r, _ := w.handlers["order#notify"](nil, Context{}); r != nil {
		t.Fatalf("effect result = %v, want nil", r)
	}
	// queue discovered from the graph
	if !w.queues["payments"] {
		t.Fatalf("payments queue not discovered: %v", w.queues)
	}
}

type dupHandlers struct{}

func (dupHandlers) InStock(o Context) (bool, error)  { return true, nil }
func (dupHandlers) In_Stock(o Context) (bool, error) { return false, nil } //nolint:revive,stylecheck

func TestRegisterHandlersRejectsCaseFoldDuplicates(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil || !strings.Contains(fmt.Sprint(r), "same step name") {
			t.Fatalf("want panic about duplicate step name, got %v", r)
		}
	}()
	NewWorker(nil, "w").RegisterHandler("order", dupHandlers{})
}

type noHandlers struct{}

func (noHandlers) NotAHandler(a, b int) int { return a + b }

func TestRegisterHandlersRejectsNoHandlerMethods(t *testing.T) {
	defer func() {
		if r := recover(); r == nil || !strings.Contains(fmt.Sprint(r), "no handler methods") {
			t.Fatalf("want panic about no handler methods, got %v", r)
		}
	}()
	NewWorker(nil, "w").RegisterHandler("order", noHandlers{})
}

type strayHandlers struct{}

func (strayHandlers) Validate(o Context) (Context, error)  { return o, nil }
func (strayHandlers) ShipItNow(o Context) (Context, error) { return o, nil } // no such step

func TestRegisterHandlersRejectsMethodMatchingNoStep(t *testing.T) {
	w := NewWorker(nil, "w").RegisterHandler("order", strayHandlers{})
	_, err := bindHandlerSet(orderGraph(), w.handlerSets[0])
	if err == nil || !strings.Contains(err.Error(), "ShipItNow") {
		t.Fatalf("want error about ShipItNow matching no step, got %v", err)
	}
}

type kindClashHandlers struct{}

// InStock returns a Context (task) but the graph "in-stock" is a gate (PREDICATE).
func (kindClashHandlers) InStock(o Context) (Context, error) { return o, nil }

func TestRegisterHandlersRejectsKindClash(t *testing.T) {
	w := NewWorker(nil, "w").RegisterHandler("order", kindClashHandlers{})
	_, err := bindHandlerSet(orderGraph(), w.handlerSets[0])
	if err == nil || !strings.Contains(err.Error(), "PREDICATE") {
		t.Fatalf("want kind-clash error, got %v", err)
	}
}

func keys(m map[string]activityHandler) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// ---- pure-binder cases (mirrors Java HandlerBinderTest / Python test_binder) ----

type eachHandlers struct{}

func (eachHandlers) Norm(base Context, item any) (any, error) { return item, nil }
func (eachHandlers) Collect(ctx Context) (Context, error)     { return ctx, nil }

func TestBindHandlerSetIsPure(t *testing.T) {
	bp := MustDefine("each", 0, func(f *Flow) {
		f.ThenForEach("per-item", "items", func(b *Flow) { b.ThenApply("norm") }).Combine("collect").
			ThenApply("after", OnQueue("special"))
	})

	w := NewWorker(nil, "w").RegisterHandler("each", eachHandlers{})
	bindings, err := bindHandlerSet(bp.Definition, w.handlerSets[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(w.handlers) != 0 || len(w.itemHandlers) != 0 {
		t.Fatalf("bindHandlerSet must not touch worker state")
	}
	byStep := map[string]binding{}
	for _, b := range bindings {
		byStep[b.step] = b
	}
	if byStep["norm"].item == nil || byStep["norm"].handler != nil {
		t.Fatalf("forEach body binds as an item handler: %+v", byStep["norm"])
	}
	if byStep["collect"].handler == nil {
		t.Fatalf("combine binds as a regular handler (verbatim wrapper)")
	}
	if byStep["collect"].queue != "each" {
		t.Fatalf("default queue = workflow name, got %q", byStep["collect"].queue)
	}
	if err := w.applyBindings(bindings); err != nil {
		t.Fatal(err)
	}
	if w.itemHandlers["each#norm"] == nil || w.handlers["each#collect"] == nil {
		t.Fatalf("applyBindings installs into the right registries")
	}
}

type badCombineHandlers struct{}

func (badCombineHandlers) A1(ctx Context) (Context, error) { return ctx, nil }
func (badCombineHandlers) B1(ctx Context) (Context, error) { return ctx, nil }
func (badCombineHandlers) Merge(ctx Context) error         { return nil } // an effect shape can't combine

func TestBindRejectsNonActivityCombine(t *testing.T) {
	bp := MustDefine("wf", 0, func(f *Flow) {
		f.ThenAllOf(func(a *Flow) { a.ThenApply("a1") }, func(b *Flow) { b.ThenApply("b1") }).Combine("merge")
	})
	w := NewWorker(nil, "w").RegisterHandler("wf", badCombineHandlers{})
	if _, err := bindHandlerSet(bp.Definition, w.handlerSets[0]); err == nil {
		t.Fatalf("a combine bound to a non-Activity shape must fail")
	}
}

// ---- compensation pairing (mirrors Java HandlerBinder / SagaCompensationTest bind rules) ----

func sagaGraph() map[string]any {
	return MustDefine("saga", 0, func(f *Flow) {
		f.ThenApplyCompensable("reserve").ThenApply("charge", OnQueue("payments"))
	}).Definition
}

type sagaHandlers struct{ got *Compensation }

func (h sagaHandlers) Reserve(o Context) (Context, error) { return o, nil }
func (h sagaHandlers) Charge(o Context) (Context, error)  { return o, nil }
func (h sagaHandlers) CompensateReserve(c Compensation) error {
	*h.got = c
	return nil
}

func TestCompensatorBindsAndSplitsSnapshots(t *testing.T) {
	var got Compensation
	w := NewWorker(nil, "w").RegisterHandler("saga", sagaHandlers{got: &got})
	bindings, err := bindHandlerSet(sagaGraph(), w.handlerSets[0])
	if err != nil {
		t.Fatal(err)
	}
	byActivity := map[string]binding{}
	for _, b := range bindings {
		byActivity[b.activity] = b
	}
	comp, ok := byActivity["saga#reserve#compensate"]
	if !ok {
		t.Fatalf("compensator not bound; have %v", bindings)
	}
	if comp.queue != "saga" {
		t.Fatalf("compensator inherits the forward step's queue, got %q", comp.queue)
	}
	// The engine stages the two snapshots as {"input":..., "result":...}; the wrapper splits them.
	out, err := comp.handler(nil, Context{
		"input":  map[string]any{"orderId": "o1"},
		"result": map[string]any{"orderId": "o1", "reservationRef": "r-9"},
	})
	if err != nil || out != nil {
		t.Fatalf("compensator handler = %v, %v (want nil, nil)", out, err)
	}
	if got.Input["orderId"] != "o1" || got.Result["reservationRef"] != "r-9" {
		t.Fatalf("snapshots not split: %+v", got)
	}
}

type undoLessHandlers struct{}

func (undoLessHandlers) Reserve(o Context) (Context, error) { return o, nil }
func (undoLessHandlers) Charge(o Context) (Context, error)  { return o, nil }

func TestCompensableStepWithoutCompensatorRefusesToBind(t *testing.T) {
	w := NewWorker(nil, "w").RegisterHandler("saga", undoLessHandlers{})
	_, err := bindHandlerSet(sagaGraph(), w.handlerSets[0])
	if err == nil || !strings.Contains(err.Error(), "Compensate") {
		t.Fatalf("want missing-compensator error, got %v", err)
	}
}

type strayCompensator struct{}

func (strayCompensator) Reserve(o Context) (Context, error)   { return o, nil }
func (strayCompensator) Charge(o Context) (Context, error)    { return o, nil }
func (strayCompensator) CompensateReserve(Compensation) error { return nil }
func (strayCompensator) CompensateCharge(Compensation) error  { return nil } // charge is NOT compensable

func TestCompensatorOnNonCompensableStepRefusesToBind(t *testing.T) {
	w := NewWorker(nil, "w").RegisterHandler("saga", strayCompensator{})
	_, err := bindHandlerSet(sagaGraph(), w.handlerSets[0])
	if err == nil || !strings.Contains(err.Error(), "CompensateCharge") {
		t.Fatalf("want stray-compensator error, got %v", err)
	}
}

func TestHandleCompensationRegistersTheUndoActivity(t *testing.T) {
	w := NewWorker(nil, "w").
		Handle("saga", "reserve", func(c Context) (Context, error) { return c, nil }).
		HandleCompensation("saga", "reserve", func(Compensation) error { return nil })
	if _, ok := w.handlers["saga#reserve#compensate"]; !ok {
		t.Fatalf("HandleCompensation must register the #compensate activity, have %v", keys(w.handlers))
	}
}
