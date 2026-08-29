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
	return Graph{Name: "order", Steps: []Node{
		Step{Name: "validate"},
		Gate{Name: "in-stock"},
		Step{Name: "charge", Queue: "payments"},
		Effect{Name: "notify"},
	}}.MustCompile().Definition
}

func TestRegisterHandlersMatchesByNameAndKind(t *testing.T) {
	w := NewWorker(nil, "w")
	w.RegisterHandlers("order", orderHandlers{})
	if err := w.bindHandlerSet(orderGraph(), w.handlerSets[0]); err != nil {
		t.Fatalf("bind: %v", err)
	}
	for _, act := range []string{"order#validate", "order#in-stock", "order#charge", "order#notify"} {
		if _, ok := w.handlers[act]; !ok {
			t.Fatalf("missing handler for %q (have %v)", act, keys(w.handlers))
		}
	}
	// gate wrapper returns the bool
	got, err := w.handlers["order#in-stock"](Context{"qty": 2.0})
	if err != nil || got != true {
		t.Fatalf("gate = %v, %v", got, err)
	}
	// task wrapper returns only the diff
	diff, _ := w.handlers["order#charge"](Context{"orderId": "o1", "qty": 1.0})
	m := diff.(Context)
	if m["paid"] != true || len(m) != 1 {
		t.Fatalf("charge diff = %v (want just paid)", m)
	}
	// effect wrapper returns nil
	if r, _ := w.handlers["order#notify"](Context{}); r != nil {
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
	NewWorker(nil, "w").RegisterHandlers("order", dupHandlers{})
}

type noHandlers struct{}

func (noHandlers) NotAHandler(a, b int) int { return a + b }

func TestRegisterHandlersRejectsNoHandlerMethods(t *testing.T) {
	defer func() {
		if r := recover(); r == nil || !strings.Contains(fmt.Sprint(r), "no handler methods") {
			t.Fatalf("want panic about no handler methods, got %v", r)
		}
	}()
	NewWorker(nil, "w").RegisterHandlers("order", noHandlers{})
}

type strayHandlers struct{}

func (strayHandlers) Validate(o Context) (Context, error) { return o, nil }
func (strayHandlers) ShipItNow(o Context) (Context, error) { return o, nil } // no such step

func TestRegisterHandlersRejectsMethodMatchingNoStep(t *testing.T) {
	w := NewWorker(nil, "w").RegisterHandlers("order", strayHandlers{})
	err := w.bindHandlerSet(orderGraph(), w.handlerSets[0])
	if err == nil || !strings.Contains(err.Error(), "ShipItNow") {
		t.Fatalf("want error about ShipItNow matching no step, got %v", err)
	}
}

type kindClashHandlers struct{}

// InStock returns a Context (task) but the graph "in-stock" is a gate (PREDICATE).
func (kindClashHandlers) InStock(o Context) (Context, error) { return o, nil }

func TestRegisterHandlersRejectsKindClash(t *testing.T) {
	w := NewWorker(nil, "w").RegisterHandlers("order", kindClashHandlers{})
	err := w.bindHandlerSet(orderGraph(), w.handlerSets[0])
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