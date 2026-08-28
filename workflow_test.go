package wiggle

import (
	"reflect"
	"testing"
)

func id(c Context) (Context, error) { return c, nil }
func yes(c Context) (bool, error)   { return true, nil }
func noop(c Context) error          { return nil }

func nodeByName(def map[string]any, name string) map[string]any {
	for _, n := range def["nodes"].([]any) {
		node := n.(map[string]any)
		if node["name"] == name {
			return node
		}
	}
	return nil
}

func countKind(def map[string]any, kind string) int {
	c := 0
	for _, n := range def["nodes"].([]any) {
		if n.(map[string]any)["kind"] == kind {
			c++
		}
	}
	return c
}

func TestLinearGraph(t *testing.T) {
	bp := Define("order").
		Step("validate", id).
		Gate("in-stock", yes).
		Step("charge", id, WithQueue("payments")).
		Effect("notify", noop).
		Build()
	def := bp.Definition
	if got := nodeByName(def, "validate")["kind"]; got != "TASK" {
		t.Fatalf("validate kind = %v", got)
	}
	if got := nodeByName(def, "validate")["activity"]; got != "order#validate" {
		t.Fatalf("validate activity = %v", got)
	}
	if got := nodeByName(def, "in-stock")["kind"]; got != "PREDICATE" {
		t.Fatalf("in-stock kind = %v", got)
	}
	if got := nodeByName(def, "charge")["queue"]; got != "payments" {
		t.Fatalf("charge queue = %v", got)
	}
	if got := nodeByName(def, "validate")["queue"]; got != "order" {
		t.Fatalf("default queue = %v (want workflow name)", got)
	}
	if def["startNode"] != nodeByName(def, "validate")["id"] {
		t.Fatalf("start node is not validate")
	}
	if nodeByName(def, "validate")["next"] != nodeByName(def, "in-stock")["id"] {
		t.Fatalf("validate does not chain to in-stock")
	}
	// handlers keyed by activity
	if _, ok := bp.handlers["order#validate"]; !ok {
		t.Fatalf("missing handler for order#validate")
	}
}

func TestGateFalseEndsGated(t *testing.T) {
	bp := Define("wf").Step("a", id).Gate("g", yes).Step("b", id).Build()
	g := nodeByName(bp.Definition, "g")
	end := findByID(bp.Definition, g["altNext"].(string))
	if end["kind"] != "END" || end["reason"] != "gated:g" {
		t.Fatalf("gate false edge = %v", end)
	}
}

func TestForkJoin(t *testing.T) {
	bp := Define("order").
		Fork(
			BranchOf("payment", func(w *Workflow) { w.Step("charge", id) }),
			BranchOf("shipping", func(w *Workflow) { w.Step("reserve", id).Step("label", id) }),
		).
		Effect("notify", noop).
		Build()
	def := bp.Definition
	if countKind(def, "FORK") != 1 || countKind(def, "JOIN") != 1 {
		t.Fatalf("want one FORK and one JOIN")
	}
	var join map[string]any
	for _, n := range def["nodes"].([]any) {
		if n.(map[string]any)["kind"] == "JOIN" {
			join = n.(map[string]any)
		}
	}
	if join["expected"] != 2 {
		t.Fatalf("join expected = %v (want 2)", join["expected"])
	}
	if join["next"] != nodeByName(def, "notify")["id"] {
		t.Fatalf("flow does not continue after the join")
	}
}

func TestDoWhileCycle(t *testing.T) {
	bp := Define("wf").
		DoWhile("has-more", yes, func(w *Workflow) { w.Step("fetch", id).Step("process", id) }).
		Step("finalize", id).
		Build()
	def := bp.Definition
	cond := nodeByName(def, "has-more")
	if cond["kind"] != "PREDICATE" {
		t.Fatalf("condition is not a predicate")
	}
	if cond["next"] != nodeByName(def, "fetch")["id"] {
		t.Fatalf("true edge does not loop back to the body start")
	}
	if cond["altNext"] != nodeByName(def, "finalize")["id"] {
		t.Fatalf("false edge does not continue")
	}
}

func TestAwaitSignalEscalation(t *testing.T) {
	bp := Define("wf").
		Step("submit", id).
		AwaitSignal("approval", 0, nil).
		Build()
	if nodeByName(bp.Definition, "approval")["kind"] != "SIGNAL" {
		t.Fatalf("approval is not a SIGNAL node")
	}
}

func TestContentVersionDeterministic(t *testing.T) {
	build := func() int {
		return Define("wf").Step("a", id).Gate("g", yes).Effect("n", noop).Build().Version
	}
	v1, v2 := build(), build()
	if v1 != v2 || v1 <= 0 {
		t.Fatalf("version not deterministic/positive: %d vs %d", v1, v2)
	}
}

func TestExplicitVersion(t *testing.T) {
	if v := Define("wf").Version(7).Step("a", id).Build().Version; v != 7 {
		t.Fatalf("explicit version = %d", v)
	}
}

func TestShallowDiff(t *testing.T) {
	got := shallowDiff(Context{"a": 1.0, "b": 2.0}, Context{"a": 1.0, "b": 3.0, "c": 4.0})
	want := Context{"b": 3.0, "c": 4.0}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("diff = %v, want %v", got, want)
	}
	// a dropped key becomes null
	got = shallowDiff(Context{"a": 1.0, "b": 2.0}, Context{"a": 1.0})
	if v, ok := got["b"]; !ok || v != nil {
		t.Fatalf("dropped key not nulled: %v", got)
	}
}

func TestDuplicateStepPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic on duplicate step name")
		}
	}()
	Define("wf").Step("a", id).Step("a", id).Build()
}

func findByID(def map[string]any, id string) map[string]any {
	for _, n := range def["nodes"].([]any) {
		node := n.(map[string]any)
		if node["id"] == id {
			return node
		}
	}
	return nil
}
