package wiggle

import (
	"reflect"
	"testing"
)

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
	bp := Graph{
		Name: "order",
		Steps: []Node{
			Step{Name: "validate"},
			Gate{Name: "in-stock"},
			Step{Name: "charge", Queue: "payments"},
			Effect{Name: "notify"},
		},
	}.MustCompile()
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
}

func TestGateFalseEndsGated(t *testing.T) {
	bp := Graph{Name: "wf", Steps: []Node{Step{Name: "a"}, Gate{Name: "g"}, Step{Name: "b"}}}.MustCompile()
	g := nodeByName(bp.Definition, "g")
	end := findByID(bp.Definition, g["altNext"].(string))
	if end["kind"] != "END" || end["reason"] != "gated:g" {
		t.Fatalf("gate false edge = %v", end)
	}
}

func TestForkJoin(t *testing.T) {
	bp := Graph{
		Name: "order",
		Steps: []Node{
			Fork{Branches: []Branch{
				{Name: "payment", Steps: []Node{Step{Name: "charge"}}},
				{Name: "shipping", Steps: []Node{Step{Name: "reserve"}, Step{Name: "label"}}},
			}},
			Effect{Name: "notify"},
		},
	}.MustCompile()
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
	bp := Graph{
		Name: "wf",
		Steps: []Node{
			DoWhile{While: "has-more", Body: []Node{Step{Name: "fetch"}, Step{Name: "process"}}},
			Step{Name: "finalize"},
		},
	}.MustCompile()
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
	bp := Graph{
		Name:  "wf",
		Steps: []Node{Step{Name: "submit"}, AwaitSignal{Name: "approval"}},
	}.MustCompile()
	if nodeByName(bp.Definition, "approval")["kind"] != "SIGNAL" {
		t.Fatalf("approval is not a SIGNAL node")
	}
}

func TestContentVersionDeterministic(t *testing.T) {
	build := func() int {
		return Graph{Name: "wf", Steps: []Node{Step{Name: "a"}, Gate{Name: "g"}, Effect{Name: "n"}}}.MustCompile().Version
	}
	v1, v2 := build(), build()
	if v1 != v2 || v1 <= 0 {
		t.Fatalf("version not deterministic/positive: %d vs %d", v1, v2)
	}
}

func TestExplicitVersion(t *testing.T) {
	if v := (Graph{Name: "wf", Version: 7, Steps: []Node{Step{Name: "a"}}}).MustCompile().Version; v != 7 {
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

func TestDuplicateStepErrors(t *testing.T) {
	_, err := Graph{Name: "wf", Steps: []Node{Step{Name: "a"}, Step{Name: "a"}}}.Compile()
	if err == nil {
		t.Fatalf("expected an error on duplicate step name")
	}
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
