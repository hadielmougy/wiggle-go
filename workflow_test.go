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
			}, Combine: "merge"},
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
	// The join flows into the mandatory combine (a TASK carrying the arm names on itemsKey),
	// and only then into the next step -- there is no implicit fold.
	combine := nodeByName(def, "merge")
	if combine["kind"] != "TASK" || combine["itemsKey"] != `["payment","shipping"]` {
		t.Fatalf("combine node wrong: %v", combine)
	}
	if join["next"] != combine["id"] {
		t.Fatalf("join must flow into the combine, got %v", join["next"])
	}
	if combine["next"] != nodeByName(def, "notify")["id"] {
		t.Fatalf("flow does not continue after the combine")
	}
}

func TestForkRequiresCombine(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("a Fork without Combine must fail to compile")
		}
	}()
	_ = Graph{
		Name: "no-combine",
		Steps: []Node{
			Fork{Branches: []Branch{
				{Name: "a", Steps: []Node{Step{Name: "a1"}}},
				{Name: "b", Steps: []Node{Step{Name: "b1"}}},
			}},
		},
	}.MustCompile()
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
	if cond["loopBudget"] != -1 {
		t.Fatalf("an unstated budget must serialize the engine-default sentinel -1, got %v", cond["loopBudget"])
	}
}

func TestDoWhileExplicitBudget(t *testing.T) {
	bp := Graph{
		Name: "wf",
		Steps: []Node{
			DoWhile{While: "has-more", MaxIterations: 500, Body: []Node{Step{Name: "fetch"}}},
		},
	}.MustCompile()
	cond := nodeByName(bp.Definition, "has-more")
	if cond["loopBudget"] != 500 {
		t.Fatalf("explicit budget must serialize, got %v", cond["loopBudget"])
	}
	// plain gates carry no budget at all
	bp2 := Graph{Name: "wf", Steps: []Node{Gate{Name: "g"}, Step{Name: "a"}}}.MustCompile()
	if _, has := nodeByName(bp2.Definition, "g")["loopBudget"]; has {
		t.Fatalf("a plain gate must not carry a loopBudget")
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

func TestTaskReturnIsSentWhole(t *testing.T) {
	// The return REPLACES the context server-side, so the handler's whole return goes on the wire.
	h := taskHandler(func(ctx Context) (Context, error) { return Context{"a": 1.0}, nil })
	got, err := h(Context{"a": 1.0, "b": 2.0})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, Context{"a": 1.0}) {
		t.Fatalf("expected the whole return, got %v", got)
	}
	// nil return = context untouched
	h = taskHandler(func(ctx Context) (Context, error) { return nil, nil })
	if got, _ := h(Context{"a": 1.0}); got != nil {
		t.Fatalf("nil return must report nil, got %v", got)
	}
}

func TestForEachEmitsCombine(t *testing.T) {
	bp := Graph{
		Name: "each",
		Steps: []Node{
			ForEach{Name: "per-item", Over: "items",
				Body: []Node{Step{Name: "price"}}, Combine: "collect"},
			Step{Name: "after"},
		},
	}.MustCompile()
	def := bp.Definition
	combine := nodeByName(def, "collect")
	if combine["kind"] != "TASK" || combine["itemsKey"] != `"per-item"` {
		t.Fatalf("forEach combine wrong: %v", combine)
	}
	var join map[string]any
	for _, n := range def["nodes"].([]any) {
		if n.(map[string]any)["kind"] == "JOIN" {
			join = n.(map[string]any)
		}
	}
	if join["next"] != combine["id"] {
		t.Fatalf("join must flow into the combine")
	}
	if combine["next"] != nodeByName(def, "after")["id"] {
		t.Fatalf("flow continues after the combine")
	}
}

func TestForEachRequiresCombine(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("a ForEach without Combine must fail to compile")
		}
	}()
	_ = Graph{Name: "bad", Steps: []Node{
		ForEach{Name: "x", Over: "items", Body: []Node{Step{Name: "s"}}},
	}}.MustCompile()
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
