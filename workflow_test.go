package wiggle

import (
	"reflect"
	"testing"
	"time"
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
	bp := MustDefine("order", 0, func(f *Flow) {
		f.ThenApply("validate").ThenFilter("in-stock").ThenApply("charge", OnQueue("payments")).ThenAccept("notify")
	})
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
	bp := MustDefine("wf", 0, func(f *Flow) { f.ThenApply("a").ThenFilter("g").ThenApply("b") })
	g := nodeByName(bp.Definition, "g")
	end := findByID(bp.Definition, g["altNext"].(string))
	if end["kind"] != "END" || end["reason"] != "gated:g" {
		t.Fatalf("gate false edge = %v", end)
	}
}

func TestForkJoin(t *testing.T) {
	bp := MustDefine("order", 0, func(f *Flow) {
		f.ThenAllOf(
			func(a *Flow) { a.ThenApply("charge") },
			func(a *Flow) { a.ThenApply("reserve").ThenApply("label") },
		).Combine("merge").
			ThenAccept("notify")
	})
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
	// The join flows into the mandatory combine (a TASK carrying the arm names),
	// and only then into the next step -- there is no implicit fold.
	combine := nodeByName(def, "merge")
	if combine["kind"] != "TASK" || !reflect.DeepEqual(combine["armNames"], []any{"charge", "label"}) {
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
	_, err := Define("no-combine", 0, func(f *Flow) {
		f.ThenAllOf(func(a *Flow) { a.ThenApply("a1") }, func(b *Flow) { b.ThenApply("b1") })
	})
	if err == nil {
		t.Fatalf("a ThenAllOf without Combine must fail to define")
	}
	_, err = Define("chained", 0, func(f *Flow) {
		f.ThenAllOf(func(a *Flow) { a.ThenApply("a1") }, func(b *Flow) { b.ThenApply("b1") })
		f.ThenApply("next")
	})
	if err == nil {
		t.Fatalf("a step after a ThenAllOf without Combine must fail to define")
	}
}

func TestDoWhileCycle(t *testing.T) {
	bp := MustDefine("wf", 0, func(f *Flow) {
		f.RepeatWhile("has-more", 0, func(b *Flow) { b.ThenApply("fetch").ThenApply("process") }).
			ThenApply("finalize")
	})
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
	bp := MustDefine("wf", 0, func(f *Flow) {
		f.RepeatWhile("has-more", 500, func(b *Flow) { b.ThenApply("fetch") })
	})
	cond := nodeByName(bp.Definition, "has-more")
	if cond["loopBudget"] != 500 {
		t.Fatalf("explicit budget must serialize, got %v", cond["loopBudget"])
	}
	// plain gates carry no budget at all
	bp2 := MustDefine("wf", 0, func(f *Flow) { f.ThenFilter("g").ThenApply("a") })
	if _, has := nodeByName(bp2.Definition, "g")["loopBudget"]; has {
		t.Fatalf("a plain gate must not carry a loopBudget")
	}
}

func TestAwaitSignalEscalation(t *testing.T) {
	bp := MustDefine("wf", 0, func(f *Flow) { f.ThenApply("submit").ThenAwait("approval", 0, nil) })
	if nodeByName(bp.Definition, "approval")["kind"] != "SIGNAL" {
		t.Fatalf("approval is not a SIGNAL node")
	}
}

func TestContentVersionDeterministic(t *testing.T) {
	build := func() int {
		return MustDefine("wf", 0, func(f *Flow) { f.ThenApply("a").ThenFilter("g").ThenAccept("n") }).Version
	}
	v1, v2 := build(), build()
	if v1 != v2 || v1 <= 0 {
		t.Fatalf("version not deterministic/positive: %d vs %d", v1, v2)
	}
}

func TestExplicitVersion(t *testing.T) {
	if v := MustDefine("wf", 7, func(f *Flow) { f.ThenApply("a") }).Version; v != 7 {
		t.Fatalf("explicit version = %d", v)
	}
}

func TestTaskReturnIsSentWhole(t *testing.T) {
	// The return REPLACES the context server-side, so the handler's whole return goes on the wire.
	h := taskHandler(func(ctx Context) (Context, error) { return Context{"a": 1.0}, nil })
	got, err := h(nil, Context{"a": 1.0, "b": 2.0})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, Context{"a": 1.0}) {
		t.Fatalf("expected the whole return, got %v", got)
	}
	// nil return = context untouched
	h = taskHandler(func(ctx Context) (Context, error) { return nil, nil })
	if got, _ := h(nil, Context{"a": 1.0}); got != nil {
		t.Fatalf("nil return must report nil, got %v", got)
	}
}

func TestForEachEmitsCombine(t *testing.T) {
	bp := MustDefine("each", 0, func(f *Flow) {
		f.ThenForEach("per-item", "items", func(b *Flow) { b.ThenApply("price") }).Combine("collect").
			ThenApply("after")
	})
	def := bp.Definition
	combine := nodeByName(def, "collect")
	if combine["kind"] != "TASK" || combine["collectKey"] != "__forEach__per-item" {
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
	_, err := Define("bad", 0, func(f *Flow) {
		f.ThenForEach("x", "items", func(b *Flow) { b.ThenApply("s") })
	})
	if err == nil {
		t.Fatalf("a ThenForEach without Combine must fail to define")
	}
}

func TestDuplicateStepErrors(t *testing.T) {
	_, err := Define("wf", 0, func(f *Flow) { f.ThenApply("a").ThenApply("a") })
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

func TestCompensateFlagSerializes(t *testing.T) {
	def := MustDefine("saga", 0, func(f *Flow) {
		f.ThenApplyCompensable("reserve").ThenApplyCompensable("audit").ThenApply("plain")
	}).Definition
	byName := map[string]map[string]any{}
	for _, n := range asList(def["nodes"]) {
		node := n.(map[string]any)
		name, _ := node["name"].(string)
		byName[name] = node
	}
	if byName["reserve"]["compensable"] != true {
		t.Fatalf("ThenApplyCompensable must serialize compensable:true, got %v", byName["reserve"])
	}
	if byName["audit"]["compensable"] != true {
		t.Fatalf("ThenApplyCompensable must serialize compensable:true, got %v", byName["audit"])
	}
	// Absent (not false) when unset — the field must not disturb existing content hashes.
	if _, present := byName["plain"]["compensable"]; present {
		t.Fatalf("a plain ThenApply must not serialize at all, got %v", byName["plain"])
	}
}

func TestDefineNestsEveryShape(t *testing.T) {
	def := MustDefine("nest", 0, func(f *Flow) {
		f.DefaultQueue("main").
			ThenApply("start").
			ThenAwait("approve", time.Minute, func(e *Flow) { e.ThenApply("escalate") }).
			ThenOneOf(
				When("vip", func(a *Flow) {
					a.ThenAllOf(func(x *Flow) { x.ThenApply("x1") }, func(y *Flow) { y.ThenFilter("y1") }).Combine("xy")
				}),
				Otherwise(func(a *Flow) { a.ThenSubFlow("", "child") }),
			).
			ThenSleep(time.Second)
	}).Definition
	if def["queues"].([]any)[0] != "main" {
		t.Fatalf("default queue not applied: %v", def["queues"])
	}
	if nodeByName(def, "approve")["altNext"] != nodeByName(def, "escalate")["id"] {
		t.Fatalf("a timeout must run the escalation")
	}
	if !reflect.DeepEqual(nodeByName(def, "xy")["armNames"], []any{"x1", "y1"}) {
		t.Fatalf("arms are named after their last step: %v", nodeByName(def, "xy"))
	}
	if nodeByName(def, "child")["kind"] != "SUB_WORKFLOW" || nodeByName(def, "sleep-1000ms")["kind"] != "SLEEP" {
		t.Fatalf("sub-flow or sleep missing")
	}
}

func TestDefineMistakes(t *testing.T) {
	for name, body := range map[string]func(*Flow){
		"no steps":             func(f *Flow) {},
		"a nameless step":      func(f *Flow) { f.ThenApply("") },
		"combine with no task": func(f *Flow) { f.Combine("c") },
		"combine after a gate": func(f *Flow) { f.ThenFilter("g").Combine("c") },
		"one arm":              func(f *Flow) { f.ThenAllOf(func(a *Flow) { a.ThenApply("a") }).Combine("c") },
		"an empty arm":         func(f *Flow) { f.ThenAllOf(func(a *Flow) { a.ThenApply("a") }, nil).Combine("c") },
		"an arm ending in sleep": func(f *Flow) {
			f.ThenAllOf(func(a *Flow) { a.ThenApply("a") }, func(a *Flow) { a.ThenSleep(time.Second) }).Combine("c")
		},
		"an empty forEach":       func(f *Flow) { f.ThenForEach("e", "xs", nil).Combine("c") },
		"otherwise first":        func(f *Flow) { f.ThenOneOf(Otherwise(func(a *Flow) { a.ThenApply("a") })) },
		"an empty loop":          func(f *Flow) { f.RepeatWhile("more", 0, nil) },
		"escalation, no timeout": func(f *Flow) { f.ThenAwait("go", 0, func(e *Flow) { e.ThenApply("x") }) },
		"a nested mistake": func(f *Flow) {
			f.ThenOneOf(When("g", func(a *Flow) { a.ThenAllOf(func(x *Flow) { x.ThenApply("x") }) }))
		},
	} {
		if _, err := Define("bad", 0, body); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}
