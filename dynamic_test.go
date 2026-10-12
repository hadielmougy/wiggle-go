package wiggle

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hadielmougy/wiggle-go/criteria"
	"github.com/hadielmougy/wiggle-go/internal/pb"
)

func TestASpawningStepCompilesToADynamicJoinAndASpawnCombine(t *testing.T) {
	def := MustDefine("wf", 0, func(f *Flow) { f.ThenApply("fan").Combine("gather").ThenApply("after") }).Definition
	byName, byID := map[string]map[string]any{}, map[string]map[string]any{}
	for _, n := range def["nodes"].([]any) {
		node := n.(map[string]any)
		byID[node["id"].(string)] = node
		if name, ok := node["name"].(string); ok {
			byName[name] = node
		}
	}
	join := byID[byName["fan"]["next"].(string)]
	if join["kind"] != "JOIN" || join["expected"] != nil {
		t.Fatalf("join = %v", join)
	}
	combine := byID[join["next"].(string)]
	if combine["name"] != "gather" || combine["collectKey"] != "__spawn__fan" || combine["next"] != byName["after"]["id"] {
		t.Fatalf("combine = %v", combine)
	}
	if !spawns(def, byName["fan"]["id"].(string)) || !spawns(def, combine["id"].(string)) || spawns(def, byName["after"]["id"].(string)) {
		t.Fatal("spawns() disagrees with the compiled shape")
	}
}

func TestDeclaredAttributesRideTheDefinition(t *testing.T) {
	plain := MustDefine("wf", 0, func(f *Flow) { f.ThenApply("a") })
	withAttr := MustDefine("wf", 0, func(f *Flow) { f.Attribute("tier", "STRING", "customer.tier").ThenApply("a") })
	want := []any{map[string]any{"name": "tier", "type": "STRING", "path": "customer.tier"}}
	if !reflect.DeepEqual(withAttr.Definition["attributes"], want) || plain.Definition["attributes"] != nil {
		t.Fatalf("attributes = %v", withAttr.Definition["attributes"])
	}
	if plain.Version == withAttr.Version {
		t.Fatal("declaring an attribute must change the content version")
	}
	for _, a := range []dispatchAttribute{{"Tier", "STRING", "x"}, {"tier", "TEXT", "x"}, {"tier", "STRING", "a..b"}} {
		if _, err := Define("wf", 0, func(f *Flow) { f.Attribute(a.Name, a.Type, a.Path).ThenApply("a") }); err == nil {
			t.Errorf("%+v was accepted", a)
		}
	}
}

func TestBranchWireForm(t *testing.T) {
	run := &StepRun{creates: true}
	run.CreateKeyed("k", map[string]any{"x": 1.0}).
		ThenApplyCompensable("reserve", OnQueue("stock"), WithRetry(RetryNone())).
		Combine("packed").
		ThenFilter("ok").
		ThenSleep(5*time.Millisecond).
		ThenAwait("go", 2*time.Second, func(e *CreatedBranch) { e.ThenApply("escalate") }).
		ThenSubFlow("ship", "shipping").
		ThenAllOf(func(a *CreatedBranch) { a.ThenApply("pay") }, func(a *CreatedBranch) { a.ThenFilter("check") }).
		Combine("settle").
		RepeatWhile("more", 4, func(b *CreatedBranch) { b.ThenApply("inc") }).
		ThenOneOf(When("bulk", func(b *CreatedBranch) { b.ThenApply("b") }), Otherwise[*CreatedBranch](nil))
	_, created, err := run.drain()
	if err != nil {
		t.Fatal(err)
	}
	cb, err := createdBranch(created[0])
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, s := range cb.Steps {
		kinds = append(kinds, s.Kind)
	}
	if strings.Join(kinds, ",") != "TASK,PREDICATE,SLEEP,SIGNAL,SUB_WORKFLOW,FORK,PREDICATE,PREDICATE" || cb.Key != "k" {
		t.Fatalf("kinds = %v key = %q", kinds, cb.Key)
	}
	r := cb.Steps[0]
	if r.Name != "reserve" || r.Queue != "stock" || !r.Compensable || r.Combine != "packed" ||
		r.Retry.GetStructValue().Fields["maxAttempts"].GetNumberValue() != 1 {
		t.Fatalf("reserve = %v", r)
	}
	if s := cb.Steps[3]; s.SleepMillis != 2000 || s.Escalation[0].Name != "escalate" {
		t.Fatalf("await = %v", s)
	}
	if f := cb.Steps[5]; f.Combine != "settle" || f.Arms[1].Steps[0].Name != "check" {
		t.Fatalf("fork = %v", f)
	}
	if l := cb.Steps[6]; l.Name != "more" || l.LoopBudget != 4 || l.Body[0].Name != "inc" {
		t.Fatalf("loop = %v", l)
	}
	if c := cb.Steps[7].Cases; c[0].Guard != "bulk" || c[1].Guard != "" || len(c[1].Steps) != 0 {
		t.Fatalf("choice = %v", c)
	}
}

func TestBranchMistakesFailTheStep(t *testing.T) {
	for name, build := range map[string]func(*CreatedBranch){
		"combine with nothing to spawn": func(b *CreatedBranch) { b.Combine("x") },
		"a gate cannot spawn":           func(b *CreatedBranch) { b.ThenFilter("g").Combine("x") },
		"one arm":                       func(b *CreatedBranch) { b.ThenAllOf(func(a *CreatedBranch) { a.ThenApply("a") }).Combine("x") },
		"arms with one name": func(b *CreatedBranch) {
			b.ThenAllOf(func(a *CreatedBranch) { a.ThenApply("a") }, func(a *CreatedBranch) { a.ThenApply("a") }).Combine("x")
		},
		"an arm ending in a sleep": func(b *CreatedBranch) {
			b.ThenAllOf(func(a *CreatedBranch) { a.ThenApply("a") }, func(a *CreatedBranch) { a.ThenSleep(time.Second) }).Combine("x")
		},
		"otherwise first": func(b *CreatedBranch) { b.ThenOneOf(Otherwise[*CreatedBranch](nil)) },
		"otherwise not last": func(b *CreatedBranch) {
			b.ThenOneOf(When[*CreatedBranch]("g", nil), Otherwise[*CreatedBranch](nil), When[*CreatedBranch]("h", nil))
		},
		"an empty loop":        func(b *CreatedBranch) { b.RepeatWhile("more", 0, nil) },
		"escalation, no limit": func(b *CreatedBranch) { b.ThenAwait("go", 0, func(e *CreatedBranch) { e.ThenApply("x") }) },
		"a negative sleep":     func(b *CreatedBranch) { b.ThenSleep(-time.Second) },
		"arms with no combine": func(b *CreatedBranch) {
			b.ThenAllOf(func(a *CreatedBranch) { a.ThenApply("a") }, func(a *CreatedBranch) { a.ThenApply("b") })
		},
	} {
		run := &StepRun{creates: true}
		build(run.Create(nil))
		if _, _, err := run.drain(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	run := &StepRun{name: "plain"}
	run.Create(nil).ThenApply("x")
	if _, _, err := run.drain(); err == nil || !strings.Contains(err.Error(), "cannot create branches") {
		t.Fatalf("Create outside a spawning step: %v", err)
	}
}

type spawnHandlers struct{}

func (spawnHandlers) BranchSteps() []string                { return []string{"reserve", "ok"} }
func (spawnHandlers) Fan(o Context) (Context, error)       { return o, nil }
func (spawnHandlers) Gather(o Context) (Context, error)    { return o, nil }
func (spawnHandlers) Reserve(o Context) (Context, error)   { o["r"] = 1.0; return o, nil }
func (spawnHandlers) CompensateReserve(Compensation) error { return nil }
func (spawnHandlers) Ok(Context) (bool, error)             { return true, nil }

func TestBranchStepsAreBoundOnDispatchNotAgainstTheGraph(t *testing.T) {
	graph := MustDefine("wf", 0, func(f *Flow) { f.ThenApply("fan").Combine("gather") }).Definition
	w := NewWorker(nil, "w").RegisterHandler("wf", spawnHandlers{})
	set := w.handlerSets[0]
	if _, err := bindHandlerSet(graph, set); err == nil {
		t.Fatal("an undeclared method matching no step must still be refused")
	}
	set.branch = w.branchSteps["wf"]
	bindings, err := bindHandlerSet(graph, set)
	if err != nil || len(bindings) != 2 {
		t.Fatalf("bindings = %v, %v", bindings, err)
	}
	h, err := bindCreated(set, "reserve", "TASK", false, false)
	if err != nil || h == nil {
		t.Fatal(err)
	}
	if out, _ := h(nil, Context{}); !reflect.DeepEqual(out, Context{"r": 1.0}) {
		t.Fatalf("reserve = %v", out)
	}
	if undo, _ := bindCreated(set, "reserve", "TASK", false, true); undo == nil {
		t.Fatal("no compensator")
	}
	if gate, _ := bindCreated(set, "ok", "PREDICATE", false, false); gate == nil {
		t.Fatal("no gate")
	}
	if _, err := bindCreated(set, "ok", "TASK", false, false); err == nil {
		t.Fatal("a gate bound to a task must be refused")
	}
	if missing, _ := bindCreated(set, "missing", "TASK", false, false); missing != nil {
		t.Fatal("bound a missing method")
	}
}

func TestCriteriaEncodeToTheWire(t *testing.T) {
	c := criteria.MustNew("pay", criteria.And(criteria.Equal(criteria.Workflow, "orders"),
		criteria.Not(criteria.In(criteria.Attempt, 3, 4)),
		criteria.Equal(criteria.Attr("amount"), criteria.Decimal("1.5")),
		criteria.Equal(criteria.Attr("vip"), true)))
	terms := criteriaProto(c).Where.GetAllOf().Terms
	if cmp := terms[0].GetCmp(); cmp.Field != "WORKFLOW" || cmp.Op != "EQUAL" || cmp.Args[0].GetText() != "orders" {
		t.Fatalf("workflow = %v", cmp)
	}
	if args := terms[1].GetNegated().GetCmp().Args; args[0].GetInteger() != 3 || args[1].GetInteger() != 4 {
		t.Fatalf("attempts = %v", args)
	}
	if cmp := terms[2].GetCmp(); cmp.Field != "ATTRIBUTE" || cmp.Attribute != "amount" || cmp.Args[0].GetDecimal() != "1.5" {
		t.Fatalf("amount = %v", cmp)
	}
	if _, ok := terms[3].GetCmp().Args[0].V.(*pb.CriteriaLiteral_Flag); !ok {
		t.Fatal("a bool must travel as a flag")
	}
}
