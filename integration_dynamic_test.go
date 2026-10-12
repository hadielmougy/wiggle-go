package wiggle_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	wiggle "github.com/hadielmougy/wiggle-go"
	"github.com/hadielmougy/wiggle-go/criteria"
)

func combined(s *wiggle.StepRun) []any {
	r, _ := s.Results()
	list, _ := r.([]any)
	return list
}

func TestIntegrationCreatedBranchesRunDifferentChains(t *testing.T) {
	client, ctx := dialTest(t)
	name := uniqueName("go-dyn-fulfil")
	wf := wiggle.Graph{Name: name, Steps: []wiggle.Node{wiggle.Step{Name: "fulfil", Combine: "summarise"}}}.MustCompile()
	w := wiggle.NewWorker(client, "dyn-fulfil").
		BranchSteps(name, "deliver-link", "reserve", "ship").
		HandleStep(name, "fulfil", func(s *wiggle.StepRun, o wiggle.Context) (wiggle.Context, error) {
			for _, it := range o["items"].([]any) {
				item := it.(map[string]any)
				if item["digital"] == true {
					s.Create(item).ThenAccept("deliver-link")
				} else {
					s.Create(item).ThenApply("reserve").ThenApply("ship")
				}
			}
			o["fulfilled"] = true
			return o, nil
		}).
		HandleEffect(name, "deliver-link", func(wiggle.Context) error { return nil }).
		HandleStep(name, "reserve", func(s *wiggle.StepRun, item wiggle.Context) (wiggle.Context, error) {
			base, _ := s.Base()
			item["reserved"] = base["id"]
			return item, nil
		}).
		Handle(name, "ship", func(item wiggle.Context) (wiggle.Context, error) {
			item["tracking"] = "T-" + item["sku"].(string)
			return item, nil
		}).
		HandleCombineStep(name, "summarise", func(s *wiggle.StepRun, o wiggle.Context) (wiggle.Context, error) {
			var out []string
			for _, r := range combined(s) {
				item := r.(map[string]any)
				if tr, ok := item["tracking"].(string); ok {
					out = append(out, tr+"/"+fmt.Sprint(item["reserved"]))
				} else {
					out = append(out, "link:"+item["sku"].(string))
				}
			}
			return wiggle.Context{"id": o["id"], "fulfilled": o["fulfilled"], "results": strings.Join(out, ",")}, nil
		})
	v := runFlow(t, client, ctx, wf, w, wiggle.Context{"id": "o-1", "items": []any{
		map[string]any{"sku": "a", "digital": false}, map[string]any{"sku": "b", "digital": true},
		map[string]any{"sku": "c", "digital": false}}})
	if v.Status != "COMPLETED" || v.Context["results"] != "T-a/o-1,link:b,T-c/o-1" || v.Context["fulfilled"] != true {
		t.Fatalf("status=%s context=%v error=%q", v.Status, v.Context, v.Error)
	}
}

type fanHandlers struct{}

func (fanHandlers) BranchSteps() []string { return []string{"double"} }

func (fanHandlers) Fan(s *wiggle.StepRun, o wiggle.Context) (wiggle.Context, error) {
	for k, x := range o["xs"].(map[string]any) {
		s.CreateKeyed(k, map[string]any{"x": x}).ThenApply("double")
	}
	return o, nil
}

func (fanHandlers) Double(o wiggle.Context) (wiggle.Context, error) {
	return wiggle.Context{"x": o["x"].(float64) * 2}, nil
}

func (fanHandlers) Gather(s *wiggle.StepRun, o wiggle.Context) (wiggle.Context, error) {
	r, _ := s.Results()
	out := map[string]any{}
	for k, v := range r.(map[string]any) {
		out[k] = v.(map[string]any)["x"]
	}
	return wiggle.Context{"doubled": out}, nil
}

func TestIntegrationKeyedBranchesFromAHandlersStruct(t *testing.T) {
	client, ctx := dialTest(t)
	name := uniqueName("go-dyn-keyed")
	wf := wiggle.Graph{Name: name, Steps: []wiggle.Node{wiggle.Step{Name: "fan", Combine: "gather"}}}.MustCompile()
	w := wiggle.NewWorker(client, "dyn-keyed").RegisterHandlers(name, fanHandlers{})
	v := runFlow(t, client, ctx, wf, w, wiggle.Context{"xs": map[string]any{"a": 1.0, "b": 2.0}})
	doubled, _ := v.Context["doubled"].(map[string]any)
	if v.Status != "COMPLETED" || doubled["a"] != 2.0 || doubled["b"] != 4.0 {
		t.Fatalf("status=%s context=%v error=%q", v.Status, v.Context, v.Error)
	}
}

func TestIntegrationASpawningStepThatCreatesNothingStillCombines(t *testing.T) {
	client, ctx := dialTest(t)
	name := uniqueName("go-dyn-empty")
	wf := wiggle.Graph{Name: name, Steps: []wiggle.Node{wiggle.Step{Name: "fan", Combine: "gather"}}}.MustCompile()
	w := wiggle.NewWorker(client, "dyn-empty").
		Handle(name, "fan", func(o wiggle.Context) (wiggle.Context, error) { return o, nil }).
		HandleCombineStep(name, "gather", func(s *wiggle.StepRun, o wiggle.Context) (wiggle.Context, error) {
			return wiggle.Context{"n": float64(len(combined(s)))}, nil
		})
	v := runFlow(t, client, ctx, wf, w, wiggle.Context{})
	if v.Status != "COMPLETED" || v.Context["n"] != 0.0 {
		t.Fatalf("status=%s context=%v error=%q", v.Status, v.Context, v.Error)
	}
}

func TestIntegrationACombineThatCreatesBranchesRunsAnotherRound(t *testing.T) {
	client, ctx := dialTest(t)
	name := uniqueName("go-dyn-rounds")
	wf := wiggle.Graph{Name: name, Steps: []wiggle.Node{wiggle.Step{Name: "seed", Combine: "again"}}}.MustCompile()
	w := wiggle.NewWorker(client, "dyn-rounds").
		BranchSteps(name, "inc").
		HandleStep(name, "seed", func(s *wiggle.StepRun, o wiggle.Context) (wiggle.Context, error) {
			s.Create(map[string]any{"v": 1.0}).ThenApply("inc")
			return wiggle.Context{"round": 1.0, "total": 0.0}, nil
		}).
		Handle(name, "inc", func(b wiggle.Context) (wiggle.Context, error) {
			return wiggle.Context{"v": b["v"].(float64) * 10}, nil
		}).
		HandleCombineStep(name, "again", func(s *wiggle.StepRun, o wiggle.Context) (wiggle.Context, error) {
			total := o["total"].(float64)
			for _, r := range combined(s) {
				total += r.(map[string]any)["v"].(float64)
			}
			round := o["round"].(float64)
			if round < 3 {
				s.Create(map[string]any{"v": round + 1}).ThenApply("inc")
				return wiggle.Context{"round": round + 1, "total": total}, nil
			}
			return wiggle.Context{"rounds": round, "total": total}, nil
		})
	v := runFlow(t, client, ctx, wf, w, wiggle.Context{})
	if v.Status != "COMPLETED" || v.Context["rounds"] != 3.0 || v.Context["total"] != 60.0 {
		t.Fatalf("status=%s context=%v error=%q", v.Status, v.Context, v.Error)
	}
}

func TestIntegrationServerSideOperatorsInABranch(t *testing.T) {
	client, ctx := dialTest(t)
	name, child := uniqueName("go-dyn-ops"), uniqueName("go-dyn-child")
	if _, err := client.Register(ctx, wiggle.Graph{Name: child, Steps: []wiggle.Node{wiggle.Step{Name: "child-work"}}}.MustCompile()); err != nil {
		t.Fatal(err)
	}
	wf := wiggle.Graph{Name: name, Steps: []wiggle.Node{wiggle.Step{Name: "plan", Combine: "done"}}}.MustCompile()
	set := func(k string, v any) wiggle.Activity {
		return func(b wiggle.Context) (wiggle.Context, error) { b[k] = v; return b, nil }
	}
	w := wiggle.NewWorker(client, "dyn-ops").
		BranchSteps(name, "inc", "more", "big", "label-big", "label-small", "left", "right", "both", "keep").
		HandleStep(name, "plan", func(s *wiggle.StepRun, o wiggle.Context) (wiggle.Context, error) {
			s.Create(map[string]any{"n": 0.0, "size": 5.0}).
				ThenSleep(50*time.Millisecond).
				RepeatWhile("more", 10, func(b *wiggle.CreatedBranch) { b.ThenApply("inc") }).
				ThenOneOf(wiggle.When("big", func(b *wiggle.CreatedBranch) { b.ThenApply("label-big") }),
					wiggle.Otherwise(func(b *wiggle.CreatedBranch) { b.ThenApply("label-small") })).
				ThenAllOf("both", func(b *wiggle.CreatedBranch) { b.ThenApply("left") },
					func(b *wiggle.CreatedBranch) { b.ThenApply("right") }).
				ThenSubFlow("child", child).
				ThenAwait("go", 0, nil).
				ThenFilter("keep")
			return o, nil
		}).
		Handle(name, "inc", func(b wiggle.Context) (wiggle.Context, error) { b["n"] = b["n"].(float64) + 1; return b, nil }).
		HandleGate(name, "more", func(b wiggle.Context) (bool, error) { return b["n"].(float64) < 3, nil }).
		HandleGate(name, "big", func(b wiggle.Context) (bool, error) { return b["size"].(float64) > 3, nil }).
		Handle(name, "label-big", set("label", "big")).
		Handle(name, "label-small", set("label", "small")).
		Handle(name, "left", func(wiggle.Context) (wiggle.Context, error) { return wiggle.Context{"side": "L"}, nil }).
		Handle(name, "right", func(wiggle.Context) (wiggle.Context, error) { return wiggle.Context{"side": "R"}, nil }).
		HandleCombine(name, "both", func(b wiggle.Context) (wiggle.Context, error) {
			l, r := b["left"].(map[string]any), b["right"].(map[string]any)
			delete(b, "left")
			delete(b, "right")
			b["sides"] = l["side"].(string) + r["side"].(string)
			return b, nil
		}).
		Handle(child, "child-work", set("child", true)).
		HandleGate(name, "keep", func(wiggle.Context) (bool, error) { return true, nil }).
		HandleCombineStep(name, "done", func(s *wiggle.StepRun, o wiggle.Context) (wiggle.Context, error) {
			return wiggle.Context{"branch": combined(s)[0]}, nil
		})
	if _, err := client.Register(ctx, wf); err != nil {
		t.Fatal(err)
	}
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	id, err := client.Start(ctx, wf, wiggle.Context{})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		if err = client.Signal(ctx, id, "go", wiggle.Context{"approved": true}); err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("signal: %v", err)
	}
	v, err := client.AwaitCompletion(ctx, id, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	branch, _ := v.Context["branch"].(map[string]any)
	if v.Status != "COMPLETED" || branch["n"] != 3.0 || branch["label"] != "big" || branch["sides"] != "LR" ||
		branch["child"] != true || branch["approved"] != true {
		t.Fatalf("status=%s context=%v error=%q", v.Status, v.Context, v.Error)
	}
}

func TestIntegrationNestedBranches(t *testing.T) {
	client, ctx := dialTest(t)
	name := uniqueName("go-dyn-nested")
	wf := wiggle.Graph{Name: name, Steps: []wiggle.Node{wiggle.Step{Name: "lines", Combine: "packed-all"}}}.MustCompile()
	w := wiggle.NewWorker(client, "dyn-nested").
		BranchSteps(name, "pick", "packed", "label", "scan").
		HandleStep(name, "lines", func(s *wiggle.StepRun, o wiggle.Context) (wiggle.Context, error) {
			for _, l := range o["lines"].([]any) {
				s.Create(l).ThenApply("pick").Combine("packed").ThenApply("label")
			}
			return o, nil
		}).
		HandleStep(name, "pick", func(s *wiggle.StepRun, line wiggle.Context) (wiggle.Context, error) {
			for u := 0.0; u < line["qty"].(float64); u++ {
				s.Create(map[string]any{"unit": u}).ThenApply("scan")
			}
			return line, nil
		}).
		Handle(name, "scan", func(u wiggle.Context) (wiggle.Context, error) { return wiggle.Context{"scanned": u["unit"]}, nil }).
		HandleCombineStep(name, "packed", func(s *wiggle.StepRun, line wiggle.Context) (wiggle.Context, error) {
			return wiggle.Context{"sku": line["sku"], "units": float64(len(combined(s)))}, nil
		}).
		Handle(name, "label", func(p wiggle.Context) (wiggle.Context, error) {
			p["label"] = fmt.Sprintf("%vx%v", p["sku"], p["units"])
			return p, nil
		}).
		HandleCombineStep(name, "packed-all", func(s *wiggle.StepRun, o wiggle.Context) (wiggle.Context, error) {
			var labels []string
			for _, r := range combined(s) {
				labels = append(labels, r.(map[string]any)["label"].(string))
			}
			return wiggle.Context{"labels": strings.Join(labels, ",")}, nil
		})
	v := runFlow(t, client, ctx, wf, w, wiggle.Context{"lines": []any{
		map[string]any{"sku": "a", "qty": 2.0}, map[string]any{"sku": "b", "qty": 3.0}}})
	if v.Status != "COMPLETED" || v.Context["labels"] != "ax2,bx3" {
		t.Fatalf("status=%s context=%v error=%q", v.Status, v.Context, v.Error)
	}
}

func TestIntegrationACompensableBranchStepIsUndone(t *testing.T) {
	client, ctx := dialTest(t)
	name := uniqueName("go-dyn-saga")
	wf := wiggle.Graph{Name: name, Steps: []wiggle.Node{
		wiggle.Step{Name: "book", Combine: "settle"}, wiggle.Step{Name: "boom"}}}.MustCompile()
	undone := make(chan wiggle.Compensation, 1)
	w := wiggle.NewWorker(client, "dyn-saga").
		BranchSteps(name, "hold").
		HandleStep(name, "book", func(s *wiggle.StepRun, o wiggle.Context) (wiggle.Context, error) {
			s.Create(map[string]any{"seat": 7.0}).ThenApply("hold", wiggle.Compensable())
			return o, nil
		}).
		Handle(name, "hold", func(b wiggle.Context) (wiggle.Context, error) { b["held"] = true; return b, nil }).
		HandleCompensation(name, "hold", func(c wiggle.Compensation) error { undone <- c; return nil }).
		HandleCombine(name, "settle", func(o wiggle.Context) (wiggle.Context, error) { return o, nil }).
		Handle(name, "boom", func(wiggle.Context) (wiggle.Context, error) { return nil, wiggle.Permanent("downstream exploded") })
	v := runFlow(t, client, ctx, wf, w, wiggle.Context{})
	if v.Status != "COMPENSATED" {
		t.Fatalf("status=%s error=%q", v.Status, v.Error)
	}
	select {
	case c := <-undone:
		if c.Result["held"] != true || c.Result["seat"] != 7.0 {
			t.Fatalf("compensation = %+v", c)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("compensator never ran")
	}
}

func TestIntegrationCreateOutsideASpawningStepFailsTheStep(t *testing.T) {
	client, ctx := dialTest(t)
	name := uniqueName("go-dyn-misuse")
	wf := wiggle.Graph{Name: name, Steps: []wiggle.Node{wiggle.Step{Name: "plain"}}}.MustCompile()
	w := wiggle.NewWorker(client, "dyn-misuse").
		HandleStep(name, "plain", func(s *wiggle.StepRun, o wiggle.Context) (wiggle.Context, error) {
			s.Create(o).ThenApply("x")
			return o, nil
		})
	v := runFlow(t, client, ctx, wf, w, wiggle.Context{})
	if v.Status != "FAILED" || !strings.Contains(v.Error, "cannot create branches") {
		t.Fatalf("status=%s error=%q", v.Status, v.Error)
	}
}

func TestIntegrationQueueCriteriaClaimOnlyTheMatchingTasks(t *testing.T) {
	client, ctx := dialTest(t)
	name := uniqueName("go-crit")
	queue := name + "-q"
	wf := wiggle.Graph{Name: name, Steps: []wiggle.Node{wiggle.Step{Name: "work", Queue: queue}},
		Attributes: []wiggle.Attribute{{Name: "tier", Type: "STRING", Path: "customer.tier"}}}.MustCompile()
	if _, err := client.Register(ctx, wf); err != nil {
		t.Fatal(err)
	}
	gold := criteria.MustNew(queue, criteria.And(
		criteria.Equal(criteria.Workflow, name),
		criteria.Equal(criteria.Attr("tier"), "gold"),
		criteria.LessThan(criteria.Attempt, 5)))
	ran := make(chan string, 4)
	w := wiggle.NewWorker(client, "crit-gold", wiggle.WithCriteria(gold)).
		Handle(name, "work", func(o wiggle.Context) (wiggle.Context, error) {
			ran <- o["customer"].(map[string]any)["tier"].(string)
			return o, nil
		})
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	goldID, _ := client.Start(ctx, wf, wiggle.Context{"customer": map[string]any{"tier": "gold"}}, wiggle.WithCorrelationID("g"))
	silverID, _ := client.Start(ctx, wf, wiggle.Context{"customer": map[string]any{"tier": "silver"}}, wiggle.WithCorrelationID("s"))
	if v, err := client.AwaitCompletion(ctx, goldID, 20*time.Second); err != nil || v.Status != "COMPLETED" {
		t.Fatalf("gold: %v %v", v, err)
	}
	time.Sleep(1500 * time.Millisecond)
	if v, _ := client.Instance(ctx, silverID); v.Status != "RUNNING" {
		t.Fatalf("silver ran on a gold-only worker: %s", v.Status)
	}
	slices, _, err := client.BacklogCoverage(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, sl := range slices {
		if sl.Workflow == name {
			found = sl.ReadyCount == 1 && sl.Coverage != "COVERED"
		}
	}
	if !found {
		t.Fatalf("backlog = %+v", slices)
	}
	w.Stop()
	other := wiggle.NewWorker(client, "crit-silver",
		wiggle.WithCriteria(criteria.MustNew(queue, criteria.In(criteria.CorrelationID, "s", "t")))).
		Handle(name, "work", func(o wiggle.Context) (wiggle.Context, error) { return o, nil })
	if err := other.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer other.Stop()
	if v, err := client.AwaitCompletion(ctx, silverID, 20*time.Second); err != nil || v.Status != "COMPLETED" {
		t.Fatalf("silver: %v %v", v, err)
	}
	if got := <-ran; got != "gold" || len(ran) != 0 {
		t.Fatalf("the gold worker ran %q (+%d more)", got, len(ran))
	}
}
