package wiggle_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	wiggle "github.com/hadielmougy/wiggle-go"
)

func uniqueName(prefix string) string { return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano()) }

// runFlow registers wf, starts w, runs one instance from initial and waits for it to settle.
func runFlow(t *testing.T, client *wiggle.Client, ctx context.Context, wf *wiggle.FlowSpec, w *wiggle.Worker,
	initial wiggle.Context) *wiggle.InstanceView {
	t.Helper()
	if _, err := client.Register(ctx, wf); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := w.Start(ctx); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	t.Cleanup(w.Stop)
	id, err := client.Start(ctx, wf, initial)
	if err != nil {
		t.Fatalf("start instance: %v", err)
	}
	v, err := client.AwaitCompletion(ctx, id, 30*time.Second)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	return v
}

func TestIntegrationForkCombine(t *testing.T) {
	client, ctx := dialTest(t)
	name := uniqueName("go-it-fork")
	wf := wiggle.Graph{Name: name, Steps: []wiggle.Node{
		wiggle.Fork{Branches: []wiggle.Branch{
			{Name: "left", Steps: []wiggle.Node{wiggle.Step{Name: "l"}}},
			{Name: "right", Steps: []wiggle.Node{wiggle.Step{Name: "r"}}},
		}, Combine: "join"},
	}}.MustCompile()
	w := wiggle.NewWorker(client, "it-fork").
		Handle(name, "l", func(o wiggle.Context) (wiggle.Context, error) { o["side"] = "L"; return o, nil }).
		Handle(name, "r", func(o wiggle.Context) (wiggle.Context, error) { o["side"] = "R"; return o, nil }).
		HandleCombine(name, "join", func(o wiggle.Context) (wiggle.Context, error) {
			left, _ := o["left"].(map[string]any)
			right, _ := o["right"].(map[string]any)
			return wiggle.Context{"n": o["n"], "sides": fmt.Sprint(left["side"], right["side"])}, nil
		})
	v := runFlow(t, client, ctx, wf, w, wiggle.Context{"n": 1.0})
	if v.Status != "COMPLETED" || v.Context["sides"] != "LR" || v.Context["n"] != 1.0 {
		t.Fatalf("status=%s context=%v error=%q", v.Status, v.Context, v.Error)
	}
	if _, staged := v.Context["left"]; staged {
		t.Fatalf("an arm key the combine dropped survived the join: %v", v.Context)
	}
}

func TestIntegrationForEach(t *testing.T) {
	client, ctx := dialTest(t)
	name := uniqueName("go-it-each")
	wf := wiggle.Graph{Name: name, Steps: []wiggle.Node{
		wiggle.ForEach{Name: "doubled", Over: "xs", Body: []wiggle.Node{wiggle.Step{Name: "double"}}, Combine: "sum"},
	}}.MustCompile()
	w := wiggle.NewWorker(client, "it-each").
		HandleItem(name, "double", func(base wiggle.Context, item any) (any, error) {
			return item.(float64) * base["factor"].(float64), nil
		}).
		HandleCombine(name, "sum", func(o wiggle.Context) (wiggle.Context, error) {
			total := 0.0
			for _, x := range o["doubled"].([]any) {
				total += x.(float64)
			}
			return wiggle.Context{"total": total}, nil
		})
	v := runFlow(t, client, ctx, wf, w, wiggle.Context{"xs": []any{1.0, 2.0, 3.0}, "factor": 2.0})
	if v.Status != "COMPLETED" || v.Context["total"] != 12.0 {
		t.Fatalf("status=%s context=%v error=%q", v.Status, v.Context, v.Error)
	}
}

func TestIntegrationChooseAndDoWhile(t *testing.T) {
	client, ctx := dialTest(t)
	name := uniqueName("go-it-loop")
	wf := wiggle.Graph{Name: name, Steps: []wiggle.Node{
		wiggle.DoWhile{While: "more", MaxIterations: 10, Body: []wiggle.Node{wiggle.Step{Name: "inc"}}},
		wiggle.Choose{Cases: []wiggle.Case{
			{When: "big", Then: []wiggle.Node{wiggle.Step{Name: "label-big"}}},
			{Then: []wiggle.Node{wiggle.Step{Name: "label-small"}}},
		}},
	}}.MustCompile()
	label := func(l string) wiggle.Activity {
		return func(o wiggle.Context) (wiggle.Context, error) { o["label"] = l; return o, nil }
	}
	w := wiggle.NewWorker(client, "it-loop").
		Handle(name, "inc", func(o wiggle.Context) (wiggle.Context, error) { o["i"] = o["i"].(float64) + 1; return o, nil }).
		HandleGate(name, "more", func(o wiggle.Context) (bool, error) { return o["i"].(float64) < 4, nil }).
		HandleGate(name, "big", func(o wiggle.Context) (bool, error) { return o["i"].(float64) > 3, nil }).
		Handle(name, "label-big", label("big")).
		Handle(name, "label-small", label("small"))
	v := runFlow(t, client, ctx, wf, w, wiggle.Context{"i": 0.0})
	if v.Status != "COMPLETED" || v.Context["i"] != 4.0 || v.Context["label"] != "big" {
		t.Fatalf("status=%s context=%v error=%q", v.Status, v.Context, v.Error)
	}
}

func TestIntegrationSignalAndSleep(t *testing.T) {
	client, ctx := dialTest(t)
	name := uniqueName("go-it-signal")
	wf := wiggle.Graph{Name: name, Steps: []wiggle.Node{
		wiggle.Sleep{For: 100 * time.Millisecond},
		wiggle.AwaitSignal{Name: "approve", Timeout: 30 * time.Second},
		wiggle.Step{Name: "finish"},
	}}.MustCompile()
	w := wiggle.NewWorker(client, "it-signal").
		Handle(name, "finish", func(o wiggle.Context) (wiggle.Context, error) { o["done"] = true; return o, nil })
	if _, err := client.Register(ctx, wf); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := w.Start(ctx); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	defer w.Stop()
	id, err := client.Start(ctx, wf, wiggle.Context{})
	if err != nil {
		t.Fatalf("start instance: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err = client.Signal(ctx, id, "approve", wiggle.Context{"by": "ops"}); err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("signal: %v", err)
	}
	v, err := client.AwaitCompletion(ctx, id, 20*time.Second)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if v.Status != "COMPLETED" || v.Context["by"] != "ops" || v.Context["done"] != true {
		t.Fatalf("status=%s context=%v error=%q", v.Status, v.Context, v.Error)
	}
}

func TestIntegrationSubWorkflow(t *testing.T) {
	client, ctx := dialTest(t)
	child := uniqueName("go-it-child")
	parent := uniqueName("go-it-parent")
	childWf := wiggle.Graph{Name: child, Steps: []wiggle.Node{wiggle.Step{Name: "work"}}}.MustCompile()
	if _, err := client.Register(ctx, childWf); err != nil {
		t.Fatalf("register child: %v", err)
	}
	wf := wiggle.Graph{Name: parent, Steps: []wiggle.Node{
		wiggle.SubWorkflow{Workflow: child},
		wiggle.Step{Name: "after"},
	}}.MustCompile()
	w := wiggle.NewWorker(client, "it-sub").
		Handle(child, "work", func(o wiggle.Context) (wiggle.Context, error) { o["child"] = true; return o, nil }).
		Handle(parent, "after", func(o wiggle.Context) (wiggle.Context, error) { o["after"] = true; return o, nil })
	v := runFlow(t, client, ctx, wf, w, wiggle.Context{})
	if v.Status != "COMPLETED" || v.Context["child"] != true || v.Context["after"] != true {
		t.Fatalf("status=%s context=%v error=%q", v.Status, v.Context, v.Error)
	}
}

// A handler's emitted event commits with its step and reaches the event feed beside the lifecycle
// entries; the report's timings feed the step statistics.
func TestIntegrationEventsAndStats(t *testing.T) {
	client, ctx := dialTest(t)
	name := uniqueName("go-it-events")
	consumer := name + "-feed"
	if _, err := client.PollEvents(ctx, consumer, 1, 0, -1); err != nil { // register the cursor
		t.Fatalf("register consumer: %v", err)
	}
	wf := wiggle.Graph{Name: name, Steps: []wiggle.Node{wiggle.Step{Name: "ship"}}}.MustCompile()
	w := wiggle.NewWorker(client, "it-events").
		HandleStep(name, "ship", func(s *wiggle.StepRun, o wiggle.Context) (wiggle.Context, error) {
			if err := s.Emit("order.shipped", map[string]any{"attempt": s.Attempt(), "step": s.Name()}); err != nil {
				return nil, err
			}
			return o, nil
		})
	v := runFlow(t, client, ctx, wf, w, wiggle.Context{"id": "o-1"})
	if v.Status != "COMPLETED" {
		t.Fatalf("status=%s error=%q", v.Status, v.Error)
	}

	seen := map[string]wiggle.EventView{}
	deadline := time.Now().Add(15 * time.Second)
	for len(seen) < 3 && time.Now().Before(deadline) {
		batch, err := client.PollEvents(ctx, consumer, 100, 2*time.Second, -1)
		if err != nil {
			t.Fatalf("poll events: %v", err)
		}
		for _, e := range batch.Events {
			if e.InstanceID == v.ID {
				seen[e.Type] = e
			}
		}
		if n := len(batch.Events); n > 0 {
			if err := client.AckEvents(ctx, consumer, batch.Events[n-1].Cursor); err != nil {
				t.Fatalf("ack: %v", err)
			}
		}
	}
	for _, typ := range []string{"wf.started", "order.shipped", "wf.completed"} {
		if _, ok := seen[typ]; !ok {
			t.Fatalf("event %s never arrived; saw %v", typ, seen)
		}
	}
	if e := seen["order.shipped"]; e.Payload["step"] != "ship" || e.Payload["attempt"] != 1.0 || e.NodeID == "" {
		t.Fatalf("emitted event = %+v", e)
	}

	stats, err := client.StepStats(ctx, name, 0)
	if err != nil {
		t.Fatalf("step stats: %v", err)
	}
	if len(stats.Nodes) != 1 || stats.Nodes[0].Name != "ship" || stats.Nodes[0].Count != 1 {
		t.Fatalf("step stats = %+v", stats)
	}
}
