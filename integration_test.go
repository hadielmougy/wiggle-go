package wiggle_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	wiggle "github.com/hadielmougy/wiggle-go"
)

func testURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("WIGGLE_TEST_URL")
	if url == "" {
		t.Skip("set WIGGLE_TEST_URL to run integration tests")
	}
	return url
}

// Opt-in end-to-end tests against a running server. Set WIGGLE_TEST_URL, e.g. after starting the
// engine image:  docker run --rm -p 8080:8080 hadielmougy/wiggle
//
//	WIGGLE_TEST_URL=localhost:8080 go test -run Integration ./...
func dialTest(t *testing.T) (*wiggle.Client, context.Context) {
	t.Helper()
	url := testURL(t)
	c, err := wiggle.Dial(url)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c, context.Background()
}

// order is the declarative topology (no handlers). Each test binds handlers by name (bindOrder).
func order(name string) *wiggle.Blueprint {
	return wiggle.Graph{
		Name: name,
		Steps: []wiggle.Node{
			wiggle.Step{Name: "validate"},
			wiggle.Gate{Name: "in-stock"},
			wiggle.Step{Name: "charge", Queue: name + "-payments"}, // unique per workflow so tests don't share a queue
			wiggle.Effect{Name: "done"},
		},
	}.MustCompile()
}

// bindOrder attaches the order handlers to a worker by (workflow, step) name.
func bindOrder(w *wiggle.Worker, name string) *wiggle.Worker {
	return w.
		Handle(name, "validate", func(o wiggle.Context) (wiggle.Context, error) {
			o["status"] = "VALIDATED"
			return o, nil
		}).
		HandleGate(name, "in-stock", func(o wiggle.Context) (bool, error) { return o["qty"].(float64) > 0, nil }).
		Handle(name, "charge", func(o wiggle.Context) (wiggle.Context, error) {
			o["paymentRef"] = fmt.Sprintf("auth-%v", o["orderId"])
			return o, nil
		}).
		HandleEffect(name, "done", func(o wiggle.Context) error { return nil })
}

func TestIntegrationFullRun(t *testing.T) {
	client, ctx := dialTest(t)
	wf := order(fmt.Sprintf("go-it-full-%d", time.Now().UnixNano()))
	if _, err := client.Register(ctx, wf); err != nil {
		t.Fatalf("register: %v", err)
	}
	w := bindOrder(wiggle.NewWorker(client, "it-full"), wf.Name)
	if err := w.Start(ctx); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	defer w.Stop()

	id, err := client.Start(ctx, wf, wiggle.Context{"orderId": "x1", "qty": 3.0})
	if err != nil {
		t.Fatalf("start instance: %v", err)
	}
	v, err := client.AwaitCompletion(ctx, id, 20*time.Second)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if v.Status != "COMPLETED" {
		t.Fatalf("status = %s", v.Status)
	}
	if v.Context["status"] != "VALIDATED" || v.Context["paymentRef"] != "auth-x1" {
		t.Fatalf("context = %v", v.Context)
	}
}

// A worker that never saw the blueprint implements every step by (workflow, step) name; Start
// reconciles against the registered graph and discovers the payments queue.
func TestIntegrationNameOnlyBinding(t *testing.T) {
	client, ctx := dialTest(t)
	name := fmt.Sprintf("go-it-bind-%d", time.Now().UnixNano())
	if _, err := client.Register(ctx, order(name)); err != nil { // register topology only
		t.Fatalf("register: %v", err)
	}
	w := wiggle.NewWorker(client, "it-bind").
		Handle(name, "validate", func(o wiggle.Context) (wiggle.Context, error) {
			o["status"] = "VALIDATED"
			return o, nil
		}).
		HandleGate(name, "in-stock", func(o wiggle.Context) (bool, error) { return o["qty"].(float64) > 0, nil }).
		Handle(name, "charge", func(o wiggle.Context) (wiggle.Context, error) {
			o["paymentRef"] = fmt.Sprintf("auth-%v", o["orderId"])
			return o, nil
		}).
		HandleEffect(name, "done", func(o wiggle.Context) error { return nil })
	if err := w.Start(ctx); err != nil {
		t.Fatalf("start (reconcile) failed: %v", err)
	}
	defer w.Stop()

	id, err := client.Start(ctx, name, wiggle.Context{"orderId": "y1", "qty": 2.0})
	if err != nil {
		t.Fatalf("start instance: %v", err)
	}
	v, err := client.AwaitCompletion(ctx, id, 20*time.Second)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if v.Status != "COMPLETED" || v.Context["paymentRef"] != "auth-y1" {
		t.Fatalf("status=%s context=%v", v.Status, v.Context)
	}
}

// unknown step name fails fast at Start via reconciliation.
func TestIntegrationReconcileRejectsUnknownStep(t *testing.T) {
	client, ctx := dialTest(t)
	name := fmt.Sprintf("go-it-bad-%d", time.Now().UnixNano())
	if _, err := client.Register(ctx, order(name)); err != nil {
		t.Fatalf("register: %v", err)
	}
	w := wiggle.NewWorker(client, "it-bad").
		Handle(name, "charrge", func(o wiggle.Context) (wiggle.Context, error) { return o, nil }) // typo
	if err := w.Start(ctx); err == nil {
		w.Stop()
		t.Fatalf("expected reconcile to reject the unknown step")
	}
}

// A saga: reserve declares Compensate; boom fails permanently; the engine's reverse pass runs the
// compensator with the step's OWN input/result snapshots and the instance lands COMPENSATED.
func TestIntegrationCompensation(t *testing.T) {
	client, ctx := dialTest(t)
	name := fmt.Sprintf("go-it-saga-%d", time.Now().UnixNano())
	wf := wiggle.Graph{Name: name, Steps: []wiggle.Node{
		wiggle.Step{Name: "reserve", Compensate: true},
		wiggle.Step{Name: "overwrite"}, // replaces the context: proves the compensator sees SNAPSHOTS
		wiggle.Step{Name: "boom"},
	}}.MustCompile()
	if _, err := client.Register(ctx, wf); err != nil {
		t.Fatalf("register: %v", err)
	}
	undone := make(chan wiggle.Compensation, 1)
	w := wiggle.NewWorker(client, "it-saga").
		Handle(name, "reserve", func(o wiggle.Context) (wiggle.Context, error) {
			return wiggle.Context{"orderId": o["orderId"], "reservationRef": "r-1"}, nil
		}).
		HandleCompensation(name, "reserve", func(c wiggle.Compensation) error {
			undone <- c
			return nil
		}).
		Handle(name, "overwrite", func(o wiggle.Context) (wiggle.Context, error) {
			return wiggle.Context{"unrelated": true}, nil // drops reservationRef from the live context
		}).
		Handle(name, "boom", func(o wiggle.Context) (wiggle.Context, error) {
			return nil, wiggle.Permanent("downstream exploded")
		})
	if err := w.Start(ctx); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	defer w.Stop()

	id, err := client.Start(ctx, wf, wiggle.Context{"orderId": "o-9"})
	if err != nil {
		t.Fatalf("start instance: %v", err)
	}
	v, err := client.AwaitCompletion(ctx, id, 30*time.Second)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if v.Status != "COMPENSATED" {
		t.Fatalf("status = %s (error %q)", v.Status, v.Error)
	}
	select {
	case c := <-undone:
		if c.Result["reservationRef"] != "r-1" {
			t.Fatalf("compensator must see the step's own result snapshot, got %v", c.Result)
		}
		if c.Input["orderId"] != "o-9" {
			t.Fatalf("compensator must see the step's input snapshot, got %v", c.Input)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("compensator never ran")
	}
}
