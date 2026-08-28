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

func order(name string) *wiggle.Blueprint {
	return wiggle.Define(name).
		Step("validate", func(o wiggle.Context) (wiggle.Context, error) {
			o["status"] = "VALIDATED"
			return o, nil
		}).
		Gate("in-stock", func(o wiggle.Context) (bool, error) { return o["qty"].(float64) > 0, nil }).
		Step("charge", func(o wiggle.Context) (wiggle.Context, error) {
			o["paymentRef"] = fmt.Sprintf("auth-%v", o["orderId"])
			return o, nil
		}, wiggle.WithQueue(name+"-payments")). // unique per workflow so tests don't share a queue
		Effect("done", func(o wiggle.Context) error { return nil }).
		Build()
}

func TestIntegrationFullRun(t *testing.T) {
	client, ctx := dialTest(t)
	wf := order(fmt.Sprintf("go-it-full-%d", time.Now().UnixNano()))
	if _, err := client.Register(ctx, wf); err != nil {
		t.Fatalf("register: %v", err)
	}
	w := wiggle.NewWorker(client, "it-full").Register(wf)
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
	w := wiggle.NewWorker(client, "it-bind", wiggle.RegisterOnStart(false)).
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
	w := wiggle.NewWorker(client, "it-bad", wiggle.RegisterOnStart(false)).
		Handle(name, "charrge", func(o wiggle.Context) (wiggle.Context, error) { return o, nil }) // typo
	if err := w.Start(ctx); err == nil {
		w.Stop()
		t.Fatalf("expected reconcile to reject the unknown step")
	}
}
