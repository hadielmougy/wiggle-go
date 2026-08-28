# Wiggle — Go client

A Go client and worker for the [Wiggle](https://github.com/hadielmougy/wiggle) workflow engine. It
speaks the same gRPC control plane as the Java and Python clients, so a Go worker **interoperates**
with them on one server — dispatch is by activity name (`"<workflow>#<step>"`), not by language.

```bash
go get github.com/hadielmougy/wiggle-go
```

Requires Go 1.25+ (the floor set by the gRPC/protobuf dependencies) and a running Wiggle server
(`docker run … hadielmougy/wiggle`, from the engine repo).

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	wiggle "github.com/hadielmougy/wiggle-go"
)

func main() {
	client, err := wiggle.Dial("localhost:8080")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()

	// Define a workflow. The context is a map[string]any; a step returns the whole context and the
	// engine merges only what changed. Whole numbers arrive as float64 (Go's JSON convention).
	wf := wiggle.Define("order").
		Step("validate", func(o wiggle.Context) (wiggle.Context, error) {
			o["status"] = "VALIDATED"
			return o, nil
		}).
		Gate("in-stock", func(o wiggle.Context) (bool, error) { return o["quantity"].(float64) > 0, nil }).
		Step("charge", func(o wiggle.Context) (wiggle.Context, error) {
			o["paymentRef"] = fmt.Sprintf("auth-%v", o["orderId"])
			return o, nil
		}, wiggle.WithQueue("payments"), wiggle.WithRetry(wiggle.RetryExponential(5, 100*time.Millisecond))).
		Effect("notify", func(o wiggle.Context) error { fmt.Println("shipped", o["orderId"]); return nil }).
		Build()

	client.Register(ctx, wf)

	worker := wiggle.NewWorker(client, "worker-1").Register(wf)
	worker.Start(ctx)
	defer worker.Stop()

	id, _ := client.Start(ctx, wf, wiggle.Context{"orderId": "A-1", "quantity": 3.0})
	view, _ := client.AwaitCompletion(ctx, id, 30*time.Second)
	fmt.Println(view.Status, view.Context) // COMPLETED map[...]
}
```

Run the bundled example against a server on `:8080`:

```bash
WIGGLE_URL=localhost:8080 go run ./examples/order
```

## The DSL

`Define(name)` starts a builder; every operator returns the builder so calls chain, and `Build()`
produces a `*Blueprint` you register and serve.

| Operator | Meaning |
|---|---|
| `Step(name, fn, opts…)` / `Then(...)` | run `fn(ctx) (ctx, error)` on a worker; only changed keys are merged back |
| `Effect(name, fn, opts…)` | run `fn(ctx) error` for a side effect; context unchanged |
| `Gate(name, test, opts…)` | continue only while `test(ctx) (bool, error)` holds; false ends the instance as `gated:<name>` |
| `Fork(BranchOf(name, body), …)` | run branches in parallel, then wait for all (join) |
| `ForkEach(name, itemsKey, itemKey, body)` | runtime fan-out: one branch per element of the list at `itemsKey` |
| `Choose(When(name, guard, body), …, Otherwise(name, body))` | exclusive choice: the first matching guard's branch runs |
| `DoWhile(name, cond, body)` | run `body`, then repeat while `cond(ctx)` holds (body runs at least once) |
| `SubWorkflow(name, child)` | run another workflow as a child; its result merges back |
| `Sleep(name, d)` | server-side timer; no worker is held |
| `AwaitSignal(name, timeout, escalation)` | wait for a signal (`Client.Signal`); on timeout, fail — or run `escalation` and rejoin |
| `DefaultQueue(q)` | queue for steps that don't set their own (defaults to the workflow name) |
| `Build()` | produce a `*Blueprint` |

Per-step options: `WithQueue(q)`, `WithRetry(r)` (`RetryExponential/RetryFixed/RetryNone/RetryForever`).

## Name-only binding (interop)

You don't have to author a workflow in Go to run its steps in Go. If a workflow is already registered
(by any client), bind handlers **by name** — no topology re-declaration:

```go
worker := wiggle.NewWorker(client, "payments-worker").
	Handle("order-fulfilment", "charge", func(o wiggle.Context) (wiggle.Context, error) {
		o["paymentRef"] = "auth-" + o["orderId"].(string)
		return o, nil
	})
worker.Start(ctx) // reconciles against the registered graph, discovering the queue `charge` polls
```

`Handle` (task), `HandleGate` (predicate), and `HandleEffect` (side effect) bind by name. On `Start`
the worker reconciles: it verifies each step exists and is the right kind (a typo fails fast with the
available step names) and discovers which queue each step polls. Pass `AwaitRegistration(d)` to ride
out a registration race.

## Client API

`Dial(target, opts…)` (opts are `grpc.DialOption`; pass credentials for TLS). Methods take a
`context.Context`:

```
Register(ctx, *Blueprint) (int, error)
GetWorkflow(ctx, name) (map[string]any, error)
Start(ctx, workflowOrName, context, opts…) (id, error)   // WithVersion, WithCorrelationID
Instance(ctx, id) (*InstanceView, error)
AwaitCompletion(ctx, id, timeout) (*InstanceView, error)
ListInstances(ctx, workflow, status, limit)
Cancel(ctx, id, reason) / Signal(ctx, id, name, payload)
Health(ctx)
// worker RPCs: Poll / Complete / Fail / Heartbeat
```

## Testing

```bash
go test ./...                                   # offline: DSL graph shapes + conversions
WIGGLE_TEST_URL=localhost:8080 go test -run Integration ./...   # end-to-end against a server
```

## Notes

- **Numbers** arrive as `float64` (Go's `encoding/json` convention), so assert `o["qty"].(float64)`.
- **Execution mode is SERVER only** (no local step chaining); interop with Java/Python workers is by
  activity name, so the Go content-hash version need not equal theirs.
- **Regenerating stubs:** `./codegen.sh` (needs `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc`).