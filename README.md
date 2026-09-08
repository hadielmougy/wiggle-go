# Wiggle — Go client

A Go client and worker for [Wiggle](https://github.com/hadielmougy/wiggle), the durable state-machine
platform. It speaks the same gRPC control plane as the Java and Python clients, so a Go worker
**interoperates** with them on one server — dispatch is by activity name (`"<workflow>#<step>"`), not
by language. In a coordinator-sharded deployment the Go client resolves the owning cell per instance.

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

	// The topology is declarative data -- a Graph that mirrors the YAML/graph schema. Compile it to a
	// *Blueprint you register. Handlers are bound separately, by name (below).
	wf := wiggle.Graph{
		Name: "order",
		Steps: []wiggle.Node{
			wiggle.Step{Name: "validate"},
			wiggle.Gate{Name: "in-stock"},
			wiggle.Step{Name: "charge", Queue: "payments", Retry: wiggle.RetryExponential(5, 100*time.Millisecond)},
			wiggle.Effect{Name: "notify"},
		},
	}.MustCompile()

	client.Register(ctx, wf)

	// A worker implements steps by name. The context is a map[string]any; a task returns the whole
	// context and the engine merges only what changed. Whole numbers arrive as float64 (Go's JSON convention).
	worker := wiggle.NewWorker(client, "worker-1").
		Handle("order", "validate", func(o wiggle.Context) (wiggle.Context, error) {
			o["status"] = "VALIDATED"
			return o, nil
		}).
		HandleGate("order", "in-stock", func(o wiggle.Context) (bool, error) { return o["quantity"].(float64) > 0, nil }).
		Handle("order", "charge", func(o wiggle.Context) (wiggle.Context, error) {
			o["paymentRef"] = fmt.Sprintf("auth-%v", o["orderId"])
			return o, nil
		}).
		HandleEffect("order", "notify", func(o wiggle.Context) error { fmt.Println("shipped", o["orderId"]); return nil })
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

## Topology

A workflow's shape is declarative Go data — a `Graph` whose `Steps` is a slice of `Node` values that
mirror the YAML/graph schema. `Compile()` turns it into a `*Blueprint` you register (it validates the
shape and returns an error); `MustCompile()` panics instead, for a literal known-good at build time.
Handlers are **not** part of the topology — a worker binds them separately, by name (see below).

```go
wf := wiggle.Graph{
	Name: "order",
	// DefaultQueue: "order",   // queue for steps that don't set their own (defaults to the name)
	Steps: []wiggle.Node{
		wiggle.Step{Name: "validate"},
		wiggle.Gate{Name: "in-stock"},
		wiggle.Fork{Branches: []wiggle.Branch{
			{Name: "payment",  Steps: []wiggle.Node{wiggle.Step{Name: "charge", Queue: "payments"}}},
			{Name: "shipping", Steps: []wiggle.Node{wiggle.Step{Name: "reserve"}, wiggle.Step{Name: "label"}}},
		}, Combine: "merge"}, // mandatory: branches rejoin at an explicit merge handler (HandleCombine)
		wiggle.Choose{Cases: []wiggle.Case{
			{When: "vip", Then: []wiggle.Node{wiggle.Step{Name: "concierge"}}},
			{Then: []wiggle.Node{wiggle.Step{Name: "thanks"}}}, // no When -> the otherwise case (must be last)
		}},
		wiggle.Effect{Name: "notify"},
	},
}.MustCompile()
```

| Node | Meaning |
|---|---|
| `Step{Name, Queue, Retry}` | a task run on a worker (`Handle`); only changed context keys are merged back |
| `Effect{Name, Queue, Retry}` | a side-effect step (`HandleEffect`); context unchanged |
| `Gate{Name, Queue, Retry}` | a predicate (`HandleGate`); false ends the instance as `gated:<name>` |
| `Fork{Branches, Combine}` | run branches in parallel on isolated context copies, then rejoin at the **mandatory** `Combine` step (`HandleCombine`) — no implicit fold |
| `ForEach{Name, Over, Body, Combine}` | runtime fan-out: one **isolated** branch per element of the list (or map) at `Over`. **The element IS the item's context** — body steps are bound with `HandleItem(base, item)` and their return replaces the item's value (scalars included). The **mandatory** `Combine` handler receives every item's final value collected under `Name` (a list, or a map keyed like the input) and returns the complete post-join context |
| `Choose{Cases}` | exclusive choice: the first `Case` whose `When` guard holds runs; a `Case` with no `When` is the otherwise (last) |
| `DoWhile{While, Body}` | run `Body`, then repeat while the `While` predicate holds (body runs at least once) |
| `SubWorkflow{Name, Workflow}` | run another workflow as a child; its result merges back |
| `Sleep{Name, For}` | server-side timer (`For` is a `time.Duration`); no worker is held |
| `AwaitSignal{Name, Timeout, Escalation}` | wait for a signal (`Client.Signal`); on `Timeout`, fail — or run `Escalation` and rejoin |

A fork's combine is bound with `HandleCombine(workflow, step, fn)`: `fn` receives the context with
each branch's result staged under the branch's name, and must return the **complete** post-join
context — the engine replaces the context with it, so keys the handler omits do not survive the
join (there is no implicit union of the arms).

```go
worker.HandleCombine("order", "merge", func(ctx wiggle.Context) (wiggle.Context, error) {
	out := wiggle.Context{}
	for k, v := range ctx { out[k] = v }          // carry the pre-fork context explicitly
	delete(out, "payment"); delete(out, "shipping") // drop the staged arm keys
	if p, ok := ctx["payment"].(map[string]any); ok {
		for k, v := range p { out[k] = v }         // fold what the payment arm produced
	}
	if s, ok := ctx["shipping"].(map[string]any); ok {
		for k, v := range s { out[k] = v }
	}
	return out, nil
})
```

Per-step: `Queue` (defaults to `DefaultQueue`, else the workflow name) and `Retry`
(`RetryExponential/RetryFixed/RetryNone/RetryForever`). `Branch{Name, Steps}` and `Case{When, Then}`
are themselves declarative slices of `Node`, so branches and bodies nest arbitrarily.

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

### A struct of handlers: `RegisterHandlers`

Instead of one `Handle(...)` call per step, hand the worker a struct whose methods *are* the steps —
matched by name, with the **Go signature picking the kind**:

```go
type OrderHandlers struct{}

func (OrderHandlers) Validate(o wiggle.Context) (wiggle.Context, error) { // -> a task
	o["status"] = "VALIDATED"
	return o, nil
}
func (OrderHandlers) InStock(o wiggle.Context) (bool, error) {           // -> a gate; matches step "in-stock"
	return o["quantity"].(float64) > 0, nil
}
func (OrderHandlers) Notify(o wiggle.Context) error {                     // -> a side effect
	fmt.Println("shipped", o["orderId"]); return nil
}

worker := wiggle.NewWorker(client, "orders").RegisterHandlers("order", OrderHandlers{})
worker.Start(ctx)
```

Each exported method shaped like a handler — `func(Context)(Context,error)` (task),
`func(Context)(bool,error)` (gate), or `func(Context)error` (effect) — is matched to a step of the
named workflow **by case-insensitive name** (`InStock` ↔ `in-stock`) on `Start`. The graph confirms
the exact name and the kind, so a signature that contradicts it (a task method for a gate step) fails
fast. Two method names that collide under case-folding panic at `RegisterHandlers`; methods of any
other shape are ignored, so helpers can live on the struct. Pass a pointer (`&OrderHandlers{}`) if your
methods use pointer receivers.

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
go test ./...                                   # offline: topology graph shapes + conversions
WIGGLE_TEST_URL=localhost:8080 go test -run Integration ./...   # end-to-end against a server
```

## Notes

- **Numbers** arrive as `float64` (Go's `encoding/json` convention), so assert `o["qty"].(float64)`.
- **Execution mode is SERVER only** (no local step chaining); interop with Java/Python workers is by
  activity name, so the Go content-hash version need not equal theirs.
- **Regenerating stubs:** `./codegen.sh` (needs `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc`).