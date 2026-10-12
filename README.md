# Wiggle — Go client

A Go client and worker for [Wiggle](https://github.com/hadielmougy/wiggle), the durable state-machine
platform. It speaks the same gRPC control plane as the Java and Python clients, so a Go worker
**interoperates** with them on one server — dispatch is by activity name (`"<workflow>#<step>"`), not
by language.

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

	// The topology is a chain of named steps, defined into a *FlowSpec you register. Handlers are
	// bound separately, by name (below).
	wf := wiggle.MustDefine("order", 1, func(f *wiggle.Flow) {
		f.ThenApply("validate").
			ThenFilter("in-stock").
			ThenApply("charge", wiggle.OnQueue("payments"), wiggle.WithRetry(wiggle.RetryExponential(5, 100*time.Millisecond))).
			ThenAccept("notify")
	})

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

A workflow is defined as a chain, in the Java client's vocabulary: `Define(name, version, body)`
runs `body` once on a `*Flow`, recording the steps it chains, and compiles them into a `*FlowSpec`
you register. It returns an error for an invalid topology; `MustDefine` panics instead, for a flow
known to be valid when it is written. Version `0` publishes under a hash of the graph's content.
Steps are only **named** here — a worker binds their handlers separately, by name (see below).

```go
wf := wiggle.MustDefine("order", 1, func(f *wiggle.Flow) {
	f.ThenApply("validate").
		ThenFilter("in-stock").
		ThenAllOf(
			func(a *wiggle.Flow) { a.ThenApply("charge", wiggle.OnQueue("payments")) },
			func(a *wiggle.Flow) { a.ThenApply("reserve").ThenApply("label") },
		).Combine("merge"). // mandatory: the arms rejoin at an explicit merge handler (HandleCombine)
		ThenOneOf(
			wiggle.When("vip", func(a *wiggle.Flow) { a.ThenApply("concierge") }),
			wiggle.Otherwise(func(a *wiggle.Flow) { a.ThenApply("thanks") }), // optional, and last
		).
		ThenAccept("notify")
})
```

| Method | Meaning |
|---|---|
| `ThenApply(name, opts…)` | a task run on a worker (`Handle`); its return becomes the context |
| `ThenApplyCompensable(name, opts…)` | a task whose undo runs in the reverse pass if the instance later fails (see [Compensation](#compensation-sagas)) |
| `ThenAccept(name, opts…)` | a side effect (`HandleEffect`); the context is unchanged |
| `ThenFilter(name, opts…)` | a gate (`HandleGate`); false ends the instance as `gated:<name>`, or ends a `ThenAllOf` arm at the join |
| `ThenAllOf(arms…).Combine(name)` | run the arms in parallel on isolated context copies, then rejoin at the **mandatory** combine (`HandleCombine`) — no implicit fold. Each arm is named after its last step, which must be a task or a gate |
| `ThenForEach(name, itemsKey, body).Combine(name)` | runtime fan-out: one **isolated** branch per element of the list (or map) at `itemsKey`. **The element IS the item's context** — body steps are bound with `HandleItem(base, item)` and their return replaces the item's value (scalars included). The **mandatory** combine receives every item's final value collected under `name` (a list, or a map keyed like the input) and returns the complete post-join context |
| `ThenOneOf(When(guard, arm)…, Otherwise(arm))` | exclusive choice: the first arm whose guard holds runs, else `Otherwise`; with no `Otherwise`, the flow goes on past it |
| `RepeatWhile(condition, maxIterations, body)` | run `body`, then repeat while the `condition` gate holds (the body runs at least once). It may hold at most `maxIterations` times (0 = the engine default, 10,000) before the instance fails |
| `ThenSubFlow(node, workflow)` | run another registered workflow as a child; its result merges back |
| `ThenSleep(d)` | a server-side timer; no worker is held |
| `ThenAwait(signal, timeout, escalation)` | wait for a signal (`Client.Signal`); on a positive `timeout`, fail — or run `escalation` and go on |
| `.Combine(name)` after `ThenApply`/`ThenAccept` | make that task a spawning step that creates branches at run time (see [below](#branches-created-at-run-time)) |

A fork's combine is bound with `HandleCombine(workflow, step, fn)`: `fn` receives the context with
each arm's result staged under the arm's name (its last step's), and must return the **complete** post-join
context — the engine replaces the context with it, so keys the handler omits do not survive the
join (there is no implicit union of the arms). The engine stages the arm results under reserved
keys; the worker hands them to `fn` under the arm names, and fails the step if an arm name is also
a key of the context itself (rename one of them).

```go
worker.HandleCombine("order", "merge", func(ctx wiggle.Context) (wiggle.Context, error) {
	out := wiggle.Context{}
	for k, v := range ctx { out[k] = v }          // carry the pre-fork context explicitly
	delete(out, "charge"); delete(out, "label")     // drop the staged arm keys
	if p, ok := ctx["charge"].(map[string]any); ok {
		for k, v := range p { out[k] = v }         // fold what the charge arm produced
	}
	if s, ok := ctx["label"].(map[string]any); ok {
		for k, v := range s { out[k] = v }
	}
	return out, nil
})
```

Every step takes `OnQueue(queue)` (default: `f.DefaultQueue(queue)`, else the workflow name) and
`WithRetry(policy)`, a `RetryPolicy` (`RetryExponential/RetryFixed/RetryNone/RetryForever`; default:
retry forever). Arms and bodies are `*Flow`s of their own, so they nest arbitrarily. A flow may also
declare `f.Attribute(name, type, path)` for [queue criteria](#queue-criteria) to test.

## Compensation (sagas)

A step chained with `ThenApplyCompensable` is undone when the instance later fails: the engine runs the
step's **compensator** in the reverse pass (newest-completed first) as a real durable task, handing
it the step's **input/result context snapshots** captured at completion — not the instance's latest
context, which a later step may have replaced. The instance settles `COMPENSATED` (or
`COMPENSATION_FAILED` if an undo exhausts its retries).

```go
w.Handle("order", "reserve", reserve).
    HandleCompensation("order", "reserve", func(c wiggle.Compensation) error {
        return releaseReservation(c.Result["reservationRef"].(string)) // idempotent!
    })
```

On a `RegisterHandler` struct the compensator is a method named `Compensate<Step>` with the shape
`func(wiggle.Compensation) error` (`CompensateReserve` undoes `reserve`). The pairing is checked
both ways at start: a compensable step served without its compensator — or a compensator targeting
a non-compensable step — refuses to bind. Compensators are at-least-once like every handler; make
them idempotent.

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
available step names) and discovers which queue each step polls. Pass `WithAwaitRegistration(d)` to ride
out a registration race.

| Binder | Handler shape | Binds |
|---|---|---|
| `Handle` | `func(Context) (Context, error)` | a task (`Step`) |
| `HandleStep` | `func(*StepRun, Context) (Context, error)` | a task that also reads its step metadata |
| `HandleGate` | `func(Context) (bool, error)` | a predicate (`Gate`, `Choose` guard, `DoWhile` condition) |
| `HandleEffect` | `func(Context) error` | a side effect (`Effect`) |
| `HandleCombine` / `HandleCombineStep` | as `Handle` / `HandleStep` | a fork, forEach or spawning step's combine |
| `HandleItem` | `func(base Context, item any) (any, error)` | a step of a `ForEach` body |
| `HandleCompensation` | `func(Compensation) error` | the undo of a compensable step |

A handler's error is retried under the step's `RetryPolicy`; return `wiggle.Permanent(format, args…)`
to fail the step without a retry.

### Worker options

`NewWorker(client, id, opts…)` takes:

| Option | Default | Meaning |
|---|---|---|
| `WithConcurrency(n)` | NumCPU | the most steps in flight |
| `WithLease(d)` | 30s | how long a claimed step may run before the server reclaims it |
| `WithQueues(names…)` | every queue it learns about | serve only these queues |
| `WithCriteria(c…)` | none | claim only the tasks [queue criteria](#queue-criteria) hold for; replaces `WithQueues` |
| `WithAwaitRegistration(d)` | 0 (fail fast) | wait up to `d` for a bound workflow's graph to be registered |

### A struct of handlers: `RegisterHandler`

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

worker := wiggle.NewWorker(client, "orders").RegisterHandler("order", OrderHandlers{})
worker.Start(ctx)
```

Each exported method shaped like a handler — `func(Context)(Context,error)` or
`func(*StepRun, Context)(Context,error)` (task),
`func(Context)(bool,error)` (gate), or `func(Context)error` (effect) — is matched to a step of the
named workflow **by case-insensitive name** (`InStock` ↔ `in-stock`) on `Start`. The graph confirms
the exact name and the kind, so a signature that contradicts it (a task method for a gate step) fails
fast. Two method names that collide under case-folding panic at `RegisterHandler`; methods of any
other shape are ignored, so helpers can live on the struct. Pass a pointer (`&OrderHandlers{}`) if your
methods use pointer receivers.

## Client API

`Dial(target, opts…)` (opts are `grpc.DialOption`; pass credentials for TLS). Methods take a
`context.Context`:

```
Register(ctx, *FlowSpec, opts…) (int, error)            // wiggle.Force() replaces a differing graph at the same version (server must allow it)
GetWorkflow(ctx, name) (map[string]any, error)
Start(ctx, workflowOrName, context, opts…) (id, error)   // WithVersion, WithCorrelationID
Instance(ctx, id) (*InstanceView, error)
AwaitCompletion(ctx, id, timeout) (*InstanceView, error)
ListInstances(ctx, workflow, status, limit)
Cancel(ctx, id, reason) / Signal(ctx, id, name, payload)
Health(ctx)
GetWorkflowVersion(ctx, name, version)
PollEvents(ctx, consumer, max, wait, startFrom) (*EventBatch, error)   // the durable event log
AckEvents(ctx, consumer, cursor)
Search(ctx, SearchQuery{...}) (*SearchResult, error)                   // needs search shards
StepStats(ctx, workflow, version) (*StepStats, error)                  // per-step duration and queue-wait percentiles
BacklogCoverage(ctx, max) ([]BacklogSlice, livePollers, error)
// worker RPCs: Poll / PollCriteria / ReportSteps / Fail / Heartbeat
```

## Step metadata and events

A handler bound with `HandleStep` (or a `RegisterHandler` method shaped
`func(*wiggle.StepRun, wiggle.Context) (wiggle.Context, error)`) also receives the running step:

```go
worker.HandleStep("order", "ship", func(s *wiggle.StepRun, o wiggle.Context) (wiggle.Context, error) {
	if err := s.Emit("order.shipped", map[string]any{"orderId": o["orderId"], "attempt": s.Attempt()}); err != nil {
		return nil, err
	}
	return o, nil
})
```

An emitted event rides the step's report and is committed in the same transaction as the step: an
attempt that fails leaves none behind, and its retry emits again. Consumers read emitted and
lifecycle (`wf.*`) events through `PollEvents` / `AckEvents`. The worker reports each step's start
and finish times, which feed `StepStats`.

## Branches created at run time

A task followed by `.Combine(name)` is a **spawning step**: its handler may open branches with
`StepRun.Create(input)` (or `CreateKeyed(key, input)`), each running its own chain of steps on an
isolated copy of `input`. The branches ride the step's report and commit with it. The combine
receives their results through `StepRun.Results()`: a `[]any` in creation order, or a
`map[string]any` by key.

```go
wf := wiggle.MustDefine("orders", 1, func(f *wiggle.Flow) {
	f.ThenApply("fulfil").Combine("summarise").ThenAccept("notify")
})

w := wiggle.NewWorker(client, "w1").
	BranchSteps("orders", "reserve", "ship"). // in no graph: bound on first dispatch
	HandleStep("orders", "fulfil", func(s *wiggle.StepRun, o wiggle.Context) (wiggle.Context, error) {
		for _, item := range o["items"].([]any) {
			s.Create(item).ThenApply("reserve").ThenApply("ship", wiggle.OnQueue("shipping"))
		}
		return o, nil
	}).
	Handle("orders", "reserve", reserve). // a HandleStep handler reads fulfil's return via s.Base()
	Handle("orders", "ship", ship).
	HandleCombineStep("orders", "summarise", func(s *wiggle.StepRun, o wiggle.Context) (wiggle.Context, error) {
		shipped, _ := s.Results()
		o["shipped"] = shipped
		return o, nil
	})
```

A `*CreatedBranch` offers `ThenApply`, `ThenApplyCompensable` (declares an undo), `ThenAccept`,
`ThenFilter` (a false gate ends the branch), `ThenSleep`, `ThenAwait(signal, timeout, escalation)`,
`ThenSubFlow(node, workflow)`, `ThenAllOf(arms...).Combine(name)`,
`RepeatWhile(condition, maxIterations, body)` and `ThenOneOf(When(guard, chain)..., Otherwise(chain))`.
Steps take `OnQueue` and `WithRetry` options; otherwise a step uses the creating step's queue and
retry policy. `.Combine(name)` after a branch task makes that task spawn too, so branches nest. A combine may
itself `Create`, which starts another round. A chain built wrongly, or `Create` outside a spawning
step or its combine, fails the step without a retry.

Branch-only steps are in no registered graph, so declare them with `Worker.BranchSteps`, or give a
`RegisterHandler` struct a `BranchSteps() []string` method. They are then bound by name the first
time one is dispatched. A branch step on a queue no graph mentions runs only on a worker that polls
that queue.

## Queue criteria

A worker can claim only some of a queue's tasks: a predicate over the task's workflow, version,
step, node, kind, attempt, correlation id, creation time, or **attributes** the workflow declares
from its context.

```go
wf := wiggle.MustDefine("orders", 1, func(f *wiggle.Flow) {
	f.Attribute("tier", "STRING", "customer.tier").
		ThenApply("charge", wiggle.OnQueue("payments"))
})

gold, err := criteria.New("payments", criteria.And(
	criteria.Equal(criteria.Attr("tier"), "gold"),
	criteria.LessThan(criteria.Attempt, 3)))
w := wiggle.NewWorker(client, "gold-1", wiggle.WithCriteria(gold)).Handle("orders", "charge", charge)
```

Package `criteria` (`github.com/hadielmougy/wiggle-go/criteria`) offers `Equal`, `NotEqual`, `In`,
`LessThan(OrEqualTo)`, `GreaterThan(OrEqualTo)`, `Between`, `Like`, `IsNull` and `IsNotNull`, combined
with `And`, `Or` and `Not`, over the task properties `Workflow`, `Version`, `Step`, `NodeID`, `Kind`,
`Attempt`, `CorrelationID`, `InstanceCreatedAt` and `Attr(name)`. A comparison on an absent value is
false. `criteria.New` (or `MustNew`) checks a predicate against the server's limits.
`Client.BacklogCoverage` shows ready work no live worker's criteria would claim.

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