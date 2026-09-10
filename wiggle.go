// Package wiggle is a Go client and worker for the Wiggle workflow engine. It speaks the same gRPC
// control plane as the Java and Python clients, so a Go worker interoperates with them on one server
// -- dispatch is by activity name ("<workflow>#<step>"), not by language.
//
// The context is a JSON object represented as a Go map[string]any (aliased as Context). Whole numbers
// arrive as float64, following Go's encoding/json convention.
package wiggle

import "fmt"

// Context is the instance context: a JSON object flowing through the steps.
type Context = map[string]any

// Activity runs a task step: it receives the context and returns the new context. The return is
// sent whole and REPLACES the previous context server-side (no diff, no merge; a nil return leaves
// it untouched). A fork/forEach combine step works the same way -- see Worker.HandleCombine.
type Activity func(Context) (Context, error)

// SideEffect runs a step for its side effect only; the context is left unchanged.
type SideEffect func(Context) error

// ItemActivity runs one step of a forEach body: base is the frozen pre-forEach context (read-only —
// items can never write it; only the combine's return reaches the shared context) and item is the
// element's current value (any JSON value, scalars included). The return replaces the item's value;
// nil leaves it untouched.
type ItemActivity func(base Context, item any) (any, error)

// Predicate evaluates a gate, a choose guard, or a do-while condition.
type Predicate func(Context) (bool, error)

// PermanentError, when returned from a handler, fails the step without retrying (a bad request, not
// a transient blip).
type PermanentError struct{ Msg string }

func (e *PermanentError) Error() string { return e.Msg }

// Compensation carries both context snapshots of the step being undone, captured by the engine at
// the step's completion — NOT read from the instance's latest context, which a later step may have
// replaced (a step's return replaces the context wholesale). Result is the primary snapshot for
// most undos (the step's own products — a payment ref — live there); Input serves
// restore-previous-value undos and undo-only data (an idempotency key derived from the input), so
// nothing has to be smuggled through the business context just to reach the compensator.
type Compensation struct {
	Input  Context // the context as the step received it
	Result Context // the context as the step left it — the post-step snapshot
}

// Compensator undoes a compensable step's external effect. It runs in the reverse pass after the
// instance fails, newest-completed first, as a real durable task with the normal retry machinery —
// and, like every handler, at-least-once: make it idempotent (refund by an idempotency key, not
// blindly). Returning a *PermanentError (or exhausting retries) parks the instance
// COMPENSATION_FAILED. Bind it with Worker.HandleCompensation, or on a RegisterHandlers struct as
// a method named Compensate<Step> with this shape.
type Compensator func(Compensation) error

// Permanent builds a non-retryable failure from a formatted message.
func Permanent(format string, args ...any) *PermanentError {
	return &PermanentError{Msg: fmt.Sprintf(format, args...)}
}
