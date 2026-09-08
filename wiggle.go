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

// Permanent builds a non-retryable failure from a formatted message.
func Permanent(format string, args ...any) *PermanentError {
	return &PermanentError{Msg: fmt.Sprintf(format, args...)}
}
