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

// Activity runs a task step: it receives the context and returns the new context. Only the changed
// keys are sent back (the engine shallow-diffs and merges).
type Activity func(Context) (Context, error)

// SideEffect runs a step for its side effect only; the context is left unchanged.
type SideEffect func(Context) error

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
