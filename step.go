package wiggle

import (
	"fmt"
	"sync"
)

// StepRun is the running step's identity, handed to a StepActivity, through which a handler emits
// domain events. Valid only while the handler runs.
type StepRun struct {
	attempt    int32
	name       string
	instanceID string

	mu      sync.Mutex
	emitted []EmittedEvent
}

// StepActivity is an Activity that also receives the running StepRun.
type StepActivity func(s *StepRun, ctx Context) (Context, error)

// Attempt is the engine-global attempt number: 1 on the first try, incremented on every retry.
func (s *StepRun) Attempt() int32 { return s.attempt }

// Name is the running step's name.
func (s *StepRun) Name() string { return s.name }

// InstanceID is the workflow instance the running step belongs to.
func (s *StepRun) InstanceID() string { return s.instanceID }

// Emit buffers a domain event onto the instance's event log. It rides the step's report, and the
// server appends it in the transaction that settles the step: committed if and only if this
// attempt completes. An attempt that fails leaves nothing behind, and its retry emits fresh.
// eventType names the fact (the "wf." prefix is reserved for the engine).
func (s *StepRun) Emit(eventType string, payload map[string]any) error {
	if eventType == "" {
		return fmt.Errorf("an event needs a type")
	}
	if payload == nil {
		return fmt.Errorf("event %q carries no payload; an event's payload is a JSON object", eventType)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.emitted = append(s.emitted, EmittedEvent{Type: eventType, Payload: payload})
	return nil
}

func (s *StepRun) drain() []EmittedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.emitted
	s.emitted = nil
	return out
}

func newStep(t *Task) *StepRun {
	return &StepRun{attempt: t.Attempt, name: t.StepName, instanceID: t.InstanceID}
}
