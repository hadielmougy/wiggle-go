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

	base       Context
	hasBase    bool
	results    any
	hasResults bool
	creates    bool

	mu      sync.Mutex
	emitted []EmittedEvent
	created []*CreatedBranch
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

// Create opens a branch over input -- its whole, isolated context -- to chain steps onto. Like Emit,
// the branch is buffered and rides the step's report: it runs once the step completes, and an
// attempt that fails leaves nothing behind. Its result reaches the combine after the step, through
// StepRun.Results. Only a spawning step (Step{Combine: ...}), or its combine, may create branches;
// anywhere else the step fails.
func (s *StepRun) Create(input any) *CreatedBranch { return s.create("", false, input) }

// CreateKeyed is Create with a key: the combine then receives a map by key. Either every branch of
// one step has a key, or none does.
func (s *StepRun) CreateKeyed(key string, input any) *CreatedBranch {
	return s.create(key, true, input)
}

func (s *StepRun) create(key string, keyed bool, input any) *CreatedBranch {
	b := &CreatedBranch{input: input, key: key, keyed: keyed}
	if !s.creates {
		b.fail(fmt.Errorf("step %q cannot create branches: only a step followed directly by a combine "+
			"(Step{Combine: ...}), or that combine, may call Create", s.name))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.created = append(s.created, b)
	return b
}

// Base is the frozen base context, where one exists: inside a forEach item step the pre-forEach
// context, inside a created branch's step what the creating step returned, and inside a combine the
// context without the results it combines. ok is false anywhere else.
func (s *StepRun) Base() (base Context, ok bool) { return s.base, s.hasBase }

// Results is what a combine combines: for a spawning step's combine, the results of the branches
// the step created -- a []any in creation order, or a map[string]any by key -- which reach it only
// here; for a forEach's combine the collected results; for a fork's combine a map of each arm's
// result by arm name. ok is false outside a combine.
func (s *StepRun) Results() (results any, ok bool) { return s.results, s.hasResults }

func (s *StepRun) drain() ([]EmittedEvent, []*CreatedBranch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	events, created := s.emitted, s.created
	s.emitted, s.created = nil, nil
	for _, b := range created {
		if err := b.Err(); err != nil {
			return nil, nil, err
		}
	}
	return events, created, nil
}

func newStep(t *Task) *StepRun {
	return &StepRun{attempt: t.Attempt, name: t.StepName, instanceID: t.InstanceID}
}
