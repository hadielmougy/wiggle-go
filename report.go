package wiggle

import (
	"context"
	"fmt"

	"github.com/hadielmougy/wiggle-go/internal/pb"
)

// StepReport is one finished step as a worker reports it: a task's Merge (its complete next
// context; nil leaves the context unchanged) or a predicate's Predicate, with when it ran (epoch
// millis; 0 = not reported) and the events it emitted.
type StepReport struct {
	NodeID     string
	Merge      any
	Predicate  *bool
	StartedAt  int64
	FinishedAt int64
	Events     []EmittedEvent
}

// EmittedEvent is a domain event a handler emitted while its step ran.
type EmittedEvent struct {
	Type    string
	Payload map[string]any
}

// RunApplied is what a reported run applied to. Any status but RUNNING means stop here.
type RunApplied struct {
	InstanceStatus string
	LeaseExpiresAt int64
	NextTaskID     string
}

// ReportRefusedError is the server refusing a reported run; the run wrote nothing. Status is
// HTTP-style (404, 409, ...).
type ReportRefusedError struct {
	TaskID  string
	Status  int32
	Message string
}

func (e *ReportRefusedError) Error() string {
	return fmt.Sprintf("report of task %s refused (%d): %s", e.TaskID, e.Status, e.Message)
}

// ReportSteps reports the finished steps of one leased task. final says this worker takes no
// continuation. A refused run returns a *ReportRefusedError.
func (c *Client) ReportSteps(ctx context.Context, taskID, leaseOwner string, steps []StepReport, final bool) (*RunApplied, error) {
	run := &pb.ReportedRun{TaskId: taskID, LeaseOwner: leaseOwner, Final: final}
	for _, s := range steps {
		r, err := stepResult(s)
		if err != nil {
			return nil, err
		}
		run.Steps = append(run.Steps, r)
	}
	res, err := c.rpc.ReportSteps(ctx, &pb.ReportStepsRequest{Runs: []*pb.ReportedRun{run}}, callReady)
	if err != nil {
		return nil, err
	}
	for _, r := range res.GetResults() {
		if r.GetTaskId() != taskID {
			continue
		}
		if r.GetErrorStatus() != 0 || r.GetApplied() == nil {
			return nil, &ReportRefusedError{TaskID: taskID, Status: r.GetErrorStatus(), Message: r.GetError()}
		}
		a := r.GetApplied()
		return &RunApplied{InstanceStatus: a.GetInstanceStatus(), LeaseExpiresAt: a.GetLeaseExpiresAt(),
			NextTaskID: a.GetNextTaskId()}, nil
	}
	return nil, &ReportRefusedError{TaskID: taskID, Status: 500, Message: "no answer for this run"}
}

func stepResult(s StepReport) (*pb.StepResult, error) {
	r := &pb.StepResult{NodeId: s.NodeID, StartedAt: s.StartedAt, FinishedAt: s.FinishedAt}
	for _, e := range s.Events {
		payload, err := toValue(e.Payload)
		if err != nil {
			return nil, fmt.Errorf("encode event %q: %w", e.Type, err)
		}
		r.Events = append(r.Events, &pb.EmittedEvent{Type: e.Type, Payload: payload})
	}
	switch {
	case s.Predicate != nil:
		r.Outcome = &pb.StepResult_PredicateValue{PredicateValue: *s.Predicate}
	case s.Merge != nil:
		val, err := toValue(s.Merge)
		if err != nil {
			return nil, fmt.Errorf("encode result: %w", err)
		}
		r.Outcome = &pb.StepResult_Merge{Merge: val}
	}
	return r, nil
}
