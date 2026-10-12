package wiggle

import (
	"context"
	"time"

	"github.com/hadielmougy/wiggle-go/internal/pb"
)

// EventView is one entry of the event log: a lifecycle transition (wf.*) or a handler-emitted
// event. (Shard, Seq) identifies it; Cursor is the consumer's place just after it, for AckEvents.
type EventView struct {
	Seq           int64
	Shard         int32
	InstanceID    string
	Workflow      string
	Version       int32
	CorrelationID string
	Type          string
	CreatedAt     int64
	Payload       map[string]any
	NodeID        string // the step that emitted it; empty for a lifecycle entry
	Cursor        string
}

// EventBatch is one PollEvents answer, plus a backpressure hold-off hint.
type EventBatch struct {
	Events           []EventView
	RetryAfterMillis int64
}

// PollEvents long-polls the event log through the named cursor consumer. The first poll registers
// it at startFrom (0 = tail, -1 = earliest retained, else a seq). Events are delivered again until
// AckEvents moves the cursor past them.
func (c *Client) PollEvents(ctx context.Context, consumer string, max int, wait time.Duration, startFrom int64) (*EventBatch, error) {
	res, err := c.rpc.PollEvents(ctx, &pb.PollEventsRequest{
		Consumer: consumer, Max: int32(max), WaitMillis: wait.Milliseconds(), StartFrom: startFrom,
	})
	if err != nil {
		return nil, err
	}
	out := &EventBatch{RetryAfterMillis: res.GetRetryAfterMillis()}
	for _, e := range res.GetEvents() {
		out.Events = append(out.Events, EventView{
			Seq: e.GetSeq(), Shard: e.GetShard(), InstanceID: e.GetInstanceId(), Workflow: e.GetWorkflow(),
			Version: e.GetVersion(), CorrelationID: e.GetCorrelationId(), Type: e.GetType(),
			CreatedAt: e.GetCreatedAt(), Payload: asMap(fromValue(e.GetPayload())), NodeID: e.GetNodeId(),
			Cursor: e.GetCursor(),
		})
	}
	return out, nil
}

// AckEvents acknowledges every event up to and including the one whose cursor is given.
func (c *Client) AckEvents(ctx context.Context, consumer, cursor string) error {
	_, err := c.rpc.AckEvents(ctx, &pb.AckEventsRequest{Consumer: consumer, AckedCursor: cursor})
	return err
}

// SearchQuery filters SearchInstances. Text's every word must occur in the instance's correlation
// id, context, termination reason or error; Semantic ranks by meaning instead.
type SearchQuery struct {
	Text                   string
	Workflow, Status       string // "" = any
	UpdatedFrom, UpdatedTo int64  // epoch millis; 0 = open
	Limit                  int32
	PartialOK, Semantic    bool
}

// SearchHit is one matching instance.
type SearchHit struct {
	InstanceID, Workflow string
	Version              int32
	Status               string
	CorrelationID        string
	CreatedAt, UpdatedAt int64
	Score                float64
	Purged               bool
}

// SearchResult is a search's hits; Partial says a search shard did not answer.
type SearchResult struct {
	Hits    []SearchHit
	Partial bool
	Model   string // the embedding model a semantic search used
}

// SearchInstances searches instances across every search shard. FAILED_PRECONDITION when search is
// not enabled on the server.
func (c *Client) SearchInstances(ctx context.Context, q SearchQuery) (*SearchResult, error) {
	req := &pb.SearchRequest{Text: q.Text, UpdatedFrom: q.UpdatedFrom, UpdatedTo: q.UpdatedTo, Limit: q.Limit,
		PartialOk: q.PartialOK, Semantic: q.Semantic}
	if q.Workflow != "" {
		req.Workflow = &q.Workflow
	}
	if q.Status != "" {
		req.Status = &q.Status
	}
	res, err := c.rpc.SearchInstances(ctx, req)
	if err != nil {
		return nil, err
	}
	out := &SearchResult{Partial: res.GetPartial(), Model: res.GetModel()}
	for _, h := range res.GetHits() {
		out.Hits = append(out.Hits, SearchHit{InstanceID: h.GetInstanceId(), Workflow: h.GetWorkflow(),
			Version: h.GetVersion(), Status: h.GetStatus(), CorrelationID: h.GetCorrelationId(),
			CreatedAt: h.GetCreatedAt(), UpdatedAt: h.GetUpdatedAt(), Score: h.GetScore(), Purged: h.GetPurged()})
	}
	return out, nil
}

// NodeStats is one step's durations and queue waits over recently settled steps.
type NodeStats struct {
	NodeID, Name                    string
	Count                           int64
	MeanMillis                      float64
	P50Millis, P95Millis, MaxMillis int64
	WaitP50Millis, WaitP95Millis    int64
}

// StepStats is per-step statistics for one workflow version.
type StepStats struct {
	Workflow string
	Version  int32
	Nodes    []NodeStats
}

// StepStats returns per-step statistics for a workflow version (0 = the latest).
func (c *Client) StepStats(ctx context.Context, workflow string, version int32) (*StepStats, error) {
	res, err := c.rpc.GetStepStats(ctx, &pb.StepStatsRequest{Workflow: workflow, Version: version})
	if err != nil {
		return nil, err
	}
	out := &StepStats{Workflow: res.GetWorkflow(), Version: res.GetVersion()}
	for _, n := range res.GetNodes() {
		out.Nodes = append(out.Nodes, NodeStats{NodeID: n.GetNodeId(), Name: n.GetName(), Count: n.GetCount(),
			MeanMillis: n.GetMeanMillis(), P50Millis: n.GetP50Millis(), P95Millis: n.GetP95Millis(),
			MaxMillis: n.GetMaxMillis(), WaitP50Millis: n.GetWaitP50Millis(), WaitP95Millis: n.GetWaitP95Millis()})
	}
	return out, nil
}

// BacklogSlice is the dispatchable work of one (workflow, version, queue), and whether a live worker
// polling this node would claim it: Coverage is COVERED, CONDITIONAL or UNCOVERED.
type BacklogSlice struct {
	Workflow          string
	Version           int32
	Queue             string
	ReadyCount        int32
	OldestAvailableAt int64
	Covered           bool
	Coverage          string
	Claimable         *int32 // nil for a conditional slice too large to count
	Criteria          []string
}

// BacklogCoverage returns the dispatchable backlog by slice, and how many pollers are live.
func (c *Client) BacklogCoverage(ctx context.Context, max int32) ([]BacklogSlice, int32, error) {
	res, err := c.rpc.GetBacklogCoverage(ctx, &pb.BacklogCoverageRequest{Max: max})
	if err != nil {
		return nil, 0, err
	}
	var out []BacklogSlice
	for _, b := range res.GetSlices() {
		out = append(out, BacklogSlice{Workflow: b.GetWorkflow(), Version: b.GetVersion(), Queue: b.GetQueue(),
			ReadyCount: b.GetReadyCount(), OldestAvailableAt: b.GetOldestAvailableAt(), Covered: b.GetCovered(),
			Coverage: b.GetCoverage(), Claimable: b.Claimable, Criteria: b.GetCriteria()})
	}
	return out, res.GetLivePollers(), nil
}
