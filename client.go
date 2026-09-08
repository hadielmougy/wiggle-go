package wiggle

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hadielmougy/wiggle-go/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TERMINAL are the instance statuses that end a run.
var terminal = map[string]bool{"COMPLETED": true, "FAILED": true, "CANCELLED": true}

// callReady makes the worker-critical RPCs (register, get-workflow, poll, complete, fail, heartbeat)
// wait for the server to become reachable instead of failing fast with UNAVAILABLE -- so a worker
// rides out a cold start, a rolling restart, or a reschedule without crashing. Query calls stay
// fail-fast.
var callReady = grpc.WaitForReady(true)

// Client is the control-plane connection: register workflows, start and track instances, deliver
// signals, manage schedules -- plus the low-level worker RPCs used by Worker.
type Client struct {
	conn *grpc.ClientConn
	rpc  pb.WiggleControlPlaneClient
}

// Dial connects to a Wiggle server at target (host:port). With no options the channel is plaintext;
// pass grpc.WithTransportCredentials(credentials.NewTLS(cfg)) for TLS/mTLS.
func Dial(target string, opts ...grpc.DialOption) (*Client, error) {
	dialOpts := []grpc.DialOption{grpc.WithChainUnaryInterceptor(logErrorInterceptor)}
	if len(opts) == 0 {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	dialOpts = append(dialOpts, opts...)
	conn, err := grpc.NewClient(stripScheme(target), dialOpts...)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, rpc: pb.NewWiggleControlPlaneClient(conn)}, nil
}

// debugEnabled surfaces the "expected" RPC failures -- NOT_FOUND and UNAVAILABLE, e.g. reconcile
// waiting for a workflow to be registered, or a server not yet up -- only when WIGGLE_DEBUG is set.
// Go's standard log has no levels, so this env var stands in for a debug level.
var debugEnabled = os.Getenv("WIGGLE_DEBUG") != ""

// logErrorInterceptor logs every failed unary RPC in one place: genuine errors always, the expected
// codes only under WIGGLE_DEBUG, and CANCELLED (worker shutdown) never.
func logErrorInterceptor(ctx context.Context, method string, req, reply any,
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		log.Printf("wiggle: rpc %s failed: %v", lastSegment(method), err)
		return err
	}
	switch st.Code() {
	case codes.Canceled:
		// expected on worker shutdown
	case codes.NotFound, codes.Unavailable:
		if debugEnabled {
			log.Printf("wiggle: [debug] rpc %s: %s: %s", lastSegment(method), st.Code(), st.Message())
		}
	default:
		log.Printf("wiggle: rpc %s failed: %s: %s", lastSegment(method), st.Code(), st.Message())
	}
	return err
}

func lastSegment(method string) string {
	if i := strings.LastIndexByte(method, '/'); i >= 0 {
		return method[i+1:]
	}
	return method
}

// Close releases the underlying connection.
func (c *Client) Close() error { return c.conn.Close() }

func stripScheme(target string) string {
	if i := strings.Index(target, "://"); i >= 0 {
		return target[i+3:]
	}
	return target
}

// ---- workflows & instances ----

// Register registers a workflow definition and returns its version (idempotent for the same graph).
func (c *Client) Register(ctx context.Context, bp *Blueprint) (int, error) {
	s, err := toStruct(bp.Definition)
	if err != nil {
		return 0, fmt.Errorf("encode definition: %w", err)
	}
	res, err := c.rpc.RegisterWorkflow(ctx, &pb.WorkflowDefinition{Definition: s}, callReady)
	if err != nil {
		return 0, err
	}
	if v, err := strconv.Atoi(res.GetVersion()); err == nil {
		return v, nil
	}
	return bp.Version, nil
}

// GetWorkflow returns the registered graph for name as a JSON map -- the server's source of truth for
// a workflow's step names, kinds, and queues. Used by Worker reconciliation.
func (c *Client) GetWorkflow(ctx context.Context, name string) (map[string]any, error) {
	res, err := c.rpc.GetWorkflow(ctx, &pb.GetWorkflowRequest{Name: name}, callReady)
	if err != nil {
		return nil, err
	}
	return fromStruct(res.GetDefinition()), nil
}

// StartOption customizes Start.
type StartOption func(*pb.StartInstanceRequest)

// WithVersion pins the workflow version to start (else the latest registered).
func WithVersion(v int32) StartOption {
	return func(r *pb.StartInstanceRequest) { r.Version = &v }
}

// WithCorrelationID attaches a correlation id to the instance.
func WithCorrelationID(id string) StartOption {
	return func(r *pb.StartInstanceRequest) { r.CorrelationId = &id }
}

// Start starts an instance and returns its id. workflow may be a name or a *Blueprint.
func (c *Client) Start(ctx context.Context, workflow any, initial Context, opts ...StartOption) (string, error) {
	name, err := workflowName(workflow)
	if err != nil {
		return "", err
	}
	val, err := toValue(initial)
	if err != nil {
		return "", fmt.Errorf("encode context: %w", err)
	}
	req := &pb.StartInstanceRequest{Workflow: name, Context: val}
	for _, o := range opts {
		o(req)
	}
	res, err := c.rpc.StartInstance(ctx, req)
	if err != nil {
		return "", err
	}
	return res.GetInstanceId(), nil
}

func workflowName(workflow any) (string, error) {
	switch w := workflow.(type) {
	case string:
		return w, nil
	case *Blueprint:
		return w.Name, nil
	case Blueprint:
		return w.Name, nil
	default:
		return "", fmt.Errorf("workflow must be a name or *Blueprint, got %T", workflow)
	}
}

// InstanceView is a snapshot of a workflow instance.
type InstanceView struct {
	ID                string
	Workflow          string
	Version           int32
	Status            string
	TerminationReason string
	Error             string
	Context           Context
	CreatedAt         int64
	UpdatedAt         int64
}

// IsTerminal reports whether the instance has reached a final state.
func (v *InstanceView) IsTerminal() bool { return terminal[v.Status] }

// Instance fetches the current state of an instance.
func (c *Client) Instance(ctx context.Context, id string) (*InstanceView, error) {
	res, err := c.rpc.GetInstance(ctx, &pb.InstanceIdRequest{InstanceId: id})
	if err != nil {
		return nil, err
	}
	return instanceView(res.GetInstance()), nil
}

func instanceView(v *pb.InstanceView) *InstanceView {
	if v == nil {
		return nil
	}
	return &InstanceView{
		ID: v.GetId(), Workflow: v.GetWorkflow(), Version: v.GetVersion(), Status: v.GetStatus(),
		TerminationReason: v.GetTerminationReason(), Error: v.GetError(),
		Context: asMap(fromValue(v.GetContext())), CreatedAt: v.GetCreatedAt(), UpdatedAt: v.GetUpdatedAt(),
	}
}

// AwaitCompletion polls until the instance reaches a terminal state or the context/timeout expires.
func (c *Client) AwaitCompletion(ctx context.Context, id string, timeout time.Duration) (*InstanceView, error) {
	deadline := time.Now().Add(timeout)
	for {
		v, err := c.Instance(ctx, id)
		if err != nil {
			return nil, err
		}
		if v.IsTerminal() {
			return v, nil
		}
		if time.Now().After(deadline) {
			return v, fmt.Errorf("instance %s still %s after %s", id, v.Status, timeout)
		}
		select {
		case <-ctx.Done():
			return v, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// Cancel cancels a running instance.
func (c *Client) Cancel(ctx context.Context, id, reason string) error {
	_, err := c.rpc.CancelInstance(ctx, &pb.CancelInstanceRequest{InstanceId: id, Reason: reason})
	return err
}

// Signal delivers a named signal to an instance; the payload merges into its context.
func (c *Client) Signal(ctx context.Context, id, signal string, payload Context) error {
	val, err := toValue(payload)
	if err != nil {
		return err
	}
	_, err = c.rpc.SignalInstance(ctx, &pb.SignalRequest{InstanceId: id, Signal: signal, Payload: val})
	return err
}

// ListInstances lists instances, optionally filtered by workflow and/or status.
func (c *Client) ListInstances(ctx context.Context, workflow, status string, limit int32) ([]*InstanceView, error) {
	req := &pb.ListInstancesRequest{Limit: limit}
	if workflow != "" {
		req.Workflow = &workflow
	}
	if status != "" {
		req.Status = &status
	}
	res, err := c.rpc.ListInstances(ctx, req)
	if err != nil {
		return nil, err
	}
	out := make([]*InstanceView, 0, len(res.GetInstances()))
	for _, v := range res.GetInstances() {
		out = append(out, instanceView(v))
	}
	return out, nil
}

// Health returns the server's health status.
func (c *Client) Health(ctx context.Context) (status, node string, leader bool, err error) {
	res, e := c.rpc.HealthCheck(ctx, &pb.Empty{})
	if e != nil {
		return "", "", false, e
	}
	return res.GetStatus(), res.GetNode(), res.GetLeader(), nil
}

// ---- worker RPCs (used by Worker) ----

// Task is one unit of work handed to a worker. For a forEach item step (IsItem), RawContext is the
// ITEM's current value (any JSON value, scalars included) and BaseContext is the frozen pre-forEach
// context; Context is then RawContext's map form (empty for scalar items).
type Task struct {
	TaskID, InstanceID, Workflow string
	Version                      int32
	NodeID, StepName, Activity   string
	Kind                         string // TASK | PREDICATE
	Attempt                      int32
	LeaseExpiresAt               int64
	LeaseOwner                   string
	Context                      Context
	RawContext                   any
	IsItem                       bool
	BaseContext                  Context
	ItemIndex                    int64
	ItemMapKey                   string
	ExecutionMode                string
}

// PollResult is the outcome of a poll: leased tasks, plus a backpressure hold-off hint.
type PollResult struct {
	Tasks            []*Task
	RetryAfterMillis int64
}

// Poll leases up to max ready tasks from the given queues.
func (c *Client) Poll(ctx context.Context, workerID string, queues []string, max int, leaseMillis, waitMillis int64) (*PollResult, error) {
	res, err := c.rpc.PollTasks(ctx, &pb.PollRequest{
		WorkerId: workerID, Queues: queues, Max: int32(max), LeaseMillis: leaseMillis, WaitMillis: waitMillis,
	}, callReady)
	if err != nil {
		return nil, err
	}
	out := &PollResult{RetryAfterMillis: res.GetRetryAfterMillis()}
	for _, t := range res.GetTasks() {
		raw := fromValue(t.GetContext())
		out.Tasks = append(out.Tasks, &Task{
			TaskID: t.GetTaskId(), InstanceID: t.GetInstanceId(), Workflow: t.GetWorkflow(),
			Version: t.GetVersion(), NodeID: t.GetNodeId(), StepName: t.GetStepName(),
			Activity: t.GetActivity(), Kind: t.GetKind(), Attempt: t.GetAttempt(),
			LeaseExpiresAt: t.GetLeaseExpiresAt(), LeaseOwner: t.GetLeaseOwner(),
			Context: asMap(raw), RawContext: raw,
			IsItem:      t.GetBaseContext() != nil,
			BaseContext: asMap(fromValue(t.GetBaseContext())),
			ItemIndex:   t.GetItemIndex(), ItemMapKey: t.GetItemMapKey(),
			ExecutionMode: t.GetExecutionMode(),
		})
	}
	return out, nil
}

// Complete reports a finished task; result is merged into the context (a predicate sends its boolean
// as {"value": bool}).
func (c *Client) Complete(ctx context.Context, taskID, leaseOwner string, result any) error {
	val, err := toValue(result)
	if err != nil {
		return err
	}
	_, err = c.rpc.CompleteTask(ctx, &pb.TaskResultRequest{TaskId: taskID, LeaseOwner: leaseOwner, Result: val}, callReady)
	return err
}

// Fail reports a failed task; retryable=false skips the retry policy.
func (c *Client) Fail(ctx context.Context, taskID, leaseOwner, message string, retryable bool) error {
	_, err := c.rpc.FailTask(ctx, &pb.TaskFailureRequest{
		TaskId: taskID, LeaseOwner: leaseOwner, Message: message, Retryable: retryable,
	}, callReady)
	return err
}

// Heartbeat extends the lease on an in-flight task and returns the new expiry.
func (c *Client) Heartbeat(ctx context.Context, taskID, leaseOwner string, extendMillis int64) (int64, error) {
	res, err := c.rpc.HeartbeatTask(ctx, &pb.HeartbeatRequest{
		TaskId: taskID, LeaseOwner: leaseOwner, ExtendMillis: extendMillis,
	}, callReady)
	if err != nil {
		return 0, err
	}
	return res.GetLeaseExpiresAt(), nil
}
