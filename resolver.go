package wiggle

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hadielmougy/wiggle-go/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// CellResolver routes calls to the cell that owns a namespace or instance, via the coordinator's
// Resolve / ActiveCells. Resolutions are cached by TTL and per-cell Clients are reused. With no
// coordinator it is a pass-through to a single static target, so existing usage is unchanged.
//
// Instance routing is directory-free: an instance id carries its namespace (see ParseID), so
// ClientForInstance resolves by namespace with no per-instance lookup. Mirror of the Java CellResolver.
type CellResolver struct {
	coordinatorURL string // "" = direct (no-coordinator) mode
	staticTarget   string
	callerRegion   string

	coordConn *grpc.ClientConn
	coord     pb.CellCoordinatorClient

	mu      sync.Mutex
	nsCache map[string]cachedEndpoint
	clients map[string]*Client
}

type cachedEndpoint struct {
	target string
	expiry time.Time
}

// NewCoordinatorResolver routes every call through the coordinator at coordinatorURL.
func NewCoordinatorResolver(coordinatorURL, callerRegion string) (*CellResolver, error) {
	conn, err := grpc.NewClient(stripScheme(coordinatorURL),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &CellResolver{
		coordinatorURL: coordinatorURL, callerRegion: callerRegion,
		coordConn: conn, coord: pb.NewCellCoordinatorClient(conn),
		nsCache: map[string]cachedEndpoint{}, clients: map[string]*Client{},
	}, nil
}

// NewDirectResolver sends every call to staticTarget (today's behaviour; no coordinator).
func NewDirectResolver(staticTarget string) *CellResolver {
	return &CellResolver{
		staticTarget: staticTarget,
		nsCache:      map[string]cachedEndpoint{}, clients: map[string]*Client{},
	}
}

// ClientForNamespace returns a client for the cell that hosts new instances of namespace.
func (r *CellResolver) ClientForNamespace(ctx context.Context, namespace string) (*Client, error) {
	t, err := r.resolveNamespace(ctx, namespace)
	if err != nil {
		return nil, err
	}
	return r.clientFor(t)
}

// ClientForInstance returns a client for the cell that owns instanceId (routed by its namespace).
func (r *CellResolver) ClientForInstance(ctx context.Context, instanceID string) (*Client, error) {
	if r.coordinatorURL == "" {
		return r.clientFor(r.staticTarget)
	}
	p, ok := ParseID(instanceID)
	if !ok {
		return nil, fmt.Errorf("cannot route a legacy instance id %q under a coordinator", instanceID)
	}
	t, err := r.resolveNamespace(ctx, p.Namespace)
	if err != nil {
		return nil, err
	}
	return r.clientFor(t)
}

// ActiveCellTargets returns the cells hosting live work for a namespace (a worker polls all of them).
func (r *CellResolver) ActiveCellTargets(ctx context.Context, namespace string) ([]string, error) {
	if r.coordinatorURL == "" {
		return []string{r.staticTarget}, nil
	}
	resp, err := r.coord.ActiveCells(ctx, &pb.ActiveCellsRequest{Namespace: namespace, CallerRegion: r.callerRegion})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(resp.GetCells()))
	for _, e := range resp.GetCells() {
		out = append(out, e.GetTarget())
	}
	return out, nil
}

// Invalidate drops a cached resolution -- call after a cell RPC fails with UNAVAILABLE/NOT_FOUND.
func (r *CellResolver) Invalidate(namespace string) {
	r.mu.Lock()
	delete(r.nsCache, namespace)
	r.mu.Unlock()
}

func (r *CellResolver) resolveNamespace(ctx context.Context, namespace string) (string, error) {
	if r.coordinatorURL == "" {
		return r.staticTarget, nil
	}
	r.mu.Lock()
	if c, ok := r.nsCache[namespace]; ok && time.Now().Before(c.expiry) {
		r.mu.Unlock()
		return c.target, nil
	}
	r.mu.Unlock()

	resp, err := r.coord.Resolve(ctx, &pb.ResolveRequest{
		By:           &pb.ResolveRequest_Namespace{Namespace: namespace},
		CallerRegion: r.callerRegion,
	})
	if err != nil {
		return "", err
	}
	target := resp.GetEndpoint().GetTarget()
	ttlSec := resp.GetEndpoint().GetTtlSeconds()
	if ttlSec < 1 {
		ttlSec = 1
	}
	r.mu.Lock()
	r.nsCache[namespace] = cachedEndpoint{target: target, expiry: time.Now().Add(time.Duration(ttlSec) * time.Second)}
	r.mu.Unlock()
	return target, nil
}

func (r *CellResolver) clientFor(target string) (*Client, error) {
	t := stripScheme(target)
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.clients[t]; ok {
		return c, nil
	}
	c, err := Dial(t)
	if err != nil {
		return nil, err
	}
	r.clients[t] = c
	return c, nil
}

// Close shuts the coordinator channel and every cached cell client.
func (r *CellResolver) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.clients {
		_ = c.Close()
	}
	r.clients = map[string]*Client{}
	if r.coordConn != nil {
		return r.coordConn.Close()
	}
	return nil
}
