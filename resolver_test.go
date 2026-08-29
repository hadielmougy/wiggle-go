package wiggle

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/hadielmougy/wiggle-go/internal/pb"
	"google.golang.org/grpc"
)

// ---- id parsing ----

func TestParseID(t *testing.T) {
	p, ok := ParseID("acme.e7.s2.01H8ABC")
	if !ok || p.Namespace != "acme" || p.Epoch != 7 || p.Shard != 2 || p.Ulid != "01H8ABC" {
		t.Fatalf("parse = %+v ok=%v", p, ok)
	}
	if _, ok := ParseID("wfi_01h8abc"); ok {
		t.Fatalf("legacy id should not parse")
	}
	if !IsLegacyID("wfi_01h8abc") || IsLegacyID("ns.e0.s0.x") {
		t.Fatalf("IsLegacyID wrong")
	}
}

// ---- fake coordinator ----

type fakeCoord struct {
	pb.UnimplementedCellCoordinatorServer
	mu           sync.Mutex
	resolveCalls int
	target       string
}

func (f *fakeCoord) Resolve(_ context.Context, req *pb.ResolveRequest) (*pb.ResolveResponse, error) {
	f.mu.Lock()
	f.resolveCalls++
	f.mu.Unlock()
	return &pb.ResolveResponse{
		Namespace: req.GetNamespace(), Epoch: 0,
		Endpoint:   &pb.Endpoint{Target: f.target, TtlSeconds: 30},
		TtlSeconds: 30,
	}, nil
}

func (f *fakeCoord) ActiveCells(_ context.Context, _ *pb.ActiveCellsRequest) (*pb.ActiveCellsResponse, error) {
	return &pb.ActiveCellsResponse{
		Generation: 1, TtlSeconds: 30,
		Cells: []*pb.Endpoint{{Target: f.target}},
	}, nil
}

func startFakeCoord(t *testing.T) (*fakeCoord, string, func()) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	f := &fakeCoord{target: "cell-a:9"}
	pb.RegisterCellCoordinatorServer(srv, f)
	go srv.Serve(lis)
	return f, lis.Addr().String(), srv.Stop
}

func TestResolverCoordinatorMode(t *testing.T) {
	f, addr, stop := startFakeCoord(t)
	defer stop()
	r, err := NewCoordinatorResolver(addr, "eu-west")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx := context.Background()

	targets, err := r.ActiveCellTargets(ctx, "acme")
	if err != nil || len(targets) != 1 || targets[0] != "cell-a:9" {
		t.Fatalf("activeCells = %v, err=%v", targets, err)
	}

	if c, err := r.ClientForNamespace(ctx, "acme"); err != nil || c == nil {
		t.Fatalf("clientForNamespace: %v", err)
	}
	// TTL cache: a second resolve of the same namespace must not re-hit the coordinator.
	if _, err := r.ClientForNamespace(ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	if f.resolveCalls != 1 {
		t.Fatalf("expected 1 Resolve call (cached), got %d", f.resolveCalls)
	}

	// by epoch-aware id → resolves via the id's namespace (still cached)
	if _, err := r.ClientForInstance(ctx, "acme.e0.s0.01H8"); err != nil {
		t.Fatalf("clientForInstance(epoch-aware): %v", err)
	}
	// legacy id under a coordinator is rejected
	if _, err := r.ClientForInstance(ctx, "wfi_legacy"); err == nil {
		t.Fatalf("expected error routing a legacy id under a coordinator")
	}
}

func TestResolverDirectMode(t *testing.T) {
	r := NewDirectResolver("static:8080")
	defer r.Close()
	ctx := context.Background()

	targets, err := r.ActiveCellTargets(ctx, "ignored")
	if err != nil || len(targets) != 1 || targets[0] != "static:8080" {
		t.Fatalf("direct activeCells = %v, err=%v", targets, err)
	}
	// direct mode routes everything to the static target, legacy ids included
	if c, err := r.ClientForInstance(ctx, "wfi_legacy"); err != nil || c == nil {
		t.Fatalf("direct clientForInstance: %v", err)
	}
	if c, err := r.ClientForNamespace(ctx, "ignored"); err != nil || c == nil {
		t.Fatalf("direct clientForNamespace: %v", err)
	}
}
