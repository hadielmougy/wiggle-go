package wiggle_test

import (
	"bytes"
	"context"
	"log"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	wiggle "github.com/hadielmougy/wiggle-go"
	"github.com/hadielmougy/wiggle-go/internal/pb"
	"google.golang.org/grpc"
)

// The client interceptor logs any failed RPC in one place. Here GetWorkflow hits the fake server,
// which doesn't implement it (Unimplemented), so the interceptor must log the failure.
func TestInterceptorLogsRpcError(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterWiggleControlPlaneServer(srv, fakeControlPlane{})
	go srv.Serve(lis)
	defer srv.Stop()

	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	client, err := wiggle.Dial(lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.GetWorkflow(context.Background(), "nope"); err == nil {
		t.Fatal("expected GetWorkflow to fail")
	}
	if !strings.Contains(buf.String(), "GetWorkflow failed") {
		t.Fatalf("interceptor did not log the error; log = %q", buf.String())
	}
}

// fakeControlPlane answers just enough for the cold-start test: RegisterWorkflow.
type fakeControlPlane struct {
	pb.UnimplementedWiggleControlPlaneServer
}

func (fakeControlPlane) RegisterWorkflow(context.Context, *pb.WorkflowDefinition) (*pb.RegisterWorkflowResult, error) {
	return &pb.RegisterWorkflowResult{Name: "cold", Version: "1", Nodes: 1}, nil
}

// A worker-critical call issued before the server exists must WAIT for it (wait_for_ready) and then
// succeed once it comes up -- rather than failing fast with UNAVAILABLE. This guards the fix that
// lets workers ride out a cold start / rolling restart without crashing.
func TestColdStartRegisterWaitsForServer(t *testing.T) {
	// Reserve a free port, then release it so nothing is listening yet.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()

	client, err := wiggle.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	bp := wiggle.Graph{Name: "cold", Steps: []wiggle.Node{wiggle.Step{Name: "a"}}}.MustCompile()

	done := make(chan error, 1)
	go func() {
		_, err := client.Register(context.Background(), bp)
		done <- err
	}()

	// It must still be blocked while nothing is listening.
	select {
	case err := <-done:
		t.Fatalf("Register returned before the server was up: %v", err)
	case <-time.After(600 * time.Millisecond):
	}

	// Bring the server up on the same address.
	lis2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("could not bind %s: %v", addr, err)
	}
	srv := grpc.NewServer()
	pb.RegisterWiggleControlPlaneServer(srv, fakeControlPlane{})
	go srv.Serve(lis2)
	defer srv.Stop()

	// Now it should complete.
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Register failed after the server came up: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Register did not complete after the server came up")
	}
}
