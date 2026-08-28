// Command order is an end-to-end example: define a workflow, register it, run a worker, submit an
// instance, and await completion.
//
//	# start a server first (from the wiggle engine repo): docker run … hadielmougy/wiggle
//	WIGGLE_URL=localhost:8080 go run ./examples/order
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	wiggle "github.com/hadielmougy/wiggle-go"
)

func main() {
	url := os.Getenv("WIGGLE_URL")
	if url == "" {
		url = "localhost:8080"
	}
	client, err := wiggle.Dial(url)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()

	wf := wiggle.Define("go-order").
		Step("validate", func(o wiggle.Context) (wiggle.Context, error) {
			o["status"] = "VALIDATED"
			return o, nil
		}).
		Gate("in-stock", func(o wiggle.Context) (bool, error) {
			return o["quantity"].(float64) > 0, nil
		}).
		Step("charge", func(o wiggle.Context) (wiggle.Context, error) {
			o["paymentRef"] = fmt.Sprintf("auth-%v", o["orderId"])
			return o, nil
		}, wiggle.WithQueue("payments"), wiggle.WithRetry(wiggle.RetryExponential(5, 100*time.Millisecond))).
		Effect("notify", func(o wiggle.Context) error {
			fmt.Printf("   [worker] notified %v (payment %v)\n", o["orderId"], o["paymentRef"])
			return nil
		}).
		Build()

	version, err := client.Register(ctx, wf)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("registered %s v%d\n", wf.Name, version)

	worker := wiggle.NewWorker(client, "go-worker-1").Register(wf)
	if err := worker.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer worker.Stop()

	id, err := client.Start(ctx, wf, wiggle.Context{"orderId": "A-1001", "quantity": 2.0})
	if err != nil {
		log.Fatal(err)
	}
	view, err := client.AwaitCompletion(ctx, id, 30*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[result] status=%s context=%v\n", view.Status, view.Context)
}
