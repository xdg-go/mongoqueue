package mongoqueue_test

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/xdg-go/mongoqueue"
)

// Run Reconcile on a ticker for the life of the queue. A failed call is
// logged and retried on the next tick.
func ExampleQueue_Reconcile() {
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatal(err)
	}
	q := mongoqueue.New(client.Database("app").Collection("jobs"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := q.Reconcile(ctx); err != nil {
					log.Printf("reconcile: %v", err)
				}
			}
		}
	}()

	// ... enqueue and claim jobs until shutdown, then cancel ctx ...
}
