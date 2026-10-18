package mongoqueue_test

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/xdg-go/mongoqueue"
)

// Resize is the body of a "resize" job: caller domain data, stored as a
// BSON subdocument.
type Resize struct {
	URL   string `bson:"url"`
	Width int    `bson:"width"`
}

// resizeKind pairs the "resize" kind with its body type at one site.
var resizeKind = mongoqueue.NewKind[Resize]("resize")

// resize stands in for the caller's work. Effects are at-least-once, so it
// must be idempotent.
func resize(_ context.Context, _ Resize) error { return nil }

// queue stands for a Queue set up as in Example.
var queue *mongoqueue.Queue

// A worker pool: enqueue a job, then run a fixed set of workers that claim,
// decode, work, and complete until interrupted.
func Example() {
	// Majority writes keep single resolution across failover.
	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://localhost:27017").
		SetWriteConcern(writeconcern.Majority()))
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	q := mongoqueue.New(client.Database("app").Collection("jobs"))
	if err := q.EnsureIndexes(ctx); err != nil {
		log.Fatal(err)
	}
	// Run q.Reconcile on a ticker for the life of the queue; see
	// ExampleQueue_Reconcile.

	// One binding pairs the kind name with its body type for both Enqueue
	// and Decode. Resize is a struct with bson tags.
	resizeKind := mongoqueue.NewKind[Resize]("resize")
	// The job id is the idempotency key: a duplicate is already enqueued.
	err = resizeKind.Enqueue(ctx, q, Resize{URL: "s3://img/1.png", Width: 640},
		mongoqueue.EnqueueOpts{JobID: "resize-1", TenantID: "acme"})
	if err != nil && !errors.Is(err, mongoqueue.ErrDuplicateJob) {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for ctx.Err() == nil {
				job, err := q.Claim(ctx, "", 30*time.Second)
				if err != nil {
					if !errors.Is(err, mongoqueue.ErrNoJob) {
						log.Printf("claim: %v", err)
					}
					select { // Back off when idle or failing.
					case <-ctx.Done():
					case <-time.After(time.Second):
					}
					continue
				}
				body, err := resizeKind.Decode(&job.Job)
				switch {
				case err != nil: // Poison: no retry will decode it.
					err = job.Complete(ctx, "undecodable")
				case resize(ctx, body) != nil:
					// On shutdown, skip Release: the lease expires instead.
					if ctx.Err() == nil {
						err = job.Release(ctx, time.Minute)
					}
				default:
					err = job.Complete(ctx, mongoqueue.ResolutionCompleted)
				}
				if err != nil {
					log.Printf("job %s: %v", job.ID, err)
				}
			}
		})
	}
	wg.Wait()
}

// Long work keeps its lease alive with a heartbeat goroutine and stops when
// the lease is lost, since another worker may then hold the job. queue
// and resizeKind are set up as in Example; resize is the caller's
// (idempotent) work.
func ExampleClaimedJob_Heartbeat() {
	ctx := context.Background()
	job, err := queue.Claim(ctx, "", 30*time.Second)
	if err != nil {
		return // see Example for ErrNoJob backoff
	}

	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(10 * time.Second) // well inside the lease
		defer ticker.Stop()
		for {
			select {
			case <-workCtx.Done():
				return
			case <-ticker.C:
				if err := job.Heartbeat(workCtx, 30*time.Second); err != nil {
					log.Printf("heartbeat %s: %v", job.ID, err)
					cancel() // ErrLeaseLost or not, stop working blind
					return
				}
			}
		}
	}()

	body, err := resizeKind.Decode(&job.Job)
	if err == nil {
		err = resize(workCtx, body)
	}
	cancel()
	<-done // no late heartbeat may race the Complete
	if err == nil {
		err = job.Complete(ctx, mongoqueue.ResolutionCompleted)
	}
	if err != nil {
		log.Printf("job %s: %v", job.ID, err)
	}
}

// A transient failure releases the job to be retried after a delay. The
// queue never caps retries, so the worker gives up itself once Attempts
// reaches its limit, recording a caller-defined resolution. queue and
// resizeKind are set up as in Example; resize is the caller's (idempotent)
// work.
func ExampleClaimedJob_Release() {
	ctx := context.Background()
	job, err := queue.Claim(ctx, "", 30*time.Second)
	if err != nil {
		return // see Example for ErrNoJob backoff
	}
	const maxAttempts = 5

	body, err := resizeKind.Decode(&job.Job)
	if err != nil { // Poison: no retry will decode it.
		err = job.Complete(ctx, "undecodable")
	} else if workErr := resize(ctx, body); workErr == nil {
		err = job.Complete(ctx, mongoqueue.ResolutionCompleted)
	} else if job.Attempts >= maxAttempts {
		log.Printf("job %s: giving up: %v", job.ID, workErr)
		err = job.Complete(ctx, "failed")
	} else {
		// Linear backoff by attempt; any policy works.
		err = job.Release(ctx, time.Duration(job.Attempts)*time.Minute)
	}
	if errors.Is(err, mongoqueue.ErrLeaseLost) {
		return // lease expired or job canceled; someone else owns it now
	}
	if err != nil {
		log.Printf("job %s: %v", job.ID, err)
	}
}

// Run Reconcile on a ticker for the life of the queue. A failed call is
// logged and retried on the next tick.
func ExampleQueue_Reconcile() {
	q := queue // set up as in Example

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
