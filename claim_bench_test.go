package mongoqueue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xdg-go/mongoqueue/internal/mongotest"
)

// seedBatchSize bounds each InsertMany so seeding large b.N stays well under
// the server's message size limit.
const seedBatchSize = 1000

// BenchmarkClaimComplete measures full dispatch cycles -- Claim then
// Complete -- with G concurrent workers on one partition. Complete is part of
// the cycle so resolved jobs leave the pending index range; a claim-only loop
// would scan a leased prefix that grows with b.N.
func BenchmarkClaimComplete(b *testing.B) {
	for _, g := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("workers=%d", g), func(b *testing.B) {
			benchClaimComplete(b, g)
		})
	}
}

func benchClaimComplete(b *testing.B, workers int) {
	ctx := context.Background()
	db := mongotest.Connect(b)
	q := New(db.Collection("jobs"))
	if err := q.EnsureIndexes(ctx); err != nil {
		b.Fatalf("EnsureIndexes: %v", err)
	}
	seedPendingJobs(ctx, b, q, b.N)

	// Open one connection before timing. The pool still ramps up to the
	// worker count inside the timed region; results include that ramp-up.
	if err := db.Client().Ping(ctx, nil); err != nil {
		b.Fatalf("warm-up ping: %v", err)
	}
	b.ResetTimer()

	// A ticket is taken before each Claim, so a worker only claims when a
	// job is guaranteed to remain; ErrNoJob is therefore a real failure.
	var (
		tickets   atomic.Int64
		completed atomic.Int64
		errOnce   sync.Once
		firstErr  error
		wg        sync.WaitGroup
	)
	recordErr := func(err error) { errOnce.Do(func() { firstErr = err }) }

	for range workers {
		wg.Go(func() {
			for {
				if tickets.Add(1) > int64(b.N) {
					return
				}
				cj, err := q.Claim(ctx, "", time.Minute)
				if err != nil {
					recordErr(fmt.Errorf("Claim: %w", err))
					return
				}
				if err := cj.Complete(ctx, ResolutionCompleted); err != nil {
					recordErr(fmt.Errorf("Complete: %w", err))
					return
				}
				completed.Add(1)
			}
		})
	}
	wg.Wait()
	b.StopTimer()

	if firstErr != nil {
		b.Fatal(firstErr)
	}
	if got := completed.Load(); got != int64(b.N) {
		b.Fatalf("completed %d cycles, want %d", got, b.N)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "cycles/s")
}

// seedPendingJobs inserts n claimable jobs directly, bypassing enqueue. Full
// Job structs are required: direct inserts skip enqueue's normalization, and
// claim depends on every field set here (string _id, the stored default
// partition, pending liveness, a numeric attempts for the claim's $add).
func seedPendingJobs(ctx context.Context, b *testing.B, q *Queue, n int) {
	b.Helper()
	visibleAt := time.Now().Add(-time.Hour)
	emptyBody, err := bson.Marshal(bson.D{})
	if err != nil {
		b.Fatalf("marshal empty body: %v", err)
	}

	batch := make([]any, 0, seedBatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if _, err := q.coll.InsertMany(ctx, batch); err != nil {
			b.Fatalf("seed InsertMany: %v", err)
		}
		batch = batch[:0]
	}
	for i := range n {
		batch = append(batch, Job{
			ID:        fmt.Sprintf("j-%07d", i),
			TenantID:  "bench",
			Partition: defaultPartition,
			Cost:      1,
			VStamp:    int64(i+1) * scale,
			Liveness:  LivenessPending,
			VisibleAt: visibleAt,
			Attempts:  0,
			Kind:      "bench",
			Body:      emptyBody,
		})
		if len(batch) == seedBatchSize {
			flush()
		}
	}
	flush()
}
