package mongoqueue

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/xdg-go/mongoqueue/internal/mongotest"
)

// enqueueJob enqueues a minimal job as a claim fixture.
func enqueueJob(ctx context.Context, t *testing.T, q *Queue, opts EnqueueOpts) {
	t.Helper()
	if err := q.enqueue(ctx, "test", rawBody(t, bson.D{}), opts); err != nil {
		t.Fatalf("enqueue %q: %v", opts.JobID, err)
	}
}

// insertJob writes a job record directly, bypassing enqueue, for fixtures
// enqueue cannot produce (resolved jobs, explicit vstamps).
func insertJob(ctx context.Context, t *testing.T, q *Queue, job Job) {
	t.Helper()
	if job.Body == nil {
		job.Body = rawBody(t, bson.D{})
	}
	if _, err := q.coll.InsertOne(ctx, job); err != nil {
		t.Fatalf("insert job %q: %v", job.ID, err)
	}
}

// serverNow samples the server's $$NOW, so time assertions are bounded by
// server time rather than the client clock.
func serverNow(ctx context.Context, t *testing.T, db *mongo.Database) time.Time {
	t.Helper()
	cur, err := db.Aggregate(ctx, mongo.Pipeline{
		{{Key: "$documents", Value: bson.A{bson.D{{Key: "now", Value: "$$NOW"}}}}},
	})
	if err != nil {
		t.Fatalf("sample $$NOW: %v", err)
	}
	var docs []struct {
		Now time.Time `bson:"now"`
	}
	if err := cur.All(ctx, &docs); err != nil || len(docs) != 1 {
		t.Fatalf("decode $$NOW sample: %v (docs=%d)", err, len(docs))
	}
	return docs[0].Now
}

// mustClaim claims from partition with a one-minute lease, failing on error.
func mustClaim(ctx context.Context, t *testing.T, q *Queue, partition string) *ClaimedJob {
	t.Helper()
	cj, err := q.Claim(ctx, partition, time.Minute)
	if err != nil {
		t.Fatalf("claim in %q: %v", partition, err)
	}
	return cj
}

// assertNoJob asserts a claim in partition finds nothing and returns no handle.
func assertNoJob(ctx context.Context, t *testing.T, q *Queue, partition string) {
	t.Helper()
	cj, err := q.Claim(ctx, partition, time.Minute)
	if !errors.Is(err, ErrNoJob) || cj != nil {
		t.Fatalf("claim in %q = (%v, %v), want (nil, ErrNoJob)", partition, cj, err)
	}
}

// TestClaimRejectsNonPositiveLease proves lease <= 0 fails with
// ErrInvalidLease before any round trip: the queue has a nil collection, so
// any driver call would panic.
func TestClaimRejectsNonPositiveLease(t *testing.T) {
	t.Parallel()

	q := New(nil)
	for _, lease := range []time.Duration{0, -time.Nanosecond, -time.Minute} {
		cj, err := q.Claim(context.Background(), "", lease)
		if !errors.Is(err, ErrInvalidLease) || cj != nil {
			t.Errorf("Claim(lease=%v) = (%v, %v), want (nil, ErrInvalidLease)", lease, cj, err)
		}
	}
}

// TestClaimedJobHandleIgnoresExportedMutation proves the handle's guards read
// private copies: overwriting the promoted Job.ID and Job.ClaimID leaves the
// captured job id and fence unchanged.
func TestClaimedJobHandleIgnoresExportedMutation(t *testing.T) {
	t.Parallel()

	cj := &ClaimedJob{
		Job:     Job{ID: "j1", ClaimID: "fence"},
		jobID:   "j1",
		claimID: "fence",
	}
	cj.ID = "forged-id"
	cj.Job.ClaimID = "forged-fence"

	if cj.jobID != "j1" {
		t.Errorf("private jobID = %q after mutating Job.ID, want %q", cj.jobID, "j1")
	}
	if cj.claimID != "fence" || cj.ClaimID() != "fence" {
		t.Errorf("fence = %q / ClaimID() = %q after mutating Job.ClaimID, want %q", cj.claimID, cj.ClaimID(), "fence")
	}
}

// TestClaimOrdersByVStampThenID proves Claim takes the lowest (vstamp, _id):
// a cheaper job with a larger id beats a costlier one, and equal vstamps on
// different tenants break ties by _id regardless of enqueue order.
func TestClaimOrdersByVStampThenID(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	// Lower vstamp, larger id.
	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "a-costly", TenantID: "t1", Cost: 10})
	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "z-cheap", TenantID: "t2", Cost: 1})
	// Equal vstamps (first job, cost 1, weight 1) on fresh tenants; the
	// larger id is enqueued first.
	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "tie-b", TenantID: "t3", Cost: 1})
	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "tie-a", TenantID: "t4", Cost: 1})

	// z-cheap and both ties share vstamp stride(1,1); _id orders them.
	// Guard the premise so the tie-break cannot pass vacuously.
	tieA, tieB, cheap := findJob(ctx, t, q, "tie-a"), findJob(ctx, t, q, "tie-b"), findJob(ctx, t, q, "z-cheap")
	if tieA.VStamp != tieB.VStamp || tieA.VStamp != cheap.VStamp {
		t.Fatalf("tie premise broken: vstamps tie-a=%d tie-b=%d z-cheap=%d, want equal", tieA.VStamp, tieB.VStamp, cheap.VStamp)
	}

	want := []string{"tie-a", "tie-b", "z-cheap", "a-costly"}
	for _, id := range want {
		if got := mustClaim(ctx, t, q, "").ID; got != id {
			t.Fatalf("claim order: got %q, want %q (full order %v)", got, id, want)
		}
	}
	assertNoJob(ctx, t, q, "")
}

// TestClaimWritesLease proves the claim's stored effect: the minted fence,
// visible_at = server now + lease (bounded by $$NOW samples around the
// claim), attempts incremented to 1, vstamp unchanged, still pending. The
// handle's snapshot reflects the post-update record.
func TestClaimWritesLease(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "j1", TenantID: "t1", Cost: 3})
	before := findJob(ctx, t, q, "j1")

	const lease = 90 * time.Second
	lo := serverNow(ctx, t, db)
	cj, err := q.Claim(ctx, "", lease)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	hi := serverNow(ctx, t, db)

	stored := findJob(ctx, t, q, "j1")
	if stored.ClaimID != cj.ClaimID() || cj.Job.ClaimID != cj.ClaimID() {
		t.Errorf("stored claim_id = %q, snapshot %q, ClaimID() %q; want all equal", stored.ClaimID, cj.Job.ClaimID, cj.ClaimID())
	}
	if stored.VisibleAt.Before(lo.Add(lease)) || stored.VisibleAt.After(hi.Add(lease)) {
		t.Errorf("visible_at = %v, want within [%v, %v]", stored.VisibleAt, lo.Add(lease), hi.Add(lease))
	}
	if !cj.VisibleAt.Equal(stored.VisibleAt) {
		t.Errorf("snapshot visible_at = %v, stored %v", cj.VisibleAt, stored.VisibleAt)
	}
	if stored.Attempts != 1 || cj.Attempts != 1 {
		t.Errorf("attempts stored=%d snapshot=%d, want 1", stored.Attempts, cj.Attempts)
	}
	if stored.VStamp != before.VStamp {
		t.Errorf("vstamp changed: %d -> %d", before.VStamp, stored.VStamp)
	}
	if stored.Liveness != LivenessPending {
		t.Errorf("liveness = %q, want %q", stored.Liveness, LivenessPending)
	}
	if cj.jobID != "j1" {
		t.Errorf("handle jobID = %q, want j1", cj.jobID)
	}
}

// TestClaimSkipsInvisibleAndResolved proves a pending job whose visible_at is
// minutes in the future and resolved jobs are never claimed, even with lower
// vstamps than the visible pending job.
func TestClaimSkipsInvisibleAndResolved(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	past := time.Now().Add(-time.Hour)

	insertJob(ctx, t, q, Job{
		ID: "future", Partition: defaultPartition, Liveness: LivenessPending,
		VisibleAt: time.Now().Add(10 * time.Minute),
	})
	insertJob(ctx, t, q, Job{
		ID: "completed", Partition: defaultPartition, Liveness: LivenessResolved,
		Resolution: ResolutionCompleted, VisibleAt: past, ResolvedAt: past,
	})
	insertJob(ctx, t, q, Job{
		ID: "canceled", Partition: defaultPartition, Liveness: LivenessResolved,
		Resolution: ResolutionCanceled, VisibleAt: past, ResolvedAt: past,
	})
	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "visible", TenantID: "t1"})

	if got := mustClaim(ctx, t, q, "").ID; got != "visible" {
		t.Fatalf("claimed %q, want %q", got, "visible")
	}
	assertNoJob(ctx, t, q, "")
}

// TestClaimPartitionScoping proves an empty partition yields ErrNoJob and a
// claim in one partition never takes another partition's job.
func TestClaimPartitionScoping(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	assertNoJob(ctx, t, q, "A")
	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "b1", Partition: "B"})
	assertNoJob(ctx, t, q, "A")

	if got := mustClaim(ctx, t, q, "B").ID; got != "b1" {
		t.Fatalf("claim in B = %q, want b1", got)
	}
}

// TestRaceClaimPartitionScoping is the race counterpart of
// TestClaimPartitionScoping: more claimers than jobs race in each of two
// partitions, and no claim crosses a partition. Each partition yields
// exactly its own jobs, once each, and the surplus claimers get ErrNoJob.
func TestRaceClaimPartitionScoping(t *testing.T) {
	t.Parallel()

	const jobsPerPartition, claimersPerPartition = 5, 12
	partitions := []string{"p1", "p2"}
	db := mongotest.Connect(t)
	ctx := raceCtx(t)
	q := New(db.Collection("jobs"))
	for _, p := range partitions {
		for i := range jobsPerPartition {
			enqueueJob(ctx, t, q, EnqueueOpts{JobID: fmt.Sprintf("%s-%03d", p, i), TenantID: "t1", Partition: p})
		}
	}

	type result struct {
		claimer   string // partition the goroutine claimed from
		id        string
		partition string // partition of the returned job
		err       error
	}
	results := make(chan result, len(partitions)*claimersPerPartition)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, p := range partitions {
		for range claimersPerPartition {
			wg.Go(func() {
				<-start
				cj, err := q.Claim(ctx, p, time.Minute)
				if err != nil {
					results <- result{claimer: p, err: err}
					return
				}
				results <- result{claimer: p, id: cj.ID, partition: cj.Partition}
			})
		}
	}
	close(start)
	wg.Wait()
	close(results)

	seen := map[string]map[string]bool{}
	noJob := map[string]int{}
	for _, p := range partitions {
		seen[p] = map[string]bool{}
	}
	for r := range results {
		switch {
		case errors.Is(r.err, ErrNoJob):
			noJob[r.claimer]++
		case r.err != nil:
			t.Errorf("unexpected claim error in %s: %v", r.claimer, r.err)
		case r.partition != r.claimer:
			t.Errorf("claim in %s returned job %q from partition %q", r.claimer, r.id, r.partition)
		case seen[r.claimer][r.id]:
			t.Errorf("job %q claimed twice", r.id)
		default:
			seen[r.claimer][r.id] = true
		}
	}
	for _, p := range partitions {
		t.Logf("%s: successes = %d, ErrNoJob = %d", p, len(seen[p]), noJob[p])
		if len(seen[p]) != jobsPerPartition || noJob[p] != claimersPerPartition-jobsPerPartition {
			t.Errorf("%s: successes = %d, ErrNoJob = %d; want %d and %d",
				p, len(seen[p]), noJob[p], jobsPerPartition, claimersPerPartition-jobsPerPartition)
		}
	}
}

// TestClaimDefaultPartitionAliases proves "" and "default" both claim jobs
// enqueued with an empty Partition.
func TestClaimDefaultPartitionAliases(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	for _, partition := range []string{"", "default"} {
		enqueueJob(ctx, t, q, EnqueueOpts{JobID: "j-" + partition})
		if got := mustClaim(ctx, t, q, partition).ID; got != "j-"+partition {
			t.Errorf("claim in %q = %q, want %q", partition, got, "j-"+partition)
		}
	}
}

// TestClaimIDFormat smoke-tests the minted fence: 24 lowercase hex
// characters, distinct across claims.
func TestClaimIDFormat(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "j1"})
	enqueueJob(ctx, t, q, EnqueueOpts{JobID: "j2"})

	hex24 := regexp.MustCompile(`^[0-9a-f]{24}$`)
	first := mustClaim(ctx, t, q, "").ClaimID()
	second := mustClaim(ctx, t, q, "").ClaimID()
	for _, id := range []string{first, second} {
		if !hex24.MatchString(id) {
			t.Errorf("claim id %q is not 24 lowercase hex characters", id)
		}
	}
	if first == second {
		t.Errorf("two claims minted the same id %q", first)
	}
}

// TestClaimConcurrentMutualExclusion races more claimers than jobs: exactly
// M claims succeed with distinct job ids and the rest get ErrNoJob. The
// one-minute lease outlives the test, so no job can be legitimately
// reclaimed mid-race.
func TestClaimConcurrentMutualExclusion(t *testing.T) {
	t.Parallel()

	const numJobs, numClaimers = 5, 20
	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	for i := range numJobs {
		enqueueJob(ctx, t, q, EnqueueOpts{JobID: string(rune('a' + i)), TenantID: "t1"})
	}

	type result struct {
		id  string
		err error
	}
	results := make(chan result, numClaimers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range numClaimers {
		wg.Go(func() {
			<-start
			cj, err := q.Claim(ctx, "", time.Minute)
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{id: cj.ID}
		})
	}
	close(start)
	wg.Wait()
	close(results)

	seen := map[string]bool{}
	noJob := 0
	for r := range results {
		switch {
		case errors.Is(r.err, ErrNoJob):
			noJob++
		case r.err != nil:
			t.Errorf("unexpected claim error: %v", r.err)
		case seen[r.id]:
			t.Errorf("job %q claimed twice", r.id)
		default:
			seen[r.id] = true
		}
	}
	if len(seen) != numJobs || noJob != numClaimers-numJobs {
		t.Errorf("successes = %d, ErrNoJob = %d; want %d and %d", len(seen), noJob, numJobs, numClaimers-numJobs)
	}
}
