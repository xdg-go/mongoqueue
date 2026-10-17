package mongoqueue

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/xdg-go/mongoqueue/internal/mongotest"
)

// opCtx returns a bounded context for a single database operation, matching the
// 10s pattern the harness uses so a dead server fails a test promptly.
func opCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// indexInfo is the subset of an index specification the tests assert on. Key is
// held as bson.Raw so the tests can inspect its element order, not merely its
// contents. ExpireAfterSeconds is a pointer because it is absent on non-TTL
// indexes.
type indexInfo struct {
	Name                    string   `bson:"name"`
	Key                     bson.Raw `bson:"key"`
	ExpireAfterSeconds      *int32   `bson:"expireAfterSeconds"`
	PartialFilterExpression bson.Raw `bson:"partialFilterExpression"`
}

// listIndexes returns every index specification on coll.
func listIndexes(ctx context.Context, t *testing.T, coll *mongo.Collection) []indexInfo {
	t.Helper()
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	var got []indexInfo
	if err := cur.All(ctx, &got); err != nil {
		t.Fatalf("decode indexes: %v", err)
	}
	return got
}

// TestNewCreatesNoIndexes proves New performs no I/O and creates no index. A
// non-existent collection reports no indexes at all, so a dummy write is used
// to materialize the collection; the only index that then exists must be the
// automatic _id_ index.
func TestNewCreatesNoIndexes(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	coll := db.Collection("jobs")

	_ = New(coll)

	// Force the collection to materialize so index listing returns something.
	if _, err := coll.InsertOne(ctx, bson.M{"_id": "dummy"}); err != nil {
		t.Fatalf("insert dummy doc: %v", err)
	}

	idx := listIndexes(ctx, t, coll)
	if len(idx) != 1 {
		t.Fatalf("after New + one write, got %d indexes, want 1 (only _id_): %+v", len(idx), idx)
	}
	if idx[0].Name != "_id_" {
		t.Errorf("sole index name = %q, want %q", idx[0].Name, "_id_")
	}
}

// TestEnsureIndexesCreatesPartialIndexes asserts EnsureIndexes creates
// exactly the claim, seed, and clock indexes with keys in the specified order
// and their partial filters, and that a second call is a no-op returning no
// error.
func TestEnsureIndexesCreatesPartialIndexes(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	coll := db.Collection("jobs")
	q := New(coll)

	if err := q.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}

	// Assert field order, sort direction, and partial filter, so a regression
	// in any of them is caught.
	pending := bson.D{{Key: "liveness", Value: "pending"}}
	claimed := bson.D{{Key: "claim_id", Value: bson.D{{Key: "$gt", Value: ""}}}}
	want := map[string]struct{ key, partial bson.D }{
		claimIndexName: {
			key: bson.D{
				{Key: "partition", Value: int32(1)},
				{Key: "vstamp", Value: int32(1)},
				{Key: "_id", Value: int32(1)},
				{Key: "visible_at", Value: int32(1)},
			},
			partial: pending,
		},
		seedIndexName: {
			key: bson.D{
				{Key: "tenant", Value: int32(1)},
				{Key: "partition", Value: int32(1)},
				{Key: "vstamp", Value: int32(-1)},
			},
			partial: pending,
		},
		clockIndexName: {
			key: bson.D{
				{Key: "partition", Value: int32(1)},
				{Key: "vstamp", Value: int32(-1)},
			},
			partial: claimed,
		},
	}
	got := map[string]indexInfo{}
	for _, idx := range listIndexes(ctx, t, coll) {
		if idx.Name != "_id_" {
			got[idx.Name] = idx
		}
	}
	if len(got) != len(want) {
		t.Fatalf("non-_id_ indexes = %+v, want %d", got, len(want))
	}
	for name, w := range want {
		g, ok := got[name]
		if !ok {
			t.Errorf("index %q not found among %+v", name, got)
			continue
		}
		var key, partial bson.D
		if err := bson.Unmarshal(g.Key, &key); err != nil {
			t.Fatalf("decode %s key: %v", name, err)
		}
		if g.PartialFilterExpression == nil {
			t.Errorf("%s index has no partialFilterExpression", name)
			continue
		}
		if err := bson.Unmarshal(g.PartialFilterExpression, &partial); err != nil {
			t.Fatalf("decode %s partial filter: %v", name, err)
		}
		if !reflect.DeepEqual(key, w.key) {
			t.Errorf("%s key = %v, want %v", name, key, w.key)
		}
		if !reflect.DeepEqual(partial, w.partial) {
			t.Errorf("%s partialFilterExpression = %v, want %v", name, partial, w.partial)
		}
	}

	// Idempotent: an identical spec is a server-side no-op, so the full
	// listing (names, keys, partial filters) is unchanged by a second call.
	before := listIndexes(ctx, t, coll)
	if err := q.EnsureIndexes(ctx); err != nil {
		t.Fatalf("second EnsureIndexes returned error: %v", err)
	}
	after := listIndexes(ctx, t, coll)
	byName := func(list []indexInfo) map[string]indexInfo {
		m := map[string]indexInfo{}
		for _, idx := range list {
			m[idx.Name] = idx
		}
		return m
	}
	if b, a := byName(before), byName(after); !reflect.DeepEqual(a, b) {
		t.Errorf("indexes changed after second EnsureIndexes:\nbefore %+v\nafter  %+v", b, a)
	}
}

// TestEnsureTTLIndex asserts EnsureTTLIndex creates a TTL index on resolved_at
// named mongoqueue_ttl, partial on resolved jobs, whose expireAfterSeconds
// matches the supplied retention (24h == 86400s).
func TestEnsureTTLIndex(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	coll := db.Collection("jobs")
	q := New(coll)

	if err := q.EnsureTTLIndex(ctx, 24*time.Hour); err != nil {
		t.Fatalf("EnsureTTLIndex: %v", err)
	}

	// Assert name, key, retention, and partial filter, so a regression in
	// any of them is caught.
	var ttl *indexInfo
	all := listIndexes(ctx, t, coll)
	for i := range all {
		if all[i].Name == ttlIndexName {
			ttl = &all[i]
			break
		}
	}
	if ttl == nil {
		t.Fatalf("index %q not found among %+v", ttlIndexName, all)
	}
	var key, partial bson.D
	if err := bson.Unmarshal(ttl.Key, &key); err != nil {
		t.Fatalf("decode TTL key: %v", err)
	}
	if want := (bson.D{{Key: "resolved_at", Value: int32(1)}}); !reflect.DeepEqual(key, want) {
		t.Errorf("TTL key = %v, want %v", key, want)
	}
	if ttl.ExpireAfterSeconds == nil {
		t.Fatal("TTL index has no expireAfterSeconds")
	}
	if *ttl.ExpireAfterSeconds != 86400 {
		t.Errorf("expireAfterSeconds = %d, want 86400", *ttl.ExpireAfterSeconds)
	}
	if ttl.PartialFilterExpression == nil {
		t.Fatal("TTL index has no partialFilterExpression")
	}
	if err := bson.Unmarshal(ttl.PartialFilterExpression, &partial); err != nil {
		t.Fatalf("decode TTL partial filter: %v", err)
	}
	if want := (bson.D{{Key: "liveness", Value: "resolved"}}); !reflect.DeepEqual(partial, want) {
		t.Errorf("TTL partialFilterExpression = %v, want %v", partial, want)
	}
}

// TestResolvedAtOnlyOnResolvedJobs drives one job through each lifecycle state
// and asserts resolved_at is present exactly when liveness is resolved. The
// TTL index is partial on liveness: resolved, so a resolved job without
// resolved_at would never expire and a pending job with it would be a lie the
// index happens to ignore. Documents are read as bson.Raw so a zero time
// decoded into Job cannot mask a missing or spurious field.
func TestResolvedAtOnlyOnResolvedJobs(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	// Each job lives in its own partition, named by its id, so a claim
	// reaches only that job. Fixture helpers take the subtest t so a
	// failure stops the right goroutine.
	enqueue := func(t *testing.T, id string, delay time.Duration) {
		enqueueJob(ctx, t, q, EnqueueOpts{JobID: id, TenantID: "t1", Cost: 1, Partition: id, Delay: delay})
	}
	claim := func(t *testing.T, id string) *ClaimedJob {
		enqueue(t, id, 0)
		return mustClaim(ctx, t, q, id)
	}

	cases := []struct {
		id       string
		setup    func(t *testing.T, id string)
		resolved bool
	}{
		{"enqueued", func(t *testing.T, id string) { enqueue(t, id, 0) }, false},
		{"delayed", func(t *testing.T, id string) { enqueue(t, id, time.Hour) }, false},
		{"claimed", func(t *testing.T, id string) { claim(t, id) }, false},
		{"heartbeated", func(t *testing.T, id string) {
			if err := claim(t, id).Heartbeat(ctx, time.Hour); err != nil {
				t.Fatalf("heartbeat %q: %v", id, err)
			}
		}, false},
		{"released", func(t *testing.T, id string) {
			if err := claim(t, id).Release(ctx, 0); err != nil {
				t.Fatalf("release %q: %v", id, err)
			}
		}, false},
		{"lease-expired", func(t *testing.T, id string) { expireLease(ctx, t, q, claim(t, id)) }, false},
		{"completed", func(t *testing.T, id string) {
			if err := claim(t, id).Complete(ctx, ResolutionCompleted); err != nil {
				t.Fatalf("complete %q: %v", id, err)
			}
		}, true},
		{"canceled-unclaimed", func(t *testing.T, id string) { enqueue(t, id, 0); mustCancel(ctx, t, q, id) }, true},
		{"canceled-claimed", func(t *testing.T, id string) { claim(t, id); mustCancel(ctx, t, q, id) }, true},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			tc.setup(t, tc.id)

			var doc bson.Raw
			if err := q.coll.FindOne(ctx, bson.D{{Key: "_id", Value: tc.id}}).Decode(&doc); err != nil {
				t.Fatalf("find %q: %v", tc.id, err)
			}
			wantLiveness := "pending"
			if tc.resolved {
				wantLiveness = "resolved"
			}
			if got, ok := doc.Lookup("liveness").StringValueOK(); !ok || got != wantLiveness {
				t.Errorf("liveness = %v, want %q", doc.Lookup("liveness"), wantLiveness)
			}
			resolvedAt, err := doc.LookupErr("resolved_at")
			switch {
			case tc.resolved && err != nil:
				t.Errorf("resolved_at missing on resolved job: %v", err)
			case tc.resolved && resolvedAt.Type != bson.TypeDateTime:
				t.Errorf("resolved_at type = %v, want datetime", resolvedAt.Type)
			case !tc.resolved && err == nil:
				t.Errorf("resolved_at = %v on pending job, want absent", resolvedAt)
			}
		})
	}
}

// ttlExpireAfter returns the TTL index's expireAfterSeconds, failing if the
// index or the field is missing.
func ttlExpireAfter(ctx context.Context, t *testing.T, coll *mongo.Collection) int32 {
	t.Helper()
	for _, idx := range listIndexes(ctx, t, coll) {
		if idx.Name == ttlIndexName {
			if idx.ExpireAfterSeconds == nil {
				t.Fatal("TTL index has no expireAfterSeconds")
			}
			return *idx.ExpireAfterSeconds
		}
	}
	t.Fatalf("index %q not found", ttlIndexName)
	return 0
}

// TestEnsureTTLIndexRecall proves re-calling EnsureTTLIndex with the same
// retention is a no-op, and with a different retention returns the server's
// index-options conflict (code 85, wrapped) and leaves the retention unchanged.
func TestEnsureTTLIndexRecall(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	coll := db.Collection("jobs")
	q := New(coll)

	if err := q.EnsureTTLIndex(ctx, 24*time.Hour); err != nil {
		t.Fatalf("first EnsureTTLIndex: %v", err)
	}
	if err := q.EnsureTTLIndex(ctx, 24*time.Hour); err != nil {
		t.Fatalf("same-retention EnsureTTLIndex: %v", err)
	}
	if got := ttlExpireAfter(ctx, t, coll); got != 86400 {
		t.Fatalf("after same-retention re-call, expireAfterSeconds = %d, want 86400", got)
	}

	err := q.EnsureTTLIndex(ctx, 48*time.Hour)
	var cmdErr mongo.CommandError
	if !errors.As(err, &cmdErr) || cmdErr.Code != 85 {
		t.Fatalf("different-retention EnsureTTLIndex = %v, want wrapped CommandError code 85 (IndexOptionsConflict)", err)
	}
	if !strings.HasPrefix(err.Error(), "mongoqueue: ") {
		t.Errorf("different-retention EnsureTTLIndex error %q lacks %q prefix", err, "mongoqueue: ")
	}
	if got := ttlExpireAfter(ctx, t, coll); got != 86400 {
		t.Errorf("after conflicting re-call, expireAfterSeconds = %d, want 86400", got)
	}
}

// TestEnsureIndexesKeepsOtherIndexes proves EnsureIndexes leaves indexes it
// does not own in place: the earlier version's unnamed (driver auto-named)
// claim and seed indexes survive the call alongside the three new ones.
func TestEnsureIndexesKeepsOtherIndexes(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	coll := db.Collection("jobs")
	q := New(coll)

	// The earlier EnsureIndexes specs (before commit 5775cb4), created
	// without names exactly as that version did.
	oldKeys := []bson.D{
		{
			{Key: "partition", Value: int32(1)},
			{Key: "liveness", Value: int32(1)},
			{Key: "vstamp", Value: int32(1)},
			{Key: "_id", Value: int32(1)},
			{Key: "visible_at", Value: int32(1)},
			{Key: "claim_id", Value: int32(1)},
		},
		{
			{Key: "tenant", Value: int32(1)},
			{Key: "partition", Value: int32(1)},
			{Key: "liveness", Value: int32(1)},
			{Key: "vstamp", Value: int32(-1)},
		},
	}
	models := make([]mongo.IndexModel, len(oldKeys))
	for i, k := range oldKeys {
		models[i] = mongo.IndexModel{Keys: k}
	}
	oldNames, err := coll.Indexes().CreateMany(ctx, models)
	if err != nil {
		t.Fatalf("create earlier indexes: %v", err)
	}
	if err := q.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}

	got := map[string]indexInfo{}
	for _, idx := range listIndexes(ctx, t, coll) {
		got[idx.Name] = idx
	}
	for i, name := range oldNames {
		g, ok := got[name]
		if !ok {
			t.Errorf("earlier index %q missing after EnsureIndexes; have %+v", name, got)
			continue
		}
		var key bson.D
		if err := bson.Unmarshal(g.Key, &key); err != nil {
			t.Fatalf("decode %s key: %v", name, err)
		}
		if !reflect.DeepEqual(key, oldKeys[i]) {
			t.Errorf("earlier index %s key = %v, want %v", name, key, oldKeys[i])
		}
	}
	for _, want := range []string{claimIndexName, seedIndexName, clockIndexName} {
		if _, ok := got[want]; !ok {
			t.Errorf("index %q missing after EnsureIndexes; have %+v", want, got)
		}
	}
}

// TestEnsureTTLIndexRejectsOutOfRangeRetention asserts that a retention which
// would truncate to expireAfterSeconds: 0 (deleting resolved jobs immediately)
// is rejected before any index is created. This needs no server: the guard runs
// before the driver call, so a nil collection is never dereferenced.
func TestEnsureTTLIndexRejectsOutOfRangeRetention(t *testing.T) {
	t.Parallel()

	q := New(nil)
	cases := []struct {
		name      string
		retention time.Duration
	}{
		{"zero", 0},
		{"negative", -time.Hour},
		{"sub-second", 500 * time.Millisecond},
		{"above MaxInt32 seconds", (math.MaxInt32 + 1) * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := q.EnsureTTLIndex(context.Background(), tc.retention); err == nil {
				t.Errorf("EnsureTTLIndex(%v) = nil, want error", tc.retention)
			}
		})
	}
}

// TestNodeID asserts the default node id is non-empty and unique across
// constructions, and that WithNodeID overrides it. New performs no I/O and
// never dereferences its collection, so a nil collection suffices here.
func TestNodeID(t *testing.T) {
	t.Parallel()

	q1 := New(nil)
	if q1.nodeID == "" {
		t.Fatal("default nodeID is empty")
	}

	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	format := regexp.MustCompile("^" + regexp.QuoteMeta(fmt.Sprintf("%s-%d-", host, os.Getpid())) + "[0-9a-f]{8}$")
	if !format.MatchString(q1.nodeID) {
		t.Errorf("default nodeID = %q, want hostname-pid-<8 hex> matching %v", q1.nodeID, format)
	}

	q2 := New(nil)
	if q1.nodeID == q2.nodeID {
		t.Errorf("two New calls yielded identical nodeID %q; expected distinct random suffixes", q1.nodeID)
	}

	custom := New(nil, WithNodeID("custom-id"))
	if custom.nodeID != "custom-id" {
		t.Errorf("WithNodeID nodeID = %q, want %q", custom.nodeID, "custom-id")
	}
}

// TestColdStartSeed verifies the vtime cache's cold-start seed: the first
// reserve for a (tenant, partition) timeline with pending records starts from
// the max pending vstamp in that partition (resolved records and other
// partitions excluded), and a tenant with no records starts at the
// partition's frontier.
func TestColdStartSeed(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	const (
		maxPending = 12 * scale
		cost       = int64(1)
		weight     = int64(1)
	)
	// Two pending records for tenant "warm" plus a resolved record with a
	// higher vstamp, which must NOT influence the seed: resolved slots are
	// already consumed and may be TTL-collected at any time. A pending record
	// for "warm" in another partition, also higher, must not influence it
	// either: each partition is its own timeline. Nothing was ever claimed,
	// so the frontier is the fallback, the min pending vstamp at any
	// visibility (j1, delayed an hour), which is below max pending.
	const minPending = 5 * scale
	later := time.Now().Add(time.Hour)
	insertJob(ctx, t, q, Job{ID: "j1", TenantID: "warm", Partition: defaultPartition, Liveness: LivenessPending, VStamp: minPending, VisibleAt: later})
	insertJob(ctx, t, q, Job{ID: "j2", TenantID: "warm", Partition: defaultPartition, Liveness: LivenessPending, VStamp: maxPending, VisibleAt: later})
	insertJob(ctx, t, q, Job{ID: "j3", TenantID: "warm", Partition: defaultPartition, Liveness: LivenessResolved, VStamp: 99 * scale, VisibleAt: later})
	insertJob(ctx, t, q, Job{ID: "j4", TenantID: "warm", Partition: "other", Liveness: LivenessPending, VStamp: 50 * scale, VisibleAt: later})

	r, err := q.vtimes.reserve(ctx, "warm", defaultPartition, cost, weight)
	if err != nil {
		t.Fatalf("reserve for seeded tenant: %v", err)
	}
	if want := maxPending + stride(cost, weight); r.vstamp != want {
		t.Errorf("seeded tenant first vstamp = %d, want %d (max pending %d + stride)", r.vstamp, want, maxPending)
	}
	r.Commit()

	// A tenant with no records at all seeds at the frontier (j1's delayed
	// stamp), so its first candidate is one stride past it.
	r2, err := q.vtimes.reserve(ctx, "fresh", defaultPartition, cost, weight)
	if err != nil {
		t.Fatalf("reserve for unseen tenant: %v", err)
	}
	if want := minPending + stride(cost, weight); r2.vstamp != want {
		t.Errorf("unseen tenant first vstamp = %d, want %d (frontier %d + stride)", r2.vstamp, want, minPending)
	}
	r2.Abort()
}

// TestSeedCountsLeasedJob proves a leased (fenced, invisible) job still
// raises its tenant's trailing edge: the seed query reads the max pending
// vstamp at any visibility, so a's lease at 10e3 wins over its visible job
// at 3e3.
func TestSeedCountsLeasedJob(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	insertJob(ctx, t, q, Job{ID: "a-visible", TenantID: "a", Partition: defaultPartition, Liveness: LivenessPending, VStamp: 3 * scale, VisibleAt: time.Now().Add(-time.Minute)})
	insertJob(ctx, t, q, Job{ID: "a-leased", TenantID: "a", Partition: defaultPartition, Liveness: LivenessPending, VStamp: 10 * scale, ClaimID: "fence-10", Attempts: 1, VisibleAt: time.Now().Add(time.Hour)})

	edge, err := q.seedTenantVtime(ctx, "a", defaultPartition)
	if err != nil {
		t.Fatalf("seedTenantVtime: %v", err)
	}
	if edge != 10*scale {
		t.Errorf("trailing edge = %d, want %d (the leased job)", edge, 10*scale)
	}
}

// TestComputeLWM proves the frontier is the partition's service clock: the
// maximum fenced (claimed, unreleased) vstamp, pending or resolved.
// A long lease at a low stamp does not lower it; released retries (fence
// cleared), delayed, never-claimed, and canceled-before-claim jobs above it
// do not raise it. A never-claimed partition falls back to its minimum
// pending vstamp, delayed jobs included; other partitions are ignored.
func TestComputeLWM(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))

	past := time.Now().Add(-time.Minute)
	future := time.Now().Add(time.Hour)
	insert := func(partition string, jobs []Job) {
		t.Helper()
		for _, j := range jobs {
			j.Partition = partition
			insertJob(ctx, t, q, j)
		}
	}

	// p1: the clock is a completed job at 6, above a long lease at 2 and an
	// in-flight lease at 5.
	insert("p1", []Job{
		{ID: "p1-lease-2", VStamp: 2 * scale, ClaimID: "fence-2", Liveness: LivenessPending, VisibleAt: future},
		{ID: "p1-lease-5", VStamp: 5 * scale, ClaimID: "fence-5", Liveness: LivenessPending, VisibleAt: future},
		{ID: "p1-done-6", VStamp: 6 * scale, ClaimID: "fence-6", Liveness: LivenessResolved, Resolution: ResolutionCompleted, VisibleAt: past, ResolvedAt: past},
		{ID: "p1-visible-7", VStamp: 7 * scale, Liveness: LivenessPending, VisibleAt: past},
		{ID: "p1-released-8", VStamp: 8 * scale, Liveness: LivenessPending, Attempts: 1, VisibleAt: future},
		{ID: "p1-delayed-9", VStamp: 9 * scale, Liveness: LivenessPending, VisibleAt: future},
		{ID: "p1-canceled-20", VStamp: 20 * scale, Liveness: LivenessResolved, Resolution: ResolutionCanceled, VisibleAt: past, ResolvedAt: past},
	})

	// p2: the clock is a pending lease at 4, above a canceled-after-claim job
	// that kept its fence.
	insert("p2", []Job{
		{ID: "p2-canceled-3", VStamp: 3 * scale, ClaimID: "fence-3", Liveness: LivenessResolved, Resolution: ResolutionCanceled, VisibleAt: past, ResolvedAt: past},
		{ID: "p2-lease-4", VStamp: 4 * scale, ClaimID: "fence-4", Liveness: LivenessPending, VisibleAt: future},
	})

	// p3: never claimed, so the fallback takes the delayed job at 2 below the
	// visible one; the canceled job at 1 is not pending.
	insert("p3", []Job{
		{ID: "p3-canceled-1", VStamp: 1 * scale, Liveness: LivenessResolved, Resolution: ResolutionCanceled, VisibleAt: past, ResolvedAt: past},
		{ID: "p3-delayed-2", VStamp: 2 * scale, Liveness: LivenessPending, VisibleAt: future},
		{ID: "p3-visible-5", VStamp: 5 * scale, Liveness: LivenessPending, VisibleAt: past},
	})

	// p4: only a job canceled before claim; no clock and nothing pending.
	insert("p4", []Job{
		{ID: "p4-canceled-3", VStamp: 3 * scale, Liveness: LivenessResolved, Resolution: ResolutionCanceled, VisibleAt: past, ResolvedAt: past},
	})

	// other: claimed and pending jobs that every partition above must ignore.
	insert("other", []Job{
		{ID: "other-done-100", VStamp: 100 * scale, ClaimID: "fence-100", Liveness: LivenessResolved, Resolution: ResolutionCompleted, VisibleAt: past, ResolvedAt: past},
		{ID: "other-pending-0", VStamp: 0, Liveness: LivenessPending, VisibleAt: past},
	})

	for _, tc := range []struct {
		partition string
		wantLWM   int64
		wantOK    bool
	}{
		{"p1", 6 * scale, true},
		{"p2", 4 * scale, true},
		{"p3", 2 * scale, true},
		{"p4", 0, false},
		{"empty", 0, false},
	} {
		lwm, ok, err := q.computeLWM(ctx, tc.partition)
		if err != nil {
			t.Fatalf("computeLWM(%q): %v", tc.partition, err)
		}
		if lwm != tc.wantLWM || ok != tc.wantOK {
			t.Errorf("computeLWM(%q) = (%d, %v), want (%d, %v)", tc.partition, lwm, ok, tc.wantLWM, tc.wantOK)
		}
	}
}
