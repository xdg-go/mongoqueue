package mongoqueue

import (
	"context"
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
	Name               string   `bson:"name"`
	Key                bson.Raw `bson:"key"`
	ExpireAfterSeconds *int32   `bson:"expireAfterSeconds"`
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

// keyFields decodes an index key document into its ordered field names.
func keyFields(t *testing.T, key bson.Raw) []string {
	t.Helper()
	var d bson.D
	if err := bson.Unmarshal(key, &d); err != nil {
		t.Fatalf("decode index key %v: %v", key, err)
	}
	fields := make([]string, len(d))
	for i, e := range d {
		fields[i] = e.Key
	}
	return fields
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

// TestEnsureIndexesCreatesClaimIndex asserts EnsureIndexes creates exactly the
// claim index with keys in the specified order, and that a second call is a
// no-op returning no error.
func TestEnsureIndexesCreatesClaimIndex(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	coll := db.Collection("jobs")
	q := New(coll)

	if err := q.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}

	want := bson.D{
		{Key: "partition", Value: int32(1)},
		{Key: "liveness", Value: int32(1)},
		{Key: "vstamp", Value: int32(1)},
		{Key: "_id", Value: int32(1)},
		{Key: "visible_at", Value: int32(1)},
	}
	var claim *indexInfo
	all := listIndexes(ctx, t, coll)
	for i := range all {
		if all[i].Name == "_id_" {
			continue
		}
		if claim != nil {
			t.Fatalf("found more than one non-_id_ index; second is %q", all[i].Name)
		}
		claim = &all[i]
	}
	if claim == nil {
		t.Fatal("no non-_id_ index found after EnsureIndexes")
	}

	// Assert both the field order and the sort direction (:1) the spec pins,
	// so a regression to a descending or hashed key is caught.
	var got bson.D
	if err := bson.Unmarshal(claim.Key, &got); err != nil {
		t.Fatalf("decode claim index key %v: %v", claim.Key, err)
	}
	if len(got) != len(want) {
		t.Fatalf("claim index key = %v, want %v", got, want)
	}
	for i := range want {
		if got[i].Key != want[i].Key || got[i].Value != want[i].Value {
			t.Fatalf("claim index key = %v, want %v (mismatch at %d)", got, want, i)
		}
	}

	// Idempotent: an identical key spec is a server-side no-op.
	if err := q.EnsureIndexes(ctx); err != nil {
		t.Fatalf("second EnsureIndexes returned error: %v", err)
	}
}

// TestEnsureTTLIndex asserts EnsureTTLIndex creates a TTL index on resolved_at
// whose expireAfterSeconds matches the supplied retention (24h == 86400s).
func TestEnsureTTLIndex(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	coll := db.Collection("jobs")
	q := New(coll)

	if err := q.EnsureTTLIndex(ctx, 24*time.Hour); err != nil {
		t.Fatalf("EnsureTTLIndex: %v", err)
	}

	var ttl *indexInfo
	all := listIndexes(ctx, t, coll)
	for i := range all {
		if fields := keyFields(t, all[i].Key); len(fields) == 1 && fields[0] == "resolved_at" {
			ttl = &all[i]
			break
		}
	}
	if ttl == nil {
		t.Fatalf("no index keyed on resolved_at found: %+v", all)
	}
	if ttl.ExpireAfterSeconds == nil {
		t.Fatal("resolved_at index has no expireAfterSeconds; not a TTL index")
	}
	if *ttl.ExpireAfterSeconds != 86400 {
		t.Errorf("expireAfterSeconds = %d, want 86400", *ttl.ExpireAfterSeconds)
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

	q2 := New(nil)
	if q1.nodeID == q2.nodeID {
		t.Errorf("two New calls yielded identical nodeID %q; expected distinct random suffixes", q1.nodeID)
	}

	custom := New(nil, WithNodeID("custom-id"))
	if custom.nodeID != "custom-id" {
		t.Errorf("WithNodeID nodeID = %q, want %q", custom.nodeID, "custom-id")
	}
}

// seedRecord names the fields the cold-start seed query reads; the full Job
// schema is not needed because the seed filters on tenant and pending
// liveness and projects vstamp alone.
type seedRecord struct {
	ID       string   `bson:"_id"`
	Tenant   string   `bson:"tenant"`
	Liveness Liveness `bson:"liveness"`
	VStamp   int64    `bson:"vstamp"`
}

// seedDoc inserts a minimal job document for cold-start seed tests.
func seedDoc(ctx context.Context, t *testing.T, coll *mongo.Collection, doc seedRecord) {
	t.Helper()
	if _, err := coll.InsertOne(ctx, doc); err != nil {
		t.Fatalf("insert seed doc %q: %v", doc.ID, err)
	}
}

// TestColdStartSeed verifies the vtime cache's cold-start seed: the first
// reserve for a tenant with pending records starts from the max pending
// vstamp (resolved records excluded), and a tenant with no records starts at
// the floor of 0. This is the minimal Phase 3.1 seed; full reconciliation is
// Phase 6.
func TestColdStartSeed(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	coll := db.Collection("jobs")

	const (
		maxPending = 12 * scale
		cost       = int64(1)
		weight     = int64(1)
	)
	// Two pending records for tenant "warm" plus a resolved record with a
	// higher vstamp, which must NOT influence the seed: resolved slots are
	// already consumed and may be TTL-collected at any time.
	seedDoc(ctx, t, coll, seedRecord{ID: "j1", Tenant: "warm", Liveness: LivenessPending, VStamp: 5 * scale})
	seedDoc(ctx, t, coll, seedRecord{ID: "j2", Tenant: "warm", Liveness: LivenessPending, VStamp: maxPending})
	seedDoc(ctx, t, coll, seedRecord{ID: "j3", Tenant: "warm", Liveness: LivenessResolved, VStamp: 99 * scale})

	q := New(coll)

	r, err := q.vtimes.reserve(ctx, "warm", cost, weight)
	if err != nil {
		t.Fatalf("reserve for seeded tenant: %v", err)
	}
	if want := maxPending + stride(cost, weight); r.vstamp != want {
		t.Errorf("seeded tenant first vstamp = %d, want %d (max pending %d + stride)", r.vstamp, want, maxPending)
	}
	r.Commit()

	// A tenant with no records at all seeds at the floor of 0, so its first
	// candidate is exactly one stride.
	r2, err := q.vtimes.reserve(ctx, "fresh", cost, weight)
	if err != nil {
		t.Fatalf("reserve for unseen tenant: %v", err)
	}
	if want := stride(cost, weight); r2.vstamp != want {
		t.Errorf("unseen tenant first vstamp = %d, want %d (floor 0 + stride)", r2.vstamp, want)
	}
	r2.Abort()
}
