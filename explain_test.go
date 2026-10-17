package mongoqueue

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/xdg-go/mongoqueue/internal/mongotest"
)

// explainResult is what the explain check asserts on: the winning plan's
// stage names and index names, and the execution counters.
type explainResult struct {
	stages  []string
	indexes []string
	keys    int64
	docs    int64
}

// hasStage reports whether the winning plan contains stage.
func (r explainResult) hasStage(stage string) bool {
	return slices.Contains(r.stages, stage)
}

// findQuery is one FindOne as the library issues it.
type findQuery struct {
	filter, sort, projection bson.D
}

// explainFindOne explains, with executionStats, the find a FindOne with
// query issues (limit 1, single batch).
func explainFindOne(ctx context.Context, t *testing.T, coll *mongo.Collection, query findQuery) explainResult {
	t.Helper()
	find := bson.D{
		{Key: "find", Value: coll.Name()},
		{Key: "filter", Value: query.filter},
		{Key: "sort", Value: query.sort},
		{Key: "limit", Value: 1},
		{Key: "singleBatch", Value: true},
	}
	if query.projection != nil {
		find = append(find, bson.E{Key: "projection", Value: query.projection})
	}
	var out struct {
		QueryPlanner struct {
			WinningPlan bson.Raw `bson:"winningPlan"`
		} `bson:"queryPlanner"`
		ExecutionStats struct {
			TotalKeysExamined int64 `bson:"totalKeysExamined"`
			TotalDocsExamined int64 `bson:"totalDocsExamined"`
		} `bson:"executionStats"`
	}
	cmd := bson.D{{Key: "explain", Value: find}, {Key: "verbosity", Value: "executionStats"}}
	if err := coll.Database().RunCommand(ctx, cmd).Decode(&out); err != nil {
		t.Fatalf("explain: %v", err)
	}
	r := explainResult{keys: out.ExecutionStats.TotalKeysExamined, docs: out.ExecutionStats.TotalDocsExamined}
	walkPlan(t, out.QueryPlanner.WinningPlan, &r)
	return r
}

// walkPlan collects every "stage" and "indexName" in a plan tree. It walks
// all subdocuments and arrays rather than named children, because the tree's
// shape differs between the classic and SBE engines.
func walkPlan(t *testing.T, doc bson.Raw, r *explainResult) {
	t.Helper()
	elems, err := doc.Elements()
	if err != nil {
		t.Fatalf("decode plan: %v", err)
	}
	for _, e := range elems {
		v := e.Value()
		switch {
		case e.Key() == "stage" && v.Type == bson.TypeString:
			r.stages = append(r.stages, v.StringValue())
		case e.Key() == "indexName" && v.Type == bson.TypeString:
			r.indexes = append(r.indexes, v.StringValue())
		case v.Type == bson.TypeEmbeddedDocument:
			walkPlan(t, v.Document(), r)
		case v.Type == bson.TypeArray:
			walkPlan(t, bson.Raw(v.Array()), r)
		}
	}
}

// TestExplainQueriesUseIndexes checks, against a partition shaped like a
// busy production one, that each library query wins on its intended index,
// that the frontier and seed queries are covered, and that keys examined do
// not grow with the backlog. Queries are built from the library's own
// filter, sort, and projection helpers so drift breaks this test. A
// COLLSCAN or blocking SORT here is a design finding to report, not a cue
// to add an index.
func TestExplainQueriesUseIndexes(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	q := New(db.Collection("jobs"))
	if err := q.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}

	const (
		tenant   = "t"
		busy     = "busy"
		dormant  = "dormant"
		resolved = 300 // claimed and completed
		inFlight = 20  // leased, short leases
		released = 5   // released retries, fence cleared, visible again
		backlog  = 2000
		delayed  = 100
		canceled = 50 // canceled before claim, stamped above everything
	)
	now := serverNow(ctx, t, db)
	past, later := now.Add(-time.Hour), now.Add(time.Hour)

	var docs []any
	stamp := int64(0)
	add := func(prefix string, n int, partition string, live Liveness, claimID string, visible time.Time) {
		for i := range n {
			stamp += scale
			docs = append(docs, Job{
				ID: fmt.Sprintf("%s-%05d", prefix, i), TenantID: tenant, Partition: partition,
				Liveness: live, ClaimID: claimID, VStamp: stamp, VisibleAt: visible,
				Body: rawBody(t, bson.D{}),
			})
		}
	}
	add("resolved", resolved, busy, LivenessResolved, "c", past)
	// The long lease sits at a low stamp, among the resolved history.
	docs = append(docs, Job{
		ID: "long-lease", TenantID: tenant, Partition: busy, Liveness: LivenessPending,
		ClaimID: "c", VStamp: 50 * scale, VisibleAt: later, Body: rawBody(t, bson.D{}),
	})
	add("inflight", inFlight, busy, LivenessPending, "c", later)
	add("released", released, busy, LivenessPending, "", past)
	add("backlog", backlog, busy, LivenessPending, "", past)
	add("delayed", delayed, busy, LivenessPending, "", later)
	stamp += 1000 * scale
	add("canceled", canceled, busy, LivenessResolved, "", past)
	add("dormant", 10, dormant, LivenessPending, "", later)
	if _, err := q.coll.InsertMany(ctx, docs); err != nil {
		t.Fatalf("insert dataset: %v", err)
	}

	cases := []struct {
		name    string
		query   findQuery
		index   string
		covered bool
		maxKeys int64
	}{
		// Claim's scan walks the invisible prefix below the first visible job
		// (leased and delayed jobs stamped below it), independent of backlog
		// size: long lease + in-flight + the first visible (released) job.
		{"claim", findQuery{claimFilter(busy), claimSort(), nil}, claimIndexName, false, 1 + inFlight + 1},
		{"seed", findQuery{seedFilter(tenant, busy), vstampSort(-1), vstampProjection()}, seedIndexName, true, 1},
		{"clock", findQuery{clockFilter(busy), vstampSort(-1), vstampProjection()}, clockIndexName, true, 1},
		{"clock never claimed", findQuery{clockFilter(dormant), vstampSort(-1), vstampProjection()}, clockIndexName, true, 1},
		{"fallback", findQuery{pendingFilter(dormant), vstampSort(1), vstampProjection()}, claimIndexName, true, 1},
	}
	for _, tc := range cases {
		r := explainFindOne(ctx, t, q.coll, tc.query)
		t.Logf("%s: stages=%v indexes=%v keys=%d docs=%d", tc.name, r.stages, r.indexes, r.keys, r.docs)
		if r.hasStage("COLLSCAN") || r.hasStage("SORT") {
			t.Errorf("%s: plan has COLLSCAN or blocking SORT: %v", tc.name, r.stages)
		}
		if len(r.indexes) != 1 || r.indexes[0] != tc.index {
			t.Errorf("%s: indexes = %v, want [%s]", tc.name, r.indexes, tc.index)
		}
		if r.keys > tc.maxKeys {
			t.Errorf("%s: keys examined = %d, want <= %d", tc.name, r.keys, tc.maxKeys)
		}
		if tc.covered && (r.docs != 0 || r.hasStage("FETCH")) {
			t.Errorf("%s: not covered: docs examined = %d, stages = %v", tc.name, r.docs, r.stages)
		}
		if !tc.covered && r.docs > 1 {
			t.Errorf("%s: docs examined = %d, want <= 1", tc.name, r.docs)
		}
	}
}
