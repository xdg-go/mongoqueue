package mongoqueue

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xdg-go/mongoqueue/internal/mongotest"
)

// TestEnqueueOptsNormalize exercises the DB-free validation path shared by
// every enqueue: required JobID, cost flooring, weight zero-means-1 and
// rejection, and empty-partition defaulting. Server-side behavior (duplicate
// ids, stored record shape) is covered by the enqueue integration tests.
func TestEnqueueOptsNormalize(t *testing.T) {
	cases := []struct {
		name    string
		opts    EnqueueOpts
		want    normalizedEnqueue
		wantErr error
	}{
		{
			name:    "missing JobID rejected",
			opts:    EnqueueOpts{TenantID: "t1", Cost: 5},
			wantErr: ErrMissingJobID,
		},
		{
			name: "zero cost floored to 1",
			opts: EnqueueOpts{JobID: "j1", Cost: 0},
			want: normalizedEnqueue{jobID: "j1", partition: defaultPartition, cost: 1, weight: 1},
		},
		{
			name: "negative cost floored to 1",
			opts: EnqueueOpts{JobID: "j1", Cost: -7},
			want: normalizedEnqueue{jobID: "j1", partition: defaultPartition, cost: 1, weight: 1},
		},
		{
			name: "zero weight means 1",
			opts: EnqueueOpts{JobID: "j1", Cost: 3, Weight: 0},
			want: normalizedEnqueue{jobID: "j1", partition: defaultPartition, cost: 3, weight: 1},
		},
		{
			name:    "negative weight rejected",
			opts:    EnqueueOpts{JobID: "j1", Weight: -1},
			wantErr: ErrInvalidWeight,
		},
		{
			name: "explicit fields pass through",
			opts: EnqueueOpts{JobID: "j1", TenantID: "t1", Partition: "p1", Cost: 10, Weight: 4},
			want: normalizedEnqueue{jobID: "j1", tenantID: "t1", partition: "p1", cost: 10, weight: 4},
		},
		{
			name: "empty partition maps to default",
			opts: EnqueueOpts{JobID: "j1", TenantID: "t1", Cost: 2, Weight: 2},
			want: normalizedEnqueue{jobID: "j1", tenantID: "t1", partition: defaultPartition, cost: 2, weight: 2},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.opts.normalize()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("normalize() error = %v, want %v", err, tc.wantErr)
			}
			if err == nil && got != tc.want {
				t.Errorf("normalize() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// rawBody marshals doc into the bson.Raw form enqueue stores as the job body.
func rawBody(t *testing.T, doc any) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal body %+v: %v", doc, err)
	}
	return raw
}

// findJob fetches the stored record for id, failing the test if it is absent.
func findJob(ctx context.Context, t *testing.T, q *Queue, id string) Job {
	t.Helper()
	var job Job
	if err := q.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&job); err != nil {
		t.Fatalf("find job %q: %v", id, err)
	}
	return job
}

// TestEnqueueDuplicateJobID proves enqueue is an idempotent insert keyed on
// JobID: a retry with the same id returns ErrDuplicateJob, the stored record
// wins (unchanged even when the retry carries a different body), and the
// tenant's vtime does not advance on the duplicate -- the next successful
// enqueue's vstamp reflects only one prior stride.
func TestEnqueueDuplicateJobID(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	opts := EnqueueOpts{JobID: "j1", TenantID: "t1", Cost: 1, Weight: 1}

	if err := q.enqueue(ctx, "invoice", rawBody(t, bson.D{{Key: "n", Value: int32(1)}}), opts); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	stored := findJob(ctx, t, q, "j1")

	// Retry with a different body: the first insert must win intact.
	err := q.enqueue(ctx, "invoice", rawBody(t, bson.D{{Key: "n", Value: int32(2)}}), opts)
	if !errors.Is(err, ErrDuplicateJob) {
		t.Fatalf("duplicate enqueue error = %v, want ErrDuplicateJob", err)
	}
	after := findJob(ctx, t, q, "j1")
	if !after.VisibleAt.Equal(stored.VisibleAt) {
		t.Errorf("visible_at changed on duplicate: %v -> %v", stored.VisibleAt, after.VisibleAt)
	}
	after.VisibleAt = stored.VisibleAt // compared above; time.Time internals may differ under ==
	if string(after.Body) != string(stored.Body) {
		t.Errorf("body changed on duplicate: %v -> %v", stored.Body, after.Body)
	}
	after.Body, stored.Body = nil, nil
	if !reflect.DeepEqual(after, stored) {
		t.Errorf("stored record changed on duplicate:\n got %+v\nwant %+v", after, stored)
	}

	// The duplicate must not have advanced t1's vtime: with cost 1 / weight 1
	// each successful enqueue strides exactly stride(1, 1), so the second
	// successful job lands at 2*stride, not 3*stride.
	if err := q.enqueue(ctx, "invoice", rawBody(t, bson.D{{Key: "n", Value: int32(3)}}),
		EnqueueOpts{JobID: "j2", TenantID: "t1", Cost: 1, Weight: 1}); err != nil {
		t.Fatalf("enqueue after duplicate: %v", err)
	}
	next := findJob(ctx, t, q, "j2")
	if want := 2 * stride(1, 1); next.VStamp != want {
		t.Errorf("vstamp after one duplicate = %d, want %d (duplicate advanced vtime)", next.VStamp, want)
	}
}

// TestEnqueueStoredRecordShape proves the envelope/body split reaches the
// server as designed: envelope fields are top-level document fields, and the
// body is a native BSON subdocument that a raw query can reach by field path
// (find on "body.customer_id").
func TestEnqueueStoredRecordShape(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	coll := db.Collection("jobs")
	q := New(coll)

	body := rawBody(t, bson.D{
		{Key: "customer_id", Value: "cust-42"},
		{Key: "amount", Value: int64(1999)},
	})
	opts := EnqueueOpts{JobID: "j1", TenantID: "t1", Partition: "p1", Cost: 3, Weight: 2}
	if err := q.enqueue(ctx, "invoice", body, opts); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Raw decode, bypassing the Job struct, so the assertion is about the
	// stored document's shape rather than the driver's tag mapping.
	var doc bson.Raw
	if err := coll.FindOne(ctx, bson.D{{Key: "body.customer_id", Value: "cust-42"}}).Decode(&doc); err != nil {
		t.Fatalf("find on body.customer_id: %v", err)
	}
	for field, want := range map[string]any{
		"_id":       "j1",
		"tenant":    "t1",
		"partition": "p1",
		"cost":      int64(3),
		"liveness":  string(LivenessPending),
		"kind":      "invoice",
	} {
		val, err := doc.LookupErr(field)
		if err != nil {
			t.Errorf("top-level field %q missing: %v", field, err)
			continue
		}
		var got any
		if err := val.Unmarshal(&got); err != nil {
			t.Errorf("unmarshal field %q: %v", field, err)
			continue
		}
		if got != want {
			t.Errorf("field %q = %v, want %v", field, got, want)
		}
	}
	if _, err := doc.LookupErr("vstamp"); err != nil {
		t.Errorf("top-level field %q missing: %v", "vstamp", err)
	}
	sub, err := doc.LookupErr("body")
	if err != nil {
		t.Fatalf("top-level field %q missing: %v", "body", err)
	}
	subdoc, ok := sub.DocumentOK()
	if !ok {
		t.Fatalf("body is BSON type %v, want embedded document", sub.Type)
	}
	if got := subdoc.Lookup("amount").Int64(); got != 1999 {
		t.Errorf("body.amount = %d, want 1999", got)
	}
}

// TestEnqueueStampsProvenance proves the enqueue-computed envelope fields land
// as specified: stamped_by carries the enqueueing node's id, visible_at is set
// from the client clock (non-zero, near now), and liveness starts pending.
func TestEnqueueStampsProvenance(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"), WithNodeID("node-under-test"))

	before := time.Now().UTC()
	if err := q.enqueue(ctx, "payment", rawBody(t, bson.D{{Key: "n", Value: int32(1)}}),
		EnqueueOpts{JobID: "j1", TenantID: "t1"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	after := time.Now().UTC()

	job := findJob(ctx, t, q, "j1")
	if job.StampedBy != "node-under-test" {
		t.Errorf("stamped_by = %q, want %q", job.StampedBy, "node-under-test")
	}
	if job.Liveness != LivenessPending {
		t.Errorf("liveness = %q, want %q", job.Liveness, LivenessPending)
	}
	// BSON datetimes have millisecond precision, so widen the window by 1ms
	// on each side rather than asserting exact bounds.
	if job.VisibleAt.IsZero() ||
		job.VisibleAt.Before(before.Add(-time.Millisecond)) ||
		job.VisibleAt.After(after.Add(time.Millisecond)) {
		t.Errorf("visible_at = %v, want within [%v, %v]", job.VisibleAt, before, after)
	}
}
