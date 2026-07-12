package mongoqueue

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xdg-go/mongoqueue/internal/mongotest"
)

// livenessDoc mirrors the envelope's liveness field in isolation so the tests
// exercise exactly the encoding the queue's guarded writes depend on.
type livenessDoc struct {
	ID       string   `bson:"_id"`
	Liveness Liveness `bson:"liveness"`
}

// TestLivenessBSONEncoding asserts each Liveness value encodes as its plain
// BSON string literal and survives a marshal/unmarshal round trip. The guarded
// writes (claim, heartbeat, complete, release, cancel) filter on the stored
// literal server-side, so the wire form must be exactly "pending"/"resolved".
func TestLivenessBSONEncoding(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		value   Liveness
		encoded string
	}{
		{"pending", LivenessPending, "pending"},
		{"resolved", LivenessResolved, "resolved"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data, err := bson.Marshal(livenessDoc{ID: tc.name, Liveness: tc.value})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			raw := bson.Raw(data)

			// Wire encoding: the field must be a BSON string holding the
			// exact literal, indistinguishable from a hand-written filter.
			el, err := raw.LookupErr("liveness")
			if err != nil {
				t.Fatalf("liveness field missing from encoded doc: %v", err)
			}
			got, ok := el.StringValueOK()
			if !ok {
				t.Fatalf("liveness encoded as BSON type %v, want string", el.Type)
			}
			if got != tc.encoded {
				t.Errorf("liveness encoded as %q, want %q", got, tc.encoded)
			}

			// Round trip preserves the typed value.
			var back livenessDoc
			if err := bson.Unmarshal(raw, &back); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if back.Liveness != tc.value {
				t.Errorf("round-tripped liveness = %q, want %q", back.Liveness, tc.value)
			}
		})
	}
}

// TestResolutionBSONEncoding asserts a Resolution value -- library constant
// or caller-defined -- encodes as its plain BSON string and round-trips
// intact. The vocabulary is open, so arbitrary strings (including empty,
// which Complete rejects at the API layer) must pass through unaltered.
func TestResolutionBSONEncoding(t *testing.T) {
	t.Parallel()

	type doc struct {
		Resolution Resolution `bson:"resolution"`
	}

	cases := []struct {
		name  string
		value Resolution
	}{
		{"completed", ResolutionCompleted},
		{"canceled", ResolutionCanceled},
		{"caller-defined", Resolution("shard-migrated")},
		{"empty", Resolution("")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data, err := bson.Marshal(doc{Resolution: tc.value})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			el, err := bson.Raw(data).LookupErr("resolution")
			if err != nil {
				t.Fatalf("resolution field missing from encoded doc: %v", err)
			}
			got, ok := el.StringValueOK()
			if !ok {
				t.Fatalf("resolution encoded as BSON type %v, want string", el.Type)
			}
			if got != string(tc.value) {
				t.Errorf("resolution encoded as %q, want %q", got, tc.value)
			}

			var back doc
			if err := bson.Unmarshal(data, &back); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if back.Resolution != tc.value {
				t.Errorf("round-tripped resolution = %q, want %q", back.Resolution, tc.value)
			}
		})
	}
}

// TestLivenessPendingFilterMatchesLiteral proves the stored liveness value is
// filter-matchable as a literal against a real server: documents inserted
// through the driver's struct encoding are found by filtering on the
// LivenessPending constant, and resolved documents are excluded. This is the
// property every guarded write's server-side filter relies on.
func TestLivenessPendingFilterMatchesLiteral(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	coll := db.Collection("jobs")

	docs := []any{
		livenessDoc{ID: "p1", Liveness: LivenessPending},
		livenessDoc{ID: "p2", Liveness: LivenessPending},
		livenessDoc{ID: "r1", Liveness: LivenessResolved},
	}
	if _, err := coll.InsertMany(ctx, docs); err != nil {
		t.Fatalf("insert fixture docs: %v", err)
	}

	cur, err := coll.Find(ctx, bson.M{"liveness": LivenessPending})
	if err != nil {
		t.Fatalf("find pending: %v", err)
	}
	var got []livenessDoc
	if err := cur.All(ctx, &got); err != nil {
		t.Fatalf("decode results: %v", err)
	}

	found := make(map[string]bool, len(got))
	for _, d := range got {
		found[d.ID] = true
	}
	if len(got) != 2 || !found["p1"] || !found["p2"] {
		t.Errorf("filter on LivenessPending matched %v, want exactly [p1 p2]", found)
	}

	// The raw string literal must match the same documents -- the constant
	// and a hand-written ops query are interchangeable.
	n, err := coll.CountDocuments(ctx, bson.M{"liveness": "pending"})
	if err != nil {
		t.Fatalf("count with raw literal: %v", err)
	}
	if n != 2 {
		t.Errorf("filter on raw literal \"pending\" matched %d docs, want 2", n)
	}
}

// mustMarshalBody marshals v into a bson.Raw suitable for the Job.Body field,
// failing the test on error.
func mustMarshalBody(t *testing.T, v any) bson.Raw {
	t.Helper()
	data, err := bson.Marshal(v)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	return bson.Raw(data)
}

// TestJobBSONRoundTrip asserts a fully populated Job survives a
// marshal/unmarshal round trip field-for-field, and that the marshaled
// document uses exactly the wire names the design pins (which indexes and
// guarded-write filters reference by literal string). Times are held at
// millisecond precision because BSON datetimes truncate to milliseconds, so
// equality here is honest rather than accidental.
func TestJobBSONRoundTrip(t *testing.T) {
	t.Parallel()

	visibleAt := time.Date(2026, 7, 11, 12, 30, 45, 123e6, time.UTC)
	resolvedAt := time.Date(2026, 7, 11, 13, 0, 0, 456e6, time.UTC)

	job := Job{
		ID:         "job-1",
		TenantID:   "tenant-a",
		Partition:  "shard-3",
		Cost:       250,
		VStamp:     1_000_000,
		Liveness:   LivenessResolved,
		Resolution: ResolutionCompleted,
		ClaimID:    "claim-xyz",
		VisibleAt:  visibleAt,
		Attempts:   2,
		StampedBy:  "node-7",
		ResolvedAt: resolvedAt,
		Kind:       "email.send",
		Body:       mustMarshalBody(t, bson.M{"customer": "acme"}),
	}

	data, err := bson.Marshal(job)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	raw := bson.Raw(data)

	// Every envelope field must appear under its pinned wire name holding
	// that field's value; a renamed or swapped tag would silently break
	// index definitions and server-side filters. Checking values (not mere
	// presence) catches two same-typed tags being exchanged.
	wireFields := []struct {
		name string
		want any
	}{
		{"_id", job.ID},
		{"tenant", job.TenantID},
		{"partition", job.Partition},
		{"cost", job.Cost},
		{"vstamp", job.VStamp},
		{"liveness", string(job.Liveness)},
		{"resolution", string(job.Resolution)},
		{"claim_id", job.ClaimID},
		{"visible_at", job.VisibleAt},
		{"attempts", int32(job.Attempts)},
		{"stamped_by", job.StampedBy},
		{"resolved_at", job.ResolvedAt},
		{"kind", job.Kind},
		{"body", job.Body},
	}
	for _, f := range wireFields {
		el, err := raw.LookupErr(f.name)
		if err != nil {
			t.Errorf("marshaled Job missing wire field %q: %v", f.name, err)
			continue
		}
		switch want := f.want.(type) {
		case string:
			if got, ok := el.StringValueOK(); !ok || got != want {
				t.Errorf("wire field %q = %v, want string %q", f.name, el, want)
			}
		case int64:
			if got, ok := el.Int64OK(); !ok || got != want {
				t.Errorf("wire field %q = %v, want int64 %d", f.name, el, want)
			}
		case int32:
			if got, ok := el.Int32OK(); !ok || got != want {
				t.Errorf("wire field %q = %v, want int32 %d", f.name, el, want)
			}
		case time.Time:
			if got, ok := el.TimeOK(); !ok || !got.Equal(want) {
				t.Errorf("wire field %q = %v, want datetime %v", f.name, el, want)
			}
		case bson.Raw:
			if got, ok := el.DocumentOK(); !ok || !bytes.Equal(got, want) {
				t.Errorf("wire field %q = %v, want subdocument %v", f.name, el, want)
			}
		default:
			t.Fatalf("wire field %q: unhandled want type %T", f.name, f.want)
		}
	}

	// No stray fields either: the document is exactly the envelope.
	elems, err := raw.Elements()
	if err != nil {
		t.Fatalf("inspect marshaled doc: %v", err)
	}
	if len(elems) != len(wireFields) {
		t.Errorf("marshaled Job has %d fields, want %d: %v", len(elems), len(wireFields), raw)
	}

	var back Job
	if err := bson.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Compare times with Equal semantics (BSON decodes into a wall-clock
	// representation whose == identity may differ), then the rest directly.
	if !back.VisibleAt.Equal(job.VisibleAt) {
		t.Errorf("round-tripped VisibleAt = %v, want %v", back.VisibleAt, job.VisibleAt)
	}
	if !back.ResolvedAt.Equal(job.ResolvedAt) {
		t.Errorf("round-tripped ResolvedAt = %v, want %v", back.ResolvedAt, job.ResolvedAt)
	}
	if !bytes.Equal(back.Body, job.Body) {
		t.Errorf("round-tripped Body = %v, want %v", back.Body, job.Body)
	}

	// Times were compared with Equal above, so normalize just them; then
	// DeepEqual sweeps every field in one shot -- Body is re-covered here,
	// but the bytes.Equal check above gives it a sharper failure message.
	// (Job holds a slice, so it is not directly comparable with ==.)
	back.VisibleAt, back.ResolvedAt = job.VisibleAt, job.ResolvedAt
	if !reflect.DeepEqual(back, job) {
		t.Errorf("round-tripped Job = %+v, want %+v", back, job)
	}
}

// TestJobPendingOmitsResolutionFields asserts that marshaling a pending Job
// (zero ResolvedAt, empty Resolution) omits both terminal fields entirely.
// Their absence-while-pending is part of the record's shape: the TTL index on
// resolved_at must not see pending jobs, and ops queries distinguish pending
// from resolved by the fields' presence.
func TestJobPendingOmitsResolutionFields(t *testing.T) {
	t.Parallel()

	job := Job{
		ID:        "job-pending",
		TenantID:  "tenant-a",
		Cost:      1,
		VStamp:    42,
		Liveness:  LivenessPending,
		VisibleAt: time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC),
		Kind:      "email.send",
		Body:      mustMarshalBody(t, bson.M{"customer": "acme"}),
	}

	data, err := bson.Marshal(job)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	raw := bson.Raw(data)

	for _, name := range []string{"resolution", "resolved_at"} {
		if el, err := raw.LookupErr(name); err == nil {
			t.Errorf("pending Job marshaled with %q = %v; want field absent", name, el)
		}
	}
}

// TestJobBodyQueryableInDatabase proves the envelope/body split's core
// promise against a real server: Body is opaque to queue code (a bson.Raw the
// library never inspects) yet stored as a native subdocument the database can
// query by field. A Job is inserted whose body carries a customer field, then
// found via a "body.customer" filter and decoded back into a Job intact.
func TestJobBodyQueryableInDatabase(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	coll := db.Collection("jobs")

	type payload struct {
		Customer string `bson:"customer"`
		Amount   int64  `bson:"amount"`
	}

	jobs := []Job{
		{
			ID:        "job-acme",
			TenantID:  "tenant-a",
			Cost:      1,
			VStamp:    100,
			Liveness:  LivenessPending,
			VisibleAt: time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC),
			Kind:      "invoice.issue",
			Body:      mustMarshalBody(t, payload{Customer: "acme", Amount: 1200}),
		},
		{
			ID:        "job-globex",
			TenantID:  "tenant-a",
			Cost:      1,
			VStamp:    200,
			Liveness:  LivenessPending,
			VisibleAt: time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC),
			Kind:      "invoice.issue",
			Body:      mustMarshalBody(t, payload{Customer: "globex", Amount: 900}),
		},
	}
	docs := make([]any, len(jobs))
	for i, j := range jobs {
		docs[i] = j
	}
	if _, err := coll.InsertMany(ctx, docs); err != nil {
		t.Fatalf("insert jobs: %v", err)
	}

	// Query by a field inside the body -- something only possible because the
	// body is stored as a native BSON subdocument, not an opaque blob.
	var got Job
	if err := coll.FindOne(ctx, bson.M{"body.customer": "acme"}).Decode(&got); err != nil {
		t.Fatalf("find by body.customer: %v", err)
	}
	if got.ID != "job-acme" {
		t.Errorf("body.customer=acme matched job %q, want %q", got.ID, "job-acme")
	}

	// The body decodes back through the caller's type untouched by the queue.
	var body payload
	if err := bson.Unmarshal(got.Body, &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Customer != "acme" || body.Amount != 1200 {
		t.Errorf("decoded body = %+v, want {acme 1200}", body)
	}
}
