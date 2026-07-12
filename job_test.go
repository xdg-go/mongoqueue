package mongoqueue

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

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
// This asserts that the Job struct is tagged correctly for the "omitempty"
// behavior.
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
