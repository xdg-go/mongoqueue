package mongoqueue

import (
	"testing"

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

// TestResolutionBSONEncoding asserts a caller-defined Resolution value encodes
// as its plain BSON string and round-trips intact. The vocabulary is open, so
// arbitrary strings (including empty) must pass through unaltered.
func TestResolutionBSONEncoding(t *testing.T) {
	t.Parallel()

	type doc struct {
		Resolution Resolution `bson:"resolution"`
	}

	cases := []struct {
		name  string
		value Resolution
	}{
		{"completed", Resolution("completed")},
		{"canceled", Resolution("canceled")},
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
