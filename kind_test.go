package mongoqueue

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/xdg-go/mongoqueue/internal/mongotest"
)

// invoiceBody and paymentBody are distinct body types for exercising the
// per-kind bindings in these tests.
type invoiceBody struct {
	CustomerID string `bson:"customer_id"`
	Amount     int64  `bson:"amount"`
}

type paymentBody struct {
	Reference string `bson:"reference"`
	Cents     int32  `bson:"cents"`
}

// TestNewKindEmptyPanics proves the empty-kind programmer error surfaces at
// binding declaration time, not at first enqueue.
func TestNewKindEmptyPanics(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Fatal("NewKind(\"\") did not panic")
		}
	}()
	NewKind[invoiceBody]("")
}

// TestKindAccessor proves the binding reports the kind string it was declared
// with, for worker-side dispatch switches.
func TestKindAccessor(t *testing.T) {
	t.Parallel()

	if got := NewKind[invoiceBody]("invoice").Kind(); got != "invoice" {
		t.Errorf("Kind() = %q, want %q", got, "invoice")
	}
}

// TestKindRoundTrip proves the typed facade end to end: Enqueue marshals the
// body under the binding's kind, and Decode on the stored record yields the
// original value.
func TestKindRoundTrip(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	invoice := NewKind[invoiceBody]("invoice")

	want := invoiceBody{CustomerID: "cust-42", Amount: 1999}
	if err := invoice.Enqueue(ctx, q, want, EnqueueOpts{JobID: "j1", TenantID: "t1"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	job := findJob(ctx, t, q, "j1")
	if job.Kind != "invoice" {
		t.Errorf("stored kind = %q, want %q", job.Kind, "invoice")
	}
	got, err := invoice.Decode(&job)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != want {
		t.Errorf("round trip = %+v, want %+v", got, want)
	}
}

// TestKindDecodeMismatch proves kind↔type drift is caught: decoding a job
// through a different binding returns ErrKindMismatch naming both kinds, not
// silent garbage.
func TestKindDecodeMismatch(t *testing.T) {
	t.Parallel()

	payment := NewKind[paymentBody]("payment")
	job := Job{ID: "j1", Kind: "invoice"}

	_, err := payment.Decode(&job)
	if !errors.Is(err, ErrKindMismatch) {
		t.Fatalf("decode error = %v, want ErrKindMismatch", err)
	}
	for _, kind := range []string{"invoice", "payment"} {
		if !strings.Contains(err.Error(), `"`+kind+`"`) {
			t.Errorf("error %q does not name kind %q", err, kind)
		}
	}
}

// TestKindHeterogeneousCoexist proves one collection carries multiple kinds:
// each binding decodes its own jobs independently, and the other binding's
// job is rejected rather than misdecoded.
func TestKindHeterogeneousCoexist(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	invoice := NewKind[invoiceBody]("invoice")
	payment := NewKind[paymentBody]("payment")

	wantInvoice := invoiceBody{CustomerID: "cust-7", Amount: 250}
	wantPayment := paymentBody{Reference: "ref-9", Cents: 4200}
	if err := invoice.Enqueue(ctx, q, wantInvoice, EnqueueOpts{JobID: "inv-1", TenantID: "t1"}); err != nil {
		t.Fatalf("enqueue invoice: %v", err)
	}
	if err := payment.Enqueue(ctx, q, wantPayment, EnqueueOpts{JobID: "pay-1", TenantID: "t1"}); err != nil {
		t.Fatalf("enqueue payment: %v", err)
	}

	invJob := findJob(ctx, t, q, "inv-1")
	payJob := findJob(ctx, t, q, "pay-1")

	gotInvoice, err := invoice.Decode(&invJob)
	if err != nil {
		t.Fatalf("decode invoice: %v", err)
	}
	if gotInvoice != wantInvoice {
		t.Errorf("invoice = %+v, want %+v", gotInvoice, wantInvoice)
	}
	gotPayment, err := payment.Decode(&payJob)
	if err != nil {
		t.Fatalf("decode payment: %v", err)
	}
	if gotPayment != wantPayment {
		t.Errorf("payment = %+v, want %+v", gotPayment, wantPayment)
	}
	if _, err := invoice.Decode(&payJob); !errors.Is(err, ErrKindMismatch) {
		t.Errorf("cross-binding decode error = %v, want ErrKindMismatch", err)
	}
}

// TestKindEnqueueNonDocumentBody proves a body that is not a BSON document
// (here an int) fails at bson.Marshal and the error is surfaced -- the facade
// adds no check of its own, so nothing reaches the server.
func TestKindEnqueueNonDocumentBody(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	counter := NewKind[int]("counter")

	err := counter.Enqueue(ctx, q, 42, EnqueueOpts{JobID: "j1", TenantID: "t1"})
	if err == nil {
		t.Fatal("enqueue of int body succeeded, want marshal error")
	}
	if errors.Is(err, ErrDuplicateJob) || errors.Is(err, ErrMissingJobID) {
		t.Fatalf("enqueue error = %v, want a marshal error", err)
	}
}

// TestKindDecodeNilBody proves a matching-kind job with no body decodes to
// T's zero value without error, while a kind mismatch still errors first.
func TestKindDecodeNilBody(t *testing.T) {
	t.Parallel()

	invoice := NewKind[invoiceBody]("invoice")

	got, err := invoice.Decode(&Job{ID: "j1", Kind: "invoice"})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != (invoiceBody{}) {
		t.Errorf("decode = %+v, want zero value", got)
	}

	payment := NewKind[paymentBody]("payment")
	if _, err := payment.Decode(&Job{ID: "j1", Kind: "invoice"}); !errors.Is(err, ErrKindMismatch) {
		t.Errorf("mismatched nil-body decode error = %v, want ErrKindMismatch", err)
	}
}

// TestKindDecodeAbsentBodyRecord proves the nil-body promise end to end: a
// record inserted outside the facade with no body field is claimable, and
// Decode yields T's zero value without error.
func TestKindDecodeAbsentBodyRecord(t *testing.T) {
	t.Parallel()

	db := mongotest.Connect(t)
	ctx := opCtx(t)
	q := New(db.Collection("jobs"))
	invoice := NewKind[invoiceBody]("invoice")

	// Marshal a full envelope, then drop the body field so the stored
	// record has no body at all.
	raw, err := bson.Marshal(Job{
		ID: "j1", TenantID: "t1", Partition: defaultPartition, Cost: 1,
		Liveness: LivenessPending, VisibleAt: time.Now().Add(-time.Minute),
		Kind: "invoice", Body: rawBody(t, bson.D{}),
	})
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	var doc bson.D
	if err := bson.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal record: %v", err)
	}
	doc = slices.DeleteFunc(doc, func(e bson.E) bool { return e.Key == "body" })
	if _, err := q.coll.InsertOne(ctx, doc); err != nil {
		t.Fatalf("insert record: %v", err)
	}

	claimed, err := q.Claim(ctx, "", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed == nil || claimed.ID != "j1" {
		t.Fatalf("claimed = %+v, want j1", claimed)
	}
	if claimed.Body != nil {
		t.Fatalf("claimed body = %v, want absent", claimed.Body)
	}
	got, err := invoice.Decode(&claimed.Job)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != (invoiceBody{}) {
		t.Errorf("decode = %+v, want zero value", got)
	}
}
