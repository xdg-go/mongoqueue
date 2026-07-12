package mongoqueue

import (
	"errors"
	"strings"
	"testing"

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
