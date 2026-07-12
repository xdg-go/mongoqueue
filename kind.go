package mongoqueue

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Kind pairs a stable kind string with a body type T. The binding is the
// single declaration site of the kind↔type contract -- typically a
// package-level var -- so enqueue and decode cannot drift apart: the binding
// is the only writer of the stored kind string, and every decode reaches the
// body through a binding, so the wrong-type-right-string mistake has no call
// site to happen at. The queue stores the kind string without interpreting
// it; it is the body's content-type, read only by Decode and by worker-side
// dispatch.
//
// One Queue carries unlimited heterogeneous kinds over the shared fairness
// timeline; generics live only here, at the API boundary, and the queue core
// stays non-generic.
type Kind[T any] struct {
	kind string
}

// NewKind returns the binding of kind to body type T. It panics on an empty
// kind: a binding is declared once at init time, so an empty kind is a
// programmer error surfaced immediately, per the regexp.MustCompile
// precedent.
func NewKind[T any](kind string) Kind[T] {
	if kind == "" {
		panic("mongoqueue: NewKind called with empty kind")
	}
	return Kind[T]{kind: kind}
}

// Kind returns the binding's kind string -- the value stored in Job.Kind for
// every job the binding enqueues. Worker-side dispatch switches on the
// envelope's Kind field and calls the matching binding's Decode.
func (k Kind[T]) Kind() string {
	return k.kind
}

// Enqueue marshals body to BSON and enqueues it on q under the binding's
// kind. Callers never build raw BSON; the binding owns the marshal, so the
// stored body is always the BSON form of a T.
//
// Bodies must marshal to a BSON document: bson.Marshal rejects top-level
// scalars and arrays, and that error is surfaced as-is. See EnqueueOpts for
// the envelope fields and Queue-level errors (ErrMissingJobID,
// ErrInvalidWeight, ErrDuplicateJob).
func (k Kind[T]) Enqueue(ctx context.Context, q *Queue, body T, opts EnqueueOpts) error {
	raw, err := bson.Marshal(body)
	if err != nil {
		return fmt.Errorf("mongoqueue: marshal %q body: %w", k.kind, err)
	}
	return q.enqueue(ctx, k.kind, raw, opts)
}

// Decode unmarshals j's body as a T after asserting j.Kind matches the
// binding's kind. A mismatch returns ErrKindMismatch wrapped with both kind
// strings rather than silently decoding another kind's body into the wrong
// type. Decode assumes a facade-produced body: a record inserted outside the
// facade with a nil body decodes to T's zero value without error.
func (k Kind[T]) Decode(j *Job) (T, error) {
	var body T
	if j.Kind != k.kind {
		return body, fmt.Errorf("mongoqueue: decode job %q: stored kind %q, binding kind %q: %w",
			j.ID, j.Kind, k.kind, ErrKindMismatch)
	}
	if err := bson.Unmarshal(j.Body, &body); err != nil {
		return body, fmt.Errorf("mongoqueue: decode job %q body as kind %q: %w", j.ID, k.kind, err)
	}
	return body, nil
}
