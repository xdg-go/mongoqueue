package mongoqueue

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Liveness is the two-value lifecycle state of a job: pending (awaiting or
// undergoing service) or resolved (terminal). There is no separate "claimed"
// state -- a claimed job is pending with a live lease, and an expired lease
// re-enters the visible set with no state transition.
//
// Liveness is stored under the BSON field "liveness", always present. Claim,
// heartbeat, complete, release, and cancel are server-side conditional writes
// that filter on the pending literal, so the stored value must match the
// constant exactly.
type Liveness string

const (
	// LivenessPending marks a job awaiting or undergoing service. Every
	// guarded write (claim, heartbeat, complete, release, cancel) filters
	// on this literal server-side.
	LivenessPending Liveness = "pending"

	// LivenessResolved marks a terminal job. Exactly one terminal write
	// (complete or cancel) wins the transition; a resolved job never
	// re-enters the visible set.
	LivenessResolved Liveness = "resolved"
)

// Resolution records how a job ended. The vocabulary is open: the library
// provides two canonical values (ResolutionCompleted, ResolutionCanceled) and
// callers are free define their own. Resolution is set by the terminal write
// (Complete or Cancel) and is absent while pending.
//
// There is no ResolutionFailed, because the queue library has no failure/retry
// concept. A failure the caller intends to retry is a Release (which returns
// the job to the visible set), not a Complete with a "failed" resolution.
// Callers are free to define their own "failed" resolution for logging or
// reporting; the queue library does not interpret it.
//
// Termination requires recording a resolution: Complete rejects an empty
// value with ErrEmptyResolution, and Cancel writes ResolutionCanceled
// unconditionally.
//
// Keep caller-defined values low-cardinality, stable, and machine-matchable.
// Human-readable detail (error text, hostnames) does not belong here.
type Resolution string

const (
	// ResolutionCompleted is the default vocabulary for a job its lease
	// holder finished normally. Callers with a richer outcome vocabulary
	// pass their own values to Complete instead.
	ResolutionCompleted Resolution = "completed"

	// ResolutionCanceled is written by Cancel unconditionally -- the
	// canceller supplies no resolution. By convention callers never pass
	// this value to Complete, which keeps a stored "canceled" a reliable
	// marker of the cancel path.
	ResolutionCanceled Resolution = "canceled"
)

// Job is the durable record of an enqueued job: the queue-interpreted
// envelope plus the caller's opaque payload.
//
// A Job returned by the library is a detached, read-only snapshot of the
// stored record. Mutating its fields touches only the in-memory copy and has
// no effect on the store -- every guard that matters (claim fencing, liveness
// transitions) is enforced server-side by conditional writes against durable
// truth, so a forged value can only make the caller's own write fail the
// fence. Callers never construct a Job: enqueue input is the separate
// EnqueueOpts struct, and the library computes the rest.
//
// Fields are exported because the MongoDB Go driver marshals only exported
// fields; the bson tags give the wire names.
type Job struct {
	// ID is the caller-supplied job id, serving as both the record's
	// Mongo "_id" and the idempotency key for enqueue.
	ID string `bson:"_id"`

	// TenantID identifies the tenant whose virtual timeline the job is
	// stamped on. Stored under the BSON field "tenant".
	TenantID string `bson:"tenant"`

	// Partition scopes claims; the empty string at enqueue means the
	// default partition.
	Partition string `bson:"partition"`

	// Cost is the caller-declared cost used to compute the job's stride
	// on the tenant's virtual timeline (floored to a small positive
	// minimum at enqueue).
	Cost int64 `bson:"cost"`

	// VStamp is the job's virtual timestamp: fixed-point virtual time as
	// an int64. Workers claim the lowest visible (VStamp, ID) pair.
	VStamp int64 `bson:"vstamp"`

	// Liveness is the job's lifecycle state, always present; guarded
	// writes filter on the pending literal server-side.
	Liveness Liveness `bson:"liveness"`

	// Resolution records how the job ended, in a caller-defined
	// vocabulary. Set by the terminal write (Complete or Cancel); absent
	// while pending.
	Resolution Resolution `bson:"resolution,omitempty"`

	// ClaimID is the fenced claim identifier minted at dispatch. It is
	// never empty while a claim is live; Release clears it.
	ClaimID string `bson:"claim_id"`

	// VisibleAt gates claim eligibility: a pending job is claimable only
	// once VisibleAt has passed. A claim pushes it forward (the lease),
	// Heartbeat extends it, and Release resets it to now plus a
	// caller-chosen delay.
	VisibleAt time.Time `bson:"visible_at"`

	// Attempts counts claims of this job, incremented at each dispatch.
	Attempts int `bson:"attempts"`

	// StampedBy is the id of the node that stamped the job's VStamp --
	// the hook for detecting unintended multi-writer enqueue for a
	// tenant.
	StampedBy string `bson:"stamped_by"`

	// ResolvedAt is set by the terminal write and keys TTL garbage
	// collection of resolved jobs. Absent (zero) while pending.
	ResolvedAt time.Time `bson:"resolved_at,omitempty"`

	// Kind is the decode discriminator -- the body's content-type,
	// keying the typed Enqueue/Decode facade. Never interpreted by the
	// queue core.
	Kind string `bson:"kind"`

	// Body is the caller's domain data, stored as a native BSON
	// subdocument: opaque to queue code, queryable in the database.
	Body bson.Raw `bson:"body"`
}
