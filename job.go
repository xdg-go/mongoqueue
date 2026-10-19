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
// Liveness is stored under the BSON field "liveness", always present, and the
// stored value matches the constants exactly (e.g. for caller queries).
type Liveness string

const (
	// LivenessPending marks a job awaiting or undergoing service.
	LivenessPending Liveness = "pending"

	// LivenessResolved marks a terminal job. Exactly one terminal write
	// (complete or cancel) wins the transition; a resolved job never
	// re-enters the visible set.
	LivenessResolved Liveness = "resolved"
)

// Resolution records how a job ended. It is set by the terminal write
// (Complete or Cancel) and is absent while pending. Complete rejects an empty
// value with ErrEmptyResolution; Cancel writes ResolutionCanceled.
//
// The vocabulary is open: beyond the two canonical values, callers may define
// their own, including a "failed" one for reporting, which the library does
// not interpret. There is no ResolutionFailed because the library has no
// retry concept: a failure to retry is a Release, not a Complete.
//
// Keep caller-defined values low-cardinality, stable, and machine-matchable;
// human-readable detail (error text, hostnames) does not belong here.
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

// Job is the durable record of an enqueued job: the library-owned envelope
// (fields the library writes, and may guard or sort on) plus the payload
// (the kind discriminator and the caller's opaque body).
//
// A Job returned by the library is a detached, read-only snapshot of the
// stored record. Mutating it changes nothing server-side. Callers never
// construct a Job: enqueue input is the separate
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
	// on its (tenant, partition) timeline (floored to a small positive
	// minimum at enqueue).
	Cost int64 `bson:"cost"`

	// VStamp is the job's virtual timestamp: fixed-point virtual time as
	// an int64. Workers claim the lowest visible (VStamp, ID) pair.
	VStamp int64 `bson:"vstamp"`

	// Liveness is the job's lifecycle state, always present.
	Liveness Liveness `bson:"liveness"`

	// Resolution records how the job ended, in a caller-defined
	// vocabulary. Set by the terminal write (Complete or Cancel); absent
	// while pending.
	Resolution Resolution `bson:"resolution,omitempty"`

	// ClaimID identifies the current claim; each claim assigns a new one.
	// It is never empty while a claim is live; Release clears it.
	ClaimID string `bson:"claim_id"`

	// VisibleAt gates claim eligibility: a pending job is claimable only
	// once VisibleAt has passed. A claim pushes it forward (the lease),
	// Heartbeat extends it, and Release resets it to now plus a
	// caller-chosen delay.
	VisibleAt time.Time `bson:"visible_at"`

	// Attempts counts claims of this job, incremented at each dispatch.
	// The queue never caps it: a job whose worker keeps failing or dying
	// is reclaimed indefinitely, so a worker that wants bounded retries
	// checks Attempts after Claim and resolves the job itself once it
	// reaches the caller's limit.
	Attempts int `bson:"attempts"`

	// StampedBy is the id of the node that stamped the job's VStamp --
	// the hook for detecting unintended multi-writer enqueue on a
	// (tenant, partition) timeline.
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
