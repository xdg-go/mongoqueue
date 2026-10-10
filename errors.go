package mongoqueue

import "errors"

// ErrDuplicateJob signals an enqueue under an id already present.
// The stored record wins.
var ErrDuplicateJob = errors.New("mongoqueue: job id already exists")

// ErrMissingJobID signals an empty job id: an enqueue with an empty
// EnqueueOpts.JobID, or a Get or Cancel with an empty jobID (rejected before
// any server round trip). The job id is the record's _id and the idempotency
// key, so the library cannot invent one; the caller must supply it.
var ErrMissingJobID = errors.New("mongoqueue: job id is required")

// ErrNoJob signals an empty visible set in the partition -- expected, not
// failure. Returned by Claim. It plays the role of mongo.ErrNoDocuments at
// the queue boundary but does not wrap it.
var ErrNoJob = errors.New("mongoqueue: no claimable job")

// ErrJobNotFound signals a Get for an unknown job id.
var ErrJobNotFound = errors.New("mongoqueue: job not found")

// ErrLeaseLost signals that a claim is no longer held. Returned by
// Heartbeat, Release, and Complete on any acknowledged fence miss: the job
// was reclaimed by another worker, released, canceled, already completed, or
// deleted. A worker that needs to tell these apart calls Get and reads the
// record.
var ErrLeaseLost = errors.New("mongoqueue: lease lost")

// ErrJobResolved signals a Cancel whose pending-only update matched nothing
// while a record with that id existed at the diagnostic read. It does not
// prove the job (or the record now present) was resolved by anyone in
// particular, nor that this cancel won; a caller that needs to know inspects
// Resolution via Get.
var ErrJobResolved = errors.New("mongoqueue: job already resolved")

// ErrInvalidLease signals a non-positive lease duration: lease <= 0 on Claim
// or extend <= 0 on Heartbeat. A lease must push visibility into the future.
var ErrInvalidLease = errors.New("mongoqueue: lease must be > 0")

// ErrInvalidWeight signals a negative EnqueueOpts.Weight. Weight must be
// >= 1; the zero value means 1.
var ErrInvalidWeight = errors.New("mongoqueue: weight must be >= 1 (zero means 1)")

// ErrInvalidDelay signals a negative delay: EnqueueOpts.Delay or the delay
// passed to ClaimedJob.Release. Zero means immediately visible (for Release,
// immediately reclaimable); a job cannot be made visible in the past.
var ErrInvalidDelay = errors.New("mongoqueue: delay must be >= 0")

// ErrKindMismatch signals a Decode through a binding whose kind does not
// match the job's stored kind -- kind↔type drift caught at the boundary
// instead of silently decoding one kind's body as another's type. The
// wrapping error carries both kind strings; match with errors.Is.
var ErrKindMismatch = errors.New("mongoqueue: job kind does not match binding")

// ErrEmptyResolution signals a Complete with an empty Resolution.
// Termination requires recording a resolution, so the zero value is rejected
// before any server round trip. (Cancel takes no resolution; it writes
// ResolutionCanceled unconditionally.)
var ErrEmptyResolution = errors.New("mongoqueue: resolution must be non-empty")
