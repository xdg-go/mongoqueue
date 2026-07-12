package mongoqueue

import "errors"

// ErrDuplicateJob signals an enqueue under an id already present.
// The stored record wins; match with errors.Is.
var ErrDuplicateJob = errors.New("mongoqueue: job id already exists")

// ErrNoJob signals an empty visible set in the partition -- expected, not
// failure. Returned by Claim; match with errors.Is. It plays the role of
// mongo.ErrNoDocuments at the queue boundary but does not wrap it.
var ErrNoJob = errors.New("mongoqueue: no claimable job")

// ErrJobNotFound signals a Get for an unknown job id; match with errors.Is.
var ErrJobNotFound = errors.New("mongoqueue: job not found")

// ErrInvalidWeight signals a negative EnqueueOpts.Weight; match with
// errors.Is. Weight must be >= 1; the zero value means 1.
var ErrInvalidWeight = errors.New("mongoqueue: weight must be >= 1 (zero means 1)")

// ErrEmptyResolution signals a Complete with an empty Resolution; match with
// errors.Is. Termination requires recording a resolution, so the zero value
// is rejected before any server round trip. (Cancel takes no resolution; it
// writes ResolutionCanceled unconditionally.)
var ErrEmptyResolution = errors.New("mongoqueue: resolution must be non-empty")
