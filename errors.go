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
