package mongoqueue

import (
	"fmt"
	"math"
)

// Virtual time (vstamps and per-(tenant, partition) vtimes) is fixed-point:
// an int64 counting units of 1/scale, i.e. 1e-3 cost-units. Fixed-point
// integer math keeps stamping deterministic and cheap, and gives sub-unit
// resolution so that weights above cost still yield distinct strides.
//
// scale splits the int64 range between sub-unit precision and headroom.
// Cost units are abstract, so the caller's choice of unit and scale are
// interchangeable; scale only sets the default precision for small integer
// costs. At 1e3, truncation error in a stride is below weight/(cost*1000),
// under 1% while weight <= 10*cost, and a partition's virtual clock holds
// 2^63/1e3 ~ 9.2e15 cumulative cost/weight: about 292 years at a sustained
// 10^6 cost-units/sec at weight 1. Memoryless flooring carries each new
// timeline up to its partition's clock, so the budget is per partition, not
// per tenant. nextVStamp guards the arithmetic so exhausting it fails the
// enqueue instead of wrapping to a negative vstamp that would sort first.
const scale = 1_000

// maxCost is the largest normalized cost whose cost*scale fits in int64.
const maxCost = math.MaxInt64 / scale

// stride returns the virtual-time advance for one enqueue:
// max(1, cost*scale/weight) in pure integer arithmetic.
//
// The max(1, ...) floor preserves the strictly-increasing-within-tenant
// invariant: integer division can yield 0 at extreme cost/weight ratios
// (cost*scale < weight), and a zero stride would let successive vstamps on a
// tenant's timeline repeat. Flooring at one fixed-point unit keeps every
// advance strictly positive.
//
// Callers must pass a normalized cost in [1, maxCost] (see normalizeCost and
// nextVStamp) and a normalized weight (>= 1, see normalizeWeight).
func stride(cost, weight int64) int64 {
	return max(1, cost*scale/weight)
}

// nextVStamp returns vtime + stride(cost, weight), or an error wrapping
// ErrVTimeOverflow if cost*scale or the sum would exceed int64. cost and
// weight must already be normalized.
func nextVStamp(vtime, cost, weight int64) (int64, error) {
	if cost > maxCost {
		return 0, fmt.Errorf("%w: cost %d exceeds %d", ErrVTimeOverflow, cost, int64(maxCost))
	}
	s := stride(cost, weight)
	if vtime > math.MaxInt64-s {
		return 0, fmt.Errorf("%w: vtime %d + stride %d", ErrVTimeOverflow, vtime, s)
	}
	return vtime + s, nil
}

// normalizeCost floors a caller-supplied cost to the small positive minimum
// of 1, so enqueue never rejects a cost: zero and negative values mean "the
// cheapest possible job" rather than an error.
func normalizeCost(cost int64) int64 {
	return max(1, cost)
}

// normalizeWeight validates and normalizes a caller-supplied weight: the zero
// value means 1 (the single-tenant / don't-care case configures nothing),
// values >= 1 pass through, and negative values return ErrInvalidWeight.
// Unlike cost, a negative weight is rejected rather than floored -- it is a
// configuration error, and silently clamping would hide a miswired producer.
func normalizeWeight(weight int64) (int64, error) {
	switch {
	case weight == 0:
		return 1, nil
	case weight < 0:
		return 0, ErrInvalidWeight
	default:
		return weight, nil
	}
}
