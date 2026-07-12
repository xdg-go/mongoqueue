package mongoqueue

// Virtual time (vstamps and per-tenant vtimes) is fixed-point: an int64
// counting units of 1/scale, i.e. 1e-6 cost-units. Fixed-point integer math
// keeps stamping deterministic and cheap, and gives sub-unit resolution so
// that high weights still yield distinct strides.
//
// Overflow is documented out of reach rather than guarded in code: at a
// sustained 10^6 cost-units/sec at weight 1, an int64 vtime lasts roughly
// 292,000 years.
const scale = 1_000_000

// stride returns the virtual-time advance for one enqueue:
// max(1, cost*scale/weight) in pure integer arithmetic.
//
// The max(1, ...) floor preserves the strictly-increasing-within-tenant
// invariant: integer division can yield 0 at extreme cost/weight ratios
// (cost*scale < weight), and a zero stride would let successive vstamps on a
// tenant's timeline repeat. Flooring at one fixed-point unit keeps every
// advance strictly positive.
//
// Callers must pass a normalized cost (>= 1, see normalizeCost) and a
// normalized weight (>= 1, see normalizeWeight). The product cost*scale
// overflows int64 only when cost exceeds about 9.2e12 (2^63/scale), far past
// any realistic per-job cost; per the design docs this bound is documented,
// not guarded.
func stride(cost, weight int64) int64 {
	return max(1, cost*scale/weight)
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
