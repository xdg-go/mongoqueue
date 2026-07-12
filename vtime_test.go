package mongoqueue

import (
	"errors"
	"testing"
)

// TestStride exercises stride over typical cost/weight combinations, extreme
// ratios in both directions, and the max(1, ...) floor. Expected values are
// independently computed literals so a formula bug in stride cannot hide
// behind the same formula in the test. Inputs are pre-normalized (>= 1) per
// stride's documented precondition; zero-means-1 weight semantics belong to
// normalizeWeight and are tested there.
func TestStride(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		cost   int64
		weight int64
		want   int64
	}{
		// Typical ratios.
		{"unit cost, unit weight", 1, 1, 1_000_000},
		{"cost 5, weight 2", 5, 2, 2_500_000},
		{"cost 10, weight 3 truncates", 10, 3, 3_333_333},
		{"cost 2, weight 3 truncates", 2, 3, 666_666},
		{"cost 100, weight 7 truncates", 100, 7, 14_285_714},

		// Extreme ratios.
		{"huge cost, weight 1", 9_000_000_000_000, 1, 9_000_000_000_000_000_000},
		{"cost 1, huge weight floors", 1, 2_000_000, 1},
		{"cost 3, weight far above cost*scale floors", 3, 5_000_000, 1},

		// Floor boundary: quotient exactly 1, then just below and above.
		{"quotient exactly 1", 1, 1_000_000, 1},
		{"quotient just above 1 truncates to 1", 1, 999_999, 1},
		{"quotient 0 floors to 1", 1, 1_000_001, 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := stride(tc.cost, tc.weight); got != tc.want {
				t.Errorf("stride(%d, %d) = %d, want %d", tc.cost, tc.weight, got, tc.want)
			}
		})
	}
}

// TestStrideComposedWithNormalizeWeight covers the real enqueue-path
// composition: a caller-supplied zero weight passes through normalizeWeight
// (zero means 1) before reaching stride.
func TestStrideComposedWithNormalizeWeight(t *testing.T) {
	t.Parallel()

	w, err := normalizeWeight(0)
	if err != nil {
		t.Fatalf("normalizeWeight(0) returned unexpected error: %v", err)
	}
	if got, want := stride(7, w), int64(7_000_000); got != want {
		t.Errorf("stride(7, normalizeWeight(0)) = %d, want %d", got, want)
	}
}

// TestNormalizeWeight verifies the zero-means-1 mapping, pass-through of
// weights >= 1, and rejection of negative weights with the ErrInvalidWeight
// sentinel. Fractional weights between 0 and 1 are unrepresentable in int64,
// so there is no clamping-of-fractions case to test.
func TestNormalizeWeight(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		weight  int64
		want    int64
		wantErr error
	}{
		{"zero means 1", 0, 1, nil},
		{"one passes through", 1, 1, nil},
		{"large passes through", 1_000_000_000, 1_000_000_000, nil},
		{"negative rejected", -1, 0, ErrInvalidWeight},
		{"large negative rejected", -1_000_000, 0, ErrInvalidWeight},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := normalizeWeight(tc.weight)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("normalizeWeight(%d) error = %v, want %v", tc.weight, err, tc.wantErr)
			}
			if err == nil && got != tc.want {
				t.Errorf("normalizeWeight(%d) = %d, want %d", tc.weight, got, tc.want)
			}
		})
	}
}

// TestNormalizeCost verifies that zero and negative costs floor to 1 (the
// cheapest possible job, never an error) and that costs >= 1 pass through
// unchanged.
func TestNormalizeCost(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		cost int64
		want int64
	}{
		{"zero floors to 1", 0, 1},
		{"negative floors to 1", -5, 1},
		{"large negative floors to 1", -1_000_000_000, 1},
		{"one passes through", 1, 1},
		{"typical passes through", 42, 42},
		{"large passes through", 9_000_000_000_000, 9_000_000_000_000},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := normalizeCost(tc.cost); got != tc.want {
				t.Errorf("normalizeCost(%d) = %d, want %d", tc.cost, got, tc.want)
			}
		})
	}
}

// TestVTimeStrictlyIncreasing checks the strictly-increasing-within-tenant
// invariant: repeatedly advancing a vtime by stride never repeats or
// regresses, from several starting points, including combinations where the
// max(1, ...) floor makes every advance the minimum fixed-point unit.
func TestVTimeStrictlyIncreasing(t *testing.T) {
	t.Parallel()

	const iterations = 5000

	starts := []int64{0, 1, 1 << 40}
	combos := []struct {
		name   string
		cost   int64
		weight int64
	}{
		{"unit cost, unit weight", 1, 1},
		{"cost 5, weight 2", 5, 2},
		{"floor active: cost 1, weight far above scale", 1, 10_000_000},
	}

	for _, combo := range combos {
		t.Run(combo.name, func(t *testing.T) {
			t.Parallel()
			for _, start := range starts {
				vtime := start
				for i := range iterations {
					next := vtime + stride(combo.cost, combo.weight)
					if next <= vtime {
						t.Fatalf("vtime did not strictly increase at start=%d iteration %d: %d -> %d",
							start, i, vtime, next)
					}
					vtime = next
				}
			}
		})
	}
}
