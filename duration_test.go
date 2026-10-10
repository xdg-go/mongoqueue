package mongoqueue

import (
	"math"
	"testing"
	"time"
)

func TestDurationMillis(t *testing.T) {
	cases := []struct {
		name string
		d    time.Duration
		want int64
	}{
		{"zero", 0, 0},
		{"1ns rounds up", time.Nanosecond, 1},
		{"999us rounds up", 999 * time.Microsecond, 1},
		{"1ms exact", time.Millisecond, 1},
		{"1500us rounds up", 1500 * time.Microsecond, 2},
		{"max int64 no overflow", time.Duration(math.MaxInt64), 9223372036855},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := durationMillis(tc.d); got != tc.want {
				t.Errorf("durationMillis(%v) = %d, want %d", tc.d, got, tc.want)
			}
		})
	}
}
