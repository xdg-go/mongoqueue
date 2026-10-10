package mongoqueue

import "time"

// durationMillis converts d to whole milliseconds, the resolution of BSON
// dates and $$NOW arithmetic. Zero stays zero; a positive duration rounds up
// so a sub-millisecond lease or delay never collapses to 0ms. Quotient plus
// remainder avoids the overflow that adding (ms - 1) before dividing would hit
// at math.MaxInt64. Callers validate the sign before converting.
func durationMillis(d time.Duration) int64 {
	ms := int64(d / time.Millisecond)
	if d%time.Millisecond != 0 {
		ms++
	}
	return ms
}
