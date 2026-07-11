package mongoqueue

import "testing"

// TestPlaceholder is a trivial placeholder so `go test ./...` runs and passes
// before any real tests exist. It is deleted in subsection 1.2 once the Mongo
// test harness lands.
func TestPlaceholder(t *testing.T) {
	if 1+1 != 2 {
		t.Fatal("arithmetic is broken")
	}
}
