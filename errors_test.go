package mongoqueue

import (
	"errors"
	"testing"
)

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	sentinels := []struct {
		name string
		err  error
	}{
		{"ErrDuplicateJob", ErrDuplicateJob},
		{"ErrNoJob", ErrNoJob},
		{"ErrJobNotFound", ErrJobNotFound},
		{"ErrInvalidWeight", ErrInvalidWeight},
		{"ErrEmptyResolution", ErrEmptyResolution},
	}

	for _, s := range sentinels {
		for _, other := range sentinels {
			got := errors.Is(s.err, other.err)
			want := s.name == other.name
			if got != want {
				t.Errorf("errors.Is(%s, %s) = %v, want %v", s.name, other.name, got, want)
			}
		}
	}
}
