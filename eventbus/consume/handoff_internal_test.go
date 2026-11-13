package consume

import (
	"testing"
	"time"
)

func TestHandOffDelayGrowsWithAge(t *testing.T) {
	for _, c := range []struct {
		age, want time.Duration
	}{
		{0, 500 * time.Millisecond},
		{3 * time.Second, 500 * time.Millisecond},
		{20 * time.Second, 2 * time.Second},
		{5 * time.Minute, 30 * time.Second},
		{time.Hour, 30 * time.Second},
	} {
		if got := handOffDelay(c.age); got != c.want {
			t.Errorf("handOffDelay(%v) = %v, want %v", c.age, got, c.want)
		}
	}
}
