package saga

import (
	"strings"
	"testing"
)

func TestStepKey(t *testing.T) {
	long := strings.Repeat("n", 300) // longer than the stack buffer
	for _, c := range []struct {
		run  string
		seq  int
		name string
		want string
	}{
		{"0196-run", 1, "pay", "0196-run:1:pay"},
		{"r", 123456, "a.b", "r:123456:a.b"},
		{"r", 2, long, "r:2:" + long},
	} {
		k := stepKey(c.run, c.seq, c.name)
		if string(k) != c.want || k.RunID() != c.run {
			t.Errorf("stepKey(%q, %d, %.10q) = %.40q (run %q)", c.run, c.seq, c.name, k, k.RunID())
		}
	}
}

func TestStepKeyAllocs(t *testing.T) {
	runID := "01968b1e-5c4a-7a3e-9f00-0123456789ab"
	if n := testing.AllocsPerRun(100, func() { _ = stepKey(runID, 42, "payment.authorize") }); n != 1 {
		t.Fatalf("stepKey allocates %v times, want 1", n)
	}
}
