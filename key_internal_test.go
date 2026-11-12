package saga

import (
	"math"
	"runtime"
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

// Keys from the arena are correct and stay unchanged while later keys fill
// the same chunk and new chunks.
func TestKeyArena(t *testing.T) {
	var a keyArena
	runID := "01968b1e-5c4a-7a3e-9f00-0123456789ab"
	long := strings.Repeat("x", keyChunk) // too long for a chunk: own allocation
	type issued struct {
		k    Key
		want string
	}
	var all []issued
	for seq := 1; seq <= 200; seq++ {
		name := "step." + strings.Repeat("n", seq%40)
		if seq%50 == 0 {
			name = long
		}
		k := a.key(runID, seq, name)
		all = append(all, issued{k, string(stepKey(runID, seq, name))})
	}
	a = keyArena{} // only the keys reference the chunks now
	runtime.GC()
	// Reuse freed memory of the chunk's size class if a chunk were wrongly
	// freed, with bytes that cannot match a key.
	junk := make([][]byte, 0, 4096)
	for range cap(junk) {
		b := make([]byte, keyChunk)
		for i := range b {
			b[i] = '#'
		}
		junk = append(junk, b)
	}
	runtime.GC()
	runtime.KeepAlive(junk)
	for i, x := range all {
		if string(x.k) != x.want || x.k.RunID() != runID {
			t.Fatalf("key %d = %.60q, want %.60q", i, x.k, x.want)
		}
	}
}

func TestKeyArenaAllocs(t *testing.T) {
	runID := "01968b1e-5c4a-7a3e-9f00-0123456789ab"
	n := testing.AllocsPerRun(100, func() {
		var a keyArena
		for seq := 1; seq <= 10; seq++ {
			_ = a.key(runID, seq, "payment.authorize")
		}
	})
	if n > 2 {
		t.Fatalf("10 keys cost %v allocations, want <= 2", n)
	}
}

// Edge cases of the room reserved per key: the longest seq, a key needing
// exactly a chunk, and keys that fill a chunk to the byte.
func TestKeyArenaEdges(t *testing.T) {
	runID := "r"
	var a keyArena
	for _, seq := range []int{math.MinInt, -1, 0, math.MaxInt} {
		if k, want := a.key(runID, seq, "s"), stepKey(runID, seq, "s"); k != want {
			t.Errorf("seq %d: %q, want %q", seq, k, want)
		}
	}
	exact := strings.Repeat("e", keyChunk-len(runID)-2-maxSeqDigits) // need == keyChunk
	if k, want := a.key(runID, 7, exact), stepKey(runID, 7, exact); k != want {
		t.Errorf("need == keyChunk: %.40q", k)
	}
	var b keyArena
	var keys []Key
	for seq := range 200 { // varied lengths hit every remaining-room case
		name := strings.Repeat("f", seq%97)
		keys = append(keys, b.key(runID, seq, name))
	}
	for seq, k := range keys {
		if want := stepKey(runID, seq, strings.Repeat("f", seq%97)); k != want {
			t.Fatalf("key %d changed: %.40q", seq, k)
		}
	}
}
