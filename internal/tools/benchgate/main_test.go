package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const sampleBench = `goos: linux
goarch: amd64
pkg: github.com/imdat99/saga
BenchmarkStepRun-6              	     500	  22015216 ns/op	         1.950 p50-ms/step	         2.546 p95-ms/step	         2.989 p99-ms/step	    3630 B/op	      40 allocs/op
BenchmarkStepRun-6              	     500	  22015216 ns/op	         1.950 p50-ms/step	         9.000 p95-ms/step	         2.989 p99-ms/step	    3630 B/op	      41 allocs/op
BenchmarkStepRun-6              	     500	  22015216 ns/op	         1.950 p50-ms/step	         2.600 p95-ms/step	         2.989 p99-ms/step	    3630 B/op	      40 allocs/op
BenchmarkRunsThreeInstances-6   	     500	   4834277 ns/op	         9.777 p50-ms/step	        17.14 p95-ms/step	        21.14 p99-ms/step	       206.9 runs/s	    7815 B/op	      95 allocs/op
BenchmarkStepRun log line without numbers
ok  	github.com/imdat99/saga	84.055s
`

func TestParseBench(t *testing.T) {
	res, err := parseBench(strings.NewReader(sampleBench))
	if err != nil {
		t.Fatal(err)
	}
	if got := res["BenchmarkStepRun"]["allocs/op"]; len(got) != 3 || got[1] != 41 {
		t.Fatalf("allocs/op = %v", got)
	}
	if got := res["BenchmarkRunsThreeInstances"]["runs/s"]; len(got) != 1 || got[0] != 206.9 {
		t.Fatalf("runs/s = %v", got)
	}
}

func TestMedianIgnoresOneOutlier(t *testing.T) {
	if m := median([]float64{2.546, 9.0, 2.6}); m != 2.6 {
		t.Fatalf("median = %v", m)
	}
	if m := median([]float64{1, 2, 3, 10}); m != 2.5 {
		t.Fatalf("median = %v", m)
	}
}

func TestEvaluate(t *testing.T) {
	res, err := parseBench(strings.NewReader(sampleBench))
	if err != nil {
		t.Fatal(err)
	}
	rules, err := parseBudget(strings.NewReader(`
# comment
BenchmarkStepRun            allocs/op    <= 45
BenchmarkStepRun            p95-ms/step  <= 5    # median 2.6: the 9.0 outlier does not fail it
BenchmarkRunsThreeInstances runs/s       >= 300
BenchmarkStepRun            ns/op        <= 1000
BenchmarkGone               allocs/op    <= 1
BenchmarkStepRun            widgets/op   <= 1
`))
	if err != nil {
		t.Fatal(err)
	}
	want := []bool{true, true, false, false, false, false}
	got := evaluate(res, rules)
	for i, v := range got {
		if v.ok != want[i] {
			t.Errorf("rule %d (%s %s): ok = %v, want %v (value %v, note %q)", i, v.bench, v.unit, v.ok, want[i], v.value, v.note)
		}
	}
	var buf bytes.Buffer
	if report(&buf, got) {
		t.Fatal("report passed with failing rules")
	}
	if !strings.Contains(buf.String(), "benchmark not in the output") || !strings.Contains(buf.String(), "unit not reported") {
		t.Fatalf("report does not explain missing metrics:\n%s", buf.String())
	}
}

func TestParseBudgetErrors(t *testing.T) {
	for _, in := range []string{"", "# only comments\n", "BenchmarkX allocs/op < 3\n", "BenchmarkX allocs/op <= x\n", "BenchmarkX <= 3\n"} {
		if _, err := parseBudget(strings.NewReader(in)); err == nil {
			t.Errorf("budget %q accepted", in)
		}
	}
}

func TestRun(t *testing.T) {
	dir := t.TempDir()
	bench := filepath.Join(dir, "bench.txt")
	budget := filepath.Join(dir, "budget.txt")
	summary := filepath.Join(dir, "summary.md")
	if err := os.WriteFile(bench, []byte(sampleBench), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(budget, []byte("BenchmarkStepRun allocs/op <= 45\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := run([]string{"-budget", budget, "-summary", summary, bench}, &out); err != nil {
		t.Fatalf("run: %v\n%s", err, out.String())
	}
	if b, err := os.ReadFile(summary); err != nil || !strings.Contains(string(b), "| BenchmarkStepRun | allocs/op | 40 |") {
		t.Fatalf("summary = %q, %v", b, err)
	}
	if err := os.WriteFile(budget, []byte("BenchmarkStepRun allocs/op <= 30\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-budget", budget, bench}, &out); err == nil {
		t.Fatal("run passed an exceeded budget")
	}
}
