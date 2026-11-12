// Command benchgate checks `go test -bench` output against a performance
// budget and fails when a metric is over (or under) its limit.
//
//	go test -run '^$' -bench . -benchtime 500x -count 5 . | tee bench.txt
//	go run ./internal/tools/benchgate -budget .github/perf-budget.txt bench.txt
//
// Each metric is the median of the runs of a benchmark (-count), which keeps
// one noisy run on a shared CI machine from failing the gate. Allocation
// counts are nearly deterministic and make the strictest gates; latencies get
// generous limits.
//
// Budget file: one rule per line, "<benchmark> <unit> <op> <limit>", with op
// "<=" or ">=" and the benchmark name without its -GOMAXPROCS suffix; '#'
// starts a comment. A rule whose benchmark or unit is missing from the output
// fails: a renamed benchmark must not silently disable its gate.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// rule is one line of the budget.
type rule struct {
	bench, unit, op string
	limit           float64
}

// results maps benchmark -> unit -> values of every run.
type results map[string]map[string][]float64

// procSuffix is the "-6" GOMAXPROCS suffix go test appends to names.
var procSuffix = regexp.MustCompile(`-\d+$`)

// parseBench reads benchmark lines: name, iterations, then value/unit pairs.
func parseBench(r io.Reader) (results, error) {
	res := results{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) < 4 || !strings.HasPrefix(f[0], "Benchmark") {
			continue
		}
		if _, err := strconv.Atoi(f[1]); err != nil {
			continue // not a result line (e.g. a log line starting with the name)
		}
		name := procSuffix.ReplaceAllString(f[0], "")
		if res[name] == nil {
			res[name] = map[string][]float64{}
		}
		for i := 2; i+1 < len(f); i += 2 {
			v, err := strconv.ParseFloat(f[i], 64)
			if err != nil {
				return nil, fmt.Errorf("%s: value %q: %w", name, f[i], err)
			}
			res[name][f[i+1]] = append(res[name][f[i+1]], v)
		}
	}
	return res, sc.Err()
}

// parseBudget reads the rules.
func parseBudget(r io.Reader) ([]rule, error) {
	var rules []rule
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		line, _, _ := strings.Cut(sc.Text(), "#")
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		if len(f) != 4 || (f[2] != "<=" && f[2] != ">=") {
			return nil, fmt.Errorf("budget line %d: want \"<benchmark> <unit> <= or >= <limit>\"", n)
		}
		limit, err := strconv.ParseFloat(f[3], 64)
		if err != nil {
			return nil, fmt.Errorf("budget line %d: limit: %w", n, err)
		}
		rules = append(rules, rule{bench: f[0], unit: f[1], op: f[2], limit: limit})
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(rules) == 0 {
		return nil, errors.New("budget has no rules")
	}
	return rules, nil
}

func median(v []float64) float64 {
	s := slices.Clone(v)
	slices.Sort(s)
	n := len(s)
	if n%2 == 1 {
		return s[n/2]
	}
	return (s[n/2-1] + s[n/2]) / 2
}

// verdict is the outcome of one rule.
type verdict struct {
	rule
	value float64
	runs  int
	ok    bool
	note  string
}

func evaluate(res results, rules []rule) []verdict {
	out := make([]verdict, 0, len(rules))
	for _, r := range rules {
		v := verdict{rule: r}
		vals := res[r.bench][r.unit]
		switch {
		case res[r.bench] == nil:
			v.note = "benchmark not in the output"
		case len(vals) == 0:
			v.note = "unit not reported"
		default:
			v.value, v.runs = median(vals), len(vals)
			v.ok = (r.op == "<=" && v.value <= r.limit) || (r.op == ">=" && v.value >= r.limit)
		}
		out = append(out, v)
	}
	return out
}

// report writes a markdown table (also suitable for $GITHUB_STEP_SUMMARY)
// and returns whether every rule passed.
func report(w io.Writer, verdicts []verdict) bool {
	all := true
	fmt.Fprintln(w, "| Benchmark | Metric | Median | Budget | Runs | Result |")
	fmt.Fprintln(w, "| --- | --- | --- | --- | --- | --- |")
	for _, v := range verdicts {
		res := "pass"
		if !v.ok {
			res, all = "FAIL", false
			if v.note != "" {
				res += ": " + v.note
			}
		}
		fmt.Fprintf(w, "| %s | %s | %s | %s %s | %d | %s |\n",
			v.bench, v.unit, strconv.FormatFloat(v.value, 'g', 6, 64), v.op,
			strconv.FormatFloat(v.limit, 'g', 6, 64), v.runs, res)
	}
	return all
}

func run(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("benchgate", flag.ContinueOnError)
	budgetPath := fs.String("budget", ".github/perf-budget.txt", "budget file")
	summary := fs.String("summary", "", "also append the report to this file (e.g. $GITHUB_STEP_SUMMARY)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: benchgate [-budget file] [-summary file] bench-output.txt")
	}
	bf, err := os.Open(*budgetPath)
	if err != nil {
		return err
	}
	defer bf.Close()
	rules, err := parseBudget(bf)
	if err != nil {
		return err
	}
	in, err := os.Open(fs.Arg(0))
	if err != nil {
		return err
	}
	defer in.Close()
	res, err := parseBench(in)
	if err != nil {
		return err
	}

	verdicts := evaluate(res, rules)
	out := stdout
	if *summary != "" {
		sf, err := os.OpenFile(*summary, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644) //nolint:gosec // path given by the CI
		if err != nil {
			return err
		}
		defer sf.Close()
		fmt.Fprintln(sf, "### Performance budget")
		fmt.Fprintln(sf)
		out = io.MultiWriter(stdout, sf)
	}
	if !report(out, verdicts) {
		return errors.New("performance budget exceeded")
	}
	return nil
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "benchgate:", err)
		os.Exit(1)
	}
}
