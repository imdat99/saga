package saga_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/internal/testdb"
)

// Benchmarks run against the testcontainers Postgres (or SAGA_TEST_DSN):
//
//	go test -run '^$' -bench . -benchtime 2000x -count 5 . | tee bench.txt
//
// Do not run them while other tests use the same machine.
//
// SAGA_BENCH_ASYNC_COMMIT=1 runs the engine's sessions with
// synchronous_commit=off, to tell commit (fsync) cost from round trips.
// SAGA_BENCH_ASYNC_START=1 sets Options.AsyncStepStart.
// SAGA_BENCH_MAX_CONNS=n sets the engines' pool size (default: pgx's,
// max(4, GOMAXPROCS)).

const benchSteps = 10

func benchEngine(b *testing.B, pool *pgxpool.Pool) *saga.Engine {
	b.Helper()
	eng, err := saga.New(pool, saga.Options{
		Logger:         slog.New(slog.DiscardHandler),
		AsyncStepStart: os.Getenv("SAGA_BENCH_ASYNC_START") == "1",
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = eng.Stop(ctx)
	})
	return eng
}

// benchPool opens a separate pool on the database of pool, honouring
// SAGA_BENCH_ASYNC_COMMIT.
func benchPool(b *testing.B, pool *pgxpool.Pool) *pgxpool.Pool {
	b.Helper()
	cfg, err := pgxpool.ParseConfig(testdb.DSN(pool))
	if err != nil {
		b.Fatal(err)
	}
	if os.Getenv("SAGA_BENCH_ASYNC_COMMIT") == "1" {
		cfg.ConnConfig.RuntimeParams["synchronous_commit"] = "off"
	}
	if n, err := strconv.Atoi(os.Getenv("SAGA_BENCH_MAX_CONNS")); err == nil && n > 0 {
		cfg.MaxConns = int32(n) //nolint:gosec // small test setting
	}
	p, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(p.Close)
	return p
}

// latencies collects per-step durations measured around Step.Run.
type latencies struct {
	mu sync.Mutex
	d  []time.Duration
}

func (l *latencies) add(d time.Duration) {
	l.mu.Lock()
	l.d = append(l.d, d)
	l.mu.Unlock()
}

func (l *latencies) report(b *testing.B) {
	b.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.d) == 0 {
		return
	}
	slices.Sort(l.d)
	q := func(p float64) float64 { return float64(l.d[int(p*float64(len(l.d)-1))].Microseconds()) / 1000 }
	b.ReportMetric(q(0.50), "p50-ms/step")
	b.ReportMetric(q(0.95), "p95-ms/step")
	b.ReportMetric(q(0.99), "p99-ms/step")
}

// timedWorkflow runs benchSteps no-op steps, timing each Step.Run: what is
// measured is the engine's overhead (two fenced checkpoints per step).
func timedWorkflow(name string, lat *latencies) *saga.Workflow[int, int] {
	noop := saga.DefineStep(name+".noop",
		func(ctx context.Context, k saga.Key, v int) (int, struct{}, error) { return v + 1, struct{}{}, nil }, nil)
	return saga.DefineWorkflow(name, 1, func(c saga.Ctx, v int) (int, error) {
		for range benchSteps {
			start := time.Now()
			var err error
			if v, err = noop.Run(c, v); err != nil {
				return 0, err
			}
			lat.add(time.Since(start))
		}
		return v, nil
	})
}

var benchKeys atomic.Int64

// BenchmarkStepRun measures the per-step overhead of an inline run on one
// instance (target from the plan: p95 < 5ms on a local Postgres).
func BenchmarkStepRun(b *testing.B) {
	pool := testdb.New(b)
	if err := saga.Migrate(context.Background(), pool); err != nil {
		b.Fatal(err)
	}
	eng := benchEngine(b, benchPool(b, pool))
	lat := &latencies{}
	wf := timedWorkflow(testdb.UniqueName(b, "wf"), lat)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := wf.Run(ctx, eng, fmt.Sprint("k", benchKeys.Add(1)), 0); err != nil {
			b.Fatal(err)
		}
	}
	lat.report(b)
}

// BenchmarkRunsThreeInstances measures throughput of runs spread over three
// engines (separate pools) sharing one database, with concurrent callers.
func BenchmarkRunsThreeInstances(b *testing.B) {
	pool := testdb.New(b)
	if err := saga.Migrate(context.Background(), pool); err != nil {
		b.Fatal(err)
	}
	engines := make([]*saga.Engine, 3)
	for i := range engines {
		engines[i] = benchEngine(b, benchPool(b, pool))
	}
	lat := &latencies{}
	wf := timedWorkflow(testdb.UniqueName(b, "wf"), lat)
	var next atomic.Int64
	b.ResetTimer() // exclude database and engine setup
	start := time.Now()
	b.ReportAllocs()
	b.SetParallelism(4) // 4 x GOMAXPROCS concurrent callers
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			i := next.Add(1)
			if _, err := wf.Run(ctx, engines[i%3], fmt.Sprint("k", i), 0); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.ReportMetric(float64(b.N)/time.Since(start).Seconds(), "runs/s")
	lat.report(b)
}
