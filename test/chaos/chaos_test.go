//go:build chaos

// Package chaos_test runs crash scenarios against real processes: the test
// binary re-executes itself as a child "instance" that is then killed
// (SIGKILL) or frozen (SIGSTOP), and the parent recovers its runs.
package chaos_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/internal/testdb"
)

// Child process protocol, passed through the environment.
const (
	envMode  = "SAGA_CHAOS_CHILD" // hang | pause | many
	envDSN   = "SAGA_CHAOS_DSN"
	envRuns  = "SAGA_CHAOS_RUNS"
	leaseTTL = time.Second
)

// db is the database of the current test (parent) or of the child process.
// Parent tests do not run in parallel.
var db atomic.Pointer[pgxpool.Pool]

// hangMode makes the reserve step stall in the child: "hang" blocks forever
// (the process gets killed), "pause" sleeps past the lease (the process gets
// frozen and then thawed).
var hangMode string

type orderIn struct{ Order int }

type orderOut struct{ Confirmation string }

// effect is the module side of a step: it records every attempt, and applies
// the effect once per key (the module's processed_requests).
func effect(ctx context.Context, step string, k saga.Key) error {
	pool := db.Load()
	if _, err := pool.Exec(ctx, `INSERT INTO chaos_attempt (key, step, pid) VALUES ($1, $2, $3)`,
		string(k), step, os.Getpid()); err != nil {
		return err
	}
	_, err := pool.Exec(ctx, `INSERT INTO chaos_effect (key, step) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING`,
		string(k), step)
	return err
}

func defineStep(name string, stall bool) *saga.Step[int, int, struct{}] {
	return saga.DefineStep(name, func(ctx context.Context, k saga.Key, v int) (int, struct{}, error) {
		if err := effect(ctx, name, k); err != nil {
			return 0, struct{}{}, err
		}
		if stall && hangMode != "" {
			if _, err := db.Load().Exec(ctx, `INSERT INTO chaos_marker (key) VALUES ($1)`, string(k)); err != nil {
				return 0, struct{}{}, err
			}
			switch hangMode {
			case "hang", "many":
				select {} // killed with SIGKILL while here
			case "pause":
				// Frozen with SIGSTOP meanwhile; wall time keeps running.
				time.Sleep(4 * leaseTTL)
			}
		}
		return v + 1, struct{}{}, nil
	}, nil)
}

var (
	validate = defineStep("chaos.validate", false)
	reserve  = defineStep("chaos.reserve", true)
	charge   = defineStep("chaos.charge", false)
	confirm  = defineStep("chaos.confirm", false)

	order = saga.DefineWorkflow("chaos.order", 1, func(c saga.Ctx, in orderIn) (orderOut, error) {
		v := in.Order
		for _, s := range []*saga.Step[int, int, struct{}]{validate, reserve, charge, confirm} {
			var err error
			if v, err = s.Run(c, v); err != nil {
				return orderOut{}, err
			}
		}
		return orderOut{Confirmation: strconv.Itoa(v)}, nil
	})
)

func TestMain(m *testing.M) {
	if mode := os.Getenv(envMode); mode != "" {
		os.Exit(child(mode))
	}
	os.Exit(m.Run())
}

// child executes runs inline until killed. For "pause" it reports the outcome of
// its run on stdout.
func child(mode string) int {
	hangMode = mode
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv(envDSN))
	if err != nil {
		fmt.Println("RESULT: connect:", err)
		return 2
	}
	db.Store(pool)
	eng, err := saga.New(pool, saga.Options{LeaseTTL: leaseTTL, Logger: quietLogger(os.Stderr)})
	if err != nil {
		fmt.Println("RESULT: engine:", err)
		return 2
	}
	n, _ := strconv.Atoi(os.Getenv(envRuns))
	var wg sync.WaitGroup
	results := make([]error, max(n, 1))
	for i := range max(n, 1) {
		wg.Go(func() {
			_, results[i] = order.Run(ctx, eng, "order-"+strconv.Itoa(i), orderIn{Order: i * 100})
		})
	}
	wg.Wait()
	fmt.Printf("RESULT: %v\n", errors.Join(results...))
	return 0
}

func quietLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelError}))
}

func setup(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testdb.New(t)
	ctx := context.Background()
	if err := saga.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE chaos_attempt (key text NOT NULL, step text NOT NULL, pid int NOT NULL);
		CREATE TABLE chaos_effect (key text PRIMARY KEY, step text NOT NULL);
		CREATE TABLE chaos_marker (key text PRIMARY KEY);`); err != nil {
		t.Fatal(err)
	}
	db.Store(pool)
	return pool
}

func spawn(t *testing.T, pool *pgxpool.Pool, mode string, runs int) (*exec.Cmd, *bytes.Buffer) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(),
		envMode+"="+mode, envDSN+"="+testdb.DSN(pool), envRuns+"="+strconv.Itoa(runs))
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return cmd, &out
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func count(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func startEngine(t *testing.T, pool *pgxpool.Pool) *saga.Engine {
	t.Helper()
	eng, err := saga.New(pool, saga.Options{
		LeaseTTL: leaseTTL, RecoveryInterval: 200 * time.Millisecond, Logger: quietLogger(t.Output()),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = eng.Stop(ctx)
	})
	return eng
}

func waitDone(t *testing.T, eng *saga.Engine, key string) orderOut {
	t.Helper()
	h, err := order.Lookup(context.Background(), eng, key)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := h.Wait(ctx)
	if err != nil {
		t.Fatalf("run %s: %v", key, err)
	}
	return out
}

// assertOnce checks every step of every run took effect exactly once, and
// that steps after the interrupted one were invoked exactly once.
func assertOnce(t *testing.T, pool *pgxpool.Pool, runs int) {
	t.Helper()
	if n := count(t, pool, `SELECT count(*) FROM chaos_effect`); n != 4*runs {
		t.Fatalf("effects = %d, want %d", n, 4*runs)
	}
	for _, step := range []string{"chaos.validate", "chaos.charge", "chaos.confirm"} {
		if n := count(t, pool, `SELECT count(*) FROM chaos_attempt WHERE step = $1`, step); n != runs {
			t.Fatalf("%s invoked %d times, want %d", step, n, runs)
		}
	}
	// reserve: once in the dead instance, once after recovery, same key.
	if n := count(t, pool, `SELECT count(DISTINCT key) FROM chaos_attempt WHERE step = 'chaos.reserve'`); n != runs {
		t.Fatalf("reserve used %d distinct keys, want %d", n, runs)
	}
	if n := count(t, pool, `SELECT count(*) FROM chaos_attempt WHERE step = 'chaos.reserve'`); n != 2*runs {
		t.Fatalf("reserve invoked %d times, want %d", n, 2*runs)
	}
}

// A: an instance is killed mid-step; another recovers the run and re-runs the
// step with the same key, so its effect happens once.
func TestKilledInstanceIsRecovered(t *testing.T) {
	pool := setup(t)
	cmd, _ := spawn(t, pool, "hang", 1)
	waitFor(t, "child to enter reserve", 30*time.Second, func() bool {
		return count(t, pool, `SELECT count(*) FROM chaos_marker`) == 1
	})
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()

	eng := startEngine(t, pool)
	if out := waitDone(t, eng, "order-0"); out.Confirmation != "4" {
		t.Fatalf("output = %+v", out)
	}
	assertOnce(t, pool, 1)
}

// B: an instance freezes past its lease (GC pause); another takes over and
// finishes. The frozen one wakes up and its checkpoint is rejected by fencing.
func TestFrozenInstanceIsFencedOff(t *testing.T) {
	pool := setup(t)
	cmd, out := spawn(t, pool, "pause", 1)
	waitFor(t, "child to enter reserve", 30*time.Second, func() bool {
		return count(t, pool, `SELECT count(*) FROM chaos_marker`) == 1
	})
	if err := cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}

	eng := startEngine(t, pool)
	waitDone(t, eng, "order-0")

	if err := cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("child: %v", err)
	}
	if got := out.String(); !bytes.Contains(out.Bytes(), []byte(saga.ErrLeaseLost.Error())) {
		t.Fatalf("child result = %q, want lease lost", got)
	}
	assertOnce(t, pool, 1)
	if n := count(t, pool, `SELECT count(*) FROM saga.run WHERE status = 'done'`); n != 1 {
		t.Fatalf("done runs = %d", n)
	}
}

// C-recovery: three instances race to recover the runs of a killed one. Each
// run is executed by exactly one of them.
func TestThreeInstancesRaceRecovery(t *testing.T) {
	const runs = 30
	pool := setup(t)
	cmd, _ := spawn(t, pool, "many", runs)
	waitFor(t, "child to enter reserve in every run", 60*time.Second, func() bool {
		return count(t, pool, `SELECT count(*) FROM chaos_marker`) == runs
	})
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()

	engines := make([]*saga.Engine, 3)
	for i := range engines {
		p, err := pgxpool.New(context.Background(), testdb.DSN(pool))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		engines[i] = startEngine(t, p)
	}
	for i := range runs {
		waitDone(t, engines[i%3], "order-"+strconv.Itoa(i))
	}
	assertOnce(t, pool, runs)
}
