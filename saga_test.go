package saga_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/internal/testdb"
)

// ledger records side effects of fake modules, keyed by step key, the way a
// real module's processed_requests table would.
type ledger struct {
	mu      sync.Mutex
	applied map[saga.Key]int
	undone  map[saga.Key]int
	order   []string
}

func newLedger() *ledger {
	return &ledger{applied: map[saga.Key]int{}, undone: map[saga.Key]int{}}
}

func (l *ledger) apply(k saga.Key, name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.applied[k]++
	l.order = append(l.order, "do:"+name)
}

func (l *ledger) undo(k saga.Key, name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.undone[k]++
	l.order = append(l.order, "undo:"+name)
}

func (l *ledger) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.order...)
}

type in struct{ N int }
type out struct{ Total int }

func setup(t *testing.T, opts saga.Options) (*pgxpool.Pool, *saga.Engine) {
	t.Helper()
	pool := testdb.New(t)
	if err := saga.Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	return pool, saga.New(pool, opts)
}

// addStep defines a step adding delta to its input; fail makes it error.
func addStep(t *testing.T, l *ledger, name string, delta int, fail func() error) *saga.Step[int, int, string] {
	return saga.DefineStep(testdb.UniqueName(t, name),
		func(ctx context.Context, k saga.Key, v int) (int, string, error) {
			if fail != nil {
				if err := fail(); err != nil {
					return 0, "", err
				}
			}
			l.apply(k, name)
			return v + delta, "undo-" + name, nil
		},
		func(ctx context.Context, k saga.Key, comp string) error {
			if comp != "undo-"+name && !saga.CompensationUncertain(ctx) {
				return fmt.Errorf("unexpected compensation data %q", comp)
			}
			l.undo(k, name)
			return nil
		})
}

func TestHappyPath(t *testing.T) {
	_, eng := setup(t, saga.Options{})
	l := newLedger()
	a := addStep(t, l, "a", 1, nil)
	b := addStep(t, l, "b", 10, nil)
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := a.Run(c, v.N)
		if err != nil {
			return out{}, err
		}
		y, err := b.Run(c, x)
		return out{Total: y}, err
	})

	got, err := wf.Run(context.Background(), eng, "k1", in{N: 5})
	if err != nil {
		t.Fatal(err)
	}
	if got.Total != 16 {
		t.Fatalf("total = %d, want 16", got.Total)
	}

	// Same idempotency key: stored result, steps not executed again.
	got2, err := wf.Run(context.Background(), eng, "k1", in{N: 999})
	if err != nil || got2.Total != 16 {
		t.Fatalf("second run = %+v, %v", got2, err)
	}
	if n := len(l.snapshot()); n != 2 {
		t.Fatalf("steps executed %d times, want 2", n)
	}
}

func TestCompensationReverseOrder(t *testing.T) {
	pool, eng := setup(t, saga.Options{})
	l := newLedger()
	a := addStep(t, l, "a", 1, nil)
	b := addStep(t, l, "b", 1, nil)
	c := addStep(t, l, "c", 1, func() error { return errors.New("card declined") })
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(cx saga.Ctx, v in) (out, error) {
		x, err := a.Run(cx, v.N)
		if err != nil {
			return out{}, err
		}
		if x, err = b.Run(cx, x); err != nil {
			return out{}, err
		}
		x, err = c.Run(cx, x)
		return out{Total: x}, err
	})

	_, err := wf.Run(context.Background(), eng, "k1", in{N: 1})
	var re *saga.RunError
	if !errors.As(err, &re) || re.Status != saga.StatusCompensated {
		t.Fatalf("err = %v, want compensated RunError", err)
	}
	want := []string{"do:a", "do:b", "undo:b", "undo:a"}
	if got := l.snapshot(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("order = %v, want %v", got, want)
	}

	var statuses []string
	rows, _ := pool.Query(context.Background(), `SELECT status FROM saga.step ORDER BY seq`)
	for rows.Next() {
		var s string
		_ = rows.Scan(&s)
		statuses = append(statuses, s)
	}
	if fmt.Sprint(statuses) != "[compensated compensated failed]" {
		t.Fatalf("step statuses = %v", statuses)
	}

	// Re-running a compensated key returns the same error without side effects.
	if _, err := wf.Run(context.Background(), eng, "k1", in{N: 1}); !errors.As(err, &re) {
		t.Fatalf("rerun err = %v", err)
	}
	if len(l.snapshot()) != 4 {
		t.Fatal("rerun executed steps")
	}
}

func TestResumeAfterCrashReplaysCompletedSteps(t *testing.T) {
	pool, eng := setup(t, saga.Options{LeaseTTL: time.Second})
	l := newLedger()
	var crash atomic.Bool
	crash.Store(true)

	a := addStep(t, l, "a", 1, nil)
	ctx, cancel := context.WithCancel(context.Background())
	b := saga.DefineStep(testdb.UniqueName(t, "b"),
		func(c context.Context, k saga.Key, v int) (int, string, error) {
			if crash.Load() {
				cancel() // the process "dies" while b is in flight
				return 0, "", c.Err()
			}
			l.apply(k, "b")
			return v * 2, "", nil
		}, nil)
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := a.Run(c, v.N)
		if err != nil {
			return out{}, err
		}
		x, err = b.Run(c, x)
		return out{Total: x}, err
	})

	if _, err := wf.Run(ctx, eng, "k1", in{N: 3}); !errors.Is(err, context.Canceled) {
		t.Fatalf("first run err = %v, want context.Canceled", err)
	}

	// While the lease is live, nobody else may run it.
	if _, err := wf.Run(context.Background(), eng, "k1", in{N: 3}); !errors.Is(err, saga.ErrRunInProgress) {
		t.Fatalf("err = %v, want ErrRunInProgress", err)
	}

	crash.Store(false)
	expireLeases(t, pool)
	eng2 := saga.New(pool, saga.Options{})
	got, err := wf.Run(context.Background(), eng2, "k1", in{N: 3})
	if err != nil {
		t.Fatal(err)
	}
	if got.Total != 8 {
		t.Fatalf("total = %d, want 8", got.Total)
	}
	if want := []string{"do:a", "do:b"}; fmt.Sprint(l.snapshot()) != fmt.Sprint(want) {
		t.Fatalf("effects = %v, want %v (a must not run twice)", l.snapshot(), want)
	}
}

func TestFencingRejectsStaleOwner(t *testing.T) {
	pool, eng := setup(t, saga.Options{LeaseTTL: time.Hour})
	l := newLedger()

	takeover := make(chan struct{})
	resume := make(chan struct{})
	var once sync.Once
	a := saga.DefineStep(testdb.UniqueName(t, "a"),
		func(c context.Context, k saga.Key, v int) (int, string, error) {
			first := false
			once.Do(func() { first = true })
			if first {
				close(takeover) // "GC pause": another instance takes over now
				<-resume
			}
			l.apply(k, "a")
			return v + 1, "", nil
		}, nil)
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := a.Run(c, v.N)
		return out{Total: x}, err
	})

	staleErr := make(chan error, 1)
	go func() {
		_, err := wf.Run(context.Background(), eng, "k1", in{N: 1})
		staleErr <- err
	}()

	<-takeover
	expireLeases(t, pool)
	eng2 := saga.New(pool, saga.Options{})
	got, err := wf.Run(context.Background(), eng2, "k1", in{N: 1})
	if err != nil || got.Total != 2 {
		t.Fatalf("takeover run = %+v, %v", got, err)
	}
	close(resume)

	if err := <-staleErr; !errors.Is(err, saga.ErrLeaseLost) {
		t.Fatalf("stale owner err = %v, want ErrLeaseLost", err)
	}
	var status string
	if err := pool.QueryRow(context.Background(), `SELECT status FROM saga.run`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "done" {
		t.Fatalf("status = %s, want done", status)
	}
}

func TestConcurrentRunsSameKeyExecuteOnce(t *testing.T) {
	pool, eng := setup(t, saga.Options{})
	engines := []*saga.Engine{eng, saga.New(pool, saga.Options{})} // two "instances"
	l := newLedger()
	gate := make(chan struct{})
	a := saga.DefineStep(testdb.UniqueName(t, "a"),
		func(c context.Context, k saga.Key, v int) (int, string, error) {
			<-gate
			l.apply(k, "a")
			return v, "", nil
		}, nil)
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := a.Run(c, v.N)
		return out{Total: x}, err
	})

	const callers = 8
	var wg sync.WaitGroup
	var ok, inProgress atomic.Int32
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(e *saga.Engine) {
			defer wg.Done()
			_, err := wf.Run(context.Background(), e, "same", in{N: 7})
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, saga.ErrRunInProgress):
				inProgress.Add(1)
			default:
				t.Errorf("unexpected err: %v", err)
			}
		}(engines[i%2])
	}
	time.Sleep(200 * time.Millisecond)
	close(gate)
	wg.Wait()

	if ok.Load() != 1 || inProgress.Load() != callers-1 {
		t.Fatalf("ok=%d inProgress=%d", ok.Load(), inProgress.Load())
	}
	if n := len(l.snapshot()); n != 1 {
		t.Fatalf("step executed %d times, want 1", n)
	}
}

func TestCompensationFailureNeedsAttention(t *testing.T) {
	pool, eng := setup(t, saga.Options{CompensateAttempts: 2, CompensateBackoff: time.Millisecond})
	var compCalls atomic.Int32
	a := saga.DefineStep(testdb.UniqueName(t, "a"),
		func(c context.Context, k saga.Key, v int) (int, string, error) { return v, "x", nil },
		func(c context.Context, k saga.Key, comp string) error {
			compCalls.Add(1)
			return errors.New("refund API down")
		})
	b := saga.DefineStep(testdb.UniqueName(t, "b"),
		func(c context.Context, k saga.Key, v int) (int, string, error) { return 0, "", errors.New("boom") }, nil)
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := a.Run(c, v.N)
		if err != nil {
			return out{}, err
		}
		x, err = b.Run(c, x)
		return out{Total: x}, err
	})

	_, err := wf.Run(context.Background(), eng, "k1", in{N: 1})
	var re *saga.RunError
	if !errors.As(err, &re) || re.Status != saga.StatusNeedsAttention {
		t.Fatalf("err = %v, want needs_attention", err)
	}
	if compCalls.Load() != 2 {
		t.Fatalf("compensator called %d times, want 2", compCalls.Load())
	}
	var owner *string
	if err := pool.QueryRow(context.Background(), `SELECT owner_id FROM saga.run`).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if owner != nil {
		t.Fatal("lease not released on needs_attention")
	}
}

func TestNonDeterministicWorkflowIsParked(t *testing.T) {
	pool, eng := setup(t, saga.Options{})
	var flip atomic.Bool
	ctx, cancel := context.WithCancel(context.Background())
	a := saga.DefineStep(testdb.UniqueName(t, "a"),
		func(c context.Context, k saga.Key, v int) (int, string, error) { return v, "", nil }, nil)
	b := saga.DefineStep(testdb.UniqueName(t, "b"),
		func(c context.Context, k saga.Key, v int) (int, string, error) { return v, "", nil }, nil)
	stop := saga.DefineStep(testdb.UniqueName(t, "stop"),
		func(c context.Context, k saga.Key, v int) (int, string, error) {
			cancel()
			return 0, "", c.Err()
		}, nil)
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		first := a
		if flip.Load() { // branching on something outside a step: forbidden
			first = b
		}
		x, err := first.Run(c, v.N)
		if err != nil {
			return out{}, err
		}
		x, err = stop.Run(c, x)
		return out{Total: x}, err
	})

	_, _ = wf.Run(ctx, eng, "k1", in{N: 1})
	flip.Store(true)
	expireLeases(t, pool)
	_, err := wf.Run(context.Background(), eng, "k1", in{N: 1})
	var re *saga.RunError
	if !errors.As(err, &re) || re.Status != saga.StatusNeedsAttention {
		t.Fatalf("err = %v, want needs_attention", err)
	}
}

func expireLeases(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`UPDATE saga.run SET lease_until = now() - interval '1 second' WHERE lease_until IS NOT NULL`); err != nil {
		t.Fatal(err)
	}
}
