package saga_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/internal/testdb"
)

// cartWorkflow locks "cart:1", runs a step, then optionally awaits "finish"
// so the test controls how long the lock is held.
func cartWorkflow(t *testing.T, ttl time.Duration, hold bool, opts ...saga.LockOption) (*saga.Workflow[in, out], *ledger) {
	t.Helper()
	l := newLedger()
	a := addStep(t, l, "a", 1, nil)
	return saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		if err := saga.Lock(c, "cart:1", ttl, opts...); err != nil {
			return out{}, err
		}
		x, err := a.Run(c, v.N)
		if err != nil {
			return out{}, err
		}
		if hold {
			if _, err := saga.Await[struct{}](c, "finish", 0); err != nil {
				return out{}, err
			}
		}
		return out{Total: x}, nil
	}), l
}

// F (fail policy): the second saga on the same lock fails at once and
// compensates; after the first finishes, the lock is free again.
func TestLockContentionFailsFast(t *testing.T) {
	t.Parallel()
	pool, eng := setup(t, saga.Options{})
	holder, _ := cartWorkflow(t, time.Hour, true)
	other, otherLedger := cartWorkflow(t, time.Hour, false)

	if _, err := holder.Run(context.Background(), eng, "a", in{N: 1}); !errors.Is(err, saga.ErrRunWaiting) {
		t.Fatalf("holder err = %v, want ErrRunWaiting (holding the lock)", err)
	}
	_, err := other.Run(context.Background(), eng, "b", in{N: 1})
	var re *saga.RunError
	if !errors.As(err, &re) || re.Status != saga.StatusCompensated || !strings.Contains(re.Cause, saga.ErrLockHeld.Error()) {
		t.Fatalf("contender err = %v, want compensated with lock held", err)
	}
	if n := len(otherLedger.snapshot()); n != 0 {
		t.Fatalf("contender ran steps: %v", otherLedger.snapshot())
	}

	h, err := holder.Lookup(context.Background(), eng, "a")
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Signal(context.Background(), h.ID(), "finish", nil); err != nil {
		t.Fatal(err)
	}
	if err := eng.Resume(context.Background(), h.ID()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Wait(waitCtx(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	var locks int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM saga.lock`).Scan(&locks); err != nil {
		t.Fatal(err)
	}
	if locks != 0 {
		t.Fatalf("locks after done = %d, want 0", locks)
	}
	if _, err := other.Run(context.Background(), eng, "c", in{N: 1}); err != nil {
		t.Fatalf("lock not free after holder finished: %v", err)
	}
}

// F (wait policy): the second saga waits, parked, until the first releases.
func TestLockContentionWaits(t *testing.T) {
	t.Parallel()
	_, eng := startedSetup(t)
	holder, _ := cartWorkflow(t, time.Hour, true)
	waiter, waiterLedger := cartWorkflow(t, time.Hour, false, saga.LockWait(saga.Exponential(20, 100*time.Millisecond)))

	if _, err := holder.Run(context.Background(), eng, "a", in{N: 1}); !errors.Is(err, saga.ErrRunWaiting) {
		t.Fatalf("holder err = %v", err)
	}
	if _, err := waiter.Run(context.Background(), eng, "b", in{N: 1}); !errors.Is(err, saga.ErrRunWaiting) {
		t.Fatalf("waiter err = %v, want ErrRunWaiting", err)
	}
	if n := len(waiterLedger.snapshot()); n != 0 {
		t.Fatal("waiter ran past the lock")
	}
	h, err := holder.Lookup(context.Background(), eng, "a")
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Signal(context.Background(), h.ID(), "finish", nil); err != nil {
		t.Fatal(err)
	}
	hw, err := waiter.Lookup(context.Background(), eng, "b")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hw.Wait(waitCtx(t, asyncTimeout)); err != nil {
		t.Fatalf("waiter: %v", err)
	}
}

// F (TTL): a run that dies holding a lock blocks others only until the TTL.
func TestLockTTLReleasesDeadRun(t *testing.T) {
	t.Parallel()
	pool, eng := setup(t, saga.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	die := saga.DefineStep(testdb.UniqueName(t, "die"),
		func(c context.Context, k saga.Key, v int) (int, string, error) {
			cancel()
			return 0, "", c.Err()
		}, nil)
	dead := saga.DefineWorkflow(testdb.UniqueName(t, "dead"), 1, func(c saga.Ctx, v in) (out, error) {
		if err := saga.Lock(c, "cart:1", time.Hour); err != nil {
			return out{}, err
		}
		x, err := die.Run(c, v.N)
		return out{Total: x}, err
	})
	if _, err := dead.Run(ctx, eng, "a", in{N: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("dead run err = %v", err)
	}

	// other takes the lock and keeps it (it parks on Await).
	other, _ := cartWorkflow(t, time.Hour, true)
	var re *saga.RunError
	if _, err := other.Run(context.Background(), eng, "b", in{N: 1}); !errors.As(err, &re) ||
		!strings.Contains(re.Cause, saga.ErrLockHeld.Error()) {
		t.Fatalf("lock of a dead run was free before its TTL: %v", err)
	}
	expireLocks(t, pool) // the TTL elapses
	if _, err := other.Run(context.Background(), eng, "c", in{N: 1}); !errors.Is(err, saga.ErrRunWaiting) {
		t.Fatalf("lock not released by TTL: %v", err)
	}

	// The dead run resumes after losing its lock: it fails and compensates
	// instead of carrying on unprotected.
	expireLeases(t, pool)
	_, err := dead.Run(context.Background(), eng, "a", in{N: 1})
	if !errors.As(err, &re) || re.Status != saga.StatusCompensated || !strings.Contains(re.Cause, saga.ErrLockLost.Error()) {
		t.Fatalf("resumed dead run err = %v, want compensated with lock lost", err)
	}
}

func expireLocks(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `UPDATE saga.lock SET until = now() - interval '1 second'`); err != nil {
		t.Fatal(err)
	}
}

func lockCount(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM saga.lock`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// A lock taken and released by another run while the holder was away is lost
// too: the holder must not carry on as if it had been exclusive.
func TestLockStolenAndReleasedIsLost(t *testing.T) {
	t.Parallel()
	pool, eng := setup(t, saga.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	die := saga.DefineStep(testdb.UniqueName(t, "die"),
		func(c context.Context, k saga.Key, v int) (int, string, error) {
			cancel()
			return 0, "", c.Err()
		}, nil)
	dead := saga.DefineWorkflow(testdb.UniqueName(t, "dead"), 1, func(c saga.Ctx, v in) (out, error) {
		if err := saga.Lock(c, "cart:1", time.Hour); err != nil {
			return out{}, err
		}
		x, err := die.Run(c, v.N)
		return out{Total: x}, err
	})
	if _, err := dead.Run(ctx, eng, "a", in{N: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("dead run err = %v", err)
	}
	expireLocks(t, pool)
	quick, _ := cartWorkflow(t, time.Hour, false)
	if _, err := quick.Run(context.Background(), eng, "b", in{N: 1}); err != nil {
		t.Fatalf("other run: %v", err)
	}
	if lockCount(t, pool) != 0 {
		t.Fatal("lock not released by the other run")
	}
	expireLeases(t, pool)
	_, err := dead.Run(context.Background(), eng, "a", in{N: 1})
	var re *saga.RunError
	if !errors.As(err, &re) || !strings.Contains(re.Cause, saga.ErrLockLost.Error()) {
		t.Fatalf("resumed run err = %v, want lock lost", err)
	}
}

// While its holder executes, the lease heartbeat keeps the lock alive past
// its TTL.
func TestLockRenewedWhileHolderExecutes(t *testing.T) {
	t.Parallel()
	// TTLs leave room for slow renewals under -race -count=10 load.
	pool, eng := setup(t, saga.Options{LeaseTTL: 2 * time.Second})
	inStep := make(chan struct{})
	entered := signalOnce(inStep)
	release := make(chan struct{})
	releaseOnce := signalOnce(release)
	t.Cleanup(releaseOnce) // never leave the holder blocked (Stop would wait)
	slow := saga.DefineStep(testdb.UniqueName(t, "slow"),
		func(c context.Context, k saga.Key, v int) (int, string, error) {
			entered()
			<-release
			return v, "", nil
		}, nil)
	holder := saga.DefineWorkflow(testdb.UniqueName(t, "holder"), 1, func(c saga.Ctx, v in) (out, error) {
		if err := saga.Lock(c, "cart:1", 2*time.Second); err != nil {
			return out{}, err
		}
		x, err := slow.Run(c, v.N)
		return out{Total: x}, err
	})
	done := make(chan error, 1)
	go func() {
		_, err := holder.Run(context.Background(), eng, "a", in{N: 1})
		done <- err
	}()
	<-inStep
	time.Sleep(5 * time.Second) // 2.5 lock TTLs: only renewal keeps it
	other, _ := cartWorkflow(t, time.Hour, false)
	var re *saga.RunError
	if _, err := other.Run(context.Background(), eng, "b", in{N: 1}); !errors.As(err, &re) ||
		!strings.Contains(re.Cause, saga.ErrLockHeld.Error()) {
		t.Fatalf("lock of an executing holder was taken: %v", err)
	}
	releaseOnce()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if lockCount(t, pool) != 0 {
		t.Fatal("lock not released when done")
	}
}

func TestLockReleaseOnCompensatedKeptOnNeedsAttention(t *testing.T) {
	t.Parallel()
	pool, eng := setup(t, saga.Options{})
	fail := saga.DefineStep(testdb.UniqueName(t, "fail"),
		func(c context.Context, k saga.Key, v int) (int, string, error) { return 0, "", errors.New("no") }, nil)
	undoFails := saga.DefineStep(testdb.UniqueName(t, "undoFails"),
		func(c context.Context, k saga.Key, v int) (int, string, error) { return v, "x", nil },
		func(c context.Context, k saga.Key, comp string) error { return errors.New("stuck") },
		saga.WithCompensatePolicy(saga.RetryPolicy{MaxAttempts: 1}))
	mk := func(key string, withStuck bool) *saga.Workflow[in, out] {
		return saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
			if err := saga.Lock(c, key, time.Hour); err != nil {
				return out{}, err
			}
			if withStuck {
				if _, err := undoFails.Run(c, v.N); err != nil {
					return out{}, err
				}
			}
			_, err := fail.Run(c, v.N)
			return out{}, err
		})
	}
	var re *saga.RunError
	if _, err := mk("k:compensated", false).Run(context.Background(), eng, "a", in{N: 1}); !errors.As(err, &re) || re.Status != saga.StatusCompensated {
		t.Fatalf("err = %v", err)
	}
	if lockCount(t, pool) != 0 {
		t.Fatal("compensated run kept its lock")
	}
	if _, err := mk("k:attention", true).Run(context.Background(), eng, "b", in{N: 1}); !errors.As(err, &re) || re.Status != saga.StatusNeedsAttention {
		t.Fatalf("err = %v", err)
	}
	if lockCount(t, pool) != 1 {
		t.Fatal("needs_attention run released its lock")
	}
}

func TestLockWaitExhaustedAndReentrant(t *testing.T) {
	t.Parallel()
	_, eng := startedSetup(t)
	holder, _ := cartWorkflow(t, time.Hour, true)
	if _, err := holder.Run(context.Background(), eng, "a", in{N: 1}); !errors.Is(err, saga.ErrRunWaiting) {
		t.Fatalf("holder err = %v", err)
	}
	waiter, _ := cartWorkflow(t, time.Hour, false, saga.LockWait(saga.Exponential(2, 10*time.Millisecond)))
	if _, err := waiter.Run(context.Background(), eng, "b", in{N: 1}); !errors.Is(err, saga.ErrRunWaiting) {
		t.Fatalf("waiter err = %v", err)
	}
	h, err := waiter.Lookup(context.Background(), eng, "b")
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Wait(waitCtx(t, asyncTimeout))
	var re *saga.RunError
	if !errors.As(err, &re) || re.Status != saga.StatusCompensated || !strings.Contains(re.Cause, saga.ErrLockHeld.Error()) {
		t.Fatalf("exhausted waiter err = %v, want compensated with lock held", err)
	}

	twice := saga.DefineWorkflow(testdb.UniqueName(t, "twice"), 1, func(c saga.Ctx, v in) (out, error) {
		if err := saga.Lock(c, "cart:2", time.Hour); err != nil {
			return out{}, err
		}
		return out{}, saga.Lock(c, "cart:2", time.Hour)
	})
	if _, err := twice.Run(context.Background(), eng, "c", in{N: 1}); err != nil {
		t.Fatalf("re-entrant lock: %v", err)
	}
}
