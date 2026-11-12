package saga_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/internal/testdb"
)

// asyncTimeout covers several River scheduler ticks (about 5s each).
const asyncTimeout = 60 * time.Second

func startedSetup(t *testing.T) (*pgxpool.Pool, *saga.Engine) {
	t.Helper()
	pool, eng := setup(t, saga.Options{RecoveryInterval: 500 * time.Millisecond})
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return pool, eng
}

func runStatus(t *testing.T, pool *pgxpool.Pool) (status string, owner *string) {
	t.Helper()
	if err := pool.QueryRow(context.Background(), `SELECT status, owner_id FROM saga.run`).Scan(&status, &owner); err != nil {
		t.Fatal(err)
	}
	return status, owner
}

func stepStatuses(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT name, status FROM saga.step ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name, status string
		if err := rows.Scan(&name, &status); err != nil {
			t.Fatal(err)
		}
		out = append(out, status)
	}
	return fmt.Sprint(out)
}

// D: a payment fails transiently 3 times, then succeeds. Retries go through
// River with backoff, and the lease is not held while waiting.
func TestTransientFailuresAreRetriedDurably(t *testing.T) {
	t.Parallel()
	pool, eng := startedSetup(t)
	var calls atomic.Int32
	keys := make(chan saga.Key, 10)
	pay := saga.DefineStep(testdb.UniqueName(t, "pay"),
		func(ctx context.Context, k saga.Key, v int) (int, string, error) {
			keys <- k
			if calls.Add(1) <= 3 {
				return 0, "", errors.New("gateway timeout")
			}
			return v * 2, "", nil
		}, nil, saga.WithRetry(saga.Exponential(5, 50*time.Millisecond)))
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := pay.Run(c, v.N)
		return out{Total: x}, err
	})

	_, err := wf.Run(context.Background(), eng, "k1", in{N: 21})
	if !errors.Is(err, saga.ErrRunWaiting) {
		t.Fatalf("first run err = %v, want ErrRunWaiting", err)
	}
	if status, owner := runStatus(t, pool); status != "waiting" || owner != nil {
		t.Fatalf("parked run: status=%s owner=%v, want waiting without owner", status, owner)
	}

	h, err := wf.Lookup(context.Background(), eng, "k1")
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.Wait(waitCtx(t, asyncTimeout))
	if err != nil || got.Total != 42 {
		t.Fatalf("wait = %+v, %v", got, err)
	}
	if calls.Load() != 4 {
		t.Fatalf("calls = %d, want 4", calls.Load())
	}
	close(keys)
	first := <-keys
	for k := range keys {
		if k != first {
			t.Fatalf("retry used key %s, first was %s", k, first)
		}
	}
	var attempt int
	if err := pool.QueryRow(context.Background(), `SELECT attempt FROM saga.step`).Scan(&attempt); err != nil {
		t.Fatal(err)
	}
	if attempt != 4 {
		t.Fatalf("recorded attempt = %d, want 4", attempt)
	}
}

func TestPermanentErrorIsNotRetried(t *testing.T) {
	t.Parallel()
	_, eng := startedSetup(t)
	var calls atomic.Int32
	pay := saga.DefineStep(testdb.UniqueName(t, "pay"),
		func(ctx context.Context, k saga.Key, v int) (int, string, error) {
			calls.Add(1)
			return 0, "", saga.Permanent(errors.New("card declined"))
		}, nil, saga.WithRetry(saga.Exponential(5, time.Millisecond)))
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := pay.Run(c, v.N)
		return out{Total: x}, err
	})
	_, err := wf.Run(context.Background(), eng, "k1", in{N: 1})
	var re *saga.RunError
	if !errors.As(err, &re) || re.Status != saga.StatusCompensated {
		t.Fatalf("err = %v, want compensated", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
}

// E: compensation fails until its policy is exhausted; the run needs
// attention; RetryCompensation then completes it.
func TestCompensationExhaustedThenManualRetry(t *testing.T) {
	t.Parallel()
	pool, eng := startedSetup(t)
	var fixed atomic.Bool
	var compCalls atomic.Int32
	reserve := saga.DefineStep(testdb.UniqueName(t, "reserve"),
		func(ctx context.Context, k saga.Key, v int) (int, string, error) { return v, "res-1", nil },
		func(ctx context.Context, k saga.Key, id string) error {
			compCalls.Add(1)
			if !fixed.Load() {
				return errors.New("inventory API down")
			}
			return nil
		}, saga.WithCompensatePolicy(saga.Exponential(2, 50*time.Millisecond)))
	pay := saga.DefineStep(testdb.UniqueName(t, "pay"),
		func(ctx context.Context, k saga.Key, v int) (int, string, error) {
			return 0, "", errors.New("declined")
		}, nil)
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := reserve.Run(c, v.N)
		if err != nil {
			return out{}, err
		}
		x, err = pay.Run(c, x)
		return out{Total: x}, err
	})

	if _, err := wf.Run(context.Background(), eng, "k1", in{N: 1}); !errors.Is(err, saga.ErrRunWaiting) {
		t.Fatalf("err = %v, want ErrRunWaiting (compensation retry scheduled)", err)
	}
	h, err := wf.Lookup(context.Background(), eng, "k1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Wait(waitCtx(t, asyncTimeout))
	var re *saga.RunError
	if !errors.As(err, &re) || re.Status != saga.StatusNeedsAttention {
		t.Fatalf("err = %v, want needs_attention", err)
	}
	if compCalls.Load() != 2 {
		t.Fatalf("compensator calls = %d, want 2", compCalls.Load())
	}
	if s := stepStatuses(t, pool); s != "[compensation_failed failed]" {
		t.Fatalf("steps = %s", s)
	}

	fixed.Store(true)
	if err := eng.RetryCompensation(context.Background(), h.ID()); err != nil {
		t.Fatal(err)
	}
	if err := eng.RetryCompensation(context.Background(), h.ID()); !errors.Is(err, saga.ErrNotNeedsAttention) {
		t.Fatalf("second RetryCompensation err = %v, want ErrNotNeedsAttention", err)
	}
	_, err = h.Wait(waitCtx(t, asyncTimeout))
	if !errors.As(err, &re) || re.Status != saga.StatusCompensated {
		t.Fatalf("after retry err = %v, want compensated", err)
	}
	if s := stepStatuses(t, pool); s != "[compensated failed]" {
		t.Fatalf("steps = %s", s)
	}
}

func TestSleepParksWithoutLease(t *testing.T) {
	t.Parallel()
	pool, eng := startedSetup(t)
	l := newLedger()
	a := addStep(t, l, "a", 1, nil)
	b := addStep(t, l, "b", 10, nil)
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := a.Run(c, v.N)
		if err != nil {
			return out{}, err
		}
		if err := saga.Sleep(c, time.Second); err != nil {
			return out{}, err
		}
		x, err = b.Run(c, x)
		return out{Total: x}, err
	})

	h, err := wf.Start(context.Background(), eng, "k1", in{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.Wait(waitCtx(t, asyncTimeout))
	if err != nil || got.Total != 12 {
		t.Fatalf("wait = %+v, %v", got, err)
	}
	if s := stepStatuses(t, pool); s != "[done done done]" {
		t.Fatalf("steps = %s", s)
	}
	if want := "[do:a do:b]"; fmt.Sprint(l.snapshot()) != want {
		t.Fatalf("effects = %v, want %s", l.snapshot(), want)
	}
}

type approval struct{ By string }

func approvalWorkflow(t *testing.T, timeout time.Duration) (*saga.Workflow[in, approval], *ledger) {
	t.Helper()
	l := newLedger()
	a := addStep(t, l, "a", 1, nil)
	return saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (approval, error) {
		if _, err := a.Run(c, v.N); err != nil {
			return approval{}, err
		}
		return saga.Await[approval](c, "approved", timeout)
	}), l
}

func TestAwaitResumesOnSignal(t *testing.T) {
	t.Parallel()
	pool, eng := startedSetup(t)
	wf, _ := approvalWorkflow(t, time.Hour)
	h, err := wf.Start(context.Background(), eng, "k1", in{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(asyncTimeout)
	for {
		if status, _ := runStatus(t, pool); status == "waiting" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("run never parked on Await")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := eng.Signal(context.Background(), h.ID(), "approved", approval{By: "alice"}); err != nil {
		t.Fatal(err)
	}
	// A duplicate signal is ignored.
	if err := eng.Signal(context.Background(), h.ID(), "approved", approval{By: "mallory"}); err != nil {
		t.Fatal(err)
	}
	got, err := h.Wait(waitCtx(t, asyncTimeout))
	if err != nil || got.By != "alice" {
		t.Fatalf("wait = %+v, %v", got, err)
	}
}

func TestSignalBeforeAwaitIsKept(t *testing.T) {
	t.Parallel()
	pool, idle := setup(t, saga.Options{}) // not started: nothing executes yet
	wf, _ := approvalWorkflow(t, time.Hour)
	h, err := wf.Start(context.Background(), idle, "k1", in{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := idle.Signal(context.Background(), h.ID(), "approved", approval{By: "bob"}); err != nil {
		t.Fatal(err)
	}
	eng := newEngine(t, pool, saga.Options{})
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := h.Wait(waitCtx(t, asyncTimeout))
	if err != nil || got.By != "bob" {
		t.Fatalf("wait = %+v, %v", got, err)
	}
	if err := eng.Signal(context.Background(), "00000000-0000-0000-0000-000000000000", "x", nil); !errors.Is(err, saga.ErrRunNotFound) {
		t.Fatalf("signal unknown run err = %v", err)
	}
}

func TestAwaitTimeoutCompensates(t *testing.T) {
	t.Parallel()
	_, eng := startedSetup(t)
	wf, l := approvalWorkflow(t, 200*time.Millisecond)
	h, err := wf.Start(context.Background(), eng, "k1", in{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Wait(waitCtx(t, asyncTimeout))
	var re *saga.RunError
	if !errors.As(err, &re) || re.Status != saga.StatusCompensated {
		t.Fatalf("err = %v, want compensated", err)
	}
	if want := "[do:a undo:a]"; fmt.Sprint(l.snapshot()) != want {
		t.Fatalf("effects = %v, want %s", l.snapshot(), want)
	}
}

func TestRunTimeoutWakesSleepingRunAndCompensates(t *testing.T) {
	t.Parallel()
	_, eng := startedSetup(t)
	l := newLedger()
	a := addStep(t, l, "a", 1, nil)
	b := addStep(t, l, "b", 1, nil)
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := a.Run(c, v.N)
		if err != nil {
			return out{}, err
		}
		if err := saga.Sleep(c, time.Hour); err != nil {
			return out{}, err
		}
		x, err = b.Run(c, x)
		return out{Total: x}, err
	}, saga.WithRunTimeout(500*time.Millisecond))

	h, err := wf.Start(context.Background(), eng, "k1", in{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Wait(waitCtx(t, asyncTimeout))
	var re *saga.RunError
	if !errors.As(err, &re) || re.Status != saga.StatusCompensated {
		t.Fatalf("err = %v, want compensated", err)
	}
	if want := "[do:a undo:a]"; fmt.Sprint(l.snapshot()) != want {
		t.Fatalf("effects = %v, want %s", l.snapshot(), want)
	}
}

func TestStepTimeoutCountsAsFailure(t *testing.T) {
	t.Parallel()
	_, eng := setup(t, saga.Options{})
	var cause atomic.Value
	slow := saga.DefineStep(testdb.UniqueName(t, "slow"),
		func(ctx context.Context, k saga.Key, v int) (int, string, error) {
			<-ctx.Done()
			cause.Store(context.Cause(ctx))
			return 0, "", ctx.Err()
		}, nil, saga.WithTimeout(100*time.Millisecond))
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := slow.Run(c, v.N)
		return out{Total: x}, err
	})
	_, err := wf.Run(context.Background(), eng, "k1", in{N: 1})
	var re *saga.RunError
	if !errors.As(err, &re) || re.Status != saga.StatusCompensated {
		t.Fatalf("err = %v, want compensated", err)
	}
	if c, _ := cause.Load().(error); !errors.Is(c, saga.ErrStepTimeout) {
		t.Fatalf("step context cause = %v, want ErrStepTimeout", c)
	}
}

func TestNowAndRandAreReplayed(t *testing.T) {
	t.Parallel()
	pool, eng := setup(t, saga.Options{})
	var crash atomic.Bool
	crash.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	stop := saga.DefineStep(testdb.UniqueName(t, "stop"),
		func(c context.Context, k saga.Key, v int) (int, string, error) {
			if crash.Load() {
				cancel()
				return 0, "", c.Err()
			}
			return v, "", nil
		}, nil)
	type seen struct {
		Now  time.Time
		Rand uint64
	}
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (seen, error) {
		now, err := saga.Now(c)
		if err != nil {
			return seen{}, err
		}
		r, err := saga.Rand(c)
		if err != nil {
			return seen{}, err
		}
		if _, err := stop.Run(c, v.N); err != nil {
			return seen{}, err
		}
		return seen{Now: now, Rand: r}, nil
	})
	if _, err := wf.Run(ctx, eng, "k1", in{N: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("first run err = %v", err)
	}
	var recNow time.Time
	var recRand uint64
	if err := pool.QueryRow(context.Background(), `
		SELECT (SELECT (output #>> '{}')::timestamptz FROM saga.step WHERE seq = 1),
		       (SELECT (output #>> '{}')::numeric FROM saga.step WHERE seq = 2)`).Scan(&recNow, &recRand); err != nil {
		t.Fatal(err)
	}
	crash.Store(false)
	expireLeases(t, pool)
	got, err := wf.Run(context.Background(), eng, "k1", in{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Now.Equal(recNow) || got.Rand != recRand {
		t.Fatalf("replayed now=%v rand=%d, recorded now=%v rand=%d", got.Now, got.Rand, recNow, recRand)
	}
}

// A timed-out attempt is transient (retried), and a step whose attempts all
// timed out is compensated as uncertain: it may have taken effect.
func TestStepTimeoutIsRetriedThenCompensatedAsUncertain(t *testing.T) {
	t.Parallel()
	_, eng := startedSetup(t)
	var calls atomic.Int32
	var uncertain atomic.Bool
	charge := saga.DefineStep(testdb.UniqueName(t, "charge"),
		func(ctx context.Context, k saga.Key, v int) (int, string, error) {
			calls.Add(1)
			<-ctx.Done() // the gateway never answers in time
			return 0, "", ctx.Err()
		},
		func(ctx context.Context, k saga.Key, comp string) error {
			uncertain.Store(saga.CompensationUncertain(ctx))
			return nil
		},
		saga.WithTimeout(50*time.Millisecond), saga.WithRetry(saga.Exponential(2, 10*time.Millisecond)))
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := charge.Run(c, v.N)
		return out{Total: x}, err
	})
	h, err := wf.Start(context.Background(), eng, "k1", in{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.Wait(waitCtx(t, asyncTimeout))
	var re *saga.RunError
	if !errors.As(err, &re) || re.Status != saga.StatusCompensated {
		t.Fatalf("err = %v, want compensated", err)
	}
	if calls.Load() != 2 || !uncertain.Load() {
		t.Fatalf("calls=%d uncertain=%v, want 2 attempts and an uncertain compensation", calls.Load(), uncertain.Load())
	}
}

// A workflow may branch on an engine error; the branch must be taken again on
// replay, or the run becomes non-deterministic.
func TestRecordedErrorKeepsSentinelOnReplay(t *testing.T) {
	t.Parallel()
	_, eng := startedSetup(t)
	slow := saga.DefineStep(testdb.UniqueName(t, "slow"),
		func(ctx context.Context, k saga.Key, v int) (int, string, error) {
			<-ctx.Done()
			return 0, "", ctx.Err()
		}, nil, saga.WithTimeout(20*time.Millisecond))
	fallback := addStep(t, newLedger(), "fallback", 100, nil)
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := slow.Run(c, v.N)
		if errors.Is(err, saga.ErrStepTimeout) {
			if err := saga.Sleep(c, 10*time.Millisecond); err != nil {
				return out{}, err // parks: the resume replays slow's failure
			}
			x, err = fallback.Run(c, v.N)
		}
		return out{Total: x}, err
	})
	h, err := wf.Start(context.Background(), eng, "k1", in{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.Wait(waitCtx(t, asyncTimeout))
	if err != nil || got.Total != 101 {
		t.Fatalf("wait = %+v, %v", got, err)
	}
}

// A parked run (retry backoff) is not executed early by an inline call with
// the same key, nor by a duplicate resume job.
func TestParkedRunIsNotTakenEarly(t *testing.T) {
	t.Parallel()
	pool, eng := setup(t, saga.Options{})
	var calls atomic.Int32
	flaky := saga.DefineStep(testdb.UniqueName(t, "flaky"),
		func(ctx context.Context, k saga.Key, v int) (int, string, error) {
			calls.Add(1)
			return 0, "", errors.New("try later")
		}, nil, saga.WithRetry(saga.Exponential(3, time.Hour)))
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := flaky.Run(c, v.N)
		return out{Total: x}, err
	})
	if _, err := wf.Run(context.Background(), eng, "k1", in{N: 1}); !errors.Is(err, saga.ErrRunWaiting) {
		t.Fatalf("err = %v, want ErrRunWaiting", err)
	}
	if _, err := wf.Run(context.Background(), eng, "k1", in{N: 1}); !errors.Is(err, saga.ErrRunWaiting) {
		t.Fatalf("second call err = %v, want ErrRunWaiting", err)
	}
	h, err := wf.Lookup(context.Background(), eng, "k1")
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Resume(context.Background(), h.ID()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1 (backoff not over)", calls.Load())
	}
	if status, owner := runStatus(t, pool); status != "waiting" || owner != nil {
		t.Fatalf("status=%s owner=%v", status, owner)
	}
}

// Same for a run in compensation backoff.
func TestCompensationBackoffIsNotTakenEarly(t *testing.T) {
	t.Parallel()
	_, eng := setup(t, saga.Options{})
	var compCalls atomic.Int32
	a := saga.DefineStep(testdb.UniqueName(t, "a"),
		func(ctx context.Context, k saga.Key, v int) (int, string, error) { return v, "x", nil },
		func(ctx context.Context, k saga.Key, comp string) error {
			compCalls.Add(1)
			return errors.New("down")
		}, saga.WithCompensatePolicy(saga.Exponential(3, time.Hour)))
	b := saga.DefineStep(testdb.UniqueName(t, "b"),
		func(ctx context.Context, k saga.Key, v int) (int, string, error) { return 0, "", errors.New("no") }, nil)
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := a.Run(c, v.N)
		if err != nil {
			return out{}, err
		}
		x, err = b.Run(c, x)
		return out{Total: x}, err
	})
	for range 2 {
		if _, err := wf.Run(context.Background(), eng, "k1", in{N: 1}); !errors.Is(err, saga.ErrRunWaiting) {
			t.Fatalf("err = %v, want ErrRunWaiting", err)
		}
	}
	h, err := wf.Lookup(context.Background(), eng, "k1")
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Resume(context.Background(), h.ID()); err != nil {
		t.Fatal(err)
	}
	if compCalls.Load() != 1 {
		t.Fatalf("compensator calls = %d, want 1 (backoff not over)", compCalls.Load())
	}
}

// The resume job of a signal is backed up by recovery, even for an Await
// without timeout or deadline.
func TestSignalWakeUpSurvivesLostJob(t *testing.T) {
	t.Parallel()
	pool, eng := setup(t, saga.Options{})
	wf, _ := approvalWorkflow(t, 0)
	if _, err := wf.Run(context.Background(), eng, "k1", in{N: 1}); !errors.Is(err, saga.ErrRunWaiting) {
		t.Fatalf("err = %v, want ErrRunWaiting", err)
	}
	h, err := wf.Lookup(context.Background(), eng, "k1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if n, err := eng.RecoverExpired(ctx); err != nil || n != 0 {
		t.Fatalf("scan of an idle await = %d, %v; want 0", n, err)
	}
	if err := eng.Signal(ctx, h.ID(), "approved", approval{By: "carol"}); err != nil {
		t.Fatal(err)
	}
	// The job is lost.
	if _, err := pool.Exec(ctx, `DELETE FROM saga.river_job`); err != nil {
		t.Fatal(err)
	}
	expireLeases(t, pool)
	if n, err := eng.RecoverExpired(ctx); err != nil || n != 1 {
		t.Fatalf("scan after lost signal job = %d, %v; want 1", n, err)
	}
	if err := eng.Resume(ctx, h.ID()); err != nil {
		t.Fatal(err)
	}
	got, err := h.Wait(waitCtx(t, 5*time.Second))
	if err != nil || got.By != "carol" {
		t.Fatalf("wait = %+v, %v", got, err)
	}
}

// A payload of the wrong shape fails the Await step instead of wedging the
// run.
func TestBadSignalPayloadFailsTheStep(t *testing.T) {
	t.Parallel()
	_, eng := setup(t, saga.Options{})
	wf, l := approvalWorkflow(t, 0)
	if _, err := wf.Run(context.Background(), eng, "k1", in{N: 1}); !errors.Is(err, saga.ErrRunWaiting) {
		t.Fatalf("err = %v, want ErrRunWaiting", err)
	}
	h, err := wf.Lookup(context.Background(), eng, "k1")
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Signal(context.Background(), h.ID(), "approved", []int{1, 2}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Resume(context.Background(), h.ID()); err != nil {
		t.Fatal(err)
	}
	_, err = h.Wait(waitCtx(t, 5*time.Second))
	var re *saga.RunError
	if !errors.As(err, &re) || re.Status != saga.StatusCompensated {
		t.Fatalf("err = %v, want compensated", err)
	}
	if want := "[do:a undo:a]"; fmt.Sprint(l.snapshot()) != want {
		t.Fatalf("effects = %v, want %s", l.snapshot(), want)
	}
	if err := eng.Signal(context.Background(), "not-a-uuid", "x", nil); !errors.Is(err, saga.ErrRunNotFound) {
		t.Fatalf("malformed id err = %v, want ErrRunNotFound", err)
	}
}

// Long-running external work: a step starts it with its Key as reference, the
// workflow awaits the key, the external system answers with SignalKey.
func TestSignalKeyCompletesExternalWork(t *testing.T) {
	t.Parallel()
	_, eng := startedSetup(t)
	submitted := make(chan saga.Key, 1)
	type report struct{ URL string }
	start := saga.DefineStep(testdb.UniqueName(t, "render.start"),
		func(ctx context.Context, k saga.Key, v int) (saga.Key, string, error) {
			submitted <- k // e.g. POST /render {reference: k}
			return k, "", nil
		}, nil)
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (report, error) {
		ref, err := start.Run(c, v.N)
		if err != nil {
			return report{}, err
		}
		return saga.Await[report](c, string(ref), time.Hour)
	})
	h, err := wf.Start(context.Background(), eng, "k1", in{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	k := <-submitted
	if k.RunID() != h.ID() {
		t.Fatalf("key %s does not name run %s", k, h.ID())
	}
	// The external worker finishes later, possibly before the run parked.
	if err := eng.SignalKey(context.Background(), k, report{URL: "s3://r/1"}); err != nil {
		t.Fatal(err)
	}
	got, err := h.Wait(waitCtx(t, asyncTimeout))
	if err != nil || got.URL != "s3://r/1" {
		t.Fatalf("wait = %+v, %v", got, err)
	}
}

// Regression: a deadline beyond the int4 range in microseconds (~35 min) was
// rejected because Postgres inferred the parameter type as int4.
func TestLongRunTimeout(t *testing.T) {
	t.Parallel()
	_, eng := setup(t, saga.Options{})
	a := addStep(t, newLedger(), "a", 1, nil)
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := a.Run(c, v.N)
		return out{Total: x}, err
	}, saga.WithRunTimeout(24*time.Hour))
	if _, err := wf.Run(context.Background(), eng, "k1", in{N: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := wf.Start(context.Background(), eng, "k2", in{N: 1}); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncStepStartRunsWorkflows(t *testing.T) {
	t.Parallel()
	_, eng := setup(t, saga.Options{AsyncStepStart: true})
	l := newLedger()
	a := addStep(t, l, "a", 1, nil)
	b := addStep(t, l, "b", 1, func() error { return errors.New("no") })
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
	if !errors.As(err, &re) || re.Status != saga.StatusCompensated {
		t.Fatalf("err = %v, want compensated", err)
	}
	if want := "[do:a undo:a]"; fmt.Sprint(l.snapshot()) != want {
		t.Fatalf("effects = %v", l.snapshot())
	}
}

// Regression: a step left "started" by a crash and failed by the run deadline
// on resume may have taken effect: it must be compensated as uncertain.
func TestDeadlineAfterCrashCompensatesStartedStep(t *testing.T) {
	t.Parallel()
	pool, eng := setup(t, saga.Options{})
	var uncertain atomic.Bool
	var compensated atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	charge := saga.DefineStep(testdb.UniqueName(t, "charge"),
		func(c context.Context, k saga.Key, v int) (int, string, error) {
			cancel() // the process dies while the charge is in flight
			return 0, "", c.Err()
		},
		func(c context.Context, k saga.Key, comp string) error {
			compensated.Add(1)
			uncertain.Store(saga.CompensationUncertain(c))
			return nil
		})
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := charge.Run(c, v.N)
		return out{Total: x}, err
	}, saga.WithRunTimeout(time.Hour))
	if _, err := wf.Run(ctx, eng, "k1", in{N: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("first run err = %v", err)
	}
	// The run resumes after its deadline.
	if _, err := pool.Exec(context.Background(), `UPDATE saga.run SET deadline = now() - interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	expireLeases(t, pool)
	_, err := wf.Run(context.Background(), eng, "k1", in{N: 1})
	var re *saga.RunError
	if !errors.As(err, &re) || re.Status != saga.StatusCompensated {
		t.Fatalf("err = %v, want compensated", err)
	}
	if compensated.Load() != 1 || !uncertain.Load() {
		t.Fatalf("compensations=%d uncertain=%v, want 1 uncertain compensation", compensated.Load(), uncertain.Load())
	}
}
