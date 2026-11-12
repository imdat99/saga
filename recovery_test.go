package saga_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/internal/testdb"
)

func waitCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// signalOnce closes ch the first time it is called, so a step re-executed
// after an unexpected takeover fails the test instead of panicking.
func signalOnce(ch chan struct{}) func() {
	var once sync.Once
	return func() { once.Do(func() { close(ch) }) }
}

func TestHeartbeatKeepsLeaseAlive(t *testing.T) {
	pool, eng := setup(t, saga.Options{LeaseTTL: 1500 * time.Millisecond})
	other := newEngine(t, pool, saga.Options{LeaseTTL: 1500 * time.Millisecond})
	inStep := make(chan struct{})
	entered := signalOnce(inStep)
	release := make(chan struct{})
	a := saga.DefineStep(testdb.UniqueName(t, "a"),
		func(ctx context.Context, k saga.Key, v int) (int, string, error) {
			entered()
			<-release
			return v, "", nil
		}, nil)
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := a.Run(c, v.N)
		return out{Total: x}, err
	})

	done := make(chan error, 1)
	go func() {
		_, err := wf.Run(context.Background(), eng, "k1", in{N: 1})
		done <- err
	}()
	<-inStep
	time.Sleep(3500 * time.Millisecond) // more than 2 TTLs: only renewal keeps the lease
	if _, err := wf.Run(context.Background(), other, "k1", in{N: 1}); !errors.Is(err, saga.ErrRunInProgress) {
		t.Fatalf("other instance err = %v, want ErrRunInProgress", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestLeaseLossCancelsRunContext(t *testing.T) {
	pool, eng := setup(t, saga.Options{LeaseTTL: 300 * time.Millisecond})
	inStep := make(chan struct{})
	entered := signalOnce(inStep)
	var cause atomic.Value
	a := saga.DefineStep(testdb.UniqueName(t, "a"),
		func(ctx context.Context, k saga.Key, v int) (int, string, error) {
			entered()
			<-ctx.Done()
			cause.Store(context.Cause(ctx))
			return 0, "", ctx.Err()
		}, nil)
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := a.Run(c, v.N)
		return out{Total: x}, err
	})

	done := make(chan error, 1)
	go func() {
		_, err := wf.Run(context.Background(), eng, "k1", in{N: 1})
		done <- err
	}()
	<-inStep
	// Another instance takes over: the version moves on.
	if _, err := pool.Exec(context.Background(),
		`UPDATE saga.run SET owner_id = 'thief', lease_version = lease_version + 1`); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, saga.ErrLeaseLost) {
			t.Fatalf("err = %v, want ErrLeaseLost", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run not cancelled after losing its lease")
	}
	if c, _ := cause.Load().(error); !errors.Is(c, saga.ErrLeaseLost) {
		t.Fatalf("step context cause = %v, want ErrLeaseLost", c)
	}
}

func TestRecoveryResumesAbandonedRun(t *testing.T) {
	pool, crashed := setup(t, saga.Options{LeaseTTL: 500 * time.Millisecond})
	l := newLedger()
	var crash atomic.Bool
	crash.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	a := addStep(t, l, "a", 1, nil)
	b := saga.DefineStep(testdb.UniqueName(t, "b"),
		func(c context.Context, k saga.Key, v int) (int, string, error) {
			if crash.Load() {
				cancel()
				return 0, "", c.Err()
			}
			l.apply(k, "b")
			return v * 10, "", nil
		}, nil)
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := a.Run(c, v.N)
		if err != nil {
			return out{}, err
		}
		x, err = b.Run(c, x)
		return out{Total: x}, err
	})
	if _, err := wf.Run(ctx, crashed, "k1", in{N: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("first run err = %v", err)
	}
	crash.Store(false)

	rescuer := newEngine(t, pool, saga.Options{RecoveryInterval: 200 * time.Millisecond})
	if err := rescuer.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	h, err := wf.Lookup(context.Background(), rescuer, "k1")
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.Wait(waitCtx(t, 15*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if got.Total != 20 {
		t.Fatalf("total = %d, want 20", got.Total)
	}
	if n := len(l.snapshot()); n != 2 {
		t.Fatalf("effects = %v, want a and b once each", l.snapshot())
	}
}

func TestStartEnqueuesAndHandleWaits(t *testing.T) {
	_, eng := setup(t, saga.Options{})
	if err := eng.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	l := newLedger()
	a := addStep(t, l, "a", 2, nil)
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := a.Run(c, v.N)
		return out{Total: x}, err
	})

	h, err := wf.Start(context.Background(), eng, "k1", in{N: 40})
	if err != nil {
		t.Fatal(err)
	}
	h2, err := wf.Start(context.Background(), eng, "k1", in{N: 0})
	if err != nil || h2.ID() != h.ID() {
		t.Fatalf("second Start = %v, %v; want same run", h2, err)
	}
	got, err := h.Wait(waitCtx(t, 10*time.Second))
	if err != nil || got.Total != 42 {
		t.Fatalf("wait = %+v, %v", got, err)
	}
	if st, err := h.Status(context.Background()); err != nil || st != saga.StatusDone {
		t.Fatalf("status = %v, %v", st, err)
	}
	if n := len(l.snapshot()); n != 1 {
		t.Fatalf("step ran %d times", n)
	}
}

func TestStopInterruptsAndReleasesLease(t *testing.T) {
	pool, eng := setup(t, saga.Options{LeaseTTL: time.Hour})
	inStep := make(chan struct{})
	var once atomic.Bool
	a := saga.DefineStep(testdb.UniqueName(t, "a"),
		func(ctx context.Context, k saga.Key, v int) (int, string, error) {
			if once.CompareAndSwap(false, true) {
				close(inStep)
				<-ctx.Done()
				return 0, "", ctx.Err()
			}
			return v + 1, "", nil
		}, nil)
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := a.Run(c, v.N)
		return out{Total: x}, err
	})

	done := make(chan error, 1)
	go func() {
		_, err := wf.Run(context.Background(), eng, "k1", in{N: 1})
		done <- err
	}()
	<-inStep
	stopCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := eng.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stop err = %v, want deadline exceeded (run still in flight)", err)
	}
	if err := <-done; !errors.Is(err, saga.ErrShutdown) {
		t.Fatalf("run err = %v, want ErrShutdown", err)
	}
	if _, err := wf.Run(context.Background(), eng, "k2", in{N: 1}); !errors.Is(err, saga.ErrEngineStopped) {
		t.Fatalf("run after stop err = %v, want ErrEngineStopped", err)
	}

	// The lease was handed back: another instance resumes at once, no TTL wait.
	other := newEngine(t, pool, saga.Options{})
	got, err := wf.Run(context.Background(), other, "k1", in{N: 1})
	if err != nil || got.Total != 2 {
		t.Fatalf("resume elsewhere = %+v, %v", got, err)
	}
}

func TestRecoverySkipsQueuedRuns(t *testing.T) {
	pool, eng := setup(t, saga.Options{}) // not started: the job stays queued
	a := addStep(t, newLedger(), "a", 1, nil)
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v in) (out, error) {
		x, err := a.Run(c, v.N)
		return out{Total: x}, err
	})
	if _, err := wf.Start(context.Background(), eng, "k1", in{N: 1}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if n, err := eng.RecoverExpired(ctx); err != nil || n != 0 {
		t.Fatalf("scan of a freshly queued run = %d, %v; want 0", n, err)
	}
	expireLeases(t, pool) // the queued job got lost
	if n, err := eng.RecoverExpired(ctx); err != nil || n != 1 {
		t.Fatalf("scan after reservation expired = %d, %v; want 1", n, err)
	}
	if n, err := eng.RecoverExpired(ctx); err != nil || n != 0 {
		t.Fatalf("scan right after re-enqueue = %d, %v; want 0", n, err)
	}
	var jobs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM saga.river_job WHERE kind = 'saga.resume_run'`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if jobs != 2 {
		t.Fatalf("resume jobs = %d, want 2 (Start + one recovery)", jobs)
	}
}

func TestResumeUnknownWorkflowLeavesLease(t *testing.T) {
	pool, eng := setup(t, saga.Options{})
	ctx := context.Background()
	var id string
	if err := pool.QueryRow(ctx, `
		INSERT INTO saga.run (id, workflow, version, idem_key, status, input, lease_until)
		VALUES (gen_random_uuid(), 'not.in.this.binary', 1, 'k1', 'running', '{}', now() - interval '1 second')
		RETURNING id::text`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if err := eng.Resume(ctx, id); err != nil {
		t.Fatal(err)
	}
	var version int64
	var owner *string
	if err := pool.QueryRow(ctx, `SELECT lease_version, owner_id FROM saga.run WHERE id = $1`, id).Scan(&version, &owner); err != nil {
		t.Fatal(err)
	}
	if version != 0 || owner != nil {
		t.Fatalf("lease taken for an unknown workflow: version=%d owner=%v", version, owner)
	}
}
