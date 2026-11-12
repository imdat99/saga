package saga_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/internal/testdb"
)

// blockingWorkflow defines a one-step workflow whose step, before returning,
// locks its own step row from another transaction: the step's "done" checkpoint
// then waits on that lock until the returned release func is called.
func blockingWorkflow(t *testing.T, pool *pgxpool.Pool) (*saga.Workflow[int, int], <-chan struct{}, func()) {
	t.Helper()
	locked := make(chan struct{})
	txc := make(chan pgx.Tx, 1)
	step := saga.DefineStep(testdb.UniqueName(t, "s"),
		func(ctx context.Context, k saga.Key, v int) (int, struct{}, error) {
			tx, err := pool.Begin(context.Background())
			if err != nil {
				return 0, struct{}{}, err
			}
			// The step row, not the run row: releasing the lease on shutdown
			// updates the run row and must not wait on the test.
			if _, err := tx.Exec(context.Background(),
				`SELECT 1 FROM saga.step WHERE run_id = $1 AND seq = 1 FOR UPDATE`, k.RunID()); err != nil {
				return 0, struct{}{}, err
			}
			txc <- tx
			close(locked)
			return v, struct{}{}, nil
		}, nil)
	wf := saga.DefineWorkflow(testdb.UniqueName(t, "wf"), 1, func(c saga.Ctx, v int) (int, error) {
		return step.Run(c, v)
	})
	release := func() {
		select {
		case tx := <-txc:
			_ = tx.Rollback(context.Background())
		default:
		}
	}
	t.Cleanup(release)
	return wf, locked, release
}

// The caller cancelling its context stops the checkpoint statement in flight
// (through the heartbeat entry's interrupt, not pgconn's watch).
func TestCallerCancelInterruptsCheckpoint(t *testing.T) {
	pool, eng := setup(t, saga.Options{})
	wf, locked, release := blockingWorkflow(t, pool)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-locked
		time.Sleep(200 * time.Millisecond) // the done checkpoint is waiting
		cancel()
	}()
	errc := make(chan error, 1)
	go func() {
		_, err := wf.Run(ctx, eng, "k", 1)
		errc <- err
	}()
	select {
	case err := <-errc:
		release()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		release() // unblock the run so the test can end
		t.Fatal("checkpoint not interrupted by the caller's cancellation")
	}
}

// Shutdown past its deadline interrupts a run whose checkpoint is waiting.
func TestShutdownInterruptsCheckpoint(t *testing.T) {
	pool := testdb.New(t)
	if err := saga.Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	eng, err := saga.New(pool, saga.Options{ShutdownGrace: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	wf, locked, release := blockingWorkflow(t, pool)
	errc := make(chan error, 1)
	go func() {
		_, err := wf.Run(context.Background(), eng, "k", 1)
		errc <- err
	}()
	<-locked
	time.Sleep(200 * time.Millisecond)
	stopCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_ = eng.Stop(stopCtx)
	select {
	case err := <-errc:
		if !errors.Is(err, saga.ErrShutdown) {
			t.Fatalf("err = %v, want ErrShutdown", err)
		}
	case <-time.After(3 * time.Second):
		release() // unblock the run so the test can end
		t.Fatal("run still blocked after shutdown")
	}
	release()
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("stop took %v", d)
	}
}

var errCallerGaveUp = errors.New("caller gave up")

// A caller's deadline with a cause interrupts the checkpoint and the run
// reports that cause.
func TestCallerDeadlineCauseInterruptsCheckpoint(t *testing.T) {
	pool, eng := setup(t, saga.Options{})
	wf, locked, release := blockingWorkflow(t, pool)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	go func() {
		<-locked
		time.Sleep(200 * time.Millisecond)
		cancel(errCallerGaveUp)
	}()
	errc := make(chan error, 1)
	go func() {
		_, err := wf.Run(ctx, eng, "k", 1)
		errc <- err
	}()
	select {
	case err := <-errc:
		release()
		if !errors.Is(err, errCallerGaveUp) {
			t.Fatalf("err = %v, want the caller's cause", err)
		}
	case <-time.After(5 * time.Second):
		release()
		t.Fatal("checkpoint not interrupted by the caller's cancellation")
	}
}

// A lease that can no longer be renewed interrupts the checkpoint in flight.
// Here the blocked checkpoint holds the run row lock, so the renewals block
// too and time out; after most of a TTL without renewal the heartbeat gives
// the lease up (ErrLeaseLost) and must stop the statement.
func TestLeaseLostInterruptsCheckpoint(t *testing.T) {
	pool, eng := setup(t, saga.Options{LeaseTTL: 600 * time.Millisecond})
	wf, _, release := blockingWorkflow(t, pool)
	errc := make(chan error, 1)
	go func() {
		_, err := wf.Run(context.Background(), eng, "k", 1)
		errc <- err
	}()
	select {
	case err := <-errc:
		release()
		if !errors.Is(err, saga.ErrLeaseLost) {
			t.Fatalf("err = %v, want ErrLeaseLost", err)
		}
	case <-time.After(5 * time.Second):
		release()
		t.Fatal("checkpoint not interrupted after the lease was lost")
	}
}
