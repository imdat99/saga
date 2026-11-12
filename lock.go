package saga

import (
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/imdat99/saga/internal/store"
)

var (
	// ErrLockHeld means the business lock is held by another run and the
	// contention policy gave up. The Lock step fails permanently.
	ErrLockHeld = errors.New("saga: lock held by another run")

	// ErrLockLost means a run resumed after its lock expired and another run
	// took it meanwhile. The Lock step is recorded failed, so the workflow
	// fails and compensates.
	ErrLockLost = errors.New("saga: lock lost")
)

const (
	kindLockHeld = "lock_held"
	kindLockLost = "lock_lost"
)

// LockOption configures Lock.
type LockOption func(*lockConfig)

type lockConfig struct {
	wait RetryPolicy
}

// LockWait makes Lock wait for a busy lock instead of failing: the run is
// parked and tries again after each backoff of p, until p is exhausted.
func LockWait(p RetryPolicy) LockOption { return func(c *lockConfig) { c.wait = p } }

// Lock takes the business lock key for the rest of the run. The lock is
// released when the run is done or compensated. While the run executes
// (forward or compensating), the lease heartbeat keeps extending it by ttl;
// when the run is parked (retry, Sleep, Await, compensation backoff), dies, or
// is parked as needs_attention, the lock lapses after ttl and another run may
// take it. A resumed run that finds its lock taken (or taken and released) in
// the meantime fails with ErrLockLost and compensates. So ttl bounds how long
// a parked run keeps others out. ttl is raised to Options.LeaseTTL at least,
// and Lock fails for ttl <= 0.
//
// Locks are polled, not queued: waiters are not served in order, and two runs
// taking two locks in opposite orders with LockWait wait for each other until
// their policies run out. Take locks in a fixed order.
//
// If another run holds the lock, Lock fails with ErrLockHeld at once, or with
// LockWait parks the run and retries with backoff (returning ErrRunWaiting
// meanwhile, like a step). Taking a lock the run already holds succeeds.
func Lock(c Ctx, key string, ttl time.Duration, opts ...LockOption) error {
	cfg := lockConfig{wait: noRetry}
	for _, o := range opts {
		o(&cfg)
	}
	if ttl <= 0 {
		return fmt.Errorf("saga: Lock(%q) with ttl %v", key, ttl)
	}
	stepName := reservedPrefix + "lock:" + key
	rc, seq, rec, seen, err := pseudoStep(c, stepName)
	if err != nil {
		return err
	}
	if seen && rec.Status == store.StepFailed {
		return &StepError{Step: stepName, Err: recordedError(rec.Error, rec.ErrorKind)}
	}
	ttl = max(ttl, rc.eng.opts.LeaseTTL)
	if seen && rec.Status == store.StepDone {
		return reassertLock(rc, seq, stepName, key, ttl)
	}

	var outcome error
	err = rc.eng.st.Fenced(rc, rc.lease, func(tx pgx.Tx) error {
		attempt, now, err := store.StartStep(rc, tx, rc.run.ID, seq, stepName)
		if err != nil {
			return err
		}
		fail := func(cause error) error {
			outcome = &StepError{Step: stepName, Err: cause}
			o := failedOutcome(store.StepFailed, cause)
			o.ErrorKind = lockKind(cause)
			return store.FinishStep(rc, tx, rc.run.ID, seq, o)
		}
		if rc.pastDeadline(now) {
			return fail(ErrRunTimeout)
		}
		ok, holder, err := store.AcquireLock(rc, tx, key, rc.run.ID, ttl)
		if err != nil {
			return err
		}
		if ok {
			return store.FinishStep(rc, tx, rc.run.ID, seq, store.Outcome{Status: store.StepDone, Output: []byte("{}")})
		}
		held := fmt.Errorf("%w: %s held by run %s", ErrLockHeld, key, holder)
		if cfg.wait.exhausted(attempt) {
			return fail(Permanent(held))
		}
		retrying := store.Outcome{Status: store.StepRetrying, Error: held.Error()}
		if err := store.FinishStep(rc, tx, rc.run.ID, seq, retrying); err != nil {
			return err
		}
		outcome = errSuspended
		wake := now.Add(cfg.wait.backoff(attempt))
		return rc.eng.park(rc, tx, rc.run.ID, &wake, "")
	})
	if err != nil {
		return rc.fail(err)
	}
	if errors.Is(outcome, errSuspended) {
		return rc.suspend()
	}
	return outcome
}

// reassertLock runs when a resumed run replays a Lock it took earlier: it
// extends the lock if this run still owns its row, or records that it was
// lost. A missing row means another run took the lock after it expired (and
// maybe released it since): the protected section was not exclusive.
func reassertLock(rc *runCtx, seq int, stepName, key string, ttl time.Duration) error {
	var outcome error
	err := rc.eng.st.Fenced(rc, rc.lease, func(tx pgx.Tx) error {
		ok, err := store.ExtendLock(rc, tx, key, rc.run.ID, ttl)
		if err != nil || ok {
			return err
		}
		lost := fmt.Errorf("%w: %s was taken by another run while this one was away", ErrLockLost, key)
		outcome = &StepError{Step: stepName, Err: lost}
		return store.FinishStep(rc, tx, rc.run.ID, seq, store.Outcome{
			Status: store.StepFailed, Error: lost.Error(), ErrorKind: kindLockLost,
		})
	})
	if err != nil {
		return rc.fail(err)
	}
	return outcome
}

func lockKind(err error) string {
	switch {
	case errors.Is(err, ErrLockHeld):
		return kindLockHeld
	case errors.Is(err, ErrLockLost):
		return kindLockLost
	}
	return errorKind(err)
}
