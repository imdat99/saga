package saga

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/imdat99/saga/internal/store"
)

// wakeState is the output of a waiting pseudo-step (Sleep, Await): when it is
// due. A nil WakeAt means no timeout.
type wakeState struct {
	WakeAt *time.Time `json:"wake_at,omitempty"`
}

// pseudoStep claims the next position for an engine pseudo-step and returns
// its recorded row, if any.
func pseudoStep(c Ctx, name string) (*runCtx, int, store.Step, bool, error) {
	rc, ok := c.(*runCtx)
	if !ok {
		return nil, 0, store.Step{}, false, ErrNotInWorkflow
	}
	if rc.fatal != nil {
		return nil, 0, store.Step{}, false, rc.fatal
	}
	seq := rc.nextSeq()
	rec, seen := rc.steps[seq]
	if seen && rec.Name != name {
		return nil, 0, store.Step{}, false, rc.nonDeterministic(seq, rec.Name, name)
	}
	return rc, seq, rec, seen, nil
}

func decodeWake(rec store.Step) (*time.Time, error) {
	var w wakeState
	if len(rec.Output) == 0 {
		return nil, nil
	}
	if err := json.Unmarshal(rec.Output, &w); err != nil {
		return nil, fmt.Errorf("decode wake time of %s: %w", rec.Name, err)
	}
	return w.WakeAt, nil
}

func (c *runCtx) pastDeadline(now time.Time) bool {
	return c.run.Deadline != nil && !now.Before(*c.run.Deadline)
}

const sleepStep = reservedPrefix + "sleep"

// Sleep pauses the workflow for d without holding the lease: the run is parked
// and a job resumes it once d has elapsed (database time), on any instance.
// Like a step, it returns ErrRunWaiting the first time; return it as is.
// River promotes scheduled jobs about every 5 seconds, so short sleeps last
// longer in practice. d <= 0 returns at once without a checkpoint (d must be
// computed deterministically, like everything between steps).
func Sleep(c Ctx, d time.Duration) error {
	if d <= 0 {
		if _, ok := c.(*runCtx); !ok {
			return ErrNotInWorkflow
		}
		return nil
	}
	rc, seq, rec, seen, err := pseudoStep(c, sleepStep)
	if err != nil {
		return err
	}
	if seen && rec.Status == store.StepDone {
		return nil
	}
	if seen && rec.Status == store.StepFailed {
		return &StepError{Step: sleepStep, Err: recordedError(rec.Error, rec.ErrorKind)}
	}

	var outcome error
	err = rc.eng.st.Fenced(rc, rc.lease, func(tx pgx.Tx) error {
		now, err := store.Now(rc, tx)
		if err != nil {
			return err
		}
		wake := now.Add(d)
		if seen {
			w, err := decodeWake(rec)
			if err != nil {
				return err
			}
			if w == nil {
				return fmt.Errorf("saga: %s at position %d has no wake time", sleepStep, seq)
			}
			if !w.After(now) {
				return store.PutStep(rc, tx, rc.run.ID, seq, sleepStep, store.Outcome{Status: store.StepDone, Output: rec.Output})
			}
			wake = *w
		}
		if rc.pastDeadline(now) {
			outcome = &StepError{Step: sleepStep, Err: ErrRunTimeout}
			return store.PutStep(rc, tx, rc.run.ID, seq, sleepStep, failedOutcome(store.StepFailed, ErrRunTimeout))
		}
		out, err := json.Marshal(wakeState{WakeAt: &wake})
		if err != nil {
			return err
		}
		waiting := store.Outcome{Status: store.StepWaiting, Output: out}
		if err := store.PutStep(rc, tx, rc.run.ID, seq, sleepStep, waiting); err != nil {
			return err
		}
		outcome = errSuspended
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

// Await waits for the signal name delivered with Engine.Signal and returns its
// payload. The run is parked meanwhile, without a lease. A signal sent before
// Await is reached is kept and consumed at once. With timeout > 0, Await fails
// with ErrAwaitTimeout once timeout has elapsed. A payload that does not
// decode into T fails the step (permanently). Like a step, it returns
// ErrRunWaiting while parked; return it as is.
//
// Signals are stored once per run and name: awaiting the same name twice
// returns the first payload again.
func Await[T any](c Ctx, name string, timeout time.Duration) (T, error) {
	var zero T
	stepName := reservedPrefix + "await:" + name
	rc, seq, rec, seen, err := pseudoStep(c, stepName)
	if err != nil {
		return zero, err
	}
	switch {
	case seen && rec.Status == store.StepDone:
		v, err := decodeSignal[T](name, rec.Output)
		if err != nil {
			// It decoded when it was recorded: the type changed since.
			return zero, rc.setFatal(fmt.Errorf("%w: %w", ErrNonDeterministic, err))
		}
		return v, nil
	case seen && rec.Status == store.StepFailed:
		return zero, &StepError{Step: stepName, Err: recordedError(rec.Error, rec.ErrorKind)}
	}

	var value T
	var outcome error
	err = rc.eng.st.Fenced(rc, rc.lease, func(tx pgx.Tx) error {
		var err error
		at := awaitAt{seq: seq, step: stepName, signal: name, rec: rec, seen: seen, timeout: timeout}
		value, outcome, err = awaitTx[T](rc, tx, at)
		return err
	})
	if err != nil {
		return zero, rc.fail(err)
	}
	if errors.Is(outcome, errSuspended) {
		return zero, rc.suspend()
	}
	if outcome != nil {
		return zero, outcome
	}
	return value, nil
}

// awaitAt is the position and state of an Await pseudo-step.
type awaitAt struct {
	seq     int
	step    string
	signal  string
	rec     store.Step
	seen    bool
	timeout time.Duration
}

// awaitTx runs inside the fenced transaction of Await. It consumes the signal
// if present, fails the step on timeout, or parks the run. outcome is a
// *StepError, errSuspended, or nil with the decoded value.
func awaitTx[T any](rc *runCtx, tx pgx.Tx, a awaitAt) (value T, outcome error, err error) {
	fail := func(cause error) error {
		outcome = &StepError{Step: a.step, Err: cause}
		return store.PutStep(rc, tx, rc.run.ID, a.seq, a.step, failedOutcome(store.StepFailed, cause))
	}
	// Under the run row lock: Engine.Signal cannot slip in between this check
	// and parking.
	p, found, err := store.GetSignal(rc, tx, rc.run.ID, a.signal)
	if err != nil {
		return value, nil, err
	}
	if found {
		if value, err = decodeSignal[T](a.signal, p); err != nil {
			return value, outcome, fail(Permanent(err))
		}
		return value, nil, store.PutStep(rc, tx, rc.run.ID, a.seq, a.step, store.Outcome{Status: store.StepDone, Output: p})
	}
	now, err := store.Now(rc, tx)
	if err != nil {
		return value, nil, err
	}
	var wake *time.Time
	if a.seen {
		if wake, err = decodeWake(a.rec); err != nil {
			return value, nil, err
		}
	} else if a.timeout > 0 {
		t := now.Add(a.timeout)
		wake = &t
	}
	switch {
	case wake != nil && !wake.After(now):
		return value, outcome, fail(ErrAwaitTimeout)
	case rc.pastDeadline(now):
		return value, outcome, fail(ErrRunTimeout)
	}
	out, err := json.Marshal(wakeState{WakeAt: wake})
	if err != nil {
		return value, nil, err
	}
	waiting := store.Outcome{Status: store.StepWaiting, Output: out}
	if err := store.PutStep(rc, tx, rc.run.ID, a.seq, a.step, waiting); err != nil {
		return value, nil, err
	}
	return value, errSuspended, rc.eng.park(rc, tx, rc.run.ID, wake, a.signal)
}

func decodeSignal[T any](name string, raw json.RawMessage) (T, error) {
	var v T
	if len(raw) == 0 || string(raw) == "null" {
		return v, nil
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return v, fmt.Errorf("decode signal %s: %w", name, err)
	}
	return v, nil
}

// Signal delivers a named signal with its payload to a run. The first signal
// of a name wins; later ones are ignored, so callers may retry. If the run is
// parked in Await on this name, a resume job is enqueued in the same
// transaction, and the run is reserved so that recovery re-enqueues it if that
// job is lost.
func (e *Engine) Signal(ctx context.Context, runID, name string, payload any) error {
	if err := validRunID(runID); err != nil {
		return err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode signal %s: %w", name, err)
	}
	err = pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
		wake, err := store.DeliverSignal(ctx, tx, runID, name, raw, e.queuedGrace())
		if err != nil || !wake {
			return err
		}
		if _, err := e.river.InsertTx(ctx, tx, resumeArgs{RunID: runID}, nil); err != nil {
			return fmt.Errorf("enqueue resume_run: %w", err)
		}
		return nil
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: %s", ErrRunNotFound, runID)
	}
	return err
}

// SignalKey delivers a signal named after a step Key to the run that owns
// the key. It serves long-running work outside the process: a step starts the
// work and hands its Key to the external system as reference, the workflow
// waits with Await(c, string(key), timeout), and the external system reports
// back with SignalKey(ctx, key, result) knowing nothing but the key.
func (e *Engine) SignalKey(ctx context.Context, key Key, payload any) error {
	return e.Signal(ctx, key.RunID(), string(key), payload)
}

// validRunID rejects ids that cannot name a run, so they report
// ErrRunNotFound instead of a Postgres syntax error.
func validRunID(id string) error {
	if _, err := uuid.Parse(id); err != nil {
		return fmt.Errorf("%w: %q", ErrRunNotFound, id)
	}
	return nil
}

// RetryCompensation resumes the compensation of a run parked as
// needs_attention (compensation attempts exhausted, or non-deterministic
// workflow): failed compensators get a fresh CompensatePolicy budget, and the
// run is enqueued for any started engine. Use Handle.Wait for the outcome.
func (e *Engine) RetryCompensation(ctx context.Context, runID string) error {
	if err := validRunID(runID); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
		ok, err := store.ReopenCompensation(ctx, tx, runID, e.queuedGrace())
		if err != nil {
			return err
		}
		if !ok {
			if _, err := e.getRun(ctx, runID); err != nil {
				return err
			}
			return fmt.Errorf("%w: %s", ErrNotNeedsAttention, runID)
		}
		if _, err := e.river.InsertTx(ctx, tx, resumeArgs{RunID: runID}, nil); err != nil {
			return fmt.Errorf("enqueue resume_run: %w", err)
		}
		return nil
	})
}

var (
	nowStep = defineStep(reservedPrefix+"now",
		func(ctx context.Context, _ Key, _ struct{}) (time.Time, struct{}, error) {
			rc, ok := ctx.Value(runCtxKey{}).(*runCtx)
			if !ok || rc == nil {
				return time.Time{}, struct{}{}, ErrNotInWorkflow
			}
			var now time.Time
			err := rc.eng.pool.QueryRow(ctx, `SELECT now()`).Scan(&now)
			return now, struct{}{}, err
		}, nil)

	randStep = defineStep(reservedPrefix+"rand",
		func(context.Context, Key, struct{}) (uint64, struct{}, error) {
			var b [8]byte
			if _, err := rand.Read(b[:]); err != nil {
				return 0, struct{}{}, err
			}
			return binary.LittleEndian.Uint64(b[:]), struct{}{}, nil
		}, nil)
)

// Now returns the current database time, recorded on first execution and
// replayed afterwards, so workflow code may branch on it deterministically.
// It costs one checkpointed step.
func Now(c Ctx) (time.Time, error) { return nowStep.Run(c, struct{}{}) }

// Rand returns a random number, recorded on first execution and replayed
// afterwards. It costs one checkpointed step.
func Rand(c Ctx) (uint64, error) { return randStep.Run(c, struct{}{}) }
