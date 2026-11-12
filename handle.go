package saga

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/imdat99/saga/internal/store"
)

// Wait polling bounds. Each waiter costs one indexed primary-key read per
// interval; the backoff keeps idle waiters cheap.
const (
	waitPollMin = 20 * time.Millisecond
	waitPollMax = time.Second
)

// Handle refers to a run that may execute in another goroutine or instance.
type Handle[O any] struct {
	e  *Engine
	id string
}

// ID returns the run id.
func (h *Handle[O]) ID() string { return h.id }

// Status returns the current status of the run.
func (h *Handle[O]) Status(ctx context.Context) (RunStatus, error) {
	r, err := h.e.getRun(ctx, h.id)
	if err != nil {
		return "", err
	}
	return RunStatus(r.Status), nil
}

// Wait blocks until the run finishes or ctx is done. It returns the output of
// a done run, or a *RunError for a compensated or needs_attention run.
func (h *Handle[O]) Wait(ctx context.Context) (O, error) {
	var zero O
	delay := waitPollMin
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-t.C:
		}
		r, err := h.e.getRun(ctx, h.id)
		if err != nil {
			return zero, err
		}
		if raw, finished, err := result(r); finished {
			if err != nil {
				return zero, err
			}
			return decodeOutput[O](r.Workflow, raw)
		}
		t.Reset(delay)
		delay = min(delay*2, waitPollMax)
	}
}

func (e *Engine) getRun(ctx context.Context, id string) (store.Run, error) {
	if err := validRunID(id); err != nil {
		return store.Run{}, err
	}
	r, err := e.st.GetRun(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, fmt.Errorf("%w: %s", ErrRunNotFound, id)
	}
	return r, err
}

func decodeOutput[O any](workflow string, raw json.RawMessage) (O, error) {
	var out O
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, fmt.Errorf("decode output of %s: %w", workflow, err)
	}
	return out, nil
}

// Start creates a run and enqueues it, in one transaction, for execution by
// any started engine (this one included) instead of running it inline. With
// an idempotency key already in use it returns a handle to the existing run.
func (w *Workflow[I, O]) Start(ctx context.Context, e *Engine, idemKey string, in I) (*Handle[O], error) {
	raw, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("encode input of %s: %w", w.name, err)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	run := store.Run{ID: id.String(), Workflow: w.name, Version: w.version, IdemKey: idemKey, Input: raw}

	inserted := false
	err = pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
		var err error
		if inserted, err = store.InsertQueuedRun(ctx, tx, run, e.queuedGrace(), w.timeout); err != nil || !inserted {
			return err
		}
		if _, err := e.river.InsertTx(ctx, tx, resumeArgs{RunID: run.ID}, nil); err != nil {
			return fmt.Errorf("enqueue resume_run: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if inserted {
		return &Handle[O]{e: e, id: run.ID}, nil
	}
	return w.Lookup(ctx, e, idemKey)
}

// Lookup returns a handle to the run of this workflow with the given
// idempotency key, e.g. after Run returned ErrRunInProgress.
func (w *Workflow[I, O]) Lookup(ctx context.Context, e *Engine, idemKey string) (*Handle[O], error) {
	r, err := e.st.GetRunByIdemKey(ctx, idemKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: idempotency key %q", ErrRunNotFound, idemKey)
	}
	if err != nil {
		return nil, err
	}
	if r.Workflow != w.name {
		return nil, fmt.Errorf("%w: key %q belongs to %s", ErrIdemKeyConflict, idemKey, r.Workflow)
	}
	return &Handle[O]{e: e, id: r.ID}, nil
}

// Handle returns a handle to the run with the given id. It does not check
// that the run exists or belongs to this workflow; Status and Wait report
// ErrRunNotFound for an unknown id.
func (w *Workflow[I, O]) Handle(e *Engine, runID string) *Handle[O] {
	return &Handle[O]{e: e, id: runID}
}

// PurgeRuns deletes up to limit done or compensated runs that ended more than
// age ago, with their steps, signals and locks (cascade), and returns how many
// it deleted. needs_attention runs are never purged. Set Options.Retention to
// run it periodically instead.
func (e *Engine) PurgeRuns(ctx context.Context, age time.Duration, limit int) (int64, error) {
	if age <= 0 || limit <= 0 {
		return 0, fmt.Errorf("saga: PurgeRuns(age %v, limit %d)", age, limit)
	}
	return e.st.PurgeRuns(ctx, age, limit)
}
