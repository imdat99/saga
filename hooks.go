package saga

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// RunInfo describes a run reaching a status, for hooks.
type RunInfo struct {
	ID       string
	Workflow string
	Version  int
	Status   RunStatus
	// Error is the failure cause of a compensated or needs_attention run.
	Error string
}

// RunHook observes runs ending. RunEnded is called inside the transaction
// that records a terminal status (done, compensated, needs_attention), under
// the run's lease, so whatever it writes commits atomically with the status;
// returning an error rolls the transition back (the run is retried later).
// The event bus uses it to release or drop the events a run staged.
//
// It runs on the engine's hot path: keep it to a few statements.
type RunHook interface {
	RunEnded(ctx context.Context, tx pgx.Tx, run RunInfo) error
}

// Observer receives engine measurements, e.g. to feed metrics. It is called
// after the fact, outside transactions; methods must be fast and must not
// block (hand off to a channel or update counters).
type Observer interface {
	// StepAttempt is called after each recorded attempt of a step defined
	// with DefineStep (not for Sleep, Await, Lock, nor compensators).
	StepAttempt(a StepAttempt)
	// RunEnded is called after a terminal status committed.
	RunEnded(run RunInfo)
}

// StepAttempt describes one step attempt.
type StepAttempt struct {
	RunID    string
	Workflow string
	Step     string
	Attempt  int
	// Outcome is "done", "failed" or "retrying".
	Outcome  string
	Duration time.Duration
	Err      error
}

type noopObserver struct{}

func (noopObserver) StepAttempt(StepAttempt) {}
func (noopObserver) RunEnded(RunInfo)        {}
