package saga

import (
	"errors"
	"fmt"

	"github.com/imdat99/saga/internal/store"
)

var (
	// ErrLeaseLost means another instance took over the run. The current
	// execution stops without compensating; the new owner continues.
	ErrLeaseLost = store.ErrLeaseLost

	// ErrRunInProgress means the run for this idempotency key is being
	// executed by someone else (its lease is still live).
	ErrRunInProgress = errors.New("saga: run in progress")

	// ErrNonDeterministic means the workflow function called a different step
	// than the one recorded at the same position during an earlier execution.
	ErrNonDeterministic = errors.New("saga: non-deterministic workflow")

	// ErrNotInWorkflow means Step.Run was called with a context that does not
	// belong to a workflow execution.
	ErrNotInWorkflow = errors.New("saga: step called outside a workflow")

	// ErrIdemKeyConflict means the idempotency key is already used by a run
	// of a different workflow.
	ErrIdemKeyConflict = errors.New("saga: idempotency key used by another workflow")
)

// RunStatus is the lifecycle state of a run.
type RunStatus string

// Run statuses.
const (
	StatusRunning        RunStatus = store.RunRunning
	StatusCompensating   RunStatus = store.RunCompensating
	StatusDone           RunStatus = store.RunDone
	StatusCompensated    RunStatus = store.RunCompensated
	StatusNeedsAttention RunStatus = store.RunNeedsAttention
)

// RunError is returned when a run ended without producing an output: it was
// compensated, or it needs manual attention.
type RunError struct {
	RunID  string
	Status RunStatus
	Cause  string
}

func (e *RunError) Error() string {
	return fmt.Sprintf("saga: run %s %s: %s", e.RunID, e.Status, e.Cause)
}

// StepError wraps the error returned by a step invocation.
type StepError struct {
	Step string
	Err  error
}

func (e *StepError) Error() string { return fmt.Sprintf("step %s: %v", e.Step, e.Err) }
func (e *StepError) Unwrap() error { return e.Err }
