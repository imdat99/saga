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

	// ErrEngineStopped means Stop was called: the engine takes no new work.
	ErrEngineStopped = errors.New("saga: engine stopped")

	// ErrShutdown is the cancellation cause of runs interrupted by
	// Engine.Stop.
	ErrShutdown = errors.New("saga: engine shutting down")

	// ErrWorkflowNotRegistered means a run belongs to a workflow name and
	// version this binary does not define (e.g. during a rolling deploy).
	ErrWorkflowNotRegistered = errors.New("saga: workflow not registered in this binary")

	// ErrRunWaiting means the run is parked (retry backoff, Sleep, Await,
	// compensation backoff) and continues asynchronously on a started engine.
	// Use Workflow.Lookup and Handle.Wait for the outcome. Step.Run, Sleep and
	// Await return it inside workflow functions: return it as is.
	ErrRunWaiting = errors.New("saga: run waiting")

	// ErrRunTimeout means the run passed the deadline set with
	// WithRunTimeout. It is checked when a step starts and when a waiting run
	// wakes up; the workflow then fails and compensates.
	ErrRunTimeout = errors.New("saga: run deadline exceeded")

	// ErrStepTimeout is the cause of a step attempt cancelled by WithTimeout.
	// It counts as a transient failure.
	ErrStepTimeout = errors.New("saga: step attempt timed out")

	// ErrAwaitTimeout means Await gave up waiting for its signal.
	ErrAwaitTimeout = errors.New("saga: signal not received in time")

	// ErrNotNeedsAttention means RetryCompensation was called for a run that
	// is not in needs_attention.
	ErrNotNeedsAttention = errors.New("saga: run does not need attention")

	// ErrRunNotFound means no run exists for the given id or idempotency key.
	ErrRunNotFound = errors.New("saga: run not found")

	// ErrIdemKeyConflict means the idempotency key is already used by a run
	// of a different workflow.
	ErrIdemKeyConflict = errors.New("saga: idempotency key used by another workflow")
)

// RunStatus is the lifecycle state of a run.
type RunStatus string

// Run statuses.
const (
	StatusRunning        RunStatus = store.RunRunning
	StatusWaiting        RunStatus = store.RunWaiting
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
