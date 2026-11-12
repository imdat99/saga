package saga

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// WorkflowFunc is the body of a workflow. It runs again from the top when a
// run is resumed, with completed steps returning their recorded outputs, so
// code between steps must be deterministic: no branching on time, randomness
// or data read outside a step.
type WorkflowFunc[I, O any] func(c Ctx, in I) (O, error)

// Workflow is a registered workflow definition.
type Workflow[I, O any] struct {
	name    string
	version int
	timeout time.Duration
	inJSON  *jsonSlot[I]
}

// WorkflowOption configures a workflow.
type WorkflowOption func(*workflowConfig)

type workflowConfig struct {
	timeout time.Duration
}

// WithRunTimeout sets a deadline of d after the run is created. It is checked
// when a step starts and while the run waits (a parked run wakes up at the
// deadline at the latest); past it, the step fails with ErrRunTimeout and the
// workflow compensates. A step already running is bounded by its own
// WithTimeout, not by this deadline.
func WithRunTimeout(d time.Duration) WorkflowOption {
	return func(c *workflowConfig) { c.timeout = d }
}

// DefineWorkflow registers a workflow under name and version. Bump the version
// when the sequence of steps changes, and keep the old definition registered
// until its runs have finished.
func DefineWorkflow[I, O any](name string, version int, fn WorkflowFunc[I, O], opts ...WorkflowOption) *Workflow[I, O] {
	var cfg workflowConfig
	for _, o := range opts {
		o(&cfg)
	}
	outJSON := newJSONSlot[O]()
	registerWorkflow(name, version, func(c *runCtx, raw json.RawMessage) (json.RawMessage, error) {
		var in I
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, c.setFatal(fmt.Errorf("decode input of %s: %w", name, err))
		}
		out, err := fn(c, in)
		if err != nil {
			return nil, err
		}
		b, err := outJSON.marshal(out)
		if err != nil {
			return nil, c.setFatal(fmt.Errorf("encode output of %s: %w", name, err))
		}
		return b, nil
	})
	return &Workflow[I, O]{name: name, version: version, timeout: cfg.timeout, inJSON: newJSONSlot[I]()}
}

// Name returns the workflow name.
func (w *Workflow[I, O]) Name() string { return w.name }

// Run starts the workflow and executes it inline, in the caller's goroutine,
// or joins the existing run with the same idempotency key: a finished run
// returns its stored result, an abandoned one (expired lease) is resumed here,
// and a live one returns ErrRunInProgress (use Lookup and Handle.Wait). If the
// run parks (retry backoff, Sleep, Await), Run returns ErrRunWaiting and the
// run continues on a started engine.
func (w *Workflow[I, O]) Run(ctx context.Context, e *Engine, idemKey string, in I) (O, error) {
	var zero O
	raw, err := w.inJSON.marshal(in)
	if err != nil {
		return zero, fmt.Errorf("encode input of %s: %w", w.name, err)
	}
	req := runRequest{workflow: w.name, version: w.version, timeout: w.timeout, idemKey: idemKey, input: raw}
	outRaw, err := e.run(ctx, req)
	if err != nil {
		return zero, err
	}
	return decodeOutput[O](w.name, outRaw)
}
