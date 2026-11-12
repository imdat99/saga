package saga

import (
	"context"
	"encoding/json"
	"fmt"
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
}

// DefineWorkflow registers a workflow under name and version. Bump the version
// when the sequence of steps changes, and keep the old definition registered
// until its runs have finished.
func DefineWorkflow[I, O any](name string, version int, fn WorkflowFunc[I, O]) *Workflow[I, O] {
	registerWorkflow(name, version, func(c *runCtx, raw json.RawMessage) (json.RawMessage, error) {
		var in I
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, c.setFatal(fmt.Errorf("decode input of %s: %w", name, err))
		}
		out, err := fn(c, in)
		if err != nil {
			return nil, err
		}
		b, err := json.Marshal(out)
		if err != nil {
			return nil, c.setFatal(fmt.Errorf("encode output of %s: %w", name, err))
		}
		return b, nil
	})
	return &Workflow[I, O]{name: name, version: version}
}

// Name returns the workflow name.
func (w *Workflow[I, O]) Name() string { return w.name }

// Run starts the workflow, or joins the existing run with the same
// idempotency key: a finished run returns its stored result, an abandoned one
// (expired lease) is resumed, a live one returns ErrRunInProgress.
func (w *Workflow[I, O]) Run(ctx context.Context, e *Engine, idemKey string, in I) (O, error) {
	var zero O
	raw, err := json.Marshal(in)
	if err != nil {
		return zero, fmt.Errorf("encode input of %s: %w", w.name, err)
	}
	outRaw, err := e.run(ctx, w.name, w.version, idemKey, raw)
	if err != nil {
		return zero, err
	}
	var out O
	if err := json.Unmarshal(outRaw, &out); err != nil {
		return zero, fmt.Errorf("decode output of %s: %w", w.name, err)
	}
	return out, nil
}
