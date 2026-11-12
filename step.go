package saga

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/imdat99/saga/internal/store"
)

// InvokeFunc performs a step. It returns the step output and the data its
// compensation needs (keep both small: IDs, not whole entities).
type InvokeFunc[I, O, C any] func(ctx context.Context, k Key, in I) (O, C, error)

// CompensateFunc undoes a step. It must be idempotent: it can run more than
// once for the same Key.
type CompensateFunc[C any] func(ctx context.Context, k Key, comp C) error

// Step is a registered saga step.
type Step[I, O, C any] struct {
	name       string
	invoke     InvokeFunc[I, O, C]
	compensate CompensateFunc[C]
}

// DefineStep registers a step. Call it at package level: names must be unique
// and stable across deploys, because recovery finds compensators by name.
// compensate may be nil for steps without side effects to undo.
func DefineStep[I, O, C any](name string, invoke InvokeFunc[I, O, C], compensate CompensateFunc[C]) *Step[I, O, C] {
	s := &Step[I, O, C]{name: name, invoke: invoke, compensate: compensate}
	var comp compensator
	if compensate != nil {
		comp = func(ctx context.Context, k Key, raw json.RawMessage) error {
			var c C
			if len(raw) > 0 {
				if err := json.Unmarshal(raw, &c); err != nil {
					return fmt.Errorf("decode compensation data of %s: %w", name, err)
				}
			}
			return compensate(ctx, k, c)
		}
	}
	registerStep(name, comp)
	return s
}

// Name returns the step name.
func (s *Step[I, O, C]) Name() string { return s.name }

// Run executes the step inside a workflow, or returns its recorded output when
// the workflow is being replayed after a resume.
func (s *Step[I, O, C]) Run(c Ctx, in I) (O, error) {
	var zero O
	rc, ok := c.(*runCtx)
	if !ok {
		return zero, ErrNotInWorkflow
	}
	if rc.fatal != nil {
		return zero, rc.fatal
	}

	seq := rc.nextSeq()
	key := stepKey(rc.run.ID, seq, s.name)

	if rec, seen := rc.steps[seq]; seen {
		if rec.Name != s.name {
			return zero, rc.setFatal(fmt.Errorf("%w: position %d recorded %q, now %q",
				ErrNonDeterministic, seq, rec.Name, s.name))
		}
		switch rec.Status {
		case store.StepDone:
			var out O
			if err := json.Unmarshal(rec.Output, &out); err != nil {
				return zero, rc.setFatal(fmt.Errorf("decode recorded output of %s: %w", s.name, err))
			}
			return out, nil
		case store.StepFailed:
			return zero, &StepError{Step: s.name, Err: fmt.Errorf("%s", rec.Error)}
		}
		// StepStarted: the previous owner died mid-step. Re-run it with the
		// same key; the module deduplicates.
	}

	if err := rc.eng.st.Fenced(rc, rc.lease, func(tx pgx.Tx) error {
		return store.StartStep(rc, tx, rc.run.ID, seq, s.name)
	}); err != nil {
		return zero, rc.setFatal(err)
	}

	out, comp, err := s.invoke(rc, key, in)
	if rc.Err() != nil {
		// Cancelled (shutdown): leave the step "started" for recovery.
		return zero, rc.setFatal(rc.Err())
	}
	if err != nil {
		if ferr := rc.eng.st.Fenced(rc, rc.lease, func(tx pgx.Tx) error {
			return store.FinishStep(rc, tx, rc.run.ID, seq, store.StepFailed, nil, nil, err.Error())
		}); ferr != nil {
			return zero, rc.setFatal(ferr)
		}
		return zero, &StepError{Step: s.name, Err: err}
	}

	outRaw, err := json.Marshal(out)
	if err != nil {
		return zero, rc.setFatal(fmt.Errorf("encode output of %s: %w", s.name, err))
	}
	compRaw, err := json.Marshal(comp)
	if err != nil {
		return zero, rc.setFatal(fmt.Errorf("encode compensation data of %s: %w", s.name, err))
	}
	if err := rc.eng.st.Fenced(rc, rc.lease, func(tx pgx.Tx) error {
		return store.FinishStep(rc, tx, rc.run.ID, seq, store.StepDone, outRaw, compRaw, "")
	}); err != nil {
		return zero, rc.setFatal(err)
	}
	return out, nil
}
