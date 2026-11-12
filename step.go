package saga

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

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
	name   string
	invoke InvokeFunc[I, O, C]
	cfg    stepConfig
}

// DefineStep registers a step. Call it at package level: names must be unique
// and stable across deploys, because recovery finds compensators by name.
// compensate may be nil for steps without side effects to undo. Names starting
// with "saga." are reserved.
func DefineStep[I, O, C any](
	name string,
	invoke InvokeFunc[I, O, C],
	compensate CompensateFunc[C],
	opts ...StepOption,
) *Step[I, O, C] {
	if isReserved(name) {
		panic(fmt.Sprintf("saga: step name %q is reserved", name))
	}
	return defineStep(name, invoke, compensate, opts...)
}

func defineStep[I, O, C any](
	name string,
	invoke InvokeFunc[I, O, C],
	compensate CompensateFunc[C],
	opts ...StepOption,
) *Step[I, O, C] {
	cfg := stepConfig{retry: noRetry, compensate: defaultCompensatePolicy}
	for _, o := range opts {
		o(&cfg)
	}
	s := &Step[I, O, C]{name: name, invoke: invoke, cfg: cfg}
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
	registerStep(name, comp, cfg.compensate)
	return s
}

// Name returns the step name.
func (s *Step[I, O, C]) Name() string { return s.name }

// Run executes the step inside a workflow, or returns its recorded output when
// the workflow is being replayed after a resume.
//
// A failed attempt is retried according to the step's RetryPolicy: the run is
// parked with its lease released and resumed after the backoff, possibly on
// another instance; Run then returns an error wrapping ErrRunWaiting, which the
// workflow function must return as is.
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

	// A step replayed as "started" was interrupted with an unknown outcome:
	// whatever happens now, it must be compensated as uncertain. Known from
	// history, so it holds even if the re-run's own writes are lost.
	priorUnknown := false
	if rec, seen := rc.steps[seq]; seen {
		priorUnknown = rec.Status == store.StepStarted
		if rec.Name != s.name {
			return zero, rc.nonDeterministic(seq, rec.Name, s.name)
		}
		switch rec.Status {
		case store.StepDone:
			var out O
			if err := json.Unmarshal(rec.Output, &out); err != nil {
				return zero, rc.setFatal(fmt.Errorf("%w: decode recorded output of %s: %w", ErrNonDeterministic, s.name, err))
			}
			return out, nil
		case store.StepFailed:
			return zero, &StepError{Step: s.name, Err: recordedError(rec.Error, rec.ErrorKind)}
		}
		// started (the previous owner died mid-step) or retrying: run it
		// again with the same key; the module deduplicates.
	}

	// Hot path: one fenced statement per checkpoint (see StartStepFenced).
	attempt, now, err := rc.eng.st.StartStepFenced(rc, rc.lease, seq, s.name, rc.eng.opts.AsyncStepStart)
	if err != nil {
		return zero, rc.fail(err)
	}
	if rc.pastDeadline(now) {
		timedOut := failedOutcome(store.StepFailed, ErrRunTimeout)
		timedOut.Uncertain = timedOut.Uncertain || priorUnknown
		if err := rc.eng.st.FinishStepFenced(rc, rc.lease, seq, s.name, timedOut); err != nil {
			return zero, rc.fail(err)
		}
		rc.observeStep(s.name, attempt, store.StepFailed, 0, ErrRunTimeout)
		return zero, &StepError{Step: s.name, Err: ErrRunTimeout}
	}

	started := time.Now()
	out, comp, err := s.call(rc, key, in)
	took := time.Since(started)
	if rc.Err() != nil {
		// Cancelled (caller, shutdown or lease lost): leave the step
		// "started" for whoever resumes the run.
		return zero, rc.setFatal(context.Cause(rc))
	}
	if err != nil {
		return zero, s.failed(rc, attemptAt{seq: seq, attempt: attempt, priorUnknown: priorUnknown}, took, err)
	}

	outRaw, err := json.Marshal(out)
	if err != nil {
		return zero, rc.setFatal(fmt.Errorf("encode output of %s: %w", s.name, err))
	}
	compRaw, err := json.Marshal(comp)
	if err != nil {
		return zero, rc.setFatal(fmt.Errorf("encode compensation data of %s: %w", s.name, err))
	}
	if err := rc.eng.st.FinishStepFenced(rc, rc.lease, seq, s.name,
		store.Outcome{Status: store.StepDone, Output: outRaw, CompData: compRaw}); err != nil {
		return zero, rc.fail(err)
	}
	rc.observeStep(s.name, attempt, store.StepDone, took, nil)
	return out, nil
}

// call invokes the step, bounded by its timeout.
func (s *Step[I, O, C]) call(rc *runCtx, key Key, in I) (O, C, error) {
	if s.cfg.timeout <= 0 {
		return s.invoke(rc, key, in)
	}
	ctx, cancel := context.WithTimeoutCause(rc, s.cfg.timeout, ErrStepTimeout)
	defer cancel()
	out, comp, err := s.invoke(ctx, key, in)
	if err != nil && errors.Is(context.Cause(ctx), ErrStepTimeout) && !errors.Is(err, ErrStepTimeout) {
		err = fmt.Errorf("%w: %w", ErrStepTimeout, err)
	}
	return out, comp, err
}

// failed records a failed attempt: either the step fails (permanent error,
// attempts exhausted), or the run is parked until the next attempt.
// attemptAt locates a step attempt.
type attemptAt struct {
	seq, attempt int
	// priorUnknown: an earlier attempt was interrupted with an unknown outcome.
	priorUnknown bool
}

func (s *Step[I, O, C]) failed(rc *runCtx, at attemptAt, took time.Duration, cause error) error {
	seq, attempt := at.seq, at.attempt
	outcome := func(status string) store.Outcome {
		o := failedOutcome(status, cause)
		o.Uncertain = o.Uncertain || at.priorUnknown
		return o
	}
	if IsPermanent(cause) || s.cfg.retry.exhausted(attempt) {
		if err := rc.eng.st.FinishStepFenced(rc, rc.lease, seq, s.name, outcome(store.StepFailed)); err != nil {
			return rc.fail(err)
		}
		rc.observeStep(s.name, attempt, store.StepFailed, took, cause)
		return &StepError{Step: s.name, Err: cause}
	}

	delay := s.cfg.retry.backoff(attempt)
	rc.eng.log.Info("step failed, retry scheduled", "run", rc.run.ID, "step", s.name,
		"attempt", attempt, "delay", delay, "err", cause)
	if err := rc.eng.st.Fenced(rc, rc.lease, func(tx pgx.Tx) error {
		// Upsert, like FinishStepFenced: the "started" row may be missing.
		if err := store.PutStep(rc, tx, rc.run.ID, seq, s.name, outcome(store.StepRetrying)); err != nil {
			return err
		}
		now, err := store.Now(rc, tx)
		if err != nil {
			return err
		}
		wake := now.Add(delay)
		return rc.eng.park(rc, tx, rc.run.ID, &wake, "")
	}); err != nil {
		return rc.fail(err)
	}
	rc.observeStep(s.name, attempt, store.StepRetrying, took, cause)
	return rc.suspend()
}

func (c *runCtx) observeStep(step string, attempt int, outcome string, took time.Duration, err error) {
	c.eng.opts.Observer.StepAttempt(StepAttempt{
		RunID: c.run.ID, Workflow: c.run.Workflow, Step: step, Attempt: attempt,
		Outcome: outcome, Duration: took, Err: err,
	})
}
