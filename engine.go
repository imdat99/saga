package saga

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"github.com/imdat99/saga/internal/store"
	"github.com/imdat99/saga/migrations"
)

// Options configures an Engine.
type Options struct {
	// InstanceID identifies this process as a lease owner. Defaults to
	// "<hostname>-<pid>-<random>"; it must be unique per running process.
	InstanceID string
	// LeaseTTL is how long a run stays owned without renewal before another
	// instance may take it over. Defaults to 30s.
	LeaseTTL time.Duration
	// CompensateAttempts is how many times a compensator is tried before the
	// run is parked as needs_attention. Defaults to 3.
	CompensateAttempts int
	// CompensateBackoff is the delay before the first compensation retry,
	// doubled on each further retry. Defaults to 100ms.
	CompensateBackoff time.Duration
	Logger            *slog.Logger
}

// Engine executes workflows. It is safe for concurrent use.
type Engine struct {
	st   *store.Store
	opts Options
	log  *slog.Logger
}

// New returns an Engine using pool. Run Migrate first.
func New(pool *pgxpool.Pool, opts Options) *Engine {
	if opts.InstanceID == "" {
		host, _ := os.Hostname()
		opts.InstanceID = fmt.Sprintf("%s-%d-%s", host, os.Getpid(), uuid.NewString()[:8])
	}
	if opts.LeaseTTL <= 0 {
		opts.LeaseTTL = 30 * time.Second
	}
	if opts.CompensateAttempts <= 0 {
		opts.CompensateAttempts = 3
	}
	if opts.CompensateBackoff <= 0 {
		opts.CompensateBackoff = 100 * time.Millisecond
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Engine{
		st:   store.New(pool),
		opts: opts,
		log:  opts.Logger.With("component", "saga", "instance", opts.InstanceID),
	}
}

// InstanceID returns the lease owner id of this engine.
func (e *Engine) InstanceID() string { return e.opts.InstanceID }

// Migrate applies the saga schema migrations. It takes a Postgres session lock,
// so several instances starting at once migrate one after another.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS saga`); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}
	db := stdlib.OpenDBFromPool(pool)
	defer func(db *sql.DB) { _ = db.Close() }(db)

	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("migration locker: %w", err)
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS,
		goose.WithTableName("saga.goose_db_version"),
		goose.WithSessionLocker(locker),
		goose.WithDisableGlobalRegistry(true))
	if err != nil {
		return fmt.Errorf("migration provider: %w", err)
	}
	if _, err := p.Up(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}

func (e *Engine) run(ctx context.Context, name string, version int, idemKey string, input json.RawMessage) (json.RawMessage, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	run := store.Run{ID: id.String(), Workflow: name, Version: version, IdemKey: idemKey, Status: store.RunRunning, Input: input}

	inserted, err := e.st.InsertRun(ctx, run, e.opts.InstanceID, e.opts.LeaseTTL)
	if err != nil {
		return nil, err
	}
	if inserted {
		run.LeaseVersion = 1
		return e.execute(ctx, run)
	}

	existing, err := e.st.GetRunByIdemKey(ctx, idemKey)
	if err != nil {
		return nil, err
	}
	if existing.Workflow != name {
		return nil, fmt.Errorf("%w: key %q belongs to %s", ErrIdemKeyConflict, idemKey, existing.Workflow)
	}
	if out, finished, err := result(existing); finished {
		return out, err
	}

	version64, ok, err := e.st.AcquireLease(ctx, existing.ID, e.opts.InstanceID, e.opts.LeaseTTL)
	if err != nil {
		return nil, err
	}
	if !ok {
		// Either live elsewhere, or it finished between our two reads.
		latest, err := e.st.GetRun(ctx, existing.ID)
		if err != nil {
			return nil, err
		}
		if out, finished, err := result(latest); finished {
			return out, err
		}
		return nil, ErrRunInProgress
	}
	existing.LeaseVersion = version64
	return e.execute(ctx, existing)
}

// result returns the outcome of a run that already reached a terminal status.
func result(r store.Run) (json.RawMessage, bool, error) {
	switch r.Status {
	case store.RunDone:
		return r.Output, true, nil
	case store.RunCompensated, store.RunNeedsAttention:
		return nil, true, &RunError{RunID: r.ID, Status: RunStatus(r.Status), Cause: r.Error}
	}
	return nil, false, nil
}

// execute runs (or resumes) a run the caller holds the lease for.
func (e *Engine) execute(ctx context.Context, run store.Run) (json.RawMessage, error) {
	lease := store.Lease{RunID: run.ID, Owner: e.opts.InstanceID, Version: run.LeaseVersion}
	log := e.log.With("run", run.ID, "workflow", run.Workflow)

	history, err := e.st.ListSteps(ctx, run.ID)
	if err != nil {
		return nil, err
	}

	if run.Status == store.RunCompensating {
		return e.compensate(ctx, run, lease, history, run.Error)
	}

	runner, ok := lookupWorkflow(run.Workflow, run.Version)
	if !ok {
		return nil, fmt.Errorf("saga: workflow %s is not registered in this binary", workflowKey(run.Workflow, run.Version))
	}

	rc := &runCtx{Context: ctx, eng: e, run: run, lease: lease, steps: make(map[int]store.Step, len(history))}
	for _, st := range history {
		rc.steps[st.Seq] = st
	}

	out, fnErr := runner(rc, run.Input)

	if rc.fatal != nil {
		if errors.Is(rc.fatal, ErrNonDeterministic) {
			log.Error("parking run", "err", rc.fatal)
			if err := e.st.Fenced(ctx, lease, func(tx pgx.Tx) error {
				return store.SetRunStatus(ctx, tx, run.ID, store.RunNeedsAttention, nil, rc.fatal.Error())
			}); err != nil {
				return nil, err
			}
			return nil, &RunError{RunID: run.ID, Status: StatusNeedsAttention, Cause: rc.fatal.Error()}
		}
		// Lease lost, cancelled or storage failure: stop here. Whoever owns
		// the run next (this caller retrying, or recovery) continues it.
		log.Warn("run interrupted", "err", rc.fatal)
		return nil, rc.fatal
	}

	if fnErr == nil {
		if err := e.st.Fenced(ctx, lease, func(tx pgx.Tx) error {
			return store.SetRunStatus(ctx, tx, run.ID, store.RunDone, out, "")
		}); err != nil {
			return nil, err
		}
		return out, nil
	}

	log.Info("workflow failed, compensating", "err", fnErr)
	if err := e.st.Fenced(ctx, lease, func(tx pgx.Tx) error {
		return store.SetRunStatus(ctx, tx, run.ID, store.RunCompensating, nil, fnErr.Error())
	}); err != nil {
		return nil, err
	}
	history, err = e.st.ListSteps(ctx, run.ID)
	if err != nil {
		return nil, err
	}
	return e.compensate(ctx, run, lease, history, fnErr.Error())
}

// compensate undoes completed steps in reverse order.
func (e *Engine) compensate(ctx context.Context, run store.Run, lease store.Lease, history []store.Step, cause string) (json.RawMessage, error) {
	for i := len(history) - 1; i >= 0; i-- {
		st := history[i]
		uncertain := st.Status == store.StepStarted
		if st.Status != store.StepDone && !uncertain && st.Status != store.StepCompensationFailed {
			continue // failed (no effect) or already compensated
		}
		comp, ok := lookupCompensator(st.Name)
		if !ok {
			continue // step declared without compensation
		}

		cctx := ctx
		if uncertain {
			cctx = context.WithValue(ctx, uncertainKey{}, true)
		}
		compData := st.CompData
		if uncertain {
			compData = nil
		}
		err := e.retryCompensation(cctx, func() error {
			return comp(cctx, stepKey(run.ID, st.Seq, st.Name), compData)
		})
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if err != nil {
			msg := fmt.Sprintf("compensating %s: %v (after: %s)", st.Name, err, cause)
			if ferr := e.st.Fenced(ctx, lease, func(tx pgx.Tx) error {
				if err := store.SetStepStatus(ctx, tx, run.ID, st.Seq, store.StepCompensationFailed, err.Error()); err != nil {
					return err
				}
				return store.SetRunStatus(ctx, tx, run.ID, store.RunNeedsAttention, nil, msg)
			}); ferr != nil {
				return nil, ferr
			}
			e.log.Error("compensation failed, run needs attention", "run", run.ID, "step", st.Name, "err", err)
			return nil, &RunError{RunID: run.ID, Status: StatusNeedsAttention, Cause: msg}
		}

		if err := e.st.Fenced(ctx, lease, func(tx pgx.Tx) error {
			return store.SetStepStatus(ctx, tx, run.ID, st.Seq, store.StepCompensated, "")
		}); err != nil {
			return nil, err
		}
	}

	if err := e.st.Fenced(ctx, lease, func(tx pgx.Tx) error {
		return store.SetRunStatus(ctx, tx, run.ID, store.RunCompensated, nil, cause)
	}); err != nil {
		return nil, err
	}
	return nil, &RunError{RunID: run.ID, Status: StatusCompensated, Cause: cause}
}

func (e *Engine) retryCompensation(ctx context.Context, fn func() error) error {
	delay := e.opts.CompensateBackoff
	var err error
	for attempt := 1; attempt <= e.opts.CompensateAttempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		if attempt == e.opts.CompensateAttempts {
			break
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
		delay *= 2
	}
	return err
}
