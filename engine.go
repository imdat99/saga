package saga

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"

	"github.com/imdat99/saga/internal/store"
)

// Options configures an Engine.
type Options struct {
	// InstanceID identifies this process as a lease owner. Defaults to
	// "<hostname>-<pid>-<random>"; it must be unique per running process.
	InstanceID string
	// LeaseTTL is how long a run stays owned without renewal before another
	// instance may take it over. Leases are renewed every LeaseTTL/3 while a
	// run executes. Defaults to 30s.
	LeaseTTL time.Duration
	// MaxWorkers is how many runs this instance resumes concurrently from the
	// queue (recovery, retries, timers). Runs started with Workflow.Run
	// execute in the caller's goroutine and do not count. Defaults to 50.
	MaxWorkers int
	// RecoveryInterval is how often the leader scans for runs whose lease
	// expired. Defaults to 10s.
	RecoveryInterval time.Duration
	// RecoveryBatch is how many runs one recovery transaction enqueues.
	// Defaults to 500.
	RecoveryBatch int
	// ShutdownGrace bounds how long Stop waits for interrupted runs to return
	// after its context is done. Defaults to 5s.
	ShutdownGrace time.Duration
	// Hooks run inside the transaction that ends a run (see RunHook), in
	// order. The event bus is one.
	Hooks []RunHook
	// AsyncStepStart commits the "started" checkpoint of each step without
	// waiting for the WAL flush (synchronous_commit=off for that statement
	// only), leaving one durable commit per step instead of two: about one
	// fsync less of latency per step. The "done" checkpoint stays durable.
	//
	// What can be lost: the "started" record of a step in flight when the
	// Postgres server crashes, until the next durable commit on the same
	// cluster flushes it (at most ~600ms; WAL is ordered, so a module that
	// commits its effect durably to the same cluster always flushes it
	// first). With the record lost, the engine no longer knows the step may
	// have taken effect. Resuming normally re-runs the step with the same
	// Key, which is the usual contract. But any path that does not re-run it
	// skips compensating it: a deploy that changed the workflow at that
	// position (non-determinism goes undetected), or the run deadline passing
	// before the resume. Enable it when step effects live in the same
	// Postgres cluster, or when that risk is acceptable. Default false.
	AsyncStepStart bool
	// Observer receives step and run measurements (default: none).
	Observer Observer
	// Retention deletes done and compensated runs (with their steps,
	// signals and locks) this long after they ended; 0 keeps them forever.
	// The leader enqueues a purge job every RetentionInterval (default 1h).
	//
	// Retention is also how long an idempotency key is deduplicated: once
	// its run is purged, the same key starts a new run (whose step keys
	// differ, so modules do not deduplicate it either).
	Retention         time.Duration
	RetentionInterval time.Duration
	Logger            *slog.Logger
}

// minLeaseTTL keeps the heartbeat interval (TTL/3) above a round trip.
const minLeaseTTL = 300 * time.Millisecond

func (o *Options) setDefaults() {
	if o.InstanceID == "" {
		host, _ := os.Hostname()
		o.InstanceID = fmt.Sprintf("%s-%d-%s", host, os.Getpid(), uuid.NewString()[:8])
	}
	if o.LeaseTTL <= 0 {
		o.LeaseTTL = 30 * time.Second
	}
	// Renewal runs every TTL/3 and must fit a database round trip.
	o.LeaseTTL = max(o.LeaseTTL, minLeaseTTL)
	if o.MaxWorkers <= 0 {
		o.MaxWorkers = 50
	}
	if o.RecoveryInterval <= 0 {
		o.RecoveryInterval = 10 * time.Second
	}
	if o.RecoveryBatch <= 0 {
		o.RecoveryBatch = 500
	}
	if o.ShutdownGrace <= 0 {
		o.ShutdownGrace = 5 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Observer == nil {
		o.Observer = noopObserver{}
	}
	if o.RetentionInterval <= 0 {
		o.RetentionInterval = time.Hour
	}
}

// Engine executes workflows. It is safe for concurrent use.
//
// Workflow.Run executes runs inline without Start. Start is needed for
// everything driven by the queue: recovery of abandoned runs, Workflow.Start,
// and (from M3) retries, timers and signals.
type Engine struct {
	pool   *pgxpool.Pool
	hbPool *pgxpool.Pool // dedicated to lease renewal, closed by Stop
	st     *store.Store
	river  *river.Client[pgx.Tx]
	hb     *heartbeat
	opts   Options
	log    *slog.Logger

	mu        sync.Mutex
	isStarted bool
	isClosing bool
	stopped   chan struct{}  // closed when Stop has finished
	active    sync.WaitGroup // executions in progress (inline and from jobs)
}

// heartbeatConns is the size of the lease renewal pool: one batched UPDATE
// per tick, plus one spare for a tick overlapping a slow one.
const heartbeatConns = 2

// New returns an Engine using pool. Run Migrate first. The engine also opens
// a small dedicated pool (same configuration, 2 connections, connected
// lazily) for lease renewal: always call Stop, even when only Workflow.Run is
// used, to close it.
func New(pool *pgxpool.Pool, opts Options) (*Engine, error) {
	opts.setDefaults()
	hbCfg := pool.Config()
	hbCfg.MaxConns = heartbeatConns
	hbCfg.MinConns = 0
	hbPool, err := pgxpool.NewWithConfig(context.Background(), hbCfg)
	if err != nil {
		return nil, fmt.Errorf("saga: heartbeat pool: %w", err)
	}
	e := &Engine{
		pool:    pool,
		hbPool:  hbPool,
		st:      store.New(pool),
		opts:    opts,
		log:     opts.Logger.With("component", "saga", "instance", opts.InstanceID),
		stopped: make(chan struct{}),
	}
	e.hb = newHeartbeat(store.New(hbPool), opts.InstanceID, opts.LeaseTTL, e.log)

	workers := river.NewWorkers()
	river.AddWorker(workers, &resumeWorker{e: e})
	river.AddWorker(workers, &recoverWorker{e: e})
	river.AddWorker(workers, &purgeWorker{e: e})
	periodic := []*river.PeriodicJob{river.NewPeriodicJob(
		river.PeriodicInterval(opts.RecoveryInterval),
		func() (river.JobArgs, *river.InsertOpts) { return recoverArgs{}, nil },
		&river.PeriodicJobOpts{ID: "saga.recover", RunOnStart: true},
	)}
	if opts.Retention > 0 {
		periodic = append(periodic, river.NewPeriodicJob(
			river.PeriodicInterval(opts.RetentionInterval),
			func() (river.JobArgs, *river.InsertOpts) { return purgeArgs{}, nil },
			&river.PeriodicJobOpts{ID: "saga.purge"},
		))
	}
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Logger: opts.Logger.With("component", "saga.river"),
		Schema: Schema,
		Queues: map[string]river.QueueConfig{
			queueName:            {MaxWorkers: opts.MaxWorkers},
			maintenanceQueueName: {MaxWorkers: 1},
			retentionQueueName:   {MaxWorkers: 1},
		},
		Workers:      workers,
		PeriodicJobs: periodic,
	})
	if err != nil {
		hbPool.Close()
		return nil, fmt.Errorf("saga: river client: %w", err)
	}
	e.river = client
	return e, nil
}

// InstanceID returns the lease owner id of this engine.
func (e *Engine) InstanceID() string { return e.opts.InstanceID }

// Start starts the queue workers: recovery (leader only), and resumption of
// runs enqueued by any instance. ctx is used for startup only; call Stop to
// shut down.
func (e *Engine) Start(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.isClosing {
		return ErrEngineStopped
	}
	if e.isStarted {
		return nil
	}
	if err := e.river.Start(context.WithoutCancel(ctx)); err != nil {
		return fmt.Errorf("saga: start river: %w", err)
	}
	e.isStarted = true
	return nil
}

// Stop shuts the engine down. It stops taking new work, then waits for runs in
// progress (inline and queued) to finish until ctx is done. Runs still going
// at that point are cancelled with cause ErrShutdown and their leases released,
// so another instance can resume them without waiting for the lease TTL; steps
// interrupted mid-flight are re-executed there with the same Key. Stop then
// waits at most Options.ShutdownGrace for them to return, and returns
// ctx.Err(). Concurrent calls wait for the first one.
func (e *Engine) Stop(ctx context.Context) error {
	e.mu.Lock()
	if e.isClosing {
		e.mu.Unlock()
		select {
		case <-e.stopped:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	e.isClosing = true
	started := e.isStarted
	e.mu.Unlock()
	defer close(e.stopped)
	defer e.hbPool.Close()
	defer e.hb.stop()

	drained := make(chan struct{})
	go func() {
		e.active.Wait()
		close(drained)
	}()

	var riverErr error
	if started {
		riverErr = e.river.Stop(ctx) // waits for running jobs, bounded by ctx
	}
	select {
	case <-drained:
	case <-ctx.Done():
	}
	select {
	case <-drained: // a clean drain wins over a deadline hit at the same time
		if riverErr != nil && ctx.Err() == nil {
			return fmt.Errorf("saga: stop river: %w", riverErr)
		}
		return nil
	default:
	}

	e.hb.cancelAll()
	grace, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.opts.ShutdownGrace)
	defer cancel()
	if started {
		_ = e.river.StopAndCancel(grace) //nolint:contextcheck // ctx is done; grace bounds the hard stop
	}
	select {
	case <-drained:
	case <-grace.Done():
		e.log.Warn("shutdown grace elapsed with runs still executing; their leases will lapse")
	}
	return ctx.Err()
}

// enter registers an execution; it fails once Stop has been called.
func (e *Engine) enter() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.isClosing {
		return false
	}
	e.active.Add(1)
	return true
}

func (e *Engine) leave() { e.active.Done() }

// runRequest is what Workflow.Run asks the engine to execute.
type runRequest struct {
	workflow string
	version  int
	timeout  time.Duration
	idemKey  string
	input    json.RawMessage
}

func (e *Engine) run(ctx context.Context, req runRequest) (json.RawMessage, error) {
	name, version, timeout, idemKey, input := req.workflow, req.version, req.timeout, req.idemKey, req.input
	if !e.enter() {
		return nil, ErrEngineStopped
	}
	defer e.leave()

	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	run := store.Run{
		ID: id.String(), Workflow: name, Version: version,
		IdemKey: idemKey, Status: store.RunRunning, Input: input,
	}

	acquired := time.Now()
	inserted, deadline, err := e.st.InsertRun(ctx, run, e.opts.InstanceID, e.opts.LeaseTTL, timeout)
	if err != nil {
		return nil, err
	}
	if inserted {
		run.LeaseVersion = 1
		run.Deadline = deadline
		return e.execute(ctx, run, acquired)
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
	if _, ok := lookupWorkflow(existing.Workflow, existing.Version); !ok && existing.Status != store.RunCompensating {
		// Do not take a lease we cannot use: another binary may resume it.
		return nil, fmt.Errorf("%w: %s", ErrWorkflowNotRegistered, workflowKey(existing.Workflow, existing.Version))
	}

	acquired = time.Now()
	leased, ok, err := e.st.AcquireLease(ctx, existing.ID, e.opts.InstanceID, e.opts.LeaseTTL)
	if err != nil {
		return nil, err
	}
	if !ok {
		// Live elsewhere, parked and not due, or it finished between our
		// two reads.
		latest, err := e.st.GetRun(ctx, existing.ID)
		if err != nil {
			return nil, err
		}
		if out, finished, err := result(latest); finished {
			return out, err
		}
		if parked(latest) {
			return nil, waiting(latest.ID)
		}
		return nil, ErrRunInProgress
	}
	return e.execute(ctx, leased, acquired)
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

func waiting(runID string) error { return fmt.Errorf("%w: run %s", ErrRunWaiting, runID) }

// parked reports whether nobody executes r because it waits for a wake-up
// (Sleep, Await, retry or compensation backoff).
func parked(r store.Run) bool {
	return r.Status == store.RunWaiting ||
		(r.Status == store.RunCompensating && r.OwnerID == "" && r.WakeAt != nil)
}

// execute runs (or resumes) a run the caller holds the lease for. run must be
// the row as of the acquisition; acquired is the local time just before it.
func (e *Engine) execute(ctx context.Context, run store.Run, acquired time.Time) (json.RawMessage, error) {
	lease := store.Lease{RunID: run.ID, Owner: e.opts.InstanceID, Version: run.LeaseVersion}
	log := e.log.With("run", run.ID, "workflow", run.Workflow)

	// A run started from inside another run's step must not look like part
	// of it to its own compensators and hooks (RunIDFrom): hide the parent.
	ctx = context.WithValue(ctx, runCtxKey{}, (*runCtx)(nil))
	ctx, untrack := e.hb.track(ctx, run.ID, run.LeaseVersion, acquired)
	defer func() {
		if errors.Is(context.Cause(ctx), ErrShutdown) {
			// Hand the run over now instead of after the TTL.
			rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.opts.ShutdownGrace)
			if err := e.st.ReleaseLease(rctx, lease); err != nil {
				log.Warn("release lease on shutdown", "err", err)
			}
			cancel()
		}
		untrack()
	}()

	history, err := e.st.ListSteps(ctx, run.ID)
	if err != nil {
		return nil, interruption(ctx, err)
	}

	if run.Status == store.RunCompensating {
		return e.compensate(ctx, run, lease, history, run.Error)
	}

	runner, ok := lookupWorkflow(run.Workflow, run.Version)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrWorkflowNotRegistered, workflowKey(run.Workflow, run.Version))
	}

	rc := &runCtx{Context: ctx, eng: e, run: run, lease: lease, steps: make(map[int]store.Step, len(history))}
	for _, st := range history {
		rc.steps[st.Seq] = st
	}

	out, fnErr := runner(rc, run.Input)

	if rc.fatal != nil {
		if errors.Is(rc.fatal, ErrRunWaiting) {
			return nil, waiting(run.ID)
		}
		if errors.Is(rc.fatal, ErrNonDeterministic) {
			log.Error("parking run", "err", rc.fatal)
			if err := e.st.Fenced(ctx, lease, func(tx pgx.Tx) error {
				return e.finish(ctx, tx, run, store.RunNeedsAttention, nil, rc.fatal.Error())
			}); err != nil {
				return nil, interruption(ctx, err)
			}
			e.ended(run, store.RunNeedsAttention, rc.fatal.Error())
			return nil, &RunError{RunID: run.ID, Status: StatusNeedsAttention, Cause: rc.fatal.Error()}
		}
		// Lease lost, cancelled or storage failure: stop here. Whoever owns
		// the run next (this caller retrying, or recovery) continues it.
		log.Warn("run interrupted", "err", rc.fatal)
		return nil, rc.fatal
	}

	if fnErr == nil {
		if err := e.st.Fenced(ctx, lease, func(tx pgx.Tx) error {
			return e.finish(ctx, tx, run, store.RunDone, out, "")
		}); err != nil {
			return nil, interruption(ctx, err)
		}
		e.ended(run, store.RunDone, "")
		return out, nil
	}

	log.Info("workflow failed, compensating", "err", fnErr)
	if err := e.st.Fenced(ctx, lease, func(tx pgx.Tx) error {
		return store.SetRunStatus(ctx, tx, run.ID, store.RunCompensating, nil, fnErr.Error())
	}); err != nil {
		return nil, interruption(ctx, err)
	}
	history, err = e.st.ListSteps(ctx, run.ID)
	if err != nil {
		return nil, interruption(ctx, err)
	}
	return e.compensate(ctx, run, lease, history, fnErr.Error())
}

// compensate undoes completed steps in reverse order. A failed compensator is
// retried durably: the run keeps status compensating, releases its lease and
// a job resumes it after the backoff of the step's CompensatePolicy. Once the
// policy is exhausted the run is parked as needs_attention.
func (e *Engine) compensate(
	ctx context.Context,
	run store.Run,
	lease store.Lease,
	history []store.Step,
	cause string,
) (json.RawMessage, error) {
	for _, st := range slices.Backward(history) {
		// Uncertain: the step may have taken effect without recording its
		// result (crash mid-step, retry pending, an attempt timed out). Such
		// steps have no compensation data.
		uncertain := st.Status == store.StepStarted || st.Status == store.StepRetrying ||
			st.Uncertain || len(st.CompData) == 0
		switch {
		case st.Status == store.StepDone, st.Status == store.StepCompensationFailed:
		case st.Status == store.StepStarted, st.Status == store.StepRetrying:
		case st.Status == store.StepFailed && st.Uncertain:
		default:
			continue // failed (no effect), waiting pseudo-step, or already compensated
		}
		comp, ok := lookupCompensator(st.Name)
		if !ok {
			continue // step declared without compensation
		}

		// Recorded compensation data is passed even when uncertain: a step
		// that succeeded on a retry after a timed-out attempt has both.
		cctx := ctx
		if uncertain {
			cctx = context.WithValue(ctx, uncertainKey{}, true)
		}
		err := comp.fn(cctx, stepKey(run.ID, st.Seq, st.Name), st.CompData)
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		if err == nil {
			if err := e.st.Fenced(ctx, lease, func(tx pgx.Tx) error {
				return store.SetStepStatus(ctx, tx, run.ID, st.Seq, store.StepCompensated, "")
			}); err != nil {
				return nil, interruption(ctx, err)
			}
			continue
		}
		return e.compensationFailed(ctx, run, lease, compFailure{step: st, policy: comp.policy, err: err, cause: cause})
	}

	if err := e.st.Fenced(ctx, lease, func(tx pgx.Tx) error {
		return e.finish(ctx, tx, run, store.RunCompensated, nil, cause)
	}); err != nil {
		return nil, interruption(ctx, err)
	}
	e.ended(run, store.RunCompensated, cause)
	return nil, &RunError{RunID: run.ID, Status: StatusCompensated, Cause: cause}
}

// compensationFailed records a failed compensation attempt and either
// schedules the next one or parks the run as needs_attention.
// compFailure is a failed compensation attempt.
type compFailure struct {
	step   store.Step
	policy RetryPolicy
	err    error  // the compensator's error
	cause  string // why the run is compensating
}

func (e *Engine) compensationFailed(
	ctx context.Context,
	run store.Run,
	lease store.Lease,
	f compFailure,
) (json.RawMessage, error) {
	st, policy, compErr, cause := f.step, f.policy, f.err, f.cause
	msg := fmt.Sprintf("compensating %s: %v (after: %s)", st.Name, compErr, cause)
	exhausted := false
	var attempts int
	if err := e.st.Fenced(ctx, lease, func(tx pgx.Tx) error {
		var err error
		if attempts, err = store.FailCompensation(ctx, tx, run.ID, st.Seq, compErr.Error()); err != nil {
			return err
		}
		if IsPermanent(compErr) || policy.exhausted(attempts) {
			exhausted = true
			if err := store.SetStepStatus(ctx, tx, run.ID, st.Seq, store.StepCompensationFailed, compErr.Error()); err != nil {
				return err
			}
			return e.finish(ctx, tx, run, store.RunNeedsAttention, nil, msg)
		}
		due, err := store.Backoff(ctx, tx, run.ID, policy.backoff(attempts), e.queuedGrace())
		if err != nil {
			return err
		}
		return e.enqueueAt(ctx, tx, run.ID, due)
	}); err != nil {
		return nil, interruption(ctx, err)
	}
	if exhausted {
		e.ended(run, store.RunNeedsAttention, msg)
		e.log.Error("compensation failed, run needs attention", "run", run.ID, "step", st.Name,
			"attempts", attempts, "err", compErr)
		return nil, &RunError{RunID: run.ID, Status: StatusNeedsAttention, Cause: msg}
	}
	e.log.Warn("compensation failed, retry scheduled", "run", run.ID, "step", st.Name,
		"attempt", attempts, "err", compErr)
	return nil, waiting(run.ID)
}

// finish records a terminal status inside tx and runs the RunHooks there, so
// their writes (e.g. releasing staged events) commit with the status.
func (e *Engine) finish(
	ctx context.Context,
	tx pgx.Tx,
	run store.Run,
	status string,
	out json.RawMessage,
	errMsg string,
) error {
	if err := store.SetRunStatus(ctx, tx, run.ID, status, out, errMsg); err != nil {
		return err
	}
	info := RunInfo{ID: run.ID, Workflow: run.Workflow, Version: run.Version, Status: RunStatus(status), Error: errMsg}
	for _, h := range e.opts.Hooks {
		if err := h.RunEnded(ctx, tx, info); err != nil {
			return fmt.Errorf("saga: run hook: %w", err)
		}
	}
	return nil
}

// ended reports a committed terminal status to the observer.
func (e *Engine) ended(run store.Run, status, errMsg string) {
	e.opts.Observer.RunEnded(RunInfo{
		ID: run.ID, Workflow: run.Workflow, Version: run.Version, Status: RunStatus(status), Error: errMsg,
	})
}

// park suspends the run inside tx (see store.Park) and schedules the job that
// wakes it, in the same transaction.
func (e *Engine) park(ctx context.Context, tx pgx.Tx, runID string, wake *time.Time, awaitName string) error {
	due, err := store.Park(ctx, tx, runID, wake, awaitName, e.queuedGrace())
	if err != nil || due == nil {
		return err // no due time: only a signal resumes it, and enqueues then
	}
	return e.enqueueAt(ctx, tx, runID, *due)
}

// enqueueAt inserts a resume job inside tx, scheduled at a database time.
func (e *Engine) enqueueAt(ctx context.Context, tx pgx.Tx, runID string, at time.Time) error {
	opts := resumeArgs{}.InsertOpts()
	opts.ScheduledAt = at
	if _, err := e.river.InsertTx(ctx, tx, resumeArgs{RunID: runID}, &opts); err != nil {
		return fmt.Errorf("enqueue resume_run: %w", err)
	}
	return nil
}
