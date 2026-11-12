package saga

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"

	"github.com/imdat99/saga/internal/store"
)

// River queues of the engine. Resume jobs execute whole runs and may be long;
// the recovery scan gets its own queue so a full resume queue cannot starve it.
const (
	queueName            = "saga"
	maintenanceQueueName = "saga_maintenance"
	retentionQueueName   = "saga_retention" // long purges never delay recovery
)

// resumeArgs continues a run on whichever instance picks the job: it takes
// the lease (only if expired) and executes or compensates the run. Duplicates
// are harmless: all but one fail to take the lease and return.
type resumeArgs struct {
	RunID string `json:"run_id"`
}

func (resumeArgs) Kind() string { return "saga.resume_run" }

func (resumeArgs) InsertOpts() river.InsertOpts {
	// Recovery is the backstop for exhausted attempts, so a few are enough.
	return river.InsertOpts{Queue: queueName, MaxAttempts: 5}
}

type resumeWorker struct {
	river.WorkerDefaults[resumeArgs]

	e *Engine
}

// Timeout disables River's job timeout: the job executes a whole run, which
// may legitimately take longer than any fixed limit. The lease, not the job,
// bounds how long a dead instance holds the run.
func (*resumeWorker) Timeout(*river.Job[resumeArgs]) time.Duration { return -1 }

func (w *resumeWorker) Work(ctx context.Context, job *river.Job[resumeArgs]) error {
	return w.e.resume(ctx, job.Args.RunID)
}

// recoverArgs is the periodic, leader-only scan for runs whose lease expired.
type recoverArgs struct{}

func (recoverArgs) Kind() string { return "saga.recover" }

func (recoverArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: maintenanceQueueName, MaxAttempts: 1}
}

type recoverWorker struct {
	river.WorkerDefaults[recoverArgs]

	e *Engine
}

func (w *recoverWorker) Work(ctx context.Context, _ *river.Job[recoverArgs]) error {
	n, err := w.e.recoverExpired(ctx)
	if n > 0 {
		w.e.log.Info("recovery reserved and enqueued runs", "count", n)
	}
	return err
}

// recoveryMaxBatches bounds one scan, so a huge backlog is drained over
// several ticks instead of one long-running job.
const recoveryMaxBatches = 10

// recoverExpired enqueues resume_run for runs whose lease expired. Each batch
// reserves its rows (SKIP LOCKED) and inserts the jobs in the same
// transaction.
func (e *Engine) recoverExpired(ctx context.Context) (int, error) {
	total := 0
	for range recoveryMaxBatches {
		n := 0
		err := pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
			ids, err := store.ReserveRecoverable(ctx, tx, e.opts.RecoveryBatch, e.queuedGrace())
			if err != nil {
				return err
			}
			n = len(ids)
			if n == 0 {
				return nil
			}
			params := make([]river.InsertManyParams, 0, n)
			for _, id := range ids {
				params = append(params, river.InsertManyParams{Args: resumeArgs{RunID: id}})
			}
			if _, err := e.river.InsertManyTx(ctx, tx, params); err != nil {
				return fmt.Errorf("enqueue resume_run: %w", err)
			}
			return nil
		})
		if err != nil {
			return total, err
		}
		total += n
		if n < e.opts.RecoveryBatch {
			break
		}
	}
	return total, nil
}

// queuedGrace is how long a run with a queued resume job is left alone by
// recovery. If the job has not taken the lease by then (lost job, saturated
// queue), the run is enqueued again. This reservation is what deduplicates
// resume jobs; River uniqueness is not used: it requires the running state in
// the unique set, so a job left running by a killed process would block its
// run until River rescues it (an hour by default), and a job skipped as a
// duplicate would leave a reserved run without a job.
func (e *Engine) queuedGrace() time.Duration {
	return max(e.opts.LeaseTTL, 5*e.opts.RecoveryInterval)
}

// resume continues a run from a River job.
func (e *Engine) resume(ctx context.Context, runID string) error {
	if !e.enter() {
		// Shutting down: hand the job back without spending an attempt.
		return river.JobSnooze(time.Second)
	}
	defer e.leave()

	run, err := e.st.GetRun(ctx, runID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // deleted by retention
	}
	if err != nil {
		return err
	}
	if _, finished, _ := result(run); finished {
		return nil
	}
	if run.Status != store.RunCompensating {
		if _, ok := lookupWorkflow(run.Workflow, run.Version); !ok {
			// Rolling deploy: this binary does not know the workflow yet (or
			// any more). Leave the run untouched; recovery re-enqueues it
			// after queuedGrace, hopefully for an instance that knows it.
			e.log.Warn("resume skipped: workflow not registered in this binary",
				"run", run.ID, "workflow", workflowKey(run.Workflow, run.Version))
			return nil
		}
	}
	acquired := time.Now()
	leased, ok, err := e.st.AcquireLease(ctx, run.ID, e.opts.InstanceID, e.opts.LeaseTTL)
	if err != nil {
		return err
	}
	if !ok {
		// Live elsewhere, or parked and not due yet (a duplicate or early
		// job). Whoever holds it, or its own wake-up job, carries on.
		return nil
	}
	_, err = e.execute(ctx, leased, acquired, false)
	var re *RunError
	switch {
	case err == nil, errors.As(err, &re), errors.Is(err, ErrLeaseLost), errors.Is(err, ErrShutdown),
		errors.Is(err, ErrRunWaiting):
		return nil
	}
	return err
}

// purgeArgs is the periodic, leader-only retention job (Options.Retention).
type purgeArgs struct{}

func (purgeArgs) Kind() string { return "saga.purge" }

func (purgeArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{Queue: retentionQueueName, MaxAttempts: 1}
}

type purgeWorker struct {
	river.WorkerDefaults[purgeArgs]

	e *Engine
}

// purgeBatch bounds one retention statement; a job repeats it until caught up
// (at most purgeMaxBatches times, the next job continues).
const (
	purgeBatch      = 1000
	purgeMaxBatches = 100
)

func (w *purgeWorker) Work(ctx context.Context, _ *river.Job[purgeArgs]) error {
	total := int64(0)
	for range purgeMaxBatches {
		n, err := w.e.PurgeRuns(ctx, w.e.opts.Retention, purgeBatch)
		total += n
		if err != nil {
			return err
		}
		if n < purgeBatch {
			break
		}
	}
	if total > 0 {
		w.e.log.Info("purged ended runs", "count", total)
	}
	return nil
}
