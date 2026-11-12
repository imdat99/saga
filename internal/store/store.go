// Package store holds the Postgres queries of the saga engine.
//
// Every write made on behalf of a running saga goes through Fenced, which locks
// the run row (FOR NO KEY UPDATE) and checks the caller still holds the lease
// version it acquired, or through a single-statement equivalent on the hot
// path (StartStepFenced, FinishStepFenced) whose CTE takes the same lock and
// checks the same columns. A takeover by another instance bumps lease_version, so a stale owner
// (GC pause, network partition) fails its next write instead of corrupting state.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrLeaseLost means another instance took over the run.
var ErrLeaseLost = errors.New("saga: lease lost")

// Run statuses.
const (
	RunRunning        = "running"
	RunWaiting        = "waiting"
	RunCompensating   = "compensating"
	RunDone           = "done"
	RunCompensated    = "compensated"
	RunNeedsAttention = "needs_attention"
)

// Step statuses.
const (
	StepStarted            = "started"
	StepRetrying           = "retrying"
	StepWaiting            = "waiting"
	StepDone               = "done"
	StepFailed             = "failed"
	StepCompensated        = "compensated"
	StepCompensationFailed = "compensation_failed"
)

// Run is a saga.run row.
type Run struct {
	ID           string
	Workflow     string
	Version      int
	IdemKey      string
	Status       string
	Input        json.RawMessage
	Output       json.RawMessage
	Error        string
	LeaseVersion int64
	AwaitName    string
	Deadline     *time.Time
	WakeAt       *time.Time
	OwnerID      string
}

// Step is a saga.step row.
type Step struct {
	Seq         int
	Name        string
	Status      string
	Attempt     int
	Output      json.RawMessage
	CompData    json.RawMessage
	Error       string
	CompAttempt int
	ErrorKind   string
	Uncertain   bool
}

// Outcome is what a step attempt leaves in its row.
type Outcome struct {
	Status    string
	Output    json.RawMessage
	CompData  json.RawMessage
	Error     string
	ErrorKind string
	// Uncertain is sticky: once an attempt may have taken effect without
	// recording it, the step stays uncertain.
	Uncertain bool
}

// Lease identifies the ownership a writer must still hold.
type Lease struct {
	RunID   string
	Owner   string
	Version int64
}

// micros converts d for SQL as "$n * interval '1 microsecond'". A Duration's
// String form ("1.5µs") is not always valid Postgres interval input.
func micros(d time.Duration) int64 { return d.Microseconds() }

// Store runs queries against the saga schema.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store backed by pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

const runColumns = `id::text, workflow, version, idem_key, status, input, output, coalesce(error, ''), lease_version,
	coalesce(await_name, ''), deadline, wake_at, coalesce(owner_id, '')`

func scanRun(row pgx.Row) (Run, error) {
	var r Run
	err := row.Scan(&r.ID, &r.Workflow, &r.Version, &r.IdemKey, &r.Status, &r.Input, &r.Output, &r.Error, &r.LeaseVersion,
		&r.AwaitName, &r.Deadline, &r.WakeAt, &r.OwnerID)
	return r, err
}

// InsertRun creates a run already leased by owner. timeout > 0 sets the run
// deadline. It returns false when a run with the same idempotency key exists
// (the caller then loads it instead), and the run deadline otherwise.
func (s *Store) InsertRun(
	ctx context.Context,
	r Run,
	owner string,
	ttl,
	timeout time.Duration,
) (bool, *time.Time, error) {
	var now time.Time
	var deadline *time.Time
	err := s.pool.QueryRow(ctx, `
		INSERT INTO saga.run (id, workflow, version, idem_key, status, input, owner_id, lease_until, lease_version, deadline)
		VALUES ($1, $2, $3, $4, 'running', $5, $6, now() + $7 * interval '1 microsecond', 1,
		        CASE WHEN $8::bigint > 0 THEN now() + $8::bigint * interval '1 microsecond' END)
		ON CONFLICT (idem_key) DO NOTHING
		RETURNING now(), deadline`,
		r.ID, r.Workflow, r.Version, r.IdemKey, r.Input, owner, micros(ttl), micros(timeout)).Scan(&now, &deadline)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, fmt.Errorf("insert run: %w", err)
	}
	return true, deadline, nil
}

// GetRunByIdemKey loads a run by its idempotency key.
func (s *Store) GetRunByIdemKey(ctx context.Context, key string) (Run, error) {
	r, err := scanRun(s.pool.QueryRow(ctx, `SELECT `+runColumns+` FROM saga.run WHERE idem_key = $1`, key))
	if err != nil {
		return r, fmt.Errorf("get run by idem key: %w", err)
	}
	return r, nil
}

// GetRun loads a run by id.
func (s *Store) GetRun(ctx context.Context, id string) (Run, error) {
	r, err := scanRun(s.pool.QueryRow(ctx, `SELECT `+runColumns+` FROM saga.run WHERE id = $1`, id))
	if err != nil {
		return r, fmt.Errorf("get run: %w", err)
	}
	return r, nil
}

// AcquireLease takes over an unfinished run nobody holds:
//   - running or compensating: its lease expired, or nobody owns it (queued
//     for a resume job, or released) and it is not in a backoff (wake_at);
//   - waiting: its wake-up time passed, or the signal it awaits arrived.
//
// A waiting run becomes running. It never re-grants a live lease, not even to
// the same owner, so two goroutines of one instance cannot both execute the
// run. It returns the run as of the acquisition: callers must use it rather
// than an earlier read, whose status may have moved on.
func (s *Store) AcquireLease(ctx context.Context, runID, owner string, ttl time.Duration) (Run, bool, error) {
	r, err := scanRun(s.pool.QueryRow(ctx, `
		UPDATE saga.run r
		SET owner_id = $2, lease_until = now() + $3 * interval '1 microsecond',
		    lease_version = lease_version + 1, updated_at = now(),
		    status = CASE WHEN status = 'waiting' THEN 'running' ELSE status END,
		    wake_at = NULL, await_name = NULL
		WHERE id = $1
		  AND (
		    (status IN ('running', 'compensating')
		      AND (lease_until < now() OR (owner_id IS NULL AND (wake_at IS NULL OR wake_at <= now()))))
		    OR
		    (status = 'waiting'
		      AND (wake_at <= now()
		        OR EXISTS (SELECT 1 FROM saga.signal g WHERE g.run_id = r.id AND g.name = r.await_name))))
		RETURNING `+runColumns, runID, owner, micros(ttl)))
	if errors.Is(err, pgx.ErrNoRows) {
		return r, false, nil
	}
	if err != nil {
		return r, false, fmt.Errorf("acquire lease: %w", err)
	}
	return r, true, nil
}

// ListSteps returns the steps of a run ordered by seq.
func (s *Store) ListSteps(ctx context.Context, runID string) ([]Step, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT seq, name, status, attempt, output, comp_data, coalesce(error, ''), comp_attempt,
		       coalesce(error_kind, ''), uncertain
		FROM saga.step WHERE run_id = $1 ORDER BY seq`, runID)
	if err != nil {
		return nil, fmt.Errorf("list steps: %w", err)
	}
	steps, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Step, error) {
		var st Step
		err := row.Scan(&st.Seq, &st.Name, &st.Status, &st.Attempt, &st.Output, &st.CompData, &st.Error, &st.CompAttempt,
			&st.ErrorKind, &st.Uncertain)
		return st, err
	})
	if err != nil {
		return nil, fmt.Errorf("list steps: %w", err)
	}
	return steps, nil
}

// Fenced runs fn in a transaction that holds a row lock on the run and
// verifies the lease. It returns ErrLeaseLost if the lease moved on.
func (s *Store) Fenced(ctx context.Context, l Lease, fn func(tx pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var one int
		err := tx.QueryRow(ctx, `
			SELECT 1 FROM saga.run
			WHERE id = $1 AND owner_id = $2 AND lease_version = $3
			FOR NO KEY UPDATE`, l.RunID, l.Owner, l.Version).Scan(&one)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrLeaseLost
		}
		if err != nil {
			return fmt.Errorf("check lease: %w", err)
		}
		return fn(tx)
	})
}

// StartStep records that a step is about to run, and returns its attempt
// number (1 for the first one; re-running a step left "started" or
// "retrying" bumps it) and the database time.
func StartStep(ctx context.Context, tx pgx.Tx, runID string, seq int, name string) (int, time.Time, error) {
	var attempt int
	var now time.Time
	err := tx.QueryRow(ctx, `
		INSERT INTO saga.step (run_id, seq, name, status)
		VALUES ($1, $2, $3, 'started')
		ON CONFLICT (run_id, seq) DO UPDATE
		SET status = 'started', attempt = saga.step.attempt + 1, started_at = now(), error = NULL,
		    uncertain = saga.step.uncertain OR saga.step.status = 'started'
		RETURNING attempt, now()`,
		runID, seq, name).Scan(&attempt, &now)
	if err != nil {
		return 0, now, fmt.Errorf("start step: %w", err)
	}
	return attempt, now, nil
}

// PutStep inserts or overwrites a step row with a final or waiting state, for
// pseudo-steps (Sleep, Await) that have no "started" phase.
func PutStep(ctx context.Context, tx pgx.Tx, runID string, seq int, name string, o Outcome) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO saga.step (run_id, seq, name, status, output, comp_data, error, error_kind, uncertain, finished_at)
		VALUES ($1, $2, $3, $4, $5, $6, nullif($7, ''), nullif($8, ''), $9,
		        CASE WHEN $4 NOT IN ('waiting', 'retrying') THEN now() END)
		ON CONFLICT (run_id, seq) DO UPDATE
		SET status = excluded.status, output = excluded.output, comp_data = excluded.comp_data,
		    error = excluded.error, error_kind = excluded.error_kind,
		    uncertain = saga.step.uncertain OR excluded.uncertain, finished_at = excluded.finished_at`,
		runID, seq, name, o.Status, o.Output, o.CompData, o.Error, o.ErrorKind, o.Uncertain)
	if err != nil {
		return fmt.Errorf("put step: %w", err)
	}
	return nil
}

// FinishStep records the outcome of a step attempt: done, failed, or
// retrying (the next attempt is scheduled).
func FinishStep(ctx context.Context, tx pgx.Tx, runID string, seq int, o Outcome) error {
	_, err := tx.Exec(ctx, `
		UPDATE saga.step
		SET status = $3, output = $4, comp_data = $5, error = nullif($6, ''), error_kind = nullif($7, ''),
		    uncertain = uncertain OR $8,
		    finished_at = CASE WHEN $3 = 'retrying' THEN NULL ELSE now() END
		WHERE run_id = $1 AND seq = $2`,
		runID, seq, o.Status, o.Output, o.CompData, o.Error, o.ErrorKind, o.Uncertain)
	if err != nil {
		return fmt.Errorf("finish step: %w", err)
	}
	return nil
}

// SetStepStatus changes only the status (and error) of a step.
func SetStepStatus(ctx context.Context, tx pgx.Tx, runID string, seq int, status, errMsg string) error {
	_, err := tx.Exec(ctx, `
		UPDATE saga.step SET status = $3, error = nullif($4, ''), finished_at = now()
		WHERE run_id = $1 AND seq = $2`, runID, seq, status, errMsg)
	if err != nil {
		return fmt.Errorf("set step status: %w", err)
	}
	return nil
}

// SetRunStatus changes the run status. Terminal statuses release the lease;
// done and compensated also release the run's business locks (needs_attention
// keeps them until their TTL, protecting what the run left half done).
func SetRunStatus(ctx context.Context, tx pgx.Tx, runID, status string, output json.RawMessage, errMsg string) error {
	if status == RunDone || status == RunCompensated {
		if _, err := tx.Exec(ctx, `DELETE FROM saga.lock WHERE run_id = $1`, runID); err != nil {
			return fmt.Errorf("release locks: %w", err)
		}
	}
	terminal := status == RunDone || status == RunCompensated || status == RunNeedsAttention
	_, err := tx.Exec(ctx, `
		UPDATE saga.run
		SET status = $2,
		    output = coalesce($3, output),
		    error = coalesce(nullif($4, ''), error),
		    owner_id = CASE WHEN $5 THEN NULL ELSE owner_id END,
		    lease_until = CASE WHEN $5 THEN NULL ELSE lease_until END,
		    wake_at = NULL, await_name = NULL,
		    updated_at = now()
		WHERE id = $1`, runID, status, output, errMsg, terminal)
	if err != nil {
		return fmt.Errorf("set run status: %w", err)
	}
	return nil
}

// RenewLeases extends the leases owner still holds, and the business locks of
// those runs, in one statement. ids and
// versions are parallel slices. It returns the set of runs renewed; a run
// missing from it was taken over, finished or parked.
func (s *Store) RenewLeases(
	ctx context.Context,
	owner string,
	ids []string,
	versions []int64,
	ttl time.Duration,
) (map[string]bool, error) {
	rows, err := s.pool.Query(ctx, `
		WITH renewed AS (
			UPDATE saga.run r
			SET lease_until = now() + $4 * interval '1 microsecond'
			FROM unnest($1::uuid[], $2::bigint[]) AS l(id, version)
			WHERE r.id = l.id AND r.lease_version = l.version AND r.owner_id = $3
			  AND r.status IN ('running', 'compensating')
			RETURNING r.id
		), locks AS (
			-- Business locks of executing runs live as long as their lease.
			-- SKIP LOCKED: never wait on a lock row a contender holds (no
			-- lock-order dependency on the CTE evaluation order, no stall on
			-- a hot key); a skipped row is extended on the next tick, well
			-- before its ttl (>= LeaseTTL) runs out.
			UPDATE saga.lock k
			SET until = greatest(k.until, now() + k.ttl_us * interval '1 microsecond')
			WHERE k.key IN (
				SELECT key FROM saga.lock
				WHERE run_id IN (SELECT id FROM renewed)
				FOR UPDATE SKIP LOCKED)
		)
		SELECT id::text FROM renewed`, ids, versions, owner, micros(ttl))
	if err != nil {
		return nil, fmt.Errorf("renew leases: %w", err)
	}
	renewed, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("renew leases: %w", err)
	}
	out := make(map[string]bool, len(renewed))
	for _, id := range renewed {
		out[id] = true
	}
	return out, nil
}

// ReleaseLease ends a lease early (graceful shutdown) so another instance can
// resume the run without waiting for the TTL. Clearing owner_id also makes any
// renewal still in flight from this owner match nothing. It is a no-op if the
// lease moved on.
func (s *Store) ReleaseLease(ctx context.Context, l Lease) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE saga.run SET owner_id = NULL, lease_until = now(), updated_at = now()
		WHERE id = $1 AND owner_id = $2 AND lease_version = $3
		  AND status IN ('running', 'compensating')`, l.RunID, l.Owner, l.Version)
	if err != nil {
		return fmt.Errorf("release lease: %w", err)
	}
	return nil
}

// InsertQueuedRun creates a run nobody owns yet, inside tx, so the caller can
// enqueue the job that executes it in the same transaction. The run is
// reserved for that job until now()+grace (owner_id NULL, any instance may
// still take it): recovery leaves it alone until then. timeout > 0 sets the
// run deadline. It returns false when a run with the same idempotency key
// exists.
func InsertQueuedRun(ctx context.Context, tx pgx.Tx, r Run, grace, timeout time.Duration) (bool, error) {
	tag, err := tx.Exec(ctx, `
		INSERT INTO saga.run (id, workflow, version, idem_key, status, input, lease_until, deadline)
		VALUES ($1, $2, $3, $4, 'running', $5, now() + $6 * interval '1 microsecond',
		        CASE WHEN $7::bigint > 0 THEN now() + $7::bigint * interval '1 microsecond' END)
		ON CONFLICT (idem_key) DO NOTHING`,
		r.ID, r.Workflow, r.Version, r.IdemKey, r.Input, micros(grace), micros(timeout))
	if err != nil {
		return false, fmt.Errorf("insert run: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// ReserveRecoverable picks, inside tx, up to limit unfinished runs whose
// lease expired, and reserves them for the resume jobs the caller enqueues in
// the same tx: owner_id is cleared (fencing out a stale owner) and lease_until
// moves to now()+grace, so later scans skip runs whose job is still queued.
// SKIP LOCKED lets concurrent scanners split the work.
func ReserveRecoverable(ctx context.Context, tx pgx.Tx, limit int, grace time.Duration) ([]string, error) {
	rows, err := tx.Query(ctx, `
		UPDATE saga.run SET owner_id = NULL, lease_until = now() + $2 * interval '1 microsecond', updated_at = now()
		WHERE id IN (
			SELECT id FROM saga.run
			WHERE status IN ('running', 'waiting', 'compensating') AND lease_until < now()
			LIMIT $1
			FOR NO KEY UPDATE SKIP LOCKED)
		RETURNING id::text`, limit, micros(grace))
	if err != nil {
		return nil, fmt.Errorf("reserve recoverable runs: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("reserve recoverable runs: %w", err)
	}
	return ids, nil
}

// Park suspends a running run inside tx (the caller's fenced transaction):
// status waiting, lease released, due at the earliest of wake and the run
// deadline (nil: no wake-up time, only a signal or the deadline resumes it).
// lease_until = due + grace lets recovery re-enqueue the run if its wake-up
// job goes missing. It returns the due time (nil if none).
func Park(
	ctx context.Context,
	tx pgx.Tx,
	runID string,
	wake *time.Time,
	awaitName string,
	grace time.Duration,
) (*time.Time, error) {
	var due *time.Time
	err := tx.QueryRow(ctx, `
		UPDATE saga.run
		SET status = 'waiting', wake_at = least($2::timestamptz, deadline), await_name = nullif($3, ''),
		    owner_id = NULL, lease_until = least($2::timestamptz, deadline) + $4 * interval '1 microsecond',
		    updated_at = now()
		WHERE id = $1
		RETURNING wake_at`, runID, wake, awaitName, micros(grace)).Scan(&due)
	if err != nil {
		return nil, fmt.Errorf("park run: %w", err)
	}
	return due, nil
}

// Backoff releases the lease of a compensating run until now()+delay, inside
// tx: only a job scheduled at the returned time (or recovery after grace)
// resumes it.
func Backoff(ctx context.Context, tx pgx.Tx, runID string, delay, grace time.Duration) (time.Time, error) {
	var due time.Time
	err := tx.QueryRow(ctx, `
		UPDATE saga.run
		SET owner_id = NULL, wake_at = now() + $2 * interval '1 microsecond',
		    lease_until = now() + ($2 + $3) * interval '1 microsecond', updated_at = now()
		WHERE id = $1
		RETURNING wake_at`, runID, micros(delay), micros(grace)).Scan(&due)
	if err != nil {
		return due, fmt.Errorf("backoff run: %w", err)
	}
	return due, nil
}

// FailCompensation records a failed compensation attempt of a step and
// returns how many have failed so far.
func FailCompensation(ctx context.Context, tx pgx.Tx, runID string, seq int, errMsg string) (int, error) {
	var n int
	err := tx.QueryRow(ctx, `
		UPDATE saga.step SET comp_attempt = comp_attempt + 1, error = $3
		WHERE run_id = $1 AND seq = $2
		RETURNING comp_attempt`, runID, seq, errMsg).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("fail compensation: %w", err)
	}
	return n, nil
}

// Now returns the database time.
func Now(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
		return now, fmt.Errorf("now: %w", err)
	}
	return now, nil
}

// GetSignal returns the payload of a delivered signal.
func GetSignal(ctx context.Context, tx pgx.Tx, runID, name string) (json.RawMessage, bool, error) {
	var payload json.RawMessage
	err := tx.QueryRow(ctx, `SELECT payload FROM saga.signal WHERE run_id = $1 AND name = $2`, runID, name).Scan(&payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get signal: %w", err)
	}
	return payload, true, nil
}

// DeliverSignal stores a signal inside tx, holding the run row lock, so it
// cannot interleave with a run parking on it (Park runs under the same lock
// via Fenced). The first signal of a name wins. It reports whether the run is
// parked waiting for this signal, i.e. needs a resume job.
func DeliverSignal(
	ctx context.Context,
	tx pgx.Tx,
	runID,
	name string,
	payload json.RawMessage,
	grace time.Duration,
) (bool, error) {
	var status, awaiting string
	err := tx.QueryRow(ctx, `
		SELECT status, coalesce(await_name, '') FROM saga.run WHERE id = $1 FOR NO KEY UPDATE`,
		runID).Scan(&status, &awaiting)
	if err != nil {
		return false, fmt.Errorf("deliver signal: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO saga.signal (run_id, name, payload) VALUES ($1, $2, $3)
		ON CONFLICT (run_id, name) DO NOTHING`, runID, name, payload)
	if err != nil {
		return false, fmt.Errorf("deliver signal: %w", err)
	}
	// A duplicate signal changes nothing: the first one already woke the run.
	if tag.RowsAffected() == 0 || status != RunWaiting || awaiting != name {
		return false, nil
	}
	// Reserve the run for the resume job, as Workflow.Start and recovery do:
	// if that job is lost or skipped, recovery re-enqueues the run after
	// grace (it is acquirable since its signal is there).
	if _, err := tx.Exec(ctx, `
		UPDATE saga.run SET lease_until = now() + $2 * interval '1 microsecond', updated_at = now()
		WHERE id = $1`, runID, micros(grace)); err != nil {
		return false, fmt.Errorf("deliver signal: %w", err)
	}
	return true, nil
}

// ReopenCompensation moves a needs_attention run back to compensating, inside
// tx, resetting the attempt counters of failed compensations, and reserves it
// for the resume job the caller enqueues. It returns false if the run is not
// in needs_attention.
func ReopenCompensation(ctx context.Context, tx pgx.Tx, runID string, grace time.Duration) (bool, error) {
	tag, err := tx.Exec(ctx, `
		UPDATE saga.run
		SET status = 'compensating', owner_id = NULL, wake_at = NULL, await_name = NULL,
		    lease_until = now() + $2 * interval '1 microsecond', updated_at = now()
		WHERE id = $1 AND status = 'needs_attention'`, runID, micros(grace))
	if err != nil {
		return false, fmt.Errorf("reopen compensation: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	if _, err := tx.Exec(ctx, `
		UPDATE saga.step SET comp_attempt = 0
		WHERE run_id = $1 AND status = 'compensation_failed'`, runID); err != nil {
		return false, fmt.Errorf("reopen compensation: %w", err)
	}
	return true, nil
}

// AcquireLock takes the business lock key for runID until now()+ttl. It
// succeeds if the lock is free, expired, or already held by runID (then it
// never shortens it), and returns the holder otherwise.
func AcquireLock(ctx context.Context, tx pgx.Tx, key, runID string, ttl time.Duration) (bool, string, error) {
	var holder string
	err := tx.QueryRow(ctx, `
		INSERT INTO saga.lock (key, run_id, until, ttl_us)
		VALUES ($1, $2, now() + $3 * interval '1 microsecond', $3)
		ON CONFLICT (key) DO UPDATE
		SET run_id = excluded.run_id,
		    until = CASE WHEN saga.lock.run_id = excluded.run_id
		                 THEN greatest(saga.lock.until, excluded.until) ELSE excluded.until END,
		    ttl_us = CASE WHEN saga.lock.run_id = excluded.run_id
		                  THEN greatest(saga.lock.ttl_us, excluded.ttl_us) ELSE excluded.ttl_us END,
		    created_at = CASE WHEN saga.lock.run_id = excluded.run_id THEN saga.lock.created_at ELSE now() END
		WHERE saga.lock.until < now() OR saga.lock.run_id = excluded.run_id
		RETURNING run_id::text`, key, runID, micros(ttl)).Scan(&holder)
	if errors.Is(err, pgx.ErrNoRows) {
		// Busy. ON CONFLICT ... WHERE false still locked the row, so the
		// holder cannot have deleted it in between.
		err := tx.QueryRow(ctx, `SELECT run_id::text FROM saga.lock WHERE key = $1`, key).Scan(&holder)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return false, "", fmt.Errorf("lock holder: %w", err)
		}
		return false, holder, nil
	}
	if err != nil {
		return false, "", fmt.Errorf("acquire lock: %w", err)
	}
	return true, holder, nil
}

// ExtendLock extends a lock runID still holds (even if expired, as long as no
// other run took it). It returns false if the row is gone or belongs to
// another run: the lock was lost, even if it is free again now.
func ExtendLock(ctx context.Context, tx pgx.Tx, key, runID string, ttl time.Duration) (bool, error) {
	tag, err := tx.Exec(ctx, `
		UPDATE saga.lock
		SET until = greatest(until, now() + $3 * interval '1 microsecond'), ttl_us = greatest(ttl_us, $3)
		WHERE key = $1 AND run_id = $2`, key, runID, micros(ttl))
	if err != nil {
		return false, fmt.Errorf("extend lock: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// PurgeRuns deletes up to limit done/compensated runs last updated more than
// age ago. Steps, signals and locks go with them (ON DELETE CASCADE).
func (s *Store) PurgeRuns(ctx context.Context, age time.Duration, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM saga.run WHERE id IN (
			SELECT id FROM saga.run
			WHERE status IN ('done', 'compensated')
			  AND updated_at < now() - $1::bigint * interval '1 microsecond'
			LIMIT $2
			FOR UPDATE SKIP LOCKED)`, micros(age), limit)
	if err != nil {
		return 0, fmt.Errorf("purge runs: %w", err)
	}
	return tag.RowsAffected(), nil
}

// StartStepFenced is Fenced + StartStep in one autocommit statement (one
// round trip instead of four): the CTE locks the run row (FOR NO KEY UPDATE)
// and checks the lease; if it no longer matches, nothing is written and
// ErrLeaseLost is returned. Under READ COMMITTED a concurrent takeover is
// re-checked on the newest row version, as in Fenced.
//
// With async, the statement commits with synchronous_commit=off (set for
// this transaction only): it does not wait for the WAL flush. Any later
// durable commit on the server, such as the module's own transaction or the
// step's done checkpoint, flushes it too.
func (s *Store) StartStepFenced(ctx context.Context, l Lease, seq int, name string, async bool) (int, time.Time, error) {
	var attempt int
	var now time.Time
	err := s.pool.QueryRow(ctx, startStepFencedSQL, l.RunID, seq, name, l.Owner, l.Version, async).Scan(&attempt, &now)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, now, ErrLeaseLost
	}
	if err != nil {
		return 0, now, fmt.Errorf("start step: %w", err)
	}
	return attempt, now, nil
}

// startStepFencedSQL is the statement of StartStepFenced. Re-running a step
// left "started" (its previous attempt's outcome is unknown) marks it
// uncertain, so it is compensated even if this attempt fails.
const startStepFencedSQL = `
	WITH lease AS (
		SELECT id FROM saga.run
		WHERE id = $1 AND owner_id = $4 AND lease_version = $5
		FOR NO KEY UPDATE
	), relaxed AS (
		SELECT set_config('synchronous_commit', 'off', true) WHERE $6::boolean
	)
	INSERT INTO saga.step (run_id, seq, name, status)
	SELECT lease.id, $2, $3, 'started' FROM lease LEFT JOIN relaxed ON true
	ON CONFLICT (run_id, seq) DO UPDATE
	SET status = 'started', attempt = saga.step.attempt + 1, started_at = now(), error = NULL,
	    uncertain = saga.step.uncertain OR saga.step.status = 'started'
	RETURNING attempt, now()`

// FinishStepFenced is Fenced + recording a step outcome in one autocommit
// statement (see StartStepFenced). It upserts: if the "started" row is
// missing (lost with Options.AsyncStepStart after a Postgres crash), it is
// recreated instead of the write being mistaken for a lost lease.
func (s *Store) FinishStepFenced(ctx context.Context, l Lease, seq int, name string, o Outcome) error {
	var one int
	err := s.pool.QueryRow(ctx, `
		WITH lease AS (
			SELECT id FROM saga.run
			WHERE id = $1 AND owner_id = $9 AND lease_version = $10
			FOR NO KEY UPDATE
		)
		INSERT INTO saga.step AS st (run_id, seq, name, status, output, comp_data, error, error_kind, uncertain, finished_at)
		SELECT id, $2, $11, $3, $4, $5, nullif($6, ''), nullif($7, ''), $8,
		       CASE WHEN $3 = 'retrying' THEN NULL ELSE now() END
		FROM lease
		ON CONFLICT (run_id, seq) DO UPDATE
		SET status = excluded.status, output = excluded.output, comp_data = excluded.comp_data,
		    error = excluded.error, error_kind = excluded.error_kind,
		    uncertain = st.uncertain OR excluded.uncertain, finished_at = excluded.finished_at
		RETURNING 1`,
		l.RunID, seq, o.Status, o.Output, o.CompData, o.Error, o.ErrorKind, o.Uncertain,
		l.Owner, l.Version, name).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("finish step: %w", err)
	}
	return nil
}
