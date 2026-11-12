// Package store holds the Postgres queries of the saga engine.
//
// Every write made on behalf of a running saga goes through Fenced, which locks
// the run row (FOR NO KEY UPDATE) and checks the caller still holds the lease version it
// acquired. A takeover by another instance bumps lease_version, so a stale owner
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
}

// Step is a saga.step row.
type Step struct {
	Seq      int
	Name     string
	Status   string
	Attempt  int
	Output   json.RawMessage
	CompData json.RawMessage
	Error    string
}

// Lease identifies the ownership a writer must still hold.
type Lease struct {
	RunID   string
	Owner   string
	Version int64
}

// Store runs queries against the saga schema.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store backed by pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

const runColumns = `id::text, workflow, version, idem_key, status, input, output, coalesce(error, ''), lease_version`

func scanRun(row pgx.Row) (Run, error) {
	var r Run
	err := row.Scan(&r.ID, &r.Workflow, &r.Version, &r.IdemKey, &r.Status, &r.Input, &r.Output, &r.Error, &r.LeaseVersion)
	return r, err
}

// InsertRun creates a run already leased by owner. It returns false when a run
// with the same idempotency key exists (the caller then loads it instead).
func (s *Store) InsertRun(ctx context.Context, r Run, owner string, ttl time.Duration) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO saga.run (id, workflow, version, idem_key, status, input, owner_id, lease_until, lease_version)
		VALUES ($1, $2, $3, $4, 'running', $5, $6, now() + $7::interval, 1)
		ON CONFLICT (idem_key) DO NOTHING`,
		r.ID, r.Workflow, r.Version, r.IdemKey, r.Input, owner, ttl.String())
	if err != nil {
		return false, fmt.Errorf("insert run: %w", err)
	}
	return tag.RowsAffected() == 1, nil
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

// AcquireLease takes over an unfinished run whose lease has expired. It never
// re-grants a live lease, not even to the same owner, so two goroutines of one
// instance cannot both execute the run.
func (s *Store) AcquireLease(ctx context.Context, runID, owner string, ttl time.Duration) (int64, bool, error) {
	var version int64
	err := s.pool.QueryRow(ctx, `
		UPDATE saga.run
		SET owner_id = $2, lease_until = now() + $3::interval,
		    lease_version = lease_version + 1, updated_at = now()
		WHERE id = $1
		  AND status IN ('running', 'compensating')
		  AND (lease_until IS NULL OR lease_until < now())
		RETURNING lease_version`, runID, owner, ttl.String()).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("acquire lease: %w", err)
	}
	return version, true, nil
}

// ListSteps returns the steps of a run ordered by seq.
func (s *Store) ListSteps(ctx context.Context, runID string) ([]Step, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT seq, name, status, attempt, output, comp_data, coalesce(error, '')
		FROM saga.step WHERE run_id = $1 ORDER BY seq`, runID)
	if err != nil {
		return nil, fmt.Errorf("list steps: %w", err)
	}
	steps, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Step, error) {
		var st Step
		err := row.Scan(&st.Seq, &st.Name, &st.Status, &st.Attempt, &st.Output, &st.CompData, &st.Error)
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

// StartStep records that a step is about to run. Re-running a step left in
// "started" by a crash bumps its attempt counter.
func StartStep(ctx context.Context, tx pgx.Tx, runID string, seq int, name string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO saga.step (run_id, seq, name, status)
		VALUES ($1, $2, $3, 'started')
		ON CONFLICT (run_id, seq) DO UPDATE
		SET status = 'started', attempt = saga.step.attempt + 1, started_at = now(), error = NULL`,
		runID, seq, name)
	if err != nil {
		return fmt.Errorf("start step: %w", err)
	}
	return nil
}

// FinishStep records the outcome of a step invocation.
func FinishStep(ctx context.Context, tx pgx.Tx, runID string, seq int, status string, output, compData json.RawMessage, errMsg string) error {
	_, err := tx.Exec(ctx, `
		UPDATE saga.step
		SET status = $3, output = $4, comp_data = $5, error = nullif($6, ''), finished_at = now()
		WHERE run_id = $1 AND seq = $2`,
		runID, seq, status, output, compData, errMsg)
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

// SetRunStatus changes the run status. Terminal statuses release the lease.
func SetRunStatus(ctx context.Context, tx pgx.Tx, runID, status string, output json.RawMessage, errMsg string) error {
	terminal := status == RunDone || status == RunCompensated || status == RunNeedsAttention
	_, err := tx.Exec(ctx, `
		UPDATE saga.run
		SET status = $2,
		    output = coalesce($3, output),
		    error = coalesce(nullif($4, ''), error),
		    owner_id = CASE WHEN $5 THEN NULL ELSE owner_id END,
		    lease_until = CASE WHEN $5 THEN NULL ELSE lease_until END,
		    updated_at = now()
		WHERE id = $1`, runID, status, output, errMsg, terminal)
	if err != nil {
		return fmt.Errorf("set run status: %w", err)
	}
	return nil
}
