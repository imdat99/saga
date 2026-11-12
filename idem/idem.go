// Package idem gives modules exactly-once effects for saga steps, which the
// engine only runs at least once.
//
// Each module owns a processed_requests table in its own schema. Do runs the
// module's change inside the module's transaction together with the insertion
// of the step key, so the change and the "processed" mark commit or roll back
// together. A step re-executed after a crash, or retried, with the same
// saga.Key gets the first result back instead of acting twice.
//
//	var requests = idem.MustNew("inventory")
//
//	func Reserve(ctx context.Context, tx pgx.Tx, k saga.Key, sku string) (string, error) {
//		return idem.Do(ctx, tx, requests, k, func(ctx context.Context) (string, error) {
//			return insertReservation(ctx, tx, sku)
//		})
//	}
package idem

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Table is a module's processed_requests table.
type Table struct {
	schema string
	name   string // quoted, schema-qualified
}

var identRE = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// New returns the processed_requests table of the module schema. The schema
// name must be a plain lower-case identifier, since it is part of the SQL.
func New(schema string) (*Table, error) {
	if !identRE.MatchString(schema) {
		return nil, fmt.Errorf("idem: invalid schema name %q", schema)
	}
	return &Table{schema: schema, name: `"` + schema + `".processed_requests`}, nil
}

// MustNew is New for package-level variables; it panics on an invalid name.
func MustNew(schema string) *Table {
	t, err := New(schema)
	if err != nil {
		panic(err)
	}
	return t
}

// Execer is satisfied by pgx.Tx, *pgx.Conn and *pgxpool.Pool.
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Querier is satisfied by pgx.Tx, *pgx.Conn and *pgxpool.Pool.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// CreateTable creates the table if it does not exist (the schema must exist).
// Modules that manage their own migrations can run the same DDL there.
func (t *Table) CreateTable(ctx context.Context, db Execer) error {
	_, err := db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+t.name+` (
			key        text PRIMARY KEY,
			result     jsonb,
			aborted    boolean NOT NULL DEFAULT false,
			created_at timestamptz NOT NULL DEFAULT now()
		)`)
	if err != nil {
		return fmt.Errorf("idem: create %s: %w", t.name, err)
	}
	if _, err := db.Exec(ctx, `CREATE INDEX IF NOT EXISTS processed_requests_created_idx
		ON `+t.name+` (created_at)`); err != nil {
		return fmt.Errorf("idem: create index on %s: %w", t.name, err)
	}
	return nil
}

// ErrAborted means the key was fenced off with Abort: the request must not
// take effect any more.
var ErrAborted = errors.New("idem: request aborted")

// Do runs fn once per key. tx must be the transaction fn writes in. The key is
// claimed first, so a concurrent Do with the same key blocks until tx ends,
// then returns the stored result (if tx committed) or runs fn itself (if tx
// rolled back). Claim and fn run in a savepoint: when fn fails, both are
// undone even if the caller commits tx anyway, and the key stays free. The
// result is stored as JSON: keep it small.
//
// Under READ COMMITTED (the default) a concurrent duplicate sees the first
// result. Under REPEATABLE READ or SERIALIZABLE it fails with a serialization
// error (SQLSTATE 40001) instead: retry the transaction.
func Do[T any, K ~string](
	ctx context.Context,
	tx pgx.Tx,
	t *Table,
	key K,
	fn func(ctx context.Context) (T, error),
) (T, error) {
	var zero T
	sp, err := tx.Begin(ctx)
	if err != nil {
		return zero, fmt.Errorf("idem: savepoint: %w", err)
	}
	v, err := do(ctx, sp, t, string(key), fn)
	if err != nil {
		_ = sp.Rollback(context.WithoutCancel(ctx))
		return zero, err
	}
	if err := sp.Commit(ctx); err != nil {
		return zero, fmt.Errorf("idem: release savepoint: %w", err)
	}
	return v, nil
}

func do[T any](
	ctx context.Context,
	tx pgx.Tx,
	t *Table,
	key string,
	fn func(ctx context.Context) (T, error),
) (T, error) {
	var zero T
	tag, err := tx.Exec(ctx, `INSERT INTO `+t.name+` (key) VALUES ($1) ON CONFLICT (key) DO NOTHING`, key)
	if err != nil {
		return zero, fmt.Errorf("idem: claim %s: %w", key, err)
	}
	if tag.RowsAffected() == 0 {
		v, state, err := lookup[T](ctx, tx, t, key)
		switch {
		case err != nil:
			return zero, err
		case state == stateAborted:
			return zero, fmt.Errorf("%w: %s", ErrAborted, key)
		case state == stateMissing:
			return zero, fmt.Errorf("idem: key %s vanished after conflict", key)
		}
		return v, nil
	}

	v, err := fn(ctx)
	if err != nil {
		return zero, err
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return zero, fmt.Errorf("idem: encode result of %s: %w", key, err)
	}
	if _, err := tx.Exec(ctx, `UPDATE `+t.name+` SET result = $2 WHERE key = $1`, key, raw); err != nil {
		return zero, fmt.Errorf("idem: store result of %s: %w", key, err)
	}
	return v, nil
}

type recordState int

const (
	stateMissing recordState = iota + 1
	stateProcessed
	stateAborted
)

func lookup[T any](ctx context.Context, q Querier, t *Table, key string) (T, recordState, error) {
	var v T
	var raw []byte
	var aborted bool
	err := q.QueryRow(ctx, `SELECT result, aborted FROM `+t.name+` WHERE key = $1`, key).Scan(&raw, &aborted)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, stateMissing, nil
	}
	if err != nil {
		return v, 0, fmt.Errorf("idem: lookup %s: %w", key, err)
	}
	if aborted {
		return v, stateAborted, nil
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &v); err != nil {
			return v, stateProcessed, fmt.Errorf("idem: decode result of %s: %w", key, err)
		}
	}
	return v, stateProcessed, nil
}

// Lookup returns the stored result of key, if the request was processed and
// committed. It does not wait for an attempt still in flight: an uncertain
// compensator should use Abort instead, which does.
func Lookup[T any, K ~string](ctx context.Context, q Querier, t *Table, key K) (T, bool, error) {
	v, state, err := lookup[T](ctx, q, t, string(key))
	return v, state == stateProcessed, err
}

// Abort settles an uncertain request for a compensator, inside its tx. If the
// request was processed, it returns its result and true: undo it. Otherwise it
// records a tombstone so that an attempt still running elsewhere (a step that
// outlived its lease) can never take effect later: its Do fails with
// ErrAborted. Abort waits for such an attempt's transaction to end first.
// Under REPEATABLE READ or SERIALIZABLE, a claim committed after the
// snapshot makes it fail with 40001: retry. Purge deletes tombstones too,
// which re-opens the fence once age has passed.
func Abort[T any, K ~string](ctx context.Context, tx pgx.Tx, t *Table, key K) (T, bool, error) {
	var zero T
	tag, err := tx.Exec(ctx,
		`INSERT INTO `+t.name+` (key, aborted) VALUES ($1, true) ON CONFLICT (key) DO NOTHING`, string(key))
	if err != nil {
		return zero, false, fmt.Errorf("idem: abort %s: %w", key, err)
	}
	if tag.RowsAffected() == 1 {
		return zero, false, nil // fenced off: it never happened and never will
	}
	v, state, err := lookup[T](ctx, tx, t, string(key))
	return v, state == stateProcessed, err
}

// Purge deletes records older than age, up to limit rows per call, and
// returns how many it deleted. Keep age well above the longest time a saga
// can take to retry or resume a step.
func Purge(ctx context.Context, db Execer, t *Table, age time.Duration, limit int) (int64, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("idem: purge limit %d", limit)
	}
	tag, err := db.Exec(ctx, `
		DELETE FROM `+t.name+` WHERE key IN (
			SELECT key FROM `+t.name+`
			WHERE created_at < now() - $1 * interval '1 microsecond'
			LIMIT $2)`, age.Microseconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("idem: purge %s: %w", t.name, err)
	}
	return tag.RowsAffected(), nil
}
