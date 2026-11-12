package saga

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"

	"github.com/imdat99/saga/migrations"
)

// Schema is the Postgres schema holding the saga tables and the River queue
// tables the engine uses. River lives here, not in public, so an application
// running its own River does not share (or migrate) the saga queue.
const Schema = "saga"

// migrateLockID serializes Migrate across instances starting at once. goose
// takes its own session lock inside it as well.
const migrateLockID = 0x5a6a_0001

const minMigrateConns = 3

// Migrate creates the saga schema and applies the saga and River migrations.
// It holds a Postgres session-level advisory lock throughout, so several
// instances starting at once migrate one after another. Run it over a direct
// connection, not through PgBouncer in transaction mode.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	// One connection holds the lock, goose's locker holds another, and the
	// migration statements need a third.
	if n := pool.Config().MaxConns; n < minMigrateConns {
		return fmt.Errorf("saga: Migrate needs a pool of at least %d connections, got %d", minMigrateConns, n)
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("migration lock: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrateLockID); err != nil {
		return fmt.Errorf("migration lock: %w", err)
	}
	defer func() {
		// Unlock even if ctx is done; a broken connection drops the lock anyway.
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, migrateLockID)
	}()

	if _, err := conn.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+Schema); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}
	if err := migrateSaga(ctx, pool); err != nil {
		return err
	}
	return migrateRiver(ctx, pool)
}

func migrateSaga(ctx context.Context, pool *pgxpool.Pool) error {
	db := stdlib.OpenDBFromPool(pool)
	defer func(db *sql.DB) { _ = db.Close() }(db)

	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("migration locker: %w", err)
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS,
		goose.WithTableName(Schema+".goose_db_version"),
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

func migrateRiver(ctx context.Context, pool *pgxpool.Pool) error {
	m, err := rivermigrate.New(riverpgxv5.New(pool), &rivermigrate.Config{Schema: Schema})
	if err != nil {
		return fmt.Errorf("river migrator: %w", err)
	}
	if _, err := m.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		return fmt.Errorf("river migrate: %w", err)
	}
	return nil
}
