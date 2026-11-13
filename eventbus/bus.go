// Package eventbus delivers domain events through a transactional outbox in
// Postgres to BullMQ (Redis), at least once to every interested service.
//
// Two roles, usable in one process or in different services:
//
//   - A Producer (NewProducer, needs Postgres and Redis) is where events are
//     emitted, with EmitTx inside the module's own transaction. Outside a saga
//     the event goes straight to eventbus.outbox; from inside a saga step it is
//     staged in eventbus.staging and released to the outbox in the transaction
//     that marks the run done (Producer is a saga.RunHook), or dropped if the
//     run is compensated. The producer's relay moves outbox rows to BullMQ.
//   - A Consumer (NewConsumer, needs Redis only) is a consumer group, typically
//     one service: its instances share one BullMQ queue named after the
//     group, and each event job runs every handler the group has for that
//     event.
//
// Routing lives in Redis, not in any process: a consumer registers the events
// its group handles when it starts, and every relay pushes one job per
// (event, interested group), with job id "<eventID>.<group>", so a relay that
// crashes after pushing and before marking the rows sent cannot deliver twice
// (BullMQ ignores a job id it already has). A service that only consumes needs
// no database access.
//
// Delivery order is not guaranteed and handlers may see an event more than
// once (retries, crashes): handlers must be idempotent by Event.ID and should
// re-read state rather than trust event order.
package eventbus

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"github.com/imdat99/saga/eventbus/consume"
	"github.com/imdat99/saga/eventbus/migrations"
)

// The consumer side lives in package consume (no Postgres, saga or River
// dependency, for services that only consume); these aliases keep it
// reachable from eventbus for services that do both.
type (
	// Event is a domain event.
	Event = consume.Event
	// Handler processes one delivery of an event.
	Handler = consume.Handler
	// Subscriber is one handler of a consumer group for one event name.
	Subscriber = consume.Subscriber
	// NeedsAttention is the payload of NeedsAttentionEvent.
	NeedsAttention = consume.NeedsAttention
	// Consumer is one instance of a consumer group.
	Consumer = consume.Consumer
	// ConsumerOptions configures a Consumer.
	ConsumerOptions = consume.Options
	// RegistryOptions locates the routing registry.
	RegistryOptions = consume.RegistryOptions
)

// NeedsAttentionEvent is emitted (to the outbox) when a run is parked as
// needs_attention. Its payload is a NeedsAttention.
const NeedsAttentionEvent = consume.NeedsAttentionEvent

// ErrStarted means Start was called twice, or Subscribe after Start.
var ErrStarted = consume.ErrStarted

// NewEvent builds an event with a JSON payload.
func NewEvent(name string, payload any) (Event, error) { return consume.NewEvent(name, payload) }

// Unrecoverable marks a handler error as not worth retrying.
func Unrecoverable(err error) error { return consume.Unrecoverable(err) }

// NewConsumer returns a consumer of group (see consume.New).
func NewConsumer(group string, opts ConsumerOptions) (*Consumer, error) {
	return consume.New(group, opts)
}

// Unregister removes a consumer group (see consume.Unregister).
func Unregister(ctx context.Context, o RegistryOptions, group string) error {
	return consume.Unregister(ctx, o, group)
}

// Forget removes one event from a group's routes (see consume.Forget).
func Forget(ctx context.Context, o RegistryOptions, group, event string) error {
	return consume.Forget(ctx, o, group, event)
}

// migrateLockID serializes Migrate across instances starting at once.
const migrateLockID = 0x5a6a_0002

// Migrate creates the eventbus schema and applies its migrations, under a
// session-level advisory lock (needs a pool of at least 3 connections). Only
// producers need it.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("eventbus: migration lock: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrateLockID); err != nil {
		return fmt.Errorf("eventbus: migration lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, migrateLockID)
	}()
	if _, err := conn.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS eventbus`); err != nil {
		return fmt.Errorf("eventbus: create schema: %w", err)
	}
	db := stdlib.OpenDBFromPool(pool)
	defer func(db *sql.DB) { _ = db.Close() }(db)
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("eventbus: migration locker: %w", err)
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS,
		goose.WithTableName("eventbus.goose_db_version"),
		goose.WithSessionLocker(locker),
		goose.WithDisableGlobalRegistry(true))
	if err != nil {
		return fmt.Errorf("eventbus: migration provider: %w", err)
	}
	if _, err := p.Up(ctx); err != nil {
		return fmt.Errorf("eventbus: migrate: %w", err)
	}
	return nil
}
