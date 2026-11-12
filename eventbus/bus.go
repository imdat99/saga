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
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/imdat99/bullmq"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"github.com/imdat99/saga/eventbus/migrations"
)

// NeedsAttentionEvent is emitted (to the outbox) when a run is parked as
// needs_attention. Its payload is a NeedsAttention.
const NeedsAttentionEvent = "saga.needs_attention"

// NeedsAttention is the payload of NeedsAttentionEvent.
type NeedsAttention struct {
	RunID    string `json:"run_id"`
	Workflow string `json:"workflow"`
	Version  int    `json:"version"`
	Error    string `json:"error"`
}

// Event is a domain event.
type Event struct {
	// ID is assigned by EmitTx (UUIDv7). Handlers deduplicate on it.
	ID      string          `json:"id"`
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload"`
	// At is when the event was emitted (database time).
	At time.Time `json:"at"`
}

// NewEvent builds an event with a JSON payload.
func NewEvent(name string, payload any) (Event, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Event{}, fmt.Errorf("eventbus: encode %s: %w", name, err)
	}
	return Event{Name: name, Payload: raw}, nil
}

// Decode unmarshals the payload into v.
func (e Event) Decode(v any) error { return json.Unmarshal(e.Payload, v) }

// Handler processes one delivery of an event. A returned error makes the
// delivery retried with the group's backoff (handlers of the same event that
// succeeded are not run again); wrap it with Unrecoverable to give up on this
// handler for this event at once (the job then ends in the failed set, the
// dead letter queue, once the other handlers are done).
type Handler func(ctx context.Context, ev Event) error

// Unrecoverable marks a handler error as not worth retrying.
func Unrecoverable(err error) error {
	if err == nil {
		return nil
	}
	return bullmq.NewUnrecoverableError(err.Error())
}

// Subscriber is one handler of a consumer group for one event name.
type Subscriber struct {
	// ID names the subscriber within its group; it is recorded in a job when
	// the handler succeeded, so that a retry of the job skips it. Stable
	// across deploys. Letters, digits, '.', '_', '-'.
	ID string
	// Event is the event name subscribed to.
	Event string
	// Handle processes deliveries.
	Handle Handler
}

// nameRE validates subscriber IDs and group names: they end up in Redis keys
// and job ids, where ':' and '|' have a meaning.
var nameRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,200}$`)

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
