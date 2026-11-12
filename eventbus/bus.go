// Package eventbus delivers domain events through a transactional outbox in
// Postgres to BullMQ (Redis), with at-least-once delivery per subscriber.
//
// Modules emit events with EmitTx inside their own transaction. Outside a saga
// the event goes straight to eventbus.outbox. From inside a saga step it is
// staged in eventbus.staging and only released to the outbox in the
// transaction that marks the run done; a compensated run's events are dropped.
// The relay moves outbox rows to BullMQ, one job per (event, subscriber) with
// job id "<eventID>.<subscriberID>", so a relay that crashes after pushing and
// before marking the rows sent cannot deliver twice (BullMQ ignores a job id
// it already has).
//
// Delivery order is not guaranteed and handlers may see an event more than
// once (BullMQ retries): handlers must be idempotent by Event.ID and should
// re-read state rather than trust event order.
package eventbus

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/imdat99/bullmq"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"github.com/imdat99/saga"
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

// Handler processes one delivery of an event. A returned error makes BullMQ
// retry with the subscriber's backoff; wrap it with Unrecoverable to fail the
// job at once (it then stays in the failed set, the dead letter queue).
type Handler func(ctx context.Context, ev Event) error

// Unrecoverable marks a handler error as not worth retrying.
func Unrecoverable(err error) error {
	if err == nil {
		return nil
	}
	return bullmq.NewUnrecoverableError(err.Error())
}

// Subscriber receives the events of one name.
type Subscriber struct {
	// ID names the subscriber; it is the BullMQ job name and part of the job
	// id, so it must be stable across deploys. Letters, digits, '.', '_', '-'.
	ID string
	// Event is the event name subscribed to.
	Event string
	// Queue is the BullMQ queue the subscriber's jobs go to (default: the
	// subscriber ID). Subscribers sharing a queue share its workers, so every
	// instance that has a handler for one of them must have handlers for all
	// of them (Start checks it): a worker that picks a job it cannot handle
	// wastes an attempt.
	Queue string
	// Attempts is the number of deliveries before the job fails (default 5).
	Attempts int
	// Backoff is the first retry delay, doubled each time (default 1s).
	Backoff time.Duration
	// Handle processes deliveries. It may be nil on instances that only
	// relay: the subscriber must still be registered there so the relay
	// fans events out to it.
	Handle Handler
}

var nameRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,200}$`)

// Options configures a Bus.
type Options struct {
	// Redis is the BullMQ connection.
	Redis bullmq.RedisConnectionOptions
	// Prefix is the BullMQ key prefix (default "bull").
	Prefix string
	// RelayInterval is the pause between relay passes when the outbox is
	// empty (default 500ms).
	RelayInterval time.Duration
	// RelayBatch is the number of outbox rows per relay transaction
	// (default 200).
	RelayBatch int
	// Retention is how long sent outbox rows, and staging rows of runs that
	// already ended, are kept (default 7 days).
	Retention time.Duration
	// CompletedRetention is how long BullMQ keeps completed jobs, which is
	// also the window in which a re-relayed job id is deduplicated
	// (default 24h).
	CompletedRetention time.Duration
	// FailedRetention is how long BullMQ keeps failed jobs, the dead letter
	// queue (default 7 days).
	FailedRetention time.Duration
	// Concurrency is the number of jobs a worker processes in parallel per
	// queue (default 10).
	Concurrency int
	// CleanupInterval is the pause between retention passes (default 1m).
	CleanupInterval time.Duration
	Logger          *slog.Logger
}

func (o *Options) setDefaults() {
	if o.Prefix == "" {
		o.Prefix = "bull"
	}
	if o.RelayInterval <= 0 {
		o.RelayInterval = 500 * time.Millisecond
	}
	if o.RelayBatch <= 0 {
		o.RelayBatch = 200
	}
	if o.Retention <= 0 {
		o.Retention = 7 * 24 * time.Hour
	}
	if o.CompletedRetention <= 0 {
		o.CompletedRetention = 24 * time.Hour
	}
	if o.FailedRetention <= 0 {
		o.FailedRetention = 7 * 24 * time.Hour
	}
	if o.Concurrency <= 0 {
		o.Concurrency = 10
	}
	if o.CleanupInterval <= 0 {
		o.CleanupInterval = time.Minute
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Bus emits, relays and dispatches events. Register subscribers, then Start.
type Bus struct {
	pool *pgxpool.Pool
	opts Options
	log  *slog.Logger

	// life serializes Start and Stop; mu guards registration and the
	// running pointer. Neither is taken by the relay goroutine or the
	// BullMQ processors, which only read the immutable *running snapshot.
	life       sync.Mutex
	mu         sync.Mutex
	subs       map[string]Subscriber // by ID
	run        *running
	isStarting bool // Start is building run: registration is closed

	// afterPush is a test hook between AddBulk and marking rows sent.
	afterPush func() error
}

// running is what Start builds. It never changes until Stop has waited for
// every goroutine reading it.
type running struct {
	subs    map[string]Subscriber   // by ID
	byEvent map[string][]Subscriber // by event name
	conn    *bullmq.RedisConnection
	queues  map[string]*bullmq.Queue
	workers []*bullmq.Worker
	cancel  context.CancelFunc
	relay   chan struct{} // closed when the relay loop exits
	cleanup chan struct{} // closed when the retention loop exits
}

// New returns a Bus using pool. Run Migrate first.
func New(pool *pgxpool.Pool, opts Options) *Bus {
	opts.setDefaults()
	return &Bus{
		pool: pool,
		opts: opts,
		log:  opts.Logger.With("component", "eventbus"),
		subs: map[string]Subscriber{},
	}
}

// ErrStarted means Subscribe was called after Start.
var ErrStarted = errors.New("eventbus: already started")

// Subscribe registers a subscriber. Every instance running the relay must
// register the same subscribers (with or without handlers), or events relayed
// there skip the missing ones.
func (b *Bus) Subscribe(s Subscriber) error {
	if !nameRE.MatchString(s.ID) {
		return fmt.Errorf("eventbus: invalid subscriber id %q", s.ID)
	}
	if s.Event == "" {
		return fmt.Errorf("eventbus: subscriber %s has no event", s.ID)
	}
	if s.Queue == "" {
		s.Queue = s.ID
	}
	if !nameRE.MatchString(s.Queue) {
		return fmt.Errorf("eventbus: invalid queue name %q", s.Queue)
	}
	if s.Attempts <= 0 {
		s.Attempts = 5
	}
	if s.Backoff <= 0 {
		s.Backoff = time.Second
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.run != nil || b.isStarting {
		return ErrStarted
	}
	if _, dup := b.subs[s.ID]; dup {
		return fmt.Errorf("eventbus: subscriber %s registered twice", s.ID)
	}
	b.subs[s.ID] = s
	return nil
}

// EmitTx records events in the caller's transaction, in one statement. If ctx
// belongs to a saga step (saga.RunIDFrom), the events are staged until the
// run is done; otherwise they go to the outbox and are delivered once tx
// commits. It sets the ID of each event.
//
// Emit from inside a step, with the step's ctx, in the same transaction as the
// step's (idempotent) effect: a context.Background() inside a step bypasses
// staging (the event is delivered even if the run compensates), and emitting
// from the workflow body would stage again on every resume.
func (b *Bus) EmitTx(ctx context.Context, tx pgx.Tx, events ...Event) error {
	if len(events) == 0 {
		return nil
	}
	ids := make([]string, len(events))
	names := make([]string, len(events))
	payloads := make([]string, len(events))
	for i := range events {
		ev := &events[i]
		if ev.Name == "" {
			return errors.New("eventbus: event without name")
		}
		id, err := uuid.NewV7()
		if err != nil {
			return err
		}
		ev.ID = id.String()
		ids[i], names[i], payloads[i] = ev.ID, ev.Name, string(ev.Payload)
		if len(ev.Payload) == 0 {
			payloads[i] = "null"
		}
	}
	var err error
	if runID, inRun := saga.RunIDFrom(ctx); inRun {
		_, err = tx.Exec(ctx, `
			INSERT INTO eventbus.staging (id, run_id, name, payload)
			SELECT id, $4, name, payload::jsonb FROM unnest($1::uuid[], $2::text[], $3::text[]) AS e(id, name, payload)`,
			ids, names, payloads, runID)
	} else {
		_, err = tx.Exec(ctx, `
			INSERT INTO eventbus.outbox (id, name, payload)
			SELECT id, name, payload::jsonb FROM unnest($1::uuid[], $2::text[], $3::text[]) AS e(id, name, payload)`,
			ids, names, payloads)
	}
	if err != nil {
		return fmt.Errorf("eventbus: emit: %w", err)
	}
	return nil
}

// RunEnded implements saga.RunHook: a done run releases its staged events to
// the outbox, any other ending drops them (events of steps that were not
// compensated included); needs_attention also emits NeedsAttentionEvent. One
// statement, run under the run's row lock.
func (b *Bus) RunEnded(ctx context.Context, tx pgx.Tx, run saga.RunInfo) error {
	var attention []byte
	if run.Status == saga.StatusNeedsAttention {
		var err error
		if attention, err = json.Marshal(NeedsAttention{
			RunID: run.ID, Workflow: run.Workflow, Version: run.Version, Error: run.Error,
		}); err != nil {
			return fmt.Errorf("eventbus: encode %s: %w", NeedsAttentionEvent, err)
		}
	}
	id, err := uuid.NewV7()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		WITH staged AS (
			DELETE FROM eventbus.staging WHERE run_id = $1
			RETURNING id, name, payload, created_at
		), released AS (
			INSERT INTO eventbus.outbox (id, name, payload, created_at)
			SELECT id, name, payload, created_at FROM staged WHERE $2
		)
		INSERT INTO eventbus.outbox (id, name, payload)
		SELECT $3, $4, $5::jsonb WHERE $5::jsonb IS NOT NULL`,
		run.ID, run.Status == saga.StatusDone, id.String(), NeedsAttentionEvent, attention); err != nil {
		return fmt.Errorf("eventbus: run %s ended: %w", run.ID, err)
	}
	return nil
}

var _ saga.RunHook = (*Bus)(nil)

// migrateLockID serializes Migrate across instances starting at once.
const migrateLockID = 0x5a6a_0002

// Migrate creates the eventbus schema and applies its migrations, under a
// session-level advisory lock (needs a pool of at least 3 connections).
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
