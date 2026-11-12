package eventbus

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/imdat99/bullmq"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/saga"
	"github.com/imdat99/saga/eventbus/internal/registry"
	"github.com/imdat99/saga/internal/uuidv7"
)

// ProducerOptions configures a Producer.
type ProducerOptions struct {
	// Redis is the BullMQ connection.
	Redis bullmq.RedisConnectionOptions
	// Prefix is the BullMQ key prefix (default "bull"); producers and
	// consumers of one bus must use the same. In Redis Cluster it needs a
	// hash tag, e.g. "{bull}".
	Prefix string
	// RelayInterval is the pause between relay passes when the outbox is
	// empty (default 500ms). A full batch is followed at once by the next.
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
	// CleanupInterval is the pause between retention passes (default 1m).
	CleanupInterval time.Duration
	// RegistryGrace is how long a relay keeps routing to the groups it
	// knew when the registry is lost (Redis restarted without persistence),
	// while the running consumers register again (default 2m; keep it above
	// the consumers' RegistryCheck, 30s by default).
	RegistryGrace time.Duration
	Logger        *slog.Logger
}

func (o *ProducerOptions) setDefaults() {
	if o.Prefix == "" {
		o.Prefix = registry.DefaultPrefix
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
	if o.CleanupInterval <= 0 {
		o.CleanupInterval = time.Minute
	}
	if o.RegistryGrace <= 0 {
		o.RegistryGrace = 2 * time.Minute
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Producer emits events into the outbox and relays the outbox to the
// consumer groups' queues. Every instance of a service that emits may run
// it: relay and retention passes use SKIP LOCKED, so instances split the work.
type Producer struct {
	pool *pgxpool.Pool
	opts ProducerOptions
	log  *slog.Logger

	// life serializes Start and Stop; mu guards run. The relay and retention
	// goroutines only use their own *relayState.
	life sync.Mutex
	mu   sync.Mutex
	run  *relayState

	// afterPush is a test hook between pushing a batch and marking it sent.
	afterPush func() error
}

// NewProducer returns a Producer on pool. Run Migrate first. EmitTx and
// RunEnded work without Start; Start runs the relay and the retention.
func NewProducer(pool *pgxpool.Pool, opts ProducerOptions) *Producer {
	opts.setDefaults()
	return &Producer{pool: pool, opts: opts, log: opts.Logger.With("component", "eventbus.producer")}
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
func (p *Producer) EmitTx(ctx context.Context, tx pgx.Tx, events ...Event) error {
	if len(events) == 0 {
		return nil
	}
	for i := range events {
		if events[i].Name == "" {
			return errors.New("eventbus: event without name")
		}
		events[i].ID = uuidv7.New()
	}
	runID, inRun := saga.RunIDFrom(ctx)
	var err error
	if len(events) == 1 {
		err = emitOne(ctx, tx, &events[0], runID, inRun)
	} else {
		err = emitMany(ctx, tx, events, runID, inRun)
	}
	if err != nil {
		return fmt.Errorf("eventbus: emit: %w", err)
	}
	return nil
}

// emitOne is the common case, without arrays. The payload goes as a string
// cast to jsonb: a []byte would be sent as bytea in the simple and exec
// query modes (PgBouncer setups), which jsonb rejects.
func emitOne(ctx context.Context, tx pgx.Tx, ev *Event, runID string, inRun bool) error {
	payload := string(ev.Payload)
	if payload == "" {
		payload = "null"
	}
	var err error
	if inRun {
		_, err = tx.Exec(ctx, `INSERT INTO eventbus.staging (id, run_id, name, payload) VALUES ($1, $2, $3, $4::text::jsonb)`,
			ev.ID, runID, ev.Name, payload)
	} else {
		_, err = tx.Exec(ctx, `INSERT INTO eventbus.outbox (id, name, payload) VALUES ($1, $2, $3::text::jsonb)`,
			ev.ID, ev.Name, payload)
	}
	return err
}

// emitMany inserts several events in one statement.
func emitMany(ctx context.Context, tx pgx.Tx, events []Event, runID string, inRun bool) error {
	ids := make([]string, len(events))
	names := make([]string, len(events))
	payloads := make([]string, len(events)) // strings, see emitOne
	for i := range events {
		ids[i], names[i], payloads[i] = events[i].ID, events[i].Name, string(events[i].Payload)
		if payloads[i] == "" {
			payloads[i] = "null"
		}
	}
	var err error
	if inRun {
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
	return err
}

// RunEnded implements saga.RunHook: a done run releases its staged events
// to the outbox, any other ending drops them (events of steps that were not
// compensated included); needs_attention also emits NeedsAttentionEvent. One
// statement, run under the run's row lock.
func (p *Producer) RunEnded(ctx context.Context, tx pgx.Tx, run saga.RunInfo) error {
	var attention *string // as text, see emitOne
	if run.Status == saga.StatusNeedsAttention {
		raw, err := json.Marshal(NeedsAttention{
			RunID: run.ID, Workflow: run.Workflow, Version: run.Version, Error: run.Error,
		})
		if err != nil {
			return fmt.Errorf("eventbus: encode %s: %w", NeedsAttentionEvent, err)
		}
		s := string(raw)
		attention = &s
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
		SELECT $3, $4, $5::text::jsonb WHERE $5::text IS NOT NULL`,
		run.ID, run.Status == saga.StatusDone, uuidv7.New(), NeedsAttentionEvent, attention); err != nil {
		return fmt.Errorf("eventbus: run %s ended: %w", run.ID, err)
	}
	return nil
}

var _ saga.RunHook = (*Producer)(nil)

// Start connects to Redis and starts the relay and retention loops.
func (p *Producer) Start(ctx context.Context) error {
	p.life.Lock()
	defer p.life.Unlock()
	p.mu.Lock()
	running := p.run != nil
	p.mu.Unlock()
	if running {
		return ErrStarted
	}
	st, err := p.prepare(ctx)
	if err != nil {
		return err
	}
	loopCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	st.cancel = cancel
	st.relay = make(chan struct{})
	st.cleanup = make(chan struct{})
	go p.relayLoop(loopCtx, st)
	go p.retentionLoop(loopCtx, st.cleanup)
	p.mu.Lock()
	p.run = st
	p.mu.Unlock()
	return nil
}

// Stop ends the relay and retention loops (waiting for a pass in progress
// until ctx is done) and closes the Redis connection.
func (p *Producer) Stop(ctx context.Context) error {
	p.life.Lock()
	defer p.life.Unlock()
	p.mu.Lock()
	st := p.run
	p.mu.Unlock()
	if st == nil {
		return nil
	}
	st.cancel()
	for _, done := range []chan struct{}{st.relay, st.cleanup} {
		select {
		case <-done:
		case <-ctx.Done():
		}
	}
	st.close()
	p.mu.Lock()
	p.run = nil
	p.mu.Unlock()
	return ctx.Err()
}

// relayState is what Start builds for the relay. Its fields after qmu are
// only used by the relay goroutine.
type relayState struct {
	conn    *bullmq.RedisConnection
	cancel  context.CancelFunc
	relay   chan struct{} // closed when the relay loop exits
	cleanup chan struct{} // closed when the retention loop exits

	qmu    sync.Mutex
	queues map[string]*bullmq.Queue // by group

	reg           *registry.Table // the routing used: loaded, plus sticky
	loaded        *registry.Table // as read from Redis
	sticky        *registry.Table // routes kept after a registry loss
	stickyUntil   time.Time
	keepCompleted *bullmq.RemoveOnFinish // shared by every job
	keepFailed    *bullmq.RemoveOnFinish

	// Scratch reused across passes, so that a pass allocates (on our side)
	// one job id per job and one boxed payload per event. The previous
	// pass's jobs are gone by then: AddBulk has returned, and BullMQ keeps
	// no reference to the data (it sent it to Redis).
	rows    []outboxRow
	arena   []byte // the event JSON of the pass, rows[i].data slices into it
	sentIDs []byte // text literal of the uuid[] of the pass: {id,id,...}
	names   map[string]string
	opts    []bullmq.JobOptions
	batches map[string][]bullmq.BulkJob
}

// Scratch kept between passes is bounded: past these, a pass that needed
// more allocates afresh and the next one starts small again.
const (
	maxKeptArena = 4 << 20
	maxKeptJobs  = 8192
)

// maxInternedNames bounds the event-name intern table (a relay sees few
// distinct names; the bound only protects against unbounded name sets).
const maxInternedNames = 4096

// outboxRow is one event read by a relay pass. data is the event's JSON as
// built by Postgres, pushed as is to every interested group.
type outboxRow struct {
	id         [16]byte
	name       string
	start, end int // the event's JSON in arena
	data       any // json.RawMessage over arena, boxed once for all the event's jobs
}

func (p *Producer) prepare(ctx context.Context) (*relayState, error) {
	conn, err := bullmq.NewRedisConnection(ctx, p.opts.Redis)
	if err != nil {
		return nil, fmt.Errorf("eventbus: redis: %w", err)
	}
	completed := int64(p.opts.CompletedRetention / time.Second)
	failed := int64(p.opts.FailedRetention / time.Second)
	return &relayState{
		conn:          conn,
		queues:        map[string]*bullmq.Queue{},
		keepCompleted: bullmq.KeepJobsPolicy(bullmq.KeepJobs{Age: &completed}),
		keepFailed:    bullmq.KeepJobsPolicy(bullmq.KeepJobs{Age: &failed}),
		batches:       map[string][]bullmq.BulkJob{},
		names:         map[string]string{},
	}, nil
}

func (st *relayState) close() {
	st.qmu.Lock()
	defer st.qmu.Unlock()
	for _, q := range st.queues {
		_ = q.Close()
	}
	st.queues = nil // queue() refuses from now on
	_ = st.conn.Close()
}

// queue returns the BullMQ queue of a group, created on first use.
func (st *relayState) queue(ctx context.Context, group, prefix string) (*bullmq.Queue, error) {
	st.qmu.Lock()
	defer st.qmu.Unlock()
	if st.queues == nil {
		return nil, errors.New("eventbus: producer stopped")
	}
	if q, ok := st.queues[group]; ok {
		return q, nil
	}
	q, err := bullmq.NewQueueWithConnection(ctx, group, st.conn, &bullmq.QueueOptions{Prefix: prefix})
	if err != nil {
		return nil, fmt.Errorf("eventbus: queue %s: %w", group, err)
	}
	st.queues[group] = q
	return q, nil
}

// refresh reloads the routing table if the registry changed since the last
// load: one round trip per pass otherwise. When the registry was lost (its
// epoch changed), the routes known before are kept for grace on top of what
// is registered again, so that a group that has not registered again yet
// still gets its events. Queues of groups no longer routed are closed.
func (st *relayState) refresh(ctx context.Context, prefix string, grace time.Duration) error {
	state, err := registry.ReadState(ctx, st.conn.Client(), prefix)
	if err != nil {
		return err
	}
	now := time.Now()
	stickyExpired := st.sticky != nil && !now.Before(st.stickyUntil)
	if st.loaded != nil && state.Version == st.loaded.Version && state.Epoch == st.loaded.Epoch && !stickyExpired {
		return nil
	}
	loaded, err := registry.Load(ctx, st.conn.Client(), prefix)
	if err != nil {
		return err
	}
	if st.loaded != nil && st.loaded.Epoch != "" && loaded.Epoch != st.loaded.Epoch {
		st.sticky, st.stickyUntil = st.reg, now.Add(grace) // st.reg includes an earlier sticky
	} else if stickyExpired {
		st.sticky = nil
	}
	st.loaded, st.reg = loaded, loaded
	if st.sticky != nil {
		st.reg = registry.Union(loaded, st.sticky)
	}
	for g := range st.batches {
		if st.reg.Groups[g] == nil {
			delete(st.batches, g)
		}
	}
	st.qmu.Lock()
	for g, q := range st.queues { // nil map after close: no iteration
		if st.reg.Groups[g] == nil {
			_ = q.Close()
			delete(st.queues, g)
		}
	}
	st.qmu.Unlock()
	return nil
}

// holding reports whether events must wait: no group was ever registered
// (and none is remembered from before a registry loss).
func (st *relayState) holding() bool { return st.loaded.Version == 0 && st.sticky == nil }

func (p *Producer) relayLoop(ctx context.Context, st *relayState) {
	defer close(st.relay)
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n, err := p.relayOnce(ctx, st)
		if err != nil && ctx.Err() == nil {
			p.log.Warn("relay pass failed", "err", err)
		}
		// A full batch means more is waiting: go again at once.
		if err == nil && n == p.opts.RelayBatch {
			t.Reset(0)
		} else {
			t.Reset(p.opts.RelayInterval)
		}
	}
}

// relaySelect reads a batch of unsent events, each with its delivery JSON
// built by Postgres (the consumer decodes it as an Event).
const relaySelect = `
	SELECT id, name, jsonb_build_object('id', id, 'name', name, 'payload', payload, 'at', created_at)::text
	FROM eventbus.outbox
	WHERE sent_at IS NULL
	ORDER BY id
	LIMIT $1
	FOR UPDATE SKIP LOCKED`

// relayOnce pushes one batch of unsent outbox events to the queues of the
// groups that handle them, and marks the batch sent, in one transaction
// holding the rows (SKIP LOCKED). If anything fails after the push, the rows
// stay unsent and are pushed again later with the same job ids, which BullMQ
// ignores. Events no group handles are marked sent (nobody to deliver to).
// It returns the number of events.
func (p *Producer) relayOnce(ctx context.Context, st *relayState) (int, error) {
	if err := st.refresh(ctx, p.opts.Prefix, p.opts.RegistryGrace); err != nil {
		return 0, err
	}
	if st.holding() {
		// No consumer group ever registered (first deploy, or Redis lost
		// the registry): hold the events instead of marking them sent to
		// nobody. Consumers register again on their own (RegistryCheck).
		return 0, nil
	}
	n := 0
	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if err := st.read(ctx, tx, p.opts.RelayBatch); err != nil {
			return err
		}
		n = len(st.rows)
		if n == 0 {
			return nil
		}
		if err := st.push(ctx, p.opts.Prefix); err != nil {
			return err
		}
		if p.afterPush != nil {
			if err := p.afterPush(); err != nil {
				return err
			}
		}
		// The ids as one text literal (one allocation per pass): pgx boxes
		// every element of a [][16]byte or []string array. A string, not the
		// []byte: the simple protocol would send a []byte as bytea.
		if _, err := tx.Exec(ctx, `UPDATE eventbus.outbox SET sent_at = now() WHERE id = ANY($1::text::uuid[])`, string(st.sentIDs)); err != nil {
			return fmt.Errorf("eventbus: mark sent: %w", err)
		}
		return nil
	})
	return n, err
}

// read loads a batch into st.rows (event JSON in st.arena) and the text
// literal of their ids into st.sentIDs. It works on the raw column values
// (binary uuid, text name, text JSON) instead of scanning, to stay off pgx's
// per-value allocations.
func (st *relayState) read(ctx context.Context, tx pgx.Tx, limit int) error {
	if cap(st.arena) > maxKeptArena {
		st.arena = nil // a burst of large events: do not keep its arena
	}
	if cap(st.opts) > maxKeptJobs {
		st.opts = nil
	}
	clear(st.rows) // no reference to the previous arena past the new length
	st.rows, st.arena = st.rows[:0], st.arena[:0]
	st.sentIDs = append(st.sentIDs[:0], '{')
	rows, err := tx.Query(ctx, relaySelect, limit)
	if err != nil {
		return fmt.Errorf("eventbus: read outbox: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		v := rows.RawValues()
		if len(v) != 3 {
			return fmt.Errorf("eventbus: read outbox: %d columns", len(v))
		}
		var r outboxRow
		if err := parseUUID(&r.id, v[0], rows.FieldDescriptions()[0].Format); err != nil {
			return fmt.Errorf("eventbus: read outbox: %w", err)
		}
		r.name = st.intern(v[1])
		r.start = len(st.arena)
		st.arena = append(st.arena, v[2]...)
		r.end = len(st.arena)
		st.rows = append(st.rows, r)
		if len(st.rows) > 1 {
			st.sentIDs = append(st.sentIDs, ',')
		}
		st.sentIDs = appendUUID(st.sentIDs, &r.id)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("eventbus: read outbox: %w", err)
	}
	st.sentIDs = append(st.sentIDs, '}')
	// The arena is complete (no more growth): slice the payloads.
	for i := range st.rows {
		r := &st.rows[i]
		r.data = json.RawMessage(st.arena[r.start:r.end:r.end])
	}
	return nil
}

// intern returns the event name of b without allocating once seen.
func (st *relayState) intern(b []byte) string {
	if s, ok := st.names[string(b)]; ok { // no allocation for the lookup
		return s
	}
	s := string(b)
	if len(st.names) < maxInternedNames {
		st.names[s] = s
	}
	return s
}

// parseUUID reads a uuid column value: 16 bytes in binary format (the
// default), the canonical 36-character text in text format (simple protocol).
func parseUUID(dst *[16]byte, v []byte, format int16) error {
	if format == pgx.BinaryFormatCode {
		if len(v) != 16 {
			return fmt.Errorf("uuid of %d bytes", len(v))
		}
		copy(dst[:], v)
		return nil
	}
	if len(v) != 36 || v[8] != '-' || v[13] != '-' || v[18] != '-' || v[23] != '-' {
		return fmt.Errorf("uuid text %q", v)
	}
	var hexDigits [32]byte
	n := copy(hexDigits[:], v[0:8])
	n += copy(hexDigits[n:], v[9:13])
	n += copy(hexDigits[n:], v[14:18])
	n += copy(hexDigits[n:], v[19:23])
	copy(hexDigits[n:], v[24:36])
	if _, err := hex.Decode(dst[:], hexDigits[:]); err != nil {
		return fmt.Errorf("uuid text %q: %w", v, err)
	}
	return nil
}

// appendUUID appends the canonical text form of id.
func appendUUID(dst []byte, id *[16]byte) []byte {
	var b [36]byte
	hex.Encode(b[0:8], id[0:4])
	b[8] = '-'
	hex.Encode(b[9:13], id[4:6])
	b[13] = '-'
	hex.Encode(b[14:18], id[6:8])
	b[18] = '-'
	hex.Encode(b[19:23], id[8:10])
	b[23] = '-'
	hex.Encode(b[24:36], id[10:16])
	return append(dst, b[:]...)
}

// jobIDMax bounds a job id: a 36-byte UUID, '.', a group name (<= 200).
const jobIDMax = 36 + 1 + 200

// push builds the jobs of st.rows, one per (event, interested group), and
// adds them with one AddBulk per group.
func (st *relayState) push(ctx context.Context, prefix string) error {
	for g, jobs := range st.batches {
		clear(jobs) // drop references to the previous pass's payloads
		st.batches[g] = jobs[:0]
	}
	total := 0
	for i := range st.rows {
		total += len(st.reg.ByEvent[st.rows[i].name])
	}
	if total == 0 {
		return nil
	}
	if cap(st.opts) < total {
		st.opts = make([]bullmq.JobOptions, total)
	}
	opts := st.opts[:total]
	k := 0
	var buf [jobIDMax]byte
	for i := range st.rows {
		r := &st.rows[i]
		routes := st.reg.ByEvent[r.name]
		if len(routes) == 0 {
			continue
		}
		appendUUID(buf[:0], &r.id)
		for _, rt := range routes {
			o := &opts[k]
			k++
			*o = bullmq.JobOptions{
				// BullMQ rejects custom ids with a single ':'; '.' is safe:
				// group names are validated, event ids are UUIDs.
				JobID:            string(append(buf[:36], rt.Suffix...)),
				Attempts:         rt.Attempts,
				Backoff:          rt.Backoff,
				RemoveOnComplete: st.keepCompleted,
				RemoveOnFail:     st.keepFailed,
			}
			st.batches[rt.Group] = append(st.batches[rt.Group], bullmq.BulkJob{Name: r.name, Data: r.data, Opts: o})
		}
	}
	return st.addAll(ctx, prefix)
}

// addAll adds every group's batch, the groups in parallel: each AddBulk is
// a few Redis round trips, which would otherwise add up per group.
func (st *relayState) addAll(ctx context.Context, prefix string) error {
	// Every queue first: no goroutine may still be pushing (reading this
	// pass's arena and options) when addAll returns, whatever fails.
	type push struct {
		group string
		q     *bullmq.Queue
		jobs  []bullmq.BulkJob
	}
	pushes := make([]push, 0, len(st.batches))
	for group, jobs := range st.batches {
		if len(jobs) == 0 {
			continue
		}
		q, err := st.queue(ctx, group, prefix)
		if err != nil {
			return err
		}
		pushes = append(pushes, push{group, q, jobs})
	}
	if len(pushes) == 1 { // the common case: no goroutine
		if _, err := pushes[0].q.AddBulk(ctx, pushes[0].jobs); err != nil {
			return fmt.Errorf("eventbus: push to %s: %w", pushes[0].group, err)
		}
		return nil
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	for _, ps := range pushes {
		group, q, jobs := ps.group, ps.q, ps.jobs
		wg.Go(func() {
			if _, err := q.AddBulk(ctx, jobs); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("eventbus: push to %s: %w", group, err))
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (p *Producer) retentionLoop(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	t := time.NewTicker(p.opts.CleanupInterval)
	defer t.Stop()
	for {
		if err := p.cleanup(ctx); err != nil && ctx.Err() == nil {
			p.log.Warn("event retention failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// cleanupBatch bounds one retention statement; a pass repeats it until a
// batch comes back short (at most cleanupMaxBatches times).
const (
	cleanupBatch      = 5000
	cleanupMaxBatches = 100
)

// cleanup deletes sent outbox rows older than Retention, and staging rows of
// runs that already ended (left by a step that committed after its run ended),
// in bounded batches. Concurrent instances skip each other's rows.
func (p *Producer) cleanup(ctx context.Context) error {
	age := p.opts.Retention.Microseconds()
	// Without the saga schema (bus used for plain outbox events) there is no
	// staging to clean.
	var withSaga bool
	if err := p.pool.QueryRow(ctx, `SELECT to_regclass('saga.run') IS NOT NULL`).Scan(&withSaga); err != nil {
		return fmt.Errorf("eventbus: retention: %w", err)
	}
	queries := []string{`
		DELETE FROM eventbus.outbox WHERE id IN (
			SELECT id FROM eventbus.outbox
			WHERE sent_at < now() - $1 * interval '1 microsecond'
			LIMIT $2 FOR UPDATE SKIP LOCKED)`, `
		DELETE FROM eventbus.staging WHERE id IN (
			SELECT s.id FROM eventbus.staging s
			WHERE s.created_at < now() - $1 * interval '1 microsecond'
			  AND NOT EXISTS (
				SELECT 1 FROM saga.run r
				WHERE r.id = s.run_id AND r.status NOT IN ('done', 'compensated', 'needs_attention'))
			LIMIT $2 FOR UPDATE SKIP LOCKED)`}
	if !withSaga {
		queries = queries[:1]
	}
	for _, q := range queries {
		for range cleanupMaxBatches {
			tag, err := p.pool.Exec(ctx, q, age, cleanupBatch)
			if err != nil {
				return fmt.Errorf("eventbus: retention: %w", err)
			}
			if tag.RowsAffected() < cleanupBatch {
				break
			}
		}
	}
	return nil
}
