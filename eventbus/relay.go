package eventbus

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/imdat99/bullmq"
	"github.com/jackc/pgx/v5"
)

// Start connects to Redis, starts a BullMQ worker per queue that has local
// handlers, the relay loop and the retention loop. Every instance may run
// them: relay and retention passes use SKIP LOCKED, so instances split the
// work. Subscribers are frozen until Stop.
func (b *Bus) Start(ctx context.Context) error {
	b.life.Lock()
	defer b.life.Unlock()
	b.mu.Lock()
	if b.run != nil || b.isStarting {
		b.mu.Unlock()
		return ErrStarted
	}
	b.isStarting = true // freezes registration until b.run is set or Start fails
	r := &running{subs: maps.Clone(b.subs), byEvent: map[string][]Subscriber{}, queues: map[string]*bullmq.Queue{}}
	b.mu.Unlock()
	ok := false
	defer func() {
		b.mu.Lock()
		b.isStarting = false
		if ok {
			b.run = r
		}
		b.mu.Unlock()
	}()
	if err := checkQueues(r.subs); err != nil {
		return err
	}
	for _, s := range r.subs {
		r.byEvent[s.Event] = append(r.byEvent[s.Event], s)
	}

	conn, err := bullmq.NewRedisConnection(ctx, b.opts.Redis)
	if err != nil {
		return fmt.Errorf("eventbus: redis: %w", err)
	}
	r.conn = conn
	handled := map[string]bool{}
	for _, s := range r.subs {
		if _, ok := r.queues[s.Queue]; !ok {
			q, err := bullmq.NewQueueWithConnection(ctx, s.Queue, conn, &bullmq.QueueOptions{Prefix: b.opts.Prefix})
			if err != nil {
				_ = r.close(time.Second) // best effort: Start already failed
				return fmt.Errorf("eventbus: queue %s: %w", s.Queue, err)
			}
			r.queues[s.Queue] = q
		}
		if s.Handle != nil {
			handled[s.Queue] = true
		} else {
			b.log.Debug("subscriber has no handler on this instance", "subscriber", s.ID)
		}
	}
	for queue := range handled {
		w, err := bullmq.NewWorkerWithConnection(ctx, queue, r.process, conn, &bullmq.WorkerOptions{
			Prefix:      b.opts.Prefix,
			Concurrency: b.opts.Concurrency,
		})
		if err != nil {
			_ = r.close(time.Second) // best effort: Start already failed
			return fmt.Errorf("eventbus: worker %s: %w", queue, err)
		}
		r.workers = append(r.workers, w)
	}

	loopCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	r.cancel = cancel
	r.relay = make(chan struct{})
	r.cleanup = make(chan struct{})
	go b.relayLoop(loopCtx, r)
	go b.retentionLoop(loopCtx, r.cleanup)
	ok = true
	return nil
}

// checkQueues rejects a queue whose subscribers have handlers for some but
// not all of them: its workers would take jobs they cannot handle.
func checkQueues(subs map[string]Subscriber) error {
	with, without := map[string]string{}, map[string]string{}
	for _, s := range subs {
		if s.Handle != nil {
			with[s.Queue] = s.ID
		} else {
			without[s.Queue] = s.ID
		}
	}
	for q, a := range with {
		if b, ok := without[q]; ok {
			return fmt.Errorf("eventbus: queue %s has a handler for %s but not for %s on this instance", q, a, b)
		}
	}
	return nil
}

// Stop ends the relay and retention loops, closes the workers (waiting for
// jobs in progress until ctx is done) and the Redis connection.
func (b *Bus) Stop(ctx context.Context) error {
	b.life.Lock()
	defer b.life.Unlock()
	b.mu.Lock()
	r := b.run
	b.mu.Unlock()
	if r == nil {
		return nil
	}
	r.cancel()
	for _, done := range []chan struct{}{r.relay, r.cleanup} {
		select {
		case <-done:
		case <-ctx.Done():
		}
	}
	timeout := time.Second
	if d, ok := ctx.Deadline(); ok {
		timeout = max(time.Until(d), 0)
	}
	err := r.close(timeout)
	b.mu.Lock()
	b.run = nil
	b.mu.Unlock()
	return errors.Join(err, ctx.Err())
}

// close closes the workers in parallel (each within timeout), then the queues
// and the connection.
func (r *running) close(timeout time.Duration) error {
	errs := make([]error, len(r.workers))
	var wg sync.WaitGroup
	for i, w := range r.workers {
		wg.Go(func() { errs[i] = w.Close(timeout) })
	}
	wg.Wait()
	for _, q := range r.queues {
		_ = q.Close()
	}
	if r.conn != nil {
		_ = r.conn.Close()
	}
	return errors.Join(errs...)
}

func (b *Bus) relayLoop(ctx context.Context, r *running) {
	defer close(r.relay)
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n, err := b.relayOnce(ctx, r)
		if err != nil && ctx.Err() == nil {
			b.log.Warn("relay pass failed", "err", err)
		}
		// A full batch means more is waiting: go again at once.
		if err == nil && n == b.opts.RelayBatch {
			t.Reset(0)
		} else {
			t.Reset(b.opts.RelayInterval)
		}
	}
}

func (b *Bus) retentionLoop(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	t := time.NewTicker(b.opts.CleanupInterval)
	defer t.Stop()
	for {
		if err := b.cleanup(ctx); err != nil && ctx.Err() == nil {
			b.log.Warn("event retention failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// relayOnce pushes one batch of unsent outbox events to BullMQ and marks them
// sent, in one transaction holding the rows (SKIP LOCKED). If anything fails
// after the push, the rows stay unsent and are pushed again later with the
// same job ids, which BullMQ ignores. It returns the number of events.
func (b *Bus) relayOnce(ctx context.Context, r *running) (int, error) {
	n := 0
	err := pgx.BeginFunc(ctx, b.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id::text, name, payload, created_at FROM eventbus.outbox
			WHERE sent_at IS NULL
			ORDER BY id
			LIMIT $1
			FOR UPDATE SKIP LOCKED`, b.opts.RelayBatch)
		if err != nil {
			return fmt.Errorf("eventbus: read outbox: %w", err)
		}
		events, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Event, error) {
			var ev Event
			err := r.Scan(&ev.ID, &ev.Name, &ev.Payload, &ev.At)
			return ev, err
		})
		if err != nil {
			return fmt.Errorf("eventbus: read outbox: %w", err)
		}
		n = len(events)
		if n == 0 {
			return nil
		}
		if err := r.push(ctx, events, b.jobOptions); err != nil {
			return err
		}
		if b.afterPush != nil {
			if err := b.afterPush(); err != nil {
				return err
			}
		}
		ids := make([]string, n)
		for i, ev := range events {
			ids[i] = ev.ID
		}
		if _, err := tx.Exec(ctx, `UPDATE eventbus.outbox SET sent_at = now() WHERE id = ANY($1::uuid[])`, ids); err != nil {
			return fmt.Errorf("eventbus: mark sent: %w", err)
		}
		return nil
	})
	return n, err
}

// push fans events out to their subscribers' queues, one AddBulk per queue.
func (r *running) push(ctx context.Context, events []Event, opts func(Event, Subscriber) *bullmq.JobOptions) error {
	jobs := map[string][]bullmq.BulkJob{}
	for _, ev := range events {
		for _, s := range r.byEvent[ev.Name] {
			jobs[s.Queue] = append(jobs[s.Queue], bullmq.BulkJob{Name: s.ID, Data: ev, Opts: opts(ev, s)})
		}
	}
	for queue, batch := range jobs {
		if _, err := r.queues[queue].AddBulk(ctx, batch); err != nil {
			return fmt.Errorf("eventbus: push to %s: %w", queue, err)
		}
	}
	return nil
}

func (b *Bus) jobOptions(ev Event, s Subscriber) *bullmq.JobOptions {
	completed := int64(b.opts.CompletedRetention / time.Second)
	failed := int64(b.opts.FailedRetention / time.Second)
	return &bullmq.JobOptions{
		// BullMQ rejects custom ids with a single ':'; '.' is safe because
		// subscriber ids are validated and event ids are UUIDs.
		JobID:            ev.ID + "." + s.ID,
		Attempts:         s.Attempts,
		Backoff:          bullmq.ExponentialBackoff(s.Backoff),
		RemoveOnComplete: bullmq.KeepJobsPolicy(bullmq.KeepJobs{Age: &completed}),
		RemoveOnFail:     bullmq.KeepJobsPolicy(bullmq.KeepJobs{Age: &failed}),
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
func (b *Bus) cleanup(ctx context.Context) error {
	age := b.opts.Retention.Microseconds()
	// Without the saga schema (bus used for plain outbox events) there is no
	// staging to clean.
	var withSaga bool
	if err := b.pool.QueryRow(ctx, `SELECT to_regclass('saga.run') IS NOT NULL`).Scan(&withSaga); err != nil {
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
			tag, err := b.pool.Exec(ctx, q, age, cleanupBatch)
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

// process dispatches a BullMQ job to its subscriber by job name.
func (r *running) process(ctx context.Context, job *bullmq.Job) (any, error) {
	s, ok := r.subs[job.Name]
	if !ok || s.Handle == nil {
		// Rolling deploy: an instance that does not know the subscriber yet
		// (or any more) lets another one take the retry.
		return nil, fmt.Errorf("eventbus: no handler for subscriber %q on this instance", job.Name)
	}
	var ev Event
	if err := job.DecodeData(&ev); err != nil {
		return nil, bullmq.NewUnrecoverableError(fmt.Sprintf("eventbus: bad event payload: %v", err))
	}
	return nil, s.Handle(ctx, ev)
}
