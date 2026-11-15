// Package outbox is the code of guide chapter 12 (Integrating an event bus):
// a transactional outbox for events emitted from saga steps, released when the
// run is done and dropped when it is compensated, through saga.RunHook, and a
// relay to any broker (Kafka, NATS, RabbitMQ, Redis...) behind Publisher.
//
// Copy it into your application and adapt the schema name, the Event type and
// the Publisher; it is not a supported library.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/imdat99/saga"
)

// Schema creates the staging and outbox tables (run it from your migrations).
const Schema = `
CREATE SCHEMA IF NOT EXISTS app;

-- Events emitted inside a saga step, waiting for the run's outcome.
CREATE TABLE IF NOT EXISTS app.event_staging (
	seq        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	id         text NOT NULL UNIQUE,     -- deterministic: derived from the step key
	run_id     uuid NOT NULL,             -- saga.run.id
	name       text NOT NULL,
	key        text,                     -- partition/ordering key for the broker (e.g. order id)
	payload    jsonb NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS event_staging_run_idx ON app.event_staging (run_id);

-- Events to publish, in order.
CREATE TABLE IF NOT EXISTS app.outbox (
	seq        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	id         text NOT NULL UNIQUE,
	name       text NOT NULL,
	key        text,
	payload    jsonb NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	sent_at    timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_unsent_idx ON app.outbox (seq) WHERE sent_at IS NULL;
`

// Event is what a module emits.
type Event struct {
	ID      string // unique and deterministic, e.g. string(k) + ":order.placed"
	Name    string
	Key     string // optional broker key (ordering, partitioning)
	Payload any
}

// Emit writes ev in the module's transaction: staged when called from a saga
// step (released or dropped when the run ends), straight to the outbox
// otherwise.
func Emit(ctx context.Context, tx pgx.Tx, ev Event) error {
	payload, err := json.Marshal(ev.Payload)
	if err != nil {
		return fmt.Errorf("encode %s: %w", ev.Name, err)
	}
	var key *string
	if ev.Key != "" {
		key = &ev.Key
	}
	if runID, inRun := saga.RunIDFrom(ctx); inRun {
		_, err = tx.Exec(ctx, `INSERT INTO app.event_staging (id, run_id, name, key, payload)
			VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO NOTHING`, ev.ID, runID, ev.Name, key, payload)
	} else {
		_, err = tx.Exec(ctx, `INSERT INTO app.outbox (id, name, key, payload)
			VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING`, ev.ID, ev.Name, key, payload)
	}
	return err
}

// Hook is the saga.RunHook of the outbox: register it in saga.Options.Hooks.
type Hook struct{}

// RunEnded releases the staged events of a done run, drops those of a
// compensated run, and emits an alert for a run that needs attention. It runs
// in the transaction that records the run's terminal status.
func (Hook) RunEnded(ctx context.Context, tx pgx.Tx, run saga.RunInfo) error {
	switch run.Status {
	case saga.StatusDone:
		_, err := tx.Exec(ctx, `
			WITH moved AS (DELETE FROM app.event_staging WHERE run_id = $1 RETURNING seq, id, name, key, payload)
			INSERT INTO app.outbox (id, name, key, payload)
			SELECT id, name, key, payload FROM moved ORDER BY seq
			ON CONFLICT (id) DO NOTHING`, run.ID)
		return err
	case saga.StatusCompensated:
		_, err := tx.Exec(ctx, `DELETE FROM app.event_staging WHERE run_id = $1`, run.ID)
		return err
	case saga.StatusNeedsAttention:
		// Staged events wait for the operator (RetryCompensation ends the run
		// compensated, which drops them). Alert now.
		payload, err := json.Marshal(map[string]string{"run_id": run.ID, "workflow": run.Workflow, "error": run.Error})
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO app.outbox (id, name, key, payload)
			VALUES (gen_random_uuid()::text, 'saga.needs_attention', $1, $2)`, run.ID, payload)
		return err
	}
	return nil
}

// Message is one outbox row handed to the broker.
type Message struct {
	Seq     int64
	ID      string
	Name    string
	Key     *string
	Payload []byte
}

// Publisher sends messages to the broker. It returns only when the broker
// acknowledged all of them (Kafka acks=all, JetStream ack, RabbitMQ confirms).
type Publisher interface {
	Publish(ctx context.Context, msgs []Message) error
}

// RelayOnce publishes up to limit unsent events, oldest first, and marks them
// sent. A failure rolls back: the same events are published again later.
func RelayOnce(ctx context.Context, pool *pgxpool.Pool, pub Publisher, limit int) (int, error) {
	n := 0
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT seq, id, name, key, payload FROM app.outbox
			WHERE sent_at IS NULL ORDER BY seq LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
		if err != nil {
			return err
		}
		msgs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Message])
		if err != nil || len(msgs) == 0 {
			return err
		}
		if err := pub.Publish(ctx, msgs); err != nil {
			return fmt.Errorf("publish: %w", err)
		}
		seqs := make([]int64, len(msgs))
		for i, m := range msgs {
			seqs[i] = m.Seq
		}
		if _, err := tx.Exec(ctx, `UPDATE app.outbox SET sent_at = now() WHERE seq = ANY($1)`, seqs); err != nil {
			return err
		}
		n = len(msgs)
		return nil
	})
	return n, err
}

// Relay runs RelayOnce until ctx is done: at once again after a full batch,
// after interval otherwise (or after an error).
func Relay(ctx context.Context, pool *pgxpool.Pool, pub Publisher, limit int, interval time.Duration) {
	for {
		n, err := RelayOnce(ctx, pool, pub, limit)
		if err == nil && n == limit {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// PurgeSent deletes sent events older than age, at most limit at a time.
func PurgeSent(ctx context.Context, pool *pgxpool.Pool, age time.Duration, limit int) (int64, error) {
	tag, err := pool.Exec(ctx, `DELETE FROM app.outbox WHERE seq IN (
		SELECT seq FROM app.outbox WHERE sent_at < now() - make_interval(secs => $1) ORDER BY seq LIMIT $2)`,
		age.Seconds(), limit)
	return tag.RowsAffected(), err
}

var _ saga.RunHook = Hook{}
