// Package consume is the consumer side of the event bus: a consumer group
// (usually one service) handling the events producers emit. It needs Redis
// only and depends on BullMQ only: a service that just reacts to events
// links neither the saga engine nor a Postgres driver. Producers live in
// package eventbus, which re-exports these types.
package consume

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/imdat99/bullmq"
)

// Event is a domain event.
type Event struct {
	// ID is assigned when the event is emitted (UUIDv7). Handlers
	// deduplicate on it.
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

// NeedsAttentionEvent is emitted by producers when a saga run is parked as
// needs_attention. Its payload is a NeedsAttention.
const NeedsAttentionEvent = "saga.needs_attention"

// NeedsAttention is the payload of NeedsAttentionEvent.
type NeedsAttention struct {
	RunID    string `json:"run_id"`
	Workflow string `json:"workflow"`
	Version  int    `json:"version"`
	Error    string `json:"error"`
}

// Handler processes one delivery of an event. A returned error makes the
// delivery retried with the group's backoff (handlers of the same event that
// succeeded are not run again); wrap it with Unrecoverable to give up on this
// handler for this event at once: the other handlers still run, and the job
// then ends in the failed set (the dead letter queue). A panic is an error.
type Handler func(ctx context.Context, ev Event) error

// Unrecoverable marks a handler error as not worth retrying. The error is
// kept as text (errors.Is on it does not see through).
func Unrecoverable(err error) error {
	if err == nil {
		return nil
	}
	return bullmq.NewUnrecoverableError(err.Error())
}

// Subscriber is one handler of a consumer group for one event name.
type Subscriber struct {
	// ID names the subscriber within its group; it is recorded in a job when
	// the handler is done with it, so that a retry of the job skips it.
	// Stable across deploys. Letters, digits, '.', '_', '-'.
	ID string
	// Event is the event name subscribed to.
	Event string
	// Handle processes deliveries.
	Handle Handler
}
