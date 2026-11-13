package consume_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/imdat99/bullmq"

	"github.com/imdat99/saga/eventbus/consume"
)

func noop(context.Context, consume.Event) error { return nil }

// Processing one job by a group with one handler (decode and dispatch).
func BenchmarkConsume(b *testing.B) {
	c, err := consume.New("billing", consume.Options{})
	if err != nil {
		b.Fatal(err)
	}
	if err := c.Subscribe(consume.Subscriber{ID: "invoice", Event: "order.created", Handle: noop}); err != nil {
		b.Fatal(err)
	}
	process := c.BenchProcess()
	data, _ := json.Marshal(consume.Event{
		ID: "0196a0b2-0000-7000-8000-000000000001", Name: "order.created",
		Payload: json.RawMessage(`{"order_id":"o-123","amount":4200}`),
	})
	job := &bullmq.Job{Name: "order.created", Data: data}
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if err := process(ctx, job); err != nil {
			b.Fatal(err)
		}
	}
}
