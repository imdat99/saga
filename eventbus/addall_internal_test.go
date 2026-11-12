package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/imdat99/bullmq"

	"github.com/imdat99/saga/internal/testredis"
)

// When a group's queue cannot be created, addAll returns before any push has
// started: no goroutine may still read the pass's arena and options after it
// returns (they are reused by the next pass).
func TestAddAllStartsNoPushOnQueueError(t *testing.T) {
	ctx := context.Background()
	redis := bullmq.RedisConnectionOptions{URL: testredis.URL(t)}
	p := NewProducer(nil, ProducerOptions{Redis: redis, Prefix: fmt.Sprintf("aa%d", time.Now().UnixNano())})
	for range 20 { // map iteration order varies: try both orders
		st, err := p.prepare(ctx)
		if err != nil {
			t.Fatal(err)
		}
		good, err := st.queue(ctx, "good", p.opts.Prefix)
		if err != nil {
			t.Fatal(err)
		}
		data := any(json.RawMessage(`{}`))
		st.batches["good"] = []bullmq.BulkJob{{Name: "e", Data: data, Opts: &bullmq.JobOptions{JobID: fmt.Sprint("j", time.Now().UnixNano())}}}
		st.batches["bad:name"] = []bullmq.BulkJob{{Name: "e", Data: data}} // ':' : BullMQ rejects the queue
		if err := st.addAll(ctx, p.opts.Prefix); err == nil {
			t.Fatal("addAll succeeded with a queue that cannot be created")
		}
		counts, err := good.GetJobCounts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if n := counts.Waiting + counts.Prioritized + counts.Delayed; n != 0 {
			t.Fatalf("%d jobs pushed by a push started before the queue error", n)
		}
		st.close()
	}
}
