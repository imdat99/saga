package app_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/saga/eventbus"
	"github.com/imdat99/saga/examples/modulith/internal/app"
	"github.com/imdat99/saga/examples/modulith/internal/flows"
	"github.com/imdat99/saga/internal/testdb"
	"github.com/imdat99/saga/internal/testredis"
)

// cluster is two instances of the modulith on one database and Redis.
type cluster struct {
	apps    []*app.App
	urls    []string
	created *events
}

type events struct {
	mu   sync.Mutex
	seen map[string]int // order id -> deliveries
	n    atomic.Int32
}

func (e *events) count(orderID string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.seen[orderID]
}

var prefixes atomic.Int64

func newCluster(t *testing.T) *cluster {
	t.Helper()
	dsn := testdb.DSN(testdb.New(t))
	c := &cluster{created: &events{seen: map[string]int{}}}
	prefix := fmt.Sprintf("e2e%d_%d", time.Now().UnixNano(), prefixes.Add(1))
	for i := range 2 {
		a, err := app.New(context.Background(), app.Config{
			DatabaseURL: dsn,
			RedisURL:    testredis.URL(t),
			RedisPrefix: prefix,
			InstanceID:  fmt.Sprintf("instance-%d", i),
			Logger:      slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelError})),
			OnOrderCreated: func(ctx context.Context, ev eventbus.Event) error {
				var p struct {
					OrderID string `json:"order_id"`
				}
				if err := ev.Decode(&p); err != nil {
					return err
				}
				c.created.mu.Lock()
				c.created.seen[p.OrderID]++
				c.created.mu.Unlock()
				c.created.n.Add(1)
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(a.Handler())
		t.Cleanup(func() {
			srv.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_ = a.Stop(ctx)
		})
		c.apps = append(c.apps, a)
		c.urls = append(c.urls, srv.URL)
	}
	if err := c.apps[0].Inventory.Restock(context.Background(), "sku-1", 10); err != nil {
		t.Fatal(err)
	}
	return c
}

type response struct {
	Code int
	Body map[string]any
}

func (c *cluster) do(t *testing.T, instance int, method, path, key string, body any) response {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequestWithContext(context.Background(), method, c.urls[instance]+path, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	out := response{Code: res.StatusCode, Body: map[string]any{}}
	_ = json.NewDecoder(res.Body).Decode(&out.Body)
	return out
}

func (c *cluster) stock(t *testing.T) int {
	t.Helper()
	n, err := c.apps[0].Inventory.Available(context.Background(), "sku-1")
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func checkout(cart string, amount int64) flows.CompleteOrderInput {
	return flows.CompleteOrderInput{CartID: cart, SKU: "sku-1", Qty: 2, Amount: amount}
}

// The steps and payment module are shared by every cluster in this process
// (step names are global), so these subtests run one after another.
func TestCompleteOrder(t *testing.T) {
	c := newCluster(t)

	t.Run("happy path, idempotent retry", func(t *testing.T) {
		first := c.do(t, 0, http.MethodPost, "/orders", "req-1", checkout("cart-1", 500))
		if first.Code != http.StatusCreated {
			t.Fatalf("status = %d %v", first.Code, first.Body)
		}
		again := c.do(t, 1, http.MethodPost, "/orders", "req-1", checkout("cart-1", 500))
		if again.Code != http.StatusCreated || again.Body["order_id"] != first.Body["order_id"] {
			t.Fatalf("retry = %d %v, want the same order", again.Code, again.Body)
		}
		if n := c.stock(t); n != 8 {
			t.Fatalf("stock = %d, want 8", n)
		}
		orderID, _ := first.Body["order_id"].(string)
		waitFor(t, "order.created delivery", func() bool { return c.created.count(orderID) >= 1 })
		time.Sleep(300 * time.Millisecond)
		if n := c.created.count(orderID); n != 1 {
			t.Fatalf("order.created delivered %d times", n)
		}
	})

	t.Run("declined payment compensates", func(t *testing.T) {
		before := c.created.n.Load()
		res := c.do(t, 0, http.MethodPost, "/orders", "req-2", checkout("cart-2", 10_000_000))
		if res.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d %v", res.Code, res.Body)
		}
		if n := c.stock(t); n != 8 {
			t.Fatalf("stock = %d, want 8 (reservation released)", n)
		}
		time.Sleep(500 * time.Millisecond)
		if c.created.n.Load() != before {
			t.Fatal("a compensated checkout emitted order.created")
		}
	})

	t.Run("transient payment failures retry in the background", func(t *testing.T) {
		c.apps[1].Payment.FailNext(2) // the flows call the last App's modules
		res := c.do(t, 1, http.MethodPost, "/orders", "req-3", checkout("cart-3", 700))
		if res.Code != http.StatusAccepted {
			t.Fatalf("status = %d %v, want 202", res.Code, res.Body)
		}
		runID, _ := res.Body["run_id"].(string)
		waitFor(t, "run done", func() bool {
			st := c.do(t, 0, http.MethodGet, "/runs/"+runID, "", nil)
			return st.Body["status"] == "done"
		})
		if n := c.stock(t); n != 6 {
			t.Fatalf("stock = %d, want 6", n)
		}
	})

	t.Run("out of stock", func(t *testing.T) {
		res := c.do(t, 0, http.MethodPost, "/orders", "req-4",
			flows.CompleteOrderInput{CartID: "cart-4", SKU: "sku-1", Qty: 100, Amount: 1})
		if res.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d %v", res.Code, res.Body)
		}
	})

	t.Run("missing idempotency key", func(t *testing.T) {
		if res := c.do(t, 0, http.MethodPost, "/orders", "", checkout("cart-5", 1)); res.Code != http.StatusBadRequest {
			t.Fatalf("status = %d", res.Code)
		}
	})
}
