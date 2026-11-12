# 7. Event bus

Package `eventbus` phát domain event qua transactional outbox trong Postgres tới BullMQ (Redis). Tính chất quan trọng với saga: event phát bên trong một step được **giữ lại cho tới khi run done**. Run bị compensate không phát gì, nên các phần khác của hệ thống không bao giờ phản ứng với một đơn hàng đã bị hoàn tác.

## Thiết lập

```go
bus := eventbus.New(pool, eventbus.Options{
	Redis: bullmq.RedisConnectionOptions{URL: redisURL},
})
if err := eventbus.Migrate(ctx, pool); err != nil { ... }    // schema "eventbus"

// Subscriber: đăng ký cùng một tập trên mọi instance có chạy relay.
err := bus.Subscribe(eventbus.Subscriber{
	ID:     "billing.invoice",      // cố định: là một phần của job id trong BullMQ
	Event:  "order.created",
	Handle: billing.OnOrderCreated, // nil trên instance chỉ relay
})

eng, err := saga.New(pool, saga.Options{Hooks: []saga.RunHook{bus}}) // nhả event khi run kết thúc
if err := bus.Start(ctx); err != nil { ... }                          // relay, worker, retention
defer bus.Stop(shutdownCtx)
```

Redis phải chạy với `maxmemory-policy noeviction` và AOF (`docker-compose.yml` đã cấu hình).

## Phát event

Phát từ module, trong cùng transaction với hiệu ứng, bằng **context của step**:

```go
func (s *Service) Create(ctx context.Context, key string, o Order) (Order, error) {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		created, err := idem.Do(ctx, tx, requests, key, func(ctx context.Context) (Order, error) {
			o.ID = key
			if _, err := tx.Exec(ctx, `INSERT INTO orders.order ...`); err != nil {
				return Order{}, err
			}
			ev, err := eventbus.NewEvent("order.created", OrderCreated{ID: o.ID})
			if err != nil {
				return Order{}, err
			}
			return o, s.bus.EmitTx(ctx, tx, ev)
		})
		o = created
		return err
	})
	return o, err
}
```

- Với `ctx` của step (`saga.RunIDFrom(ctx)` tìm thấy run), event được **staging**. Nó chuyển sang outbox trong transaction đánh dấu run `done`, và bị bỏ nếu run bị compensate.
- Với context không thuộc step (HTTP handler, background job), event vào thẳng outbox và được giao khi `tx` commit.
- Không phát event từ thân workflow: thân workflow chạy lại ở mỗi lần resume và sẽ staging event lần nữa. Không dùng `context.Background()` bên trong step: làm vậy sẽ bỏ qua staging.
- Phát bên trong `idem.Do` làm event cũng chỉ phát một lần mỗi key.

## Xử lý

```go
func OnOrderCreated(ctx context.Context, ev eventbus.Event) error {
	var p OrderCreated
	if err := ev.Decode(&p); err != nil {
		return eventbus.Unrecoverable(err) // payload hỏng: không retry
	}
	return invoices.CreateOnce(ctx, ev.ID, p.ID) // khử trùng lặp theo ev.ID
}
```

Giao nhận là **ít nhất một lần** và **không theo thứ tự**:

- khử trùng lặp theo `Event.ID` (ví dụ `idem.Do` với key là nó);
- đọc lại state hiện tại thay vì tin vào thứ tự event;
- lỗi trả về sẽ retry theo backoff của subscriber (`Attempts` mặc định 5, `Backoff` mặc định 1 s gấp đôi); `eventbus.Unrecoverable(err)` làm job thất bại ngay. Job thất bại được giữ 7 ngày trong failed set của BullMQ (dead letter queue).

Mặc định mỗi subscriber có hàng đợi BullMQ riêng (dùng `Queue` để gộp; khi đó instance nào có handler cho một subscriber trong nhóm thì phải có handler cho tất cả).

## Event needs_attention

Khi một run bị park ở `needs_attention`, bus phát `eventbus.NeedsAttentionEvent` (`"saga.needs_attention"`) với payload `eventbus.NeedsAttention` (`RunID`, `Workflow`, `Version`, `Error`) trong cùng transaction. Subscribe để báo cho người trực:

```go
_ = bus.Subscribe(eventbus.Subscriber{
	ID:    "ops.alert",
	Event: eventbus.NeedsAttentionEvent,
	Handle: func(ctx context.Context, ev eventbus.Event) error {
		var na eventbus.NeedsAttention
		if err := ev.Decode(&na); err != nil {
			return eventbus.Unrecoverable(err)
		}
		return pager.Notify(ctx, ev.ID, fmt.Sprintf("saga %s (%s) needs attention: %s", na.RunID, na.Workflow, na.Error))
	},
})
```

## Vận hành

Xem [Operations: Event bus](../operations.md#event-bus) về độ bền của Redis, hành vi của relay, gỡ subscriber và retention.
