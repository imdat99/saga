# 7. Event bus

Package `eventbus` phát domain event qua transactional outbox trong Postgres tới BullMQ (Redis), và giao mỗi event ít nhất một lần tới **mọi service xử lý nó**. Tính chất quan trọng với saga: event phát bên trong một step được **giữ lại cho tới khi run done**. Run bị compensate không phát gì, nên các service khác không bao giờ phản ứng với một đơn hàng đã bị hoàn tác.

## Hai vai trò

| Vai trò | Tạo bằng | Cần | Làm gì |
| --- | --- | --- | --- |
| **Producer** | `eventbus.NewProducer(pool, opts)` | Postgres (outbox) và Redis | `EmitTx` trong transaction của module, `RunEnded` (hook của saga), relay outbox → các queue, retention |
| **Consumer** | `eventbus.NewConsumer(group, opts)` | **Chỉ Redis** | Chạy các handler của một **consumer group** |

Một **consumer group** thường là một service (một codebase): `billing`, `mail`, `analytics`. Mọi instance của một group dùng chung một queue BullMQ đặt theo tên group, và mỗi job của một event chạy mọi handler mà group có cho event đó. Các group khác nhau mỗi group nhận một bản riêng của mọi event chúng xử lý.

```
            Postgres                            Redis
 tx module ─► outbox ─► relay (producer bất kỳ) ─┬─► queue "shop"    ─► các instance shop    (handler audit, mail)
                                                 ├─► queue "billing" ─► các instance billing (handler invoice)
                                                 └─► (event không group nào nghe: bỏ)
```

Bảng định tuyến nằm trong Redis: consumer đăng ký các event của group khi `Start`, và mọi relay đọc bảng đó. Một service chỉ phản ứng với event **không cần truy cập** database của producer, và thêm một service consumer không bao giờ đòi deploy lại producer.

## Thiết lập ở service producer

```go
events := eventbus.NewProducer(pool, eventbus.ProducerOptions{
	Redis: bullmq.RedisConnectionOptions{URL: redisURL},
})
if err := eventbus.Migrate(ctx, pool); err != nil { ... }               // schema "eventbus"
eng, err := saga.New(pool, saga.Options{Hooks: []saga.RunHook{events}}) // nhả các event đang staging
if err := events.Start(ctx); err != nil { ... }                         // relay + retention
defer events.Stop(shutdownCtx)
```

Instance nào cũng chạy relay được: mỗi lượt dùng `SKIP LOCKED`, nên các instance chia nhau outbox.

## Thiết lập ở service consumer

```go
billing, err := eventbus.NewConsumer("billing", eventbus.ConsumerOptions{
	Redis:       bullmq.RedisConnectionOptions{URL: redisURL},
	Concurrency: 20,              // số job chạy song song mỗi instance (mặc định 10)
	Attempts:    8,               // số lần giao trước khi job thất bại (mặc định 5)
	Backoff:     2 * time.Second, // độ trễ retry đầu tiên, gấp đôi dần (mặc định 1s)
})
_ = billing.Subscribe(eventbus.Subscriber{ID: "invoice", Event: "order.created", Handle: invoices.OnOrderCreated})
_ = billing.Subscribe(eventbus.Subscriber{ID: "ledger", Event: "order.created", Handle: ledger.OnOrderCreated})
if err := billing.Start(ctx); err != nil { ... } // đăng ký event của group, chạy worker
defer billing.Stop(shutdownCtx)
```

Service vừa phát vừa xử lý event thì tạo cả hai (example modulith làm vậy). Producer và consumer của cùng một bus phải dùng chung Redis và `Prefix`.

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
			return o, s.events.EmitTx(ctx, tx, ev)
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
- lỗi trả về làm lần giao tới group được retry theo backoff của group, **chỉ chạy lại các handler đã lỗi**: các handler đã thành công được ghi lại trong job;
- `eventbus.Unrecoverable(err)` bỏ cuộc ngay với handler đó cho event đó; các handler khác vẫn chạy và retry bình thường, rồi job kết thúc trong failed set của BullMQ (dead letter queue, giữ 7 ngày).

## Deploy consumer

- **Mọi instance của một group chạy cùng các handler.** Trong rolling deploy, instance cũ nhận phải job của một event mà chỉ code mới xử lý sẽ trả nó về queue (không tốn attempt) cho instance có handler.
- **Định tuyến là phép hợp.** Gỡ một handler khỏi code không gỡ định tuyến của event đó (instance cũ có thể vẫn xử lý). Khi không còn instance nào xử lý, gỡ định tuyến: `eventbus.Forget(ctx, eventbus.RegistryOptions{Redis: ...}, "billing", "order.created")`.
- **Ngừng hẳn một service:** `eventbus.Unregister(ctx, eventbus.RegistryOptions{Redis: ...}, "billing")`, rồi xử lý hết hoặc xoá queue của nó. Chỉ tắt các instance là chưa đủ: queue vẫn tiếp tục nhận job chừng nào group còn được đăng ký.
- **Group mới** nhận các event được relay sau lần `Start` đầu tiên của nó, không nhận các event trước đó.
- Group đang tắt vẫn tiếp tục nhận job vào queue (đăng ký là bền vững) và xử lý khi chạy lại: theo dõi kích thước queue trong Redis.

## Event needs_attention

Khi một run bị park ở `needs_attention`, producer phát `eventbus.NeedsAttentionEvent` (`"saga.needs_attention"`) với payload `eventbus.NeedsAttention` (`RunID`, `Workflow`, `Version`, `Error`) trong cùng transaction. Xử lý nó trong một group vận hành để báo người trực:

```go
ops, _ := eventbus.NewConsumer("ops", eventbus.ConsumerOptions{Redis: redisOpts})
_ = ops.Subscribe(eventbus.Subscriber{
	ID:    "page",
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

Xem [Operations: Event bus](../operations.md#event-bus) về độ bền của Redis, hành vi của relay và retention.
