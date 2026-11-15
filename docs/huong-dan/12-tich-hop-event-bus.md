# 12. Tích hợp event bus

Chương 7 giải thích vì sao event phải được giữ lại trong step và được thả bởi hook khi run kết thúc. Chương này là phần tích hợp đầy đủ: cam kết của engine, các bảng, hook, relay sang broker của bạn (Kafka, NATS, RabbitMQ, Redis...), consumer, vận hành và kiểm thử. Toàn bộ code nằm ở [examples/outbox](../../examples/outbox), có test chạy trên Postgres trong CI; hãy sao chép vào ứng dụng của bạn rồi điều chỉnh. Code trong chương giữ nguyên như ví dụ (chú thích tiếng Anh) để hai nơi luôn khớp nhau.

## Engine cam kết những gì

Phần tích hợp dựa trên ba điểm mở rộng công khai: `saga.RunHook`, `saga.RunInfo` và `saga.RunIDFrom`.

**`RunHook.RunEnded(ctx, tx, run)`**

- Được gọi **bên trong transaction ghi trạng thái cuối** (`done`, `compensated`, `needs_attention`), dưới lease của run và khi row của run đang bị khoá. Mọi thứ nó ghi được commit nguyên tử cùng trạng thái, hoặc không gì cả.
- Được gọi một lần cho mỗi lần chuyển sang trạng thái cuối được commit. Nếu nó trả lỗi (hoặc instance chết trước khi commit), bước chuyển bị rollback cùng mọi thứ hook đã ghi, và run được tiếp tục sau, tới hook lần nữa: hook chạy lại luôn bắt đầu từ trạng thái sạch.
- Một run có thể tới hook **hai lần**: `needs_attention` trước, rồi `compensated` sau khi người vận hành gọi `Engine.RetryCompensation`. Run `needs_attention` không bao giờ thành `done`.
- Các hook chạy theo thứ tự trong `Options.Hooks`. `RunInfo` cho biết `ID`, `Workflow`, `Version`, `Status` của run và, với run compensated hoặc needs_attention, `Error` gây ra nó.

**`RunIDFrom(ctx)`: ở đâu nó trả về một run**

| Code chạy trong | `RunIDFrom` | Event ghi ở đó |
| --- | --- | --- |
| Hàm của step (và code module được gọi với `ctx` của nó) | id của run | được giữ lại: thả nếu run done, bỏ nếu run bị compensate |
| Compensator | **không có** | phát thẳng, dù run đang bị compensate: đúng cho "order cancelled", "payment refunded" |
| Các step của workflow con | id của **run con** | theo kết quả của run con, không theo run cha |
| Mọi thứ ngoài run (HTTP handler, nơi gửi signal, consumer) | không có | phát thẳng |

Không bao giờ phát event từ thân hàm workflow (ngoài step): nó chạy lại mỗi lần resume.

**Database.** Hook ghi bằng transaction của engine, nên bảng staging và outbox phải nằm **trong cùng database Postgres với schema saga**, và module cũng phải ghi event được giữ lại vào database đó (schema riêng của module trong database đó thì được). Module sở hữu một database riêng không thể giữ event nguyên tử cùng thay đổi nghiệp vụ: hãy cho nó outbox riêng và phát từ đó, nằm ngoài cam kết của saga.

## Vòng đời của một event

| Transaction module của step... | Run kết thúc `done` | Run kết thúc `compensated` | Run park ở `needs_attention` |
| --- | --- | --- | --- |
| commit thay đổi nghiệp vụ và event được giữ lại | event chuyển sang outbox | event bị xoá | event được giữ tới khi người vận hành quyết định (rồi bị `compensated` bỏ) |
| bị retry hoặc chạy lại (cùng key) | id tất định khiến lần insert thứ hai không làm gì | như trên | như trên |
| rollback | không có event | không có event | không có event |

## Schema

```sql
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
```

Đưa vào migration của bạn. Index trên `event_staging.run_id` quan trọng: hook lọc theo nó ở cuối mỗi run.

## Phát event

```go
type Event struct {
	ID      string // unique and deterministic, e.g. string(k) + ":order.placed"
	Name    string
	Key     string // optional broker key (ordering, partitioning)
	Payload any
}

// Emit writes ev in the module's transaction: staged when called from a saga
// step, straight to the outbox otherwise.
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
```

Module gọi nó trong cùng transaction với thay đổi nghiệp vụ, với `ctx` mà step truyền cho nó:

```go
func (m *Orders) Place(ctx context.Context, k saga.Key, o Order) error {
	return pgx.BeginFunc(ctx, m.pool, func(tx pgx.Tx) error {
		if err := insertOrder(ctx, tx, o); err != nil {
			return err
		}
		return outbox.Emit(ctx, tx, outbox.Event{
			ID: string(k) + ":order.placed", Name: "order.placed", Key: o.ID,
			Payload: OrderPlaced{OrderID: o.ID},
		})
	})
}
```

Id của event:

- **Từ step:** suy ra từ key của step (`string(k) + ":" + name`, thêm chỉ số nếu step phát nhiều event cùng tên). Key giống nhau ở mọi lần chạy lại, nên step chạy hai lần chỉ giữ event một lần.
- **Từ compensator:** compensator nhận cùng key đó; dùng hậu tố khác (`string(k) + ":order.cancelled"`).
- **Ngoài run:** dùng idempotency key của bạn (của request), hoặc id ngẫu nhiên nếu bên gọi không retry.

Giữ payload nhỏ (các id và vài trường consumer cần), giống output của step.

## Hook

```go
type Hook struct{}

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

eng, err := saga.New(pool, saga.Options{Hooks: []saga.RunHook{outbox.Hook{}}})
```

- `ORDER BY seq` giữ các event của run theo đúng thứ tự các step đã phát.
- Id ngẫu nhiên của cảnh báo là an toàn: nếu bước chuyển bị rollback, dòng cảnh báo bị rollback theo.
- **Chi phí:** không có hook, kết thúc một run là một câu lệnh có fencing (một round trip). Có bất kỳ hook nào, nó thành một transaction (begin, kiểm tra lease, nhả lock, ghi trạng thái, commit) cộng các câu lệnh của hook: thêm vài round trip ở cuối **mọi** run, trong lúc giữ row lock của run. Giữ hook ở mức các câu lệnh có index, không gọi mạng, không truy vấn chậm.
- Một hook lỗi mãi (ví dụ bảng của nó chưa được migrate) chặn mọi run ở bước cuối: chúng được tiếp tục và thử lại mãi, không bao giờ bị park. Hãy migrate trước khi deploy hook, và theo dõi các lỗi bọc `saga: run hook`.

## Relay

Relay đọc outbox theo thứ tự, phát sang broker, và đánh dấu đã gửi, trong một transaction:

```go
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
```

Chạy nó trong một vòng lặp trên mọi instance (`outbox.Relay`: chạy lại ngay sau một lô đầy, sau một khoảng nghỉ trong các trường hợp khác). Tính chất:

- **Ít nhất một lần.** Crash hoặc lỗi sau `Publish` mà trước commit sẽ phát lại cả lô. Consumer khử trùng lặp theo id của event (bên dưới), một số broker cũng làm được.
- **Nhiều instance.** `FOR UPDATE SKIP LOCKED` giao cho mỗi relay một lô khác nhau: bình thường không có gì bị phát hai lần, và thông lượng tăng theo số instance.
- **Thứ tự.** Một relay phát theo thứ tự `seq`. Nhiều relay phát các lô song song, nên hai event cùng key có thể vượt nhau. Nếu consumer cần thứ tự theo key, hãy chạy một relay duy nhất (ví dụ: instance giữ `pg_try_advisory_lock` trên một connection riêng), hoặc chia relay theo partition của key (`WHERE (hashtext(key) & 2147483647) % N = i`; các dòng không có key giao cho một relay, ví dụ `key IS NULL AND i = 0`).
- **Giữ lock trong lúc phát.** Lô bị khoá suốt lúc gọi broker: giới hạn bằng timeout trên `ctx`, và giữ lô vừa phải (100 tới 500). Broker chậm làm relay chậm; nó không làm mất event.
- **Event độc** (broker từ chối mọi lần) chặn lô của nó mãi mãi. Hãy phát hiện (tuổi của dòng chưa gửi lâu nhất) và tách nó ra bằng tay, hoặc thêm bộ đếm số lần thử và bảng dead-letter nếu broker của bạn có thể gặp chuyện này.
- **Độ trễ.** Polling thêm tối đa một khoảng nghỉ (ví dụ 200 ms tới 1 s). Muốn trễ thấp hơn, `NOTIFY` trong hook và `LISTEN` trong relay, giữ polling làm dự phòng.

## Lưu ý theo broker

`Publisher` là phần code duy nhất phụ thuộc broker. Mỗi broker cần gì để giữ các tính chất trên:

| Broker | Phát được xác nhận khi | Key | Xử lý trùng lặp |
| --- | --- | --- | --- |
| Kafka | `acks=all` (idempotent producer), sau khi flush cả lô | message key = `Key` (thứ tự theo partition) | consumer khử trùng lặp theo id của event (đặt trong header) |
| NATS JetStream | mỗi lần publish được stream xác nhận | subject theo tên event, `Key` trong subject hoặc header | `Nats-Msg-Id` = id của event: server bỏ trùng lặp trong cửa sổ của nó |
| RabbitMQ | publisher confirms | routing key | `message_id` = id của event; consumer khử trùng lặp |
| Redis Streams / list | lệnh trả về (với AOF `appendfsync always` để sống sót khi Redis crash) | stream theo key hoặc theo tên | consumer khử trùng lặp |

Đổi broker về sau chỉ thay `Publisher`; staging, hook, outbox và relay giữ nguyên.

## Consumer

Việc giao là ít nhất một lần và, giữa các key, không có thứ tự. Một consumer:

- khử trùng lặp theo id của event, trong cùng transaction với hiệu ứng của nó, ví dụ bằng `idem` (chương 6):

```go
err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
	_, err := idem.Do(ctx, tx, processed, eventID, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, invoices.Create(ctx, tx, orderID)
	})
	return err
})
```

- đọc lại trạng thái hiện tại thay vì tin vào thứ tự event;
- có thể tự khởi chạy một saga: dùng id của event (hoặc key suy ra từ nó) làm idempotency key của run, khi đó lần giao trùng sẽ nhập vào cùng run.

## Vận hành

- **Retention.** Xoá định kỳ các dòng đã gửi, theo lô (`outbox.PurgeSent(ctx, pool, 7*24*time.Hour, 5000)`). Dòng staging do hook xoá; chỉ run `needs_attention` còn giữ của nó, tới khi bị compensate.
- **Giám sát.** Cảnh báo theo tuổi của dòng chưa gửi lâu nhất (relay hoặc broker hỏng) và theo dòng staging của các run đã kết thúc (lỗi ở hook):

```sql
SELECT now() - min(created_at) FROM app.outbox WHERE sent_at IS NULL;

SELECT count(*) FROM app.event_staging s JOIN saga.run r ON r.id = s.run_id
WHERE r.status IN ('done', 'compensated');
```

- **Vacuum.** Cả hai bảng thay đổi liên tục (insert, update hoặc delete cho mỗi event): hạ `autovacuum_vacuum_scale_factor` của chúng (ví dụ 0.01), giống `saga.step` (chương 9).
- **Tải Postgres.** Mỗi event: một insert staging trong transaction của module, một lần chuyển trong hook, sau đó một update trong relay và một delete khi purge.

## Kiểm thử phần tích hợp

Các test của [examples/outbox](../../examples/outbox/outbox_test.go) là những test đáng giữ trong ứng dụng của bạn:

- run kết thúc `done` đưa event của nó vào outbox, và bảng staging trống;
- run kết thúc `compensated` không phát gì từ các step, nhưng phát những gì compensator của nó phát;
- step chạy lại chỉ giữ event một lần;
- `needs_attention` giữ lại event và phát cảnh báo; sau đó `RetryCompensation` bỏ chúng;
- `Publisher` lỗi để event ở trạng thái chưa gửi; hai relay chạy cùng lúc phát mỗi event đúng một lần.

## Checklist

- [ ] Staging và outbox nằm trong database của saga; có index trên `event_staging.run_id`.
- [ ] Event được ghi trong transaction của module, với `ctx` của step, id tất định.
- [ ] Không phát gì từ thân workflow; compensator chủ động phát event của riêng nó.
- [ ] Hook được đăng ký trên engine của mọi instance; bảng được migrate trước khi deploy hook.
- [ ] Relay trên mọi instance (hoặc chỉ một nếu cần thứ tự theo key), `Publisher` chờ broker xác nhận.
- [ ] Consumer khử trùng lặp theo id của event.
- [ ] Retention cho các dòng đã gửi; cảnh báo theo tuổi outbox và dòng staging còn sót.
