# Kế hoạch: Saga Orchestration cho Go (modulith, nhiều instance)

> Tài liệu gốc của dự án. Đọc cùng [`TASKS.md`](../TASKS.md) (tiến độ) và [`AGENTS.md`](../AGENTS.md) (quy ước làm việc).
> Khi quyết định thay đổi, sửa file này và ghi lý do vào mục "Nhật ký quyết định" ở cuối.

## Context

Người dùng xây một modulith Go theo mô hình Medusa: nhiều module cô lập (không chia sẻ transaction), app chạy nhiều instance. Khi phân tích Medusa (`packages/core/orchestration`, `packages/modules/workflow-engine-redis`, `packages/modules/event-bus-redis`), ta thấy các điểm yếu không nên mang sang:
- Chỉ 1/342 workflow trong `core-flows` lưu checkpoint (`store: true`). Crash giữa chừng thì không khôi phục được, và compensation không chạy.
- Compensation lỗi là trạng thái cuối (`DEFAULT_RETRIES = 0`), không có lối xử lý tiếp.
- Step không idempotent khi bị chạy lại.
- Lock checkpoint có TTL 2 giây, nhả lock không kiểm tra chủ sở hữu, không có fencing.
- Ghi Redis và Postgres song song, không nguyên tử.
- Event: ghi hai nơi (DB và Redis) không có outbox. Staging có TTL 600 giây. Release chỉ log khi lỗi. Subscriber lỗi bị nuốt.

Mục tiêu: một thư viện saga Go với Postgres là nguồn sự thật, khôi phục được sau crash khi chạy nhiều instance, và event bus dùng `github.com/imdat99/bullmq` (bản port BullMQ của người dùng) được cấp dữ liệu qua outbox transactional.

## Quyết định đã chốt

- **Repo Go mới riêng** tại `/home/dat/projects/saga`, module path `github.com/imdat99/saga`.
- **Stack:** `pgx/v5` + `goose` + River (cho timer, retry và resume của saga) + `bullmq` Go (cho giao event tới subscriber). Test bằng `testcontainers-go`, gồm Postgres 16 và Redis 7.
- **Phạm vi MVP:** lõi saga và recovery; step async, retry và timer; event bus kèm staging; modulith mẫu với 3 module.
- **Mô hình thực thi:** step được khai báo trước (registry) và chạy bên trong một hàm workflow Go thường, theo kiểu replay. Khi resume, hàm workflow chạy lại từ đầu, các step đã xong trả về output đã checkpoint. Compensation được tra theo **tên step** trong registry, nên recovery không cần chạy lại hàm workflow.
- **Phân vai hai hệ queue:** River dùng cho nội bộ saga, vì cần enqueue **cùng transaction** với checkpoint. Bullmq chỉ dùng để giao event, và được cấp từ bảng outbox trong Postgres, nên không có ghi hai nơi.

## Cấu trúc repo

```
saga/                         # thư viện (public API)
  saga.go                     # Engine, Options, Run/Signal/Cancel
  step.go                     # DefineStep[I,O,C], Step.Run(c, in)
  ctx.go                      # saga.Ctx: RunID, StepKey, Sleep, Await, Lock
  policy.go                   # RetryPolicy, CompensatePolicy, Timeout
  errors.go                   # ErrPermanent, ErrNeedsAttention, ...
  internal/store/             # truy vấn pgx: runs, steps, leases, locks
  internal/exec/              # vòng thực thi, stack compensation, replay
  internal/recovery/          # River periodic job quét lease hết hạn, resume worker
  internal/jobs/              # River args: resume_run, step_timeout, wake_timer
  migrations/                 # goose SQL (embed)
eventbus/                     # Bus interface + staging/outbox + relay -> bullmq
  bus.go  relay.go  subscriber.go  migrations/
idem/                         # helper idempotency cho module (processed_requests)
examples/modulith/            # app mẫu
  cmd/server/  internal/modules/{inventory,payment,order}/  internal/flows/  internal/api/
test/chaos/                   # test crash, nhiều instance (build tag chaos)
docker-compose.yml  Makefile  .golangci.yml (depguard)  README.md  docs/
```

## Mô hình dữ liệu (schema `saga`, goose)

- `saga.run(id, workflow, version, idem_key UNIQUE, status, input jsonb, output jsonb, error jsonb, owner_id, lease_until, lease_version bigint, wake_at, created_at, updated_at)`
  - Các giá trị `status`: `running | waiting | compensating | done | compensated | needs_attention`
- `saga.step(run_id, seq, name, status, attempt, output jsonb, comp_data jsonb, error text, started_at, finished_at, PK(run_id, seq))`. Trạng thái: `started | done | failed | compensated | compensation_failed` (compensate ghi lên chính dòng của step, không có cột `kind`). Mỗi step một dòng, sau đó chỉ đổi trạng thái; **không ghi lại toàn bộ flow** (tránh lỗi O(N²) của Medusa).
- `saga.lock(key PK, run_id, until)`: lock nghiệp vụ kéo dài qua nhiều step.
- `saga.signal(run_id, name, payload, PK(run_id, name))`: dữ liệu cho step `Await`.
- `eventbus.staging(run_id, seq, event jsonb)` và `eventbus.outbox(id, event jsonb, created_at, sent_at)`.
- Module (mẫu): mỗi module có schema riêng và DB role riêng, cùng bảng `processed_requests(key PK, result jsonb, created_at)`.

## Thuật toán chính

1. **Run(ctx, workflow, idemKey, input):** `INSERT ... ON CONFLICT (idem_key) DO NOTHING RETURNING`. Nếu run đã tồn tại thì trả handle hoặc output cũ. Instance gọi `Run` nhận lease (`owner_id`, `lease_until = now()+30s`, `lease_version++`) rồi **chạy luôn trong process**, để không tốn độ trễ queue. Một goroutine heartbeat gia hạn lease cho tới khi xong hoặc tạm dừng.
2. **Step.Run(c, in):**
   - Nếu `step(run_id, seq)` đã ở trạng thái `done` thì trả output đã lưu (replay).
   - Nếu chưa, ghi `started`, gọi `invoke(ctx, in)` với `saga.StepKey(c) = runID:seq:name`, rồi ghi `done` kèm `output` và `comp_data`.
   - Mọi lần ghi đều kèm `WHERE run.lease_version = $v` (fencing). Ghi thất bại nghĩa là instance đã mất quyền, phải dừng ngay mà không compensate.
3. **Lỗi step:**
   - Lỗi tạm thời và còn lượt theo `RetryPolicy`: ghi `attempt`, đặt `status=waiting`, `wake_at = backoff`, rồi `InsertTx` River job `resume_run{run_id}` với `ScheduledAt` **trong cùng transaction**. Sau đó nhả lease.
   - Lỗi vĩnh viễn hoặc hết lượt: chuyển `compensating`.
4. **Compensate:** duyệt các step `done` theo thứ tự `seq` giảm dần. Gọi compensator trong registry với `comp_data`, dùng key `runID:seq:name:compensate`. Compensation có `CompensatePolicy` riêng (mặc định 10 lượt, backoff mũ). Hết lượt thì chuyển `needs_attention`, phát event `saga.needs_attention`, và cung cấp API `Engine.RetryCompensation(runID)`.
5. **Async:**
   - `saga.Sleep(c, d)` và `saga.Await(c, name, timeout)`: ghi `waiting` và lên lịch River job (`wake_timer` hoặc `step_timeout`) trong cùng transaction, rồi nhả lease.
   - `Engine.Signal(runID, name, payload)`: ghi `saga.signal` và `InsertTx` `resume_run`. River `UniqueOpts` theo `run_id` để chống trùng.
6. **Recovery (nhiều instance):** River periodic job (leader-only) chạy mỗi 10 giây, `SELECT id FROM saga.run WHERE status IN ('running','compensating') AND lease_until < now() FOR UPDATE SKIP LOCKED LIMIT n`, rồi enqueue `resume_run`. Worker `resume_run` nhận lease với fencing, sau đó:
   - `running`: replay hàm workflow (các step đã xong trả từ cache). Riêng step đang `started` mà không có `done` thì chạy lại với **cùng `StepKey`**, nên module tự chống trùng.
   - `compensating`: chạy tiếp compensation.
   - Mọi mốc thời gian dùng `now()` của Postgres.
7. **Lock nghiệp vụ:** `saga.Lock(c, key, ttl)` là một step: `INSERT ... ON CONFLICT DO UPDATE WHERE lock.until < now() OR lock.run_id = $run`. Lock được nhả khi run `done` hoặc `compensated`; TTL lo trường hợp run chết.
8. **Event bus:**
   - `bus.EmitTx(ctx, tx, ev)` chạy trong transaction của module. Nếu `ctx` thuộc một saga run thì ghi vào `eventbus.staging`, nếu không thì ghi vào `eventbus.outbox`.
   - Khi run `done`: trong **cùng transaction** với việc đặt `status=done`, chuyển staging sang outbox. Khi run `compensated`: xóa staging.
   - **Relay:** một River worker hoặc vòng lặp leader đọc outbox bằng `SKIP LOCKED` theo lô. Với mỗi cặp event và subscriber, gọi `bullmq.Queue.AddBulk` với `JobID = eventID:subscriberID` (chống trùng khi gửi lại), `Attempts` và `Backoff` theo subscriber, `RemoveOnComplete` theo thời gian (lâu hơn cửa sổ retry của relay), và `RemoveOnFail` 7 ngày (đóng vai trò DLQ). Sau đó đặt `sent_at`.
   - **Worker bullmq:** mỗi queue phục vụ một nhóm subscriber, dispatch theo `job.Name = subscriberID`. Subscriber không còn tồn tại thì trả lỗi thường để retry (an toàn khi rolling deploy). Payload lỗi thì trả `NewUnrecoverableError`.
9. **Version:** `run.version` lưu version của workflow. Registry giữ cả hàm của version cũ cho tới khi các run cũ chạy xong.

## Public API (phác thảo)

```go
var ReserveStock = saga.DefineStep("inventory.reserve",
    func(ctx context.Context, k saga.Key, in ReserveIn) (ReserveOut, CompData, error),
    func(ctx context.Context, k saga.Key, c CompData) error,
    saga.WithRetry(saga.Exponential(5, time.Second)))

var CompleteOrder = saga.DefineWorkflow("order.complete", 1,
    func(c saga.Ctx, in CompleteIn) (OrderOut, error) { ... ReserveStock.Run(c, x) ... })

eng := saga.New(pool, riverClient, saga.Options{InstanceID, LeaseTTL, Bus})
h, err := eng.Run(ctx, CompleteOrder, idemKey, in)   // h.Wait(ctx) / h.Status()
eng.Signal(ctx, runID, "payment.captured", payload)
```

## Các mốc

| Mốc | Nội dung | Tiêu chí xong |
| --- | --- | --- |
| M0 | Khung repo, docker-compose (pg16, redis7), Makefile, golangci (depguard), helper testcontainers, goose embed | `make test` chạy được |
| M1 | Lõi đồng bộ: registry, Run và idem_key, checkpoint từng step, replay, compensation LIFO | Unit và integration xanh với `-race` |
| M2 | Lease, heartbeat, fencing, River recovery và `resume_run` | Test chaos A, B, C bên dưới đều xanh |
| M3 | RetryPolicy và backoff, Sleep, Await và Signal, timeout, CompensatePolicy, `needs_attention`, `RetryCompensation` | Test D, E xanh |
| M4 | Lock nghiệp vụ, package `idem` cho module | Test F xanh |
| M5 | Event bus: staging, outbox, relay sang bullmq, worker dispatch, DLQ | Test G, H xanh |
| M6 | Modulith mẫu (inventory, payment giả có idempotency, order), flow `complete-order`, API HTTP, benchmark, hook metrics bằng slog | Kịch bản e2e và báo cáo benchmark |

## Kiểm thử

Các lệnh: `docker compose up -d`, `go test ./... -race`, `go test -tags chaos ./test/chaos/...`, `go run ./examples/modulith/cmd/server`.

Kịch bản bắt buộc:
- **A.** Kill instance (hủy ctx và chặn heartbeat) giữa step 2 trên 4. Instance B khôi phục; step 2 chạy lại cùng `StepKey`, nên inventory chỉ giữ hàng **một lần**.
- **B.** Instance A treo (giả lập GC pause) quá lease. B nhận run. A tỉnh lại và ghi checkpoint thất bại do fencing; không có ghi trùng.
- **C.** Hai instance cùng gọi `Run` với một `idem_key`: chỉ có một run và một lần thực thi.
- **D.** Payment lỗi tạm thời 3 lần rồi thành công: retry có backoff qua River, không giữ lease trong lúc chờ.
- **E.** Compensation lỗi hết lượt: chuyển `needs_attention`, có event, và `RetryCompensation` hoàn tất được.
- **F.** Hai saga cùng `Lock("cart:1")`: saga thứ hai chờ hoặc thất bại đúng chính sách; TTL nhả lock khi run chết.
- **G.** Saga compensate: không event nào trong staging được giao. Saga xong: event được giao đúng một lần cho mỗi subscriber (subscriber đếm theo key).
- **H.** Kill relay giữa `AddBulk` và `sent_at`: gửi lại bị chặn bởi `JobID`, subscriber vẫn chỉ xử lý một lần.
- **Benchmark:** độ trễ thêm cho mỗi step (mục tiêu p95 dưới 5ms trên Postgres local) và số run mỗi giây khi có 3 instance.

## Rủi ro và cách giảm

- **Tải Postgres:** khoảng 2 lần ghi mỗi step, cộng River và outbox. Cần autovacuum riêng cho `saga.step`, `river_job`, `eventbus.outbox`; job dọn retention; chỉ lưu ID trong output và `comp_data`. Cân nhắc partition theo ngày cho `saga.step` sau M6.
- **Connection pool:** mỗi run chạy inline giữ một connection ngắn cho mỗi lần ghi, không giữ suốt run. River cần một connection LISTEN, không đi qua được PgBouncer ở chế độ transaction pooling. Ghi rõ cách định cỡ pool trong docs.
- **RAM:** heartbeat là một goroutine mỗi run đang chạy. Hãy gom thành một ticker cho mỗi instance để gia hạn hàng loạt (`UPDATE ... WHERE id = ANY($1)`).
- **Replay không deterministic:** code giữa các step không được rẽ nhánh theo thời gian hoặc random. Ghi rõ trong docs, và cung cấp `saga.Now(c)` / `saga.Rand(c)` có checkpoint.
- **Redis cho event:** cần `maxmemory-policy noeviction`, AOF, và alert bộ nhớ. Mất Redis chỉ làm chậm việc giao event (outbox giữ lại), không mất dữ liệu.
- **Thứ tự event không được đảm bảo:** subscriber đọc lại state từ DB. Ghi rõ trong docs.

## Nhật ký quyết định

Ghi theo thứ tự thời gian. Mỗi dòng: ngày, quyết định, lý do.

- **2026-11-12 (M1)** Bảng `saga.step` không có cột `kind`. Trạng thái compensate ghi trên dòng của step. Lý do: mỗi step chỉ được compensate tối đa một lần thành công, nên một dòng là đủ và truy vấn đơn giản hơn.
- **2026-11-12 (M1)** Fencing dùng `SELECT ... FOR NO KEY UPDATE` trên dòng run trong mỗi transaction ghi (`store.Fenced`). Lý do: chặn việc instance khác lấy lease giữa lúc kiểm tra và lúc ghi, và tránh deadlock do nâng cấp lock từ `FOR SHARE`. FK từ `saga.step` chỉ cần `FOR KEY SHARE` nên không xung đột.
- **2026-11-12 (M1)** `AcquireLease` không cấp lại lease còn sống, kể cả cho cùng `owner_id`. Lý do: hai goroutine trong cùng một instance gọi cùng `idem_key` không được chạy run hai lần.
- **2026-11-12 (M1)** Retry compensation tạm thời chạy trong process (`Options.CompensateAttempts`, `CompensateBackoff`). Bản bền vững qua River sẽ thay ở M3.
- **2026-11-12 (M1)** Step bị bỏ dở ở trạng thái `started` khi compensate: compensator nhận dữ liệu rỗng, và `saga.CompensationUncertain(ctx)` trả `true`. Lý do: không biết invoke đã có tác dụng hay chưa; module phải tra theo `saga.Key`.
- **2026-11-12 (M1)** Registry step/workflow là toàn cục và panic khi trùng tên. Trong test dùng `testdb.UniqueName` (có bộ đếm) vì `-count=N` chạy lại test trong cùng process.
- **2026-11-12 (M1)** Nếu ctx bị hủy sau khi invoke đã thành công, kết quả bị bỏ và step vẫn ở `started` để recovery chạy lại với cùng `Key`. Lý do: không ghi checkpoint bằng ctx đã hủy; module tự chống trùng.
