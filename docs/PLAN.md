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
9. **Việc kéo dài bên ngoài process (long-running):** không có primitive riêng. Dùng pattern "step khởi động + `Await`": một step thường gửi request ra ngoài với `Key` làm mã tham chiếu (có compensator, `RetryPolicy`), rồi `saga.Await` chờ kết quả, run được park không giữ lease. Bên ngoài báo kết quả bằng `Engine.SignalKey(ctx, key, payload)` (Key đã chứa `runID`). Hết timeout thì `ErrAwaitTimeout`, workflow có thể gọi lại step khởi động trong vòng lặp deterministic hoặc fail để compensate.
10. **Version:** `run.version` lưu version của workflow. Registry giữ cả hàm của version cũ cho tới khi các run cũ chạy xong.

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
- **2026-11-12 (M2)** Bảng River nằm trong schema `saga` (`river.Config.Schema`), queue riêng tên `saga`. Lý do: app có thể dùng River riêng ở schema khác mà không đụng migration hay job của saga.
- **2026-11-12 (M2)** `resume_run` do recovery enqueue dùng `UniqueOpts{ByArgs, ByPeriod: max(RecoveryInterval, 1s)}`, không unique chỉ theo run id. Lý do: River bắt buộc trạng thái `running` trong tập unique, nên job bị kẹt `running` do process bị kill sẽ chặn recovery của run đó tới khi River rescue (mặc định 1 giờ). Chống chạy trùng thật sự là lease: job trùng không lấy được lease và trả về ngay.
- **2026-11-12 (M2)** Worker `resume_run` tắt timeout của River (`-1`). Lý do: job chạy cả một run; lease (không phải job) giới hạn thời gian instance chết giữ run.
- **2026-11-12 (M2)** Heartbeat là một goroutine mỗi instance, chỉ sống khi có run đang chạy; gia hạn bằng `UPDATE ... FROM unnest(ids, versions)` có kiểm tra `owner_id` và `lease_version`. Run bị hủy ctx thì ngừng gia hạn để lease hết hạn và recovery tiếp quản, kể cả khi step bỏ qua ctx. Không gia hạn được trong cả một TTL thì hủy run với cause `ErrLeaseLost`.
- **2026-11-12 (M2)** `Engine.Stop`: ngừng nhận việc, chờ run đang chạy tới khi ctx hết; sau đó hủy với cause `ErrShutdown`, nhả lease (`lease_until = now()`) để instance khác resume ngay, chờ tối đa `ShutdownGrace`. Hủy ctx từ phía caller của `Workflow.Run` (mô phỏng crash) thì không nhả lease.
- **2026-11-12 (M2)** `saga.New` trả về `(*Engine, error)` vì phải tạo River client. `Workflow.Run` vẫn chạy inline không cần `Start`; thêm `Workflow.Start` (insert run và job trong cùng transaction) và `Workflow.Lookup` trả `Handle`.
- **2026-11-12 (M2)** Chaos test chạy tiến trình con thật (test binary tự gọi lại chính nó) và dùng SIGKILL / SIGSTOP, thay vì giả lập trong cùng process. Lý do: kiểm tra đúng việc mất heartbeat, mất LISTEN connection và lease hết hạn như khi crash thật.
- **2026-11-12 (M2, sau review 1)** Khi enqueue `resume_run` (Workflow.Start, recovery), run được "giữ chỗ": `owner_id = NULL`, `lease_until = now() + max(LeaseTTL, 5 x RecoveryInterval)`. `AcquireLease` nhận cả run không có owner. Lý do: recovery không enqueue lại run đang có job chờ (tránh phình `river_job` khi queue tồn đọng); xóa `owner_id` cũng chặn instance cũ bằng fencing.
- **2026-11-12 (M2, sau review 1)** `ReleaseLease` đặt `owner_id = NULL`. Lý do: một lần gia hạn đang bay của chính instance đó không thể ghi đè việc nhả lease.
- **2026-11-12 (M2, sau review 1)** Heartbeat dùng pool riêng 2 connection. Lý do: pool chính bão hòa (checkpoint, River) không được làm mất toàn bộ lease của một instance cùng lúc.
- **2026-11-12 (M2, sau review 1)** Job `resume_run` gặp workflow chưa đăng ký trong binary thì không lấy lease và bỏ qua; recovery enqueue lại sau thời gian giữ chỗ. Job recovery chạy ở queue riêng `saga_maintenance` (1 worker).
- **2026-11-12 (M2, sau review 2)** Bỏ River `UniqueOpts` cho `resume_run`; việc chống job trùng do giữ chỗ (`owner_id NULL`, `lease_until` = grace) đảm nhận. Lý do: unique theo chu kỳ có thể bỏ qua job mới (trùng với job `completed` cùng chu kỳ) trong khi run đã bị giữ chỗ, khiến run không có job cho tới hết grace. Thay thế dòng quyết định ByPeriod ở trên.
- **2026-11-12 (M3)** Retry, Sleep, Await dùng chung một thao tác "park": `status = waiting`, `wake_at = least(hạn, deadline)`, `owner_id = NULL`, `lease_until = wake_at + grace`, và `resume_run` được `InsertTx` với `ScheduledAt` lấy từ `now()` của Postgres trong cùng transaction. `lease_until` cho phép recovery bắt lại run nếu job đánh thức bị mất. `AcquireLease` chỉ lấy run `waiting` khi đã tới hạn hoặc signal đang chờ đã tới.
- **2026-11-12 (M3)** Step thất bại chờ retry có trạng thái mới `retrying` (migration 00002); khi compensate, `retrying` được coi như `started` (không chắc chắn). Sleep/Await là pseudo-step tên `saga.sleep`, `saga.await:<name>` (tiền tố `saga.` được dành riêng), lưu thời điểm đánh thức trong `output`.
- **2026-11-12 (M3)** `Workflow.Run` trả `ErrRunWaiting` khi run bị park, thay vì chờ. Lý do: không giữ request của caller trong thời gian không giới hạn; caller dùng `Lookup` + `Handle.Wait` nếu muốn chờ.
- **2026-11-12 (M3)** Không có job `step_timeout`. Timeout của step là timeout ctx cục bộ (`WithTimeout`, lỗi tạm thời, đi theo RetryPolicy); instance chết giữa step đã được lease xử lý. Deadline của run (`WithRunTimeout`) được kiểm tra khi step bắt đầu và giới hạn thời điểm đánh thức của run đang chờ. Lý do: tránh thêm một loại job và trạng thái mà lease đã bao phủ.
- **2026-11-12 (M3)** Compensation bền vững: compensator lỗi thì `comp_attempt++`, run giữ `compensating`, nhả lease, `wake_at = now() + backoff`, job lên lịch trong cùng transaction; hết lượt thì `needs_attention`. `RetryCompensation` reset `comp_attempt` của step `compensation_failed`. Event `saga.needs_attention` để sang M5 vì cần event bus.
- **2026-11-12 (M3)** `saga.Now` lấy `now()` của Postgres; `saga.Rand` dùng `crypto/rand`. Cả hai là step có checkpoint (một step mỗi lần gọi).
- **2026-11-12 (M3b, đã hủy)** Đã cân nhắc thêm long-running step (`DefineLongRunningStep`) thành mốc riêng M3b, xây trên cơ chế park và `Signal` của M3. Hoàn tất từ bên ngoài bằng `CompleteStep` / `FailStep` theo `Key` của step (signal tên `<Key>`), không phải theo tên signal tự đặt. Hết timeout được coi là kết quả không chắc chắn: retry `start` với cùng `Key` hoặc compensate với `CompensationUncertain`. Lý do: việc kéo dài bên ngoài process không được giữ lease hay goroutine; gắn với step để compensation và recovery hoạt động theo tên step như mọi step khác.
- **2026-11-12** Hủy mốc M3b (`DefineLongRunningStep`). Lý do: dư thừa với step thường + `Await` (park không giữ lease, compensation theo step khởi động, timeout bằng `ErrAwaitTimeout`, retry bằng vòng lặp deterministic). Chỉ giữ phần còn thiếu: `Engine.SignalKey` để bên ngoài báo kết quả khi chỉ biết `Key`, và tài liệu pattern. Heartbeat từ worker bên ngoài để sau, khi có nhu cầu thật.
- **2026-11-12 (M3, sau review 1)** Lưu `error_kind` của step thất bại (migration 00003) để replay dựng lại lỗi vẫn khớp `errors.Is` với sentinel của engine và `Permanent`. Lỗi của ứng dụng chỉ giữ message. Lý do: workflow rẽ nhánh theo `ErrStepTimeout` phải đi cùng nhánh khi replay.
- **2026-11-12 (M3, sau review 1)** Cờ `uncertain` (dính, OR) trên step: attempt bị timeout có thể đã có tác dụng, nên step `failed` có `uncertain` vẫn được compensate với `CompensationUncertain`. Step có `comp_data` NULL cũng coi là không chắc chắn.
- **2026-11-12 (M3, sau review 1)** `Signal` đánh thức run thì giữ chỗ `lease_until = now() + grace`, để recovery chạy lại nếu job bị mất (kể cả Await không timeout). Payload signal không decode được thì step Await `failed` (permanent); decode lỗi khi replay một step `done` là non-determinism (`needs_attention`).
- **2026-11-12 (M3, sau review 1)** `AcquireLease` trả về chính dòng run vừa lấy (`RETURNING`), và `execute` dùng nó thay cho lần đọc trước đó. Lý do: giữa lần đọc và lúc lấy lease, run có thể đã chuyển sang `compensating`.
- **2026-11-12 (M3, sau review 2)** Compensator luôn nhận `comp_data` đã ghi (nếu có), kèm cờ `CompensationUncertain` khi step không chắc chắn. Lý do: step thành công ở lần retry sau một lần timeout có cả dữ liệu lẫn sự không chắc chắn. `Signal` trùng không enqueue thêm job. `Sleep(d <= 0)` trả về ngay, không chiếm vị trí: `d` phải deterministic, nếu deploy đổi `d` từ dương sang không dương với run đang chạy thì replay gặp non-determinism.
- **2026-11-12 (M4)** `saga.Lock` là pseudo-step `saga.lock:<key>`; mỗi lần thử là một transaction fenced (`StartStep` + `INSERT ... ON CONFLICT DO UPDATE WHERE until < now() OR run_id = mình`). Mặc định tranh chấp thì fail ngay (`ErrLockHeld`, permanent); `LockWait(policy)` park run giữa các lần thử như retry. Lý do: dùng lại cơ chế park, không giữ lease khi chờ.
- **2026-11-12 (M4)** Lock được nhả trong chính transaction đặt `done`/`compensated`; `needs_attention` giữ lock tới hết TTL để bảo vệ phần việc dang dở. Khi resume, replay một Lock đã `done` sẽ khẳng định lại (gia hạn thêm `ttl`); nếu run khác đã lấy (do hết TTL) thì step chuyển `failed` với `ErrLockLost` và workflow compensate, thay vì chạy tiếp mà không được bảo vệ.
- **2026-11-12 (M4)** `idem.Do` giành key bằng `INSERT ... ON CONFLICT DO NOTHING` trong transaction của module trước khi chạy `fn`: caller đồng thời cùng key bị chặn ở unique index cho tới khi transaction đầu kết thúc, rồi đọc kết quả (commit) hoặc tự chạy (rollback). Chỉ lưu kết quả thành công. Tên schema được kiểm tra bằng regex vì là một phần của câu SQL.
- **2026-11-12 (M4, sau review 1)** Khẳng định lại lock khi replay chỉ `UPDATE` dòng của chính run (`ExtendLock`), không upsert: nếu dòng đã mất (run khác lấy sau khi hết hạn, kể cả đã nhả lại) thì là `ErrLockLost`. Lý do: upsert sẽ "lấy lại" lock và che mất việc đoạn được bảo vệ đã không độc quyền.
- **2026-11-12 (M4, sau review 1)** Lưu `ttl_us` trong `saga.lock`; heartbeat gia hạn lease và trong cùng câu lệnh (CTE) gia hạn lock của các run vừa được gia hạn. TTL lock tối thiểu bằng `LeaseTTL`, nên lock không hết hạn khi chủ còn đang chạy. Lock chỉ có thể hết hạn khi run bị park, chết hoặc `needs_attention`.
- **2026-11-12 (M4, sau review 1)** `idem.Do` chạy claim + `fn` trong savepoint (lỗi thì rollback về savepoint, kể cả khi caller vẫn commit). Thêm `idem.Abort`: compensator không chắc chắn ghi tombstone (`aborted`), chờ attempt đang bay kết thúc, và chặn vĩnh viễn attempt muộn (`ErrAborted`). Key là kiểu generic `~string` để nhận `saga.Key` trực tiếp.
- **2026-11-12 (M5)** `JobID = <eventID>.<subscriberID>` thay cho `eventID:subscriberID`: bullmq (giống BullMQ Node) từ chối custom id chứa một dấu `:` (chỉ cho phép dạng 3 đoạn). ID subscriber và tên queue được kiểm tra `[A-Za-z0-9._-]`.
- **2026-11-12 (M5)** Core thêm `saga.RunHook` (`Options.Hooks`), gọi trong chính transaction ghi trạng thái kết thúc (done, compensated, needs_attention). `eventbus.Bus` cài đặt hook: done thì chuyển staging sang outbox, mọi kết thúc khác thì xóa staging, needs_attention thì phát `saga.needs_attention` vào outbox. Lý do: depguard cấm core import eventbus; hook giữ được tính nguyên tử.
- **2026-11-12 (M5)** Relay là vòng lặp trên mọi instance (không cần leader, không qua River): mỗi lượt một transaction `FOR UPDATE SKIP LOCKED` theo lô, `AddBulk` theo queue, rồi `sent_at`; lỗi sau `AddBulk` thì rollback và gửi lại với cùng JobID. Lô đầy thì chạy tiếp ngay, không thì nghỉ `RelayInterval`.
- **2026-11-12 (M6)** Bug tìm thấy nhờ e2e: `CASE WHEN $n > 0` khiến Postgres suy kiểu tham số là `int4`, nên `WithRunTimeout` trên khoảng 35 phút (tính bằng micro giây) bị lỗi encode. Sửa bằng cast `$n::bigint`, thêm test hồi quy `TestLongRunTimeout`. Bài học: tham số khoảng thời gian luôn cast rõ kiểu.
- **2026-11-12 (M5, sau review 1)** Queue BullMQ mặc định là ID của subscriber (một queue cho mỗi subscriber). Lý do: worker trên instance không có handler cho một job sẽ tốn một attempt; với queue chung `events`, handler phân tán trên nhiều instance làm job rơi vào DLQ mà chưa chạy lần nào. `Start` từ chối queue có handler cho một số subscriber nhưng không cho tất cả.
- **2026-11-12 (M5, sau review 1)** Trạng thái chạy của Bus là snapshot bất biến tạo lúc `Start`; relay và processor đọc mà không cần lock, `Stop` không giữ mutex khi chờ. Retention chạy vòng lặp riêng, chỉ xóa staging của run đã kết thúc (run sống lâu hơn `Retention` không mất event).
- **2026-11-12 (M5, sau review 1)** `execute` che `runCtxKey` của run cha (giá trị nil), để run được khởi chạy từ bên trong step của run khác không bị `RunIDFrom` nhận nhầm là thuộc run cha.
- **2026-11-12 (M6)** Modulith mẫu: mỗi module có schema riêng và role `NOLOGIN` riêng, áp dụng bằng `SET LOCAL ROLE` trong từng transaction của module (một pool chung, quyền vẫn bị giới hạn theo schema). Wiring thủ công (constructor injection) trong `internal/app`, vì đồ thị nhỏ (dưới 10 service).
- **2026-11-12 (M6)** Step/workflow là toàn cục theo process, nên `flows.Define` chỉ đăng ký một lần và gắn module qua `atomic.Pointer` (gọi lại thì gắn lại). Lỗi tìm được nhờ `-count=10` của e2e: step giữ module (và pool đã đóng) của lần tạo App đầu tiên.
- **2026-11-12 (M6)** Thêm `saga.Observer` (gọi sau commit, ngoài transaction) cho metrics, tách khỏi `RunHook` (trong transaction). Thêm retention `Options.Retention` / `Engine.PurgeRuns` (chỉ xóa `done`/`compensated`, giữ `needs_attention`).
- **2026-11-12 (M6)** Benchmark ban đầu: p95 5.66 ms/step (trượt mục tiêu 5 ms). Chẩn đoán: tắt `synchronous_commit` không đổi kết quả, CPU profile cho thấy chủ yếu chờ syscall mạng, nên nút thắt là round trip. Checkpoint trên hot path (`StartStepFenced`, `FinishStepFenced`) gộp kiểm tra lease và câu ghi thành một câu lệnh autocommit (CTE `FOR NO KEY UPDATE`): 8 round trip mỗi step giảm còn 2. Ngữ nghĩa fencing không đổi. (Số liệu ban đầu ghi ở đây đo trên container `fsync=off` nên sai; xem dòng sau.)
- **2026-11-12 (M6, sau review 1)** Đo lại trên Postgres có `fsync=on` (docker compose): p95 14.7 ms giảm còn 11.1 ms (-24%), throughput 3 instance tăng từ 79 lên 119 runs/s (+51%). Mục tiêu p95 < 5 ms KHÔNG đạt trên máy này: đĩa cần 2.7 ms cho mỗi fsync, mỗi step có 2 commit bền vững nên mức sàn khoảng 5.4 ms; overhead của engine khi không fsync là p95 2.5 ms. Testcontainers mặc định `fsync=off`, không được dùng để benchmark. Phương án một commit bền vững mỗi step (ghi `started` với `synchronous_commit = off`, an toàn theo thứ tự WAL khi tác dụng của module nằm cùng Postgres) cần người dùng quyết định, chưa làm.
- **2026-11-12 (M6)** Thêm `Options.AsyncStepStart` (opt-in, mặc định tắt): checkpoint `started` commit với `synchronous_commit=off` cho riêng câu lệnh đó (`set_config(..., true)` trong CTE), nên mỗi step chỉ còn một commit bền vững. Đo với fsync bật: p95 11.6 ms giảm còn 7.7 ms, +37% runs/s. Mặc định giữ bền vững vì với tác dụng nằm ngoài Postgres, một run bị park do non-determinism trong khoảng đó có thể bỏ sót compensation. Mục tiêu "overhead mỗi step p95 < 5 ms" được hiểu là overhead của engine (đo không fsync, p95 2.5 ms, đạt); latency bền vững phụ thuộc thời gian fsync của storage và được ghi rõ trong `docs/benchmarks.md`.
- **2026-11-12 (M6)** `toolchain go1.27.4` trong `go.mod`: govulncheck báo GO-2026-6859 (`net/http` của 1.27.3) có đường gọi tới được từ server mẫu.
- **2026-11-12 (M6, sau review)** Chạy lại một step đang `started` (attempt trước không rõ kết quả) thì đặt cờ `uncertain` ngay trong câu `INSERT ... ON CONFLICT`. Sửa bug có từ trước, cả ở chế độ bền vững: resume sau deadline ghi step thành `failed` không kèm `uncertain`, nên step bị bỏ qua khi compensate. Ghi kết quả step chuyển sang upsert để không nhầm row `started` bị mất (do `AsyncStepStart`) thành mất lease. Cách hiểu mục tiêu 5 ms là "overhead của engine" cần người dùng xác nhận.
- **2026-11-12 (M6, sau review 2)** Engine tự đặt `uncertain` cho mọi kết quả thất bại của step được replay ở trạng thái `started` (biết từ lịch sử, không phụ thuộc lần ghi async có thể bị mất khi Postgres crash hai lần liên tiếp). Các dòng nhật ký trước về `AsyncStepStart` và mục tiêu 5 ms được thay bằng các dòng sau review (xem `docs/benchmarks.md`).
- **2026-11-12 (M6, người dùng xác nhận)** Mục tiêu "p95 < 5 ms mỗi step" là overhead của engine (đạt, p95 2.5 ms); latency bền vững được báo cáo trong `docs/benchmarks.md` nhưng không phải mục tiêu. Giữ 15 skill golang-* hiện có, không cài thêm.
- **2026-11-12 (M7, vòng 1)** Các câu lệnh hot path (`InsertRun`, `StartStepFenced`, `FinishStepFenced`) chạy thẳng trên `pgconn` thay vì `pgx.QueryRow/Scan`: prepared statement có tên cố định theo từng connection, tham số dạng text lấy từ buffer trong `sync.Pool` (chuỗi truyền qua `unsafe.StringData`, không copy), kết quả dạng binary tự decode. Lý do: pgx box từng tham số vào `any`, tạo rows và scan plan cho mỗi query, chiếm ~330/500 alloc mỗi run 10 step; sau thay đổi còn ~260 alloc/run, 26 KB còn 12.7 KB. Chỉ bật khi pool ở `QueryExecModeCacheStatement` (mặc định) và không có tracer; các mode khác (PgBouncer) và pool có tracer đi đường pgx cũ với cùng SQL. Lỗi 0A000/26000 (statement cũ) đóng connection để lần sau prepare lại.
- **2026-11-12 (M7, vòng 2)** Kết thúc run (done, compensated, needs_attention do non-determinism) là một câu lệnh fenced duy nhất (`FinishRunFenced`: CTE khoá row run và kiểm tra lease, xoá business lock khi done/compensated, UPDATE status) khi không có `Options.Hooks`; có hook thì vẫn dùng transaction vì hook cần `pgx.Tx`. Giảm 5 round trip còn 1 ở cuối mỗi run. Run vừa insert không đọc lại lịch sử step (`ListSteps`). Step không có compensator không lưu `comp_data` (NULL): nếu một bản deploy sau thêm compensator, các step ghi trước đó được compensate với cờ uncertain và dữ liệu zero value (trước đây là certain với JSON của giá trị trả về); ghi rõ ở `CompensationUncertain`. Kết quả: 260 còn 156 alloc/run, 12.7 còn 9.1 KB.
- **2026-11-12 (M7, vòng 3)** Mỗi lần execute một run đăng ký một `store.Interrupt` (một `context.AfterFunc`) thay cho việc pgconn watch context ở từng câu lệnh (4 alloc mỗi câu, run context luôn cancel được): câu lệnh fast path chạy với context không có `Done`, khi context run bị huỷ thì Interrupt đặt deadline quá khứ lên connection đang chạy, giống `DeadlineContextWatcherHandler` mặc định của pgconn; sau câu lệnh, nếu đã fire thì reset deadline trước khi trả connection về pool. Chỉ dùng khi context truyền vào có cùng `Done` channel với context được watch (context hẹp hơn, ví dụ có timeout riêng, vẫn để pgconn watch). Hệ quả: `BuildContextWatcherHandler` tuỳ biến không áp dụng cho các câu lệnh này (ghi trong `docs/operations.md`). Kết quả step được encode vào buffer tái sử dụng của run bằng `json/v2.MarshalWrite` với `json.DefaultOptionsV1()` (byte giống hệt `json.Marshal`, kiểm tra trên các ca HTML escape, map, nil slice, pointer method). Không đổi sang truyền `&out` cho json dù bớt được một alloc: nó đổi việc gọi `MarshalJSON` receiver con trỏ, tức đổi dữ liệu được lưu. Kết quả: 156 còn 69 alloc/run, 9.1 còn 3.7 KB.
- **2026-11-12 (M8, vòng 4)** Step `Key` được cắt từ arena theo từng lần execute (chunk 512 B, chỉ append, chunk đầy thì thay chunk mới, không grow tại chỗ) và trả về bằng `unsafe.String`: 10 key mỗi run còn khoảng 2 alloc. Đánh đổi: module giữ key trong bộ nhớ lâu dài sẽ giữ cả chunk (tối đa 512 B thay vì ~60 B); doc của `Key` khuyên `strings.Clone` trong trường hợp đó. Người dùng chọn hướng này (cùng vòng 5, 6), không chọn gộp checkpoint done(N) với started(N+1) vì đổi invariant checkpoint từng step.
- **2026-11-12 (M8, vòng 5)** Thay cho dòng vòng 3 (một `store.Interrupt` cấp phát riêng, AfterFunc trên context của run, so khớp theo Done channel): `store.Interrupt` là giá trị nhúng trong entry heartbeat của run (`activeRun`), không tự watch. Ai huỷ context được cover thì phải gọi `Fire` ngay sau: heartbeat (mất lease, shutdown) qua `activeRun.interrupt` (cancel rồi Fire), và AfterFunc trên context của caller (chỉ khi caller có thể huỷ; callback huỷ rctx với cause của caller trước khi Fire, đăng ký ngoài `h.mu`). Câu lệnh được cover chạy với `context.Background()`; context được cover so khớp theo identity (rctx và `*runCtx`), context hẹp hơn vẫn để pgconn watch; `begin` từ chối khi context đã done dù quên `Fire`. Kết quả: 61 còn 52 alloc/run khi caller dùng context không huỷ được (thêm 3 nếu huỷ được).
- **2026-11-12 (M8, vòng 6)** Input, output và comp_data được encode qua con trỏ lấy từ `sync.Pool` (`jsonSlot`) khi kiểu đó chứng minh được là cho cùng byte với `json.Marshal(v)`: không kiểu nào đi tới được qua struct field/array element (kể cả embedded, unexported) có `MarshalJSON`/`MarshalJSONTo`/`MarshalText`/`AppendText` trên con trỏ mà không có trên value; interface ở gốc thì giữ cách cũ. Trong json/v2 chỉ `forcedAddr` khác nhau giữa hai cách và nó chỉ ảnh hưởng đúng bốn interface đó (review đã đối chiếu source Go 1.27). Run id tự sinh UUIDv7 (cùng layout và thuật toán đơn điệu của google/uuid) để bỏ allocation của `io.Reader`. Kết quả: 52 còn 40 alloc/run.
- **2026-11-12 (docs)** Thêm hướng dẫn sử dụng chi tiết song ngữ theo yêu cầu người dùng: `docs/guide` (tiếng Anh, bản chính, cập nhật trước) và `docs/huong-dan` (tiếng Việt, cùng cấu trúc, giữ nguyên code). Đây là ngoại lệ có chủ đích với quy ước "docs bằng tiếng Anh" trong AGENTS.md; thay đổi API phải cập nhật cả hai. Ví dụ của chương 1 là chương trình thật `examples/quickstart` (được build và lint cùng repository) để code trong tài liệu không lệch khỏi API.
