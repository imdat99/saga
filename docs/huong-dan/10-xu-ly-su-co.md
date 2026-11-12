# 10. Xử lý sự cố

## Triệu chứng

### `Run` trả về `ErrRunInProgress`

Một người gọi hoặc instance khác đang giữ lease của run (request đồng thời với cùng key, hoặc lần thử trước vẫn đang chạy). Hãy chờ nó: `wf.Lookup(ctx, eng, key)` rồi `h.Wait(ctx)`. Nếu bên giữ lease đã crash, lease hết hạn sau `LeaseTTL` và lần gọi kế tiếp (hoặc recovery) sẽ tiếp quản.

### `Run` trả về `ErrRunWaiting` và run không bao giờ xong

Run đang park (retry, `Sleep`, `Await`, lock bận) và cần một engine **đã Start** để tiếp tục.

1. Có ít nhất một instance gọi `eng.Start` không? Deployment chỉ chạy inline sẽ không bao giờ tiếp tục run đã park.
2. Nó đang chờ gì?

```sql
SELECT id, status, wake_at, await_name, deadline, lease_until, owner_id
FROM saga.run WHERE id = $1;
```

- `await_name` có giá trị, `wake_at` NULL: đang chờ một signal chưa ai gửi. Kiểm tra bên gửi (`saga.signal` chứa các signal đã giao).
- `wake_at` trong tương lai: đang chờ backoff của retry hoặc `Sleep`; khoảng chờ bền vững làm tròn lên khoảng 5 s.
- `wake_at` trong quá khứ: job resume lẽ ra phải chạy; kiểm tra River (`SELECT * FROM saga.river_job WHERE args->>'run_id' = $1`) và log của các instance.

### Run kẹt ở `running` mà không ai chạy

```sql
SELECT id, owner_id, lease_until, now() - lease_until AS expired_for
FROM saga.run WHERE status IN ('running', 'compensating') AND lease_until < now()
ORDER BY lease_until LIMIT 20;
```

Lease hết hạn được leader nhặt lên trong lần quét recovery mỗi `RecoveryInterval`. Nếu các dòng vẫn còn đây: không instance nào gọi `Start`, chưa bầu được leader của River, mọi `MaxWorkers` đều bận, hoặc `name@version` của workflow không được đăng ký trong binary nào đang chạy (log ghi "resume skipped: workflow not registered").

### Run ở `needs_attention`

```sql
SELECT r.id, r.workflow, r.version, r.error, s.seq, s.name, s.status, s.error, s.comp_attempt
FROM saga.run r JOIN saga.step s ON s.run_id = r.id
WHERE r.status = 'needs_attention' AND r.id = $1 ORDER BY s.seq;
```

- Có step ở `compensation_failed`: compensator của nó lỗi tới khi hết `CompensatePolicy` (hoặc trả `Permanent`). Sửa nguyên nhân rồi gọi `eng.RetryCompensation(ctx, runID)`.
- Cause nhắc tới `non-deterministic`: code đã đổi dưới một workflow đang chạy (chương 3). Khôi phục version cũ, hoặc đăng ký lại nó dưới version cũ, rồi `RetryCompensation` để compensate những gì đã làm.

### `ErrLockHeld`

Một run khác đang giữ business lock. Bình thường với các thao tác đồng thời trên cùng thực thể; dùng `LockWait` để chờ thay vì thất bại. Xem ai đang giữ:

```sql
SELECT key, run_id, until, until < now() AS expired FROM saga.lock WHERE key = $1;
```

### `ErrLockLost`

Run đã park lâu hơn `ttl` của lock (hoặc instance của nó chết), và run khác đã lấy lock trong lúc đó. Run sẽ compensate. Tăng `ttl` vượt khoảng chờ dài nhất dự kiến, hoặc sắp xếp lại để lấy lock sau các đoạn chờ dài.

### `ErrLeaseLost` trong log, hoặc step chạy hai lần

Một instance mất lease (treo, mất kết nối, database không truy cập được trong phần lớn `LeaseTTL`) và instance khác đã tiếp quản; step đang chạy dở chạy lại với cùng key. Đây là hành vi dự kiến và an toàn nếu module khử trùng lặp theo key. Nếu xảy ra thường xuyên: GC pause hoặc thiếu CPU (tăng `LeaseTTL`), hoặc database quá tải.

### `ErrWorkflowNotRegistered`

`name@version` của workflow không được định nghĩa trong binary này. Trong rolling deploy là bình thường; nếu không thì một version đã bị gỡ khi run vẫn dùng nó. Hãy đăng ký lại.

### `ErrIdemKeyConflict`

Idempotency key đã được một run của workflow khác dùng. Thêm tiền tố workflow hoặc domain vào key.

### Step chậm

- Mỗi step là hai round trip cộng flush khi commit; trên storage chậm, commit chiếm phần lớn ([benchmarks](../benchmarks.md)).
- Chờ connection: xem `pool.Stat()` (`EmptyAcquireCount`, `AcquireDuration`); tăng `MaxConns`.
- Row lock giữ lâu trên `saga.run` (truy vấn thủ công, transaction dài đụng tới run) chặn các checkpoint.

## Truy vấn kiểm tra

```sql
-- Run theo trạng thái
SELECT status, count(*) FROM saga.run GROUP BY status;

-- Một run và các step của nó
SELECT * FROM saga.run WHERE id = $1;
SELECT seq, name, status, attempt, uncertain, error, error_kind, started_at, finished_at
FROM saga.step WHERE run_id = $1 ORDER BY seq;

-- Run theo idempotency key
SELECT id, status FROM saga.run WHERE idem_key = $1;

-- Các run chờ lâu nhất
SELECT id, workflow, wake_at, await_name FROM saga.run
WHERE status = 'waiting' ORDER BY updated_at LIMIT 20;

-- Step retry nhiều nhất
SELECT name, count(*), max(attempt) FROM saga.step
WHERE status IN ('retrying', 'started') GROUP BY name ORDER BY count(*) DESC;

-- Business lock đang giữ
SELECT key, run_id, until FROM saga.lock ORDER BY until;

-- Tồn đọng outbox (event bus)
SELECT count(*), min(created_at) FROM eventbus.outbox WHERE sent_at IS NULL;
```

Không bao giờ sửa tay `saga.run` hay `saga.step` của một run có thể đang chạy: mọi lần ghi của engine đều được fencing theo lease, và một lần ghi thủ công có thể phá vỡ điều đó. Hãy dùng `RetryCompensation`, `Signal`, hoặc để lease hết hạn.
