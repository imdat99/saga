# 9. Triển khai production

## Cấu hình engine

```go
eng, err := saga.New(pool, saga.Options{
	LeaseTTL:          30 * time.Second, // thời gian chờ tiếp quản sau crash; gia hạn mỗi TTL/3
	MaxWorkers:        50,               // số run từ hàng đợi chạy đồng thời mỗi instance
	RecoveryInterval:  10 * time.Second, // leader quét lease hết hạn
	ShutdownGrace:     5 * time.Second,  // Stop chờ bao lâu sau khi huỷ run
	Retention:         30 * 24 * time.Hour, // xoá run đã kết thúc; cũng là cửa sổ khử trùng lặp key
	Hooks:             []saga.RunHook{bus},  // event bus
	Observer:          metrics,              // xem phần Giám sát
	Logger:            logger,
	// InstanceID: mặc định "<hostname>-<pid>-<random>", duy nhất mỗi process
	// AsyncStepStart: false (xem bên dưới)
})
```

| Tuỳ chọn | Mặc định | Tăng khi | Giảm khi |
| --- | --- | --- | --- |
| `LeaseTTL` | 30 s | GC pause hoặc mạng chập chờn gây tiếp quản sai | Cần khôi phục nhanh hơn sau crash (tối thiểu 300 ms) |
| `MaxWorkers` | 50 | Nhiều run trong hàng đợi phải chờ (retry, Start, signal) | Step nặng làm quá tải instance |
| `RecoveryInterval` | 10 s | Số run rất lớn làm việc quét tốn kém | Cần khôi phục nhanh hơn |
| `Retention` | 0 (giữ mãi) | Cần khử trùng lặp key lâu hơn | Bảng phình quá lớn |

`AsyncStepStart: true` tiết kiệm khoảng một lần fsync mỗi step (một commit bền vững thay vì hai), đổi lại một rủi ro cụ thể: khi Postgres crash, bản ghi "step đã bắt đầu" có thể mất. Đọc tài liệu của option này và [benchmarks](../benchmarks.md) trước khi bật.

Mọi instance vừa phục vụ API vừa thực thi run, hay tách vai trò: [chương 11](11-api-hay-worker.md#mô-hình-triển-khai).

## Khởi động và tắt

```go
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool := mustPool(ctx)
	must(saga.Migrate(ctx, pool))        // chạy trên mọi instance được: có advisory lock
	must(eventbus.Migrate(ctx, pool))
	eng := mustEngine(pool)
	must(eng.Start(ctx))
	must(bus.Start(ctx))
	srv := startHTTP()

	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)   // 1. ngừng nhận request
	_ = eng.Stop(shutdown)       // 2. chờ run xong; huỷ và bàn giao phần còn lại
	_ = bus.Stop(shutdown)       // 3. dừng relay và worker
	pool.Close()
}
```

`Stop` chờ các run đang chạy tới khi context của nó kết thúc, rồi huỷ chúng với `saga.ErrShutdown` và nhả lease để instance khác tiếp tục ngay. Cho orchestrator (`terminationGracePeriodSeconds` của Kubernetes) nhiều thời gian hơn timeout của `Stop` cộng `ShutdownGrace`.

## Database

- **Migration:** `saga.Migrate` và `eventbus.Migrate` dùng advisory lock cấp session: chạy qua kết nối trực tiếp (không qua PgBouncer chế độ transaction), với pool ít nhất 3 connection.
- **Cỡ pool:** mỗi step có hai checkpoint ngắn; định cỡ theo số checkpoint đồng thời, không phải số run đồng thời. Bắt đầu với `max_conns = 10 + số checkpoint đồng thời` và theo dõi thời gian chờ acquire (`pool.Stat().EmptyAcquireCount()`): một checkpoint phải chờ sẽ làm chậm mọi step. Pool thiếu connection còn làm tăng allocation vì mỗi lần chờ tạo một waiter ([benchmarks](../benchmarks.md)). Chi tiết: [Operations](../operations.md#connection-pool-sizing).
- **PgBouncer:** River cần `LISTEN` và advisory lock; trỏ pool của engine thẳng tới Postgres hoặc dùng session pooling.
- **Cấu hình pgx:** các checkpoint trên hot path dùng đường ít allocation khi pool ở chế độ mặc định `QueryExecModeCacheStatement` và không có `ConnConfig.Tracer`; có tracer hoặc mode khác thì đi đường pgx thường (vẫn đúng, gấp khoảng đôi allocation). Xem [Operations](../operations.md#hot-path-statements-and-pgx-settings).
- **Vacuum:** `saga.run`, `saga.step`, `saga.river_job` và `eventbus.outbox` thay đổi liên tục; hạ `autovacuum_vacuum_scale_factor` của chúng (ví dụ 0.01).
- **Độ bền và độ trễ:** với `fsync=on` mỗi step tốn một hoặc hai lần flush khi commit; đo storage của bạn (`pg_test_fsync`).

## Deploy

- Workflow và step phải được đăng ký trong mọi binary có thể tiếp tục run của chúng. Khi rolling deploy, instance không biết `name@version` của run sẽ để nó cho instance khác.
- Chỉ đổi chuỗi step bằng version workflow mới, và giữ version cũ cho tới khi các run của nó xong (chương 3).
- Không đổi tên step đã có dữ liệu ghi; không đổi kiểu output hay dữ liệu compensation một cách không tương thích.
- Thêm compensator cho step trước đó không có: các step đã ghi trước đó không có dữ liệu compensation và được compensate ở chế độ uncertain (`CompensationUncertain` là true, dữ liệu zero). Viết compensator mới sao cho chạy được chỉ từ key.
- Subscriber của event bus phải được đăng ký trên mọi instance có relay trước khi event cho chúng được phát.

## Giám sát

Cài đặt `saga.Observer` (được gọi sau sự kiện, không được chặn):

```go
type metrics struct {
	stepDuration *prometheus.HistogramVec // label: workflow, step, outcome
	runsEnded    *prometheus.CounterVec   // label: workflow, status
}

func (m *metrics) StepAttempt(a saga.StepAttempt) {
	m.stepDuration.WithLabelValues(a.Workflow, a.Step, a.Outcome).Observe(a.Duration.Seconds())
}

func (m *metrics) RunEnded(r saga.RunInfo) {
	m.runsEnded.WithLabelValues(r.Workflow, string(r.Status)).Inc()
}
```

Cảnh báo khi:

| Tín hiệu | Lý do |
| --- | --- |
| Có run kết thúc ở `needs_attention` (hoặc event `saga.needs_attention`) | Cần người xử lý |
| Tỉ lệ step có outcome `retrying` | Một phụ thuộc đang lỗi |
| p95/p99 thời gian step | Phụ thuộc chậm hoặc chờ pool |
| Số run `running` có `lease_until < now()` kéo dài quá vài `RecoveryInterval` | Recovery không theo kịp, hoặc không có instance nào đã `Start` |
| Tuổi của dòng outbox chưa gửi lâu nhất | Lỗi relay hoặc Redis |
| Thời gian chờ acquire của pool | Pool quá nhỏ |

Các truy vấn hữu ích ở [chương 10](10-xu-ly-su-co.md).

## Retention

- `Options.Retention` xoá các run `done` và `compensated` (cùng step, signal, lock); run `needs_attention` được giữ lại. Đây cũng là thời gian idempotency key được khử trùng lặp.
- Purge `processed_requests` của các module bằng `idem.Purge` với tuổi **dài hơn**.
- River tự xoá job đã xong (24 h); event bus xoá dòng outbox đã gửi sau `Retention` của nó (7 ngày).

## Checklist production

- [ ] Mọi instance gọi `eng.Start`, và `eng.Stop` khi tắt, với grace period dài hơn timeout của stop.
- [ ] Pool được định cỡ cho số checkpoint đồng thời; kết nối trực tiếp hoặc session pooling.
- [ ] Module của mọi step khử trùng lặp theo `Key`; compensator chạy được khi uncertain.
- [ ] Workflow có version; version cũ còn được đăng ký khi còn run của nó.
- [ ] Đã đặt `Retention`; đã lên lịch `idem.Purge` với tuổi dài hơn.
- [ ] Có cảnh báo cho `needs_attention`, retry, thời gian chạy, độ trễ recovery, tuổi outbox.
- [ ] Redis (nếu dùng) với `noeviction` và AOF.
- [ ] Có runbook cho `needs_attention` (chương 4, chương 10).
