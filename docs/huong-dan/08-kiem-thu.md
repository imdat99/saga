# 8. Kiểm thử

Saga nên được kiểm thử với Postgres thật: các đảm bảo nằm ở SQL (lock, fencing, unique key), một bản giả trong bộ nhớ sẽ kiểm thử thứ khác. Mỗi test một database giúp các test độc lập mà vẫn đủ nhanh.

## Mỗi test binary một Postgres, mỗi test một database

`internal/testdb` của repository là mẫu tốt để sao chép:

- một container cho mỗi test binary (testcontainers, `postgres:16-alpine`), hoặc một server có sẵn qua biến môi trường (`SAGA_TEST_DSN`);
- `New(t)` tạo database mới, trả về pool, và xoá database khi test kết thúc;
- container chạy `fsync=off` cho nhanh (đừng đo độ bền ở đó).

```go
func setup(t *testing.T) (*pgxpool.Pool, *saga.Engine) {
	t.Helper()
	pool := testdb.New(t)
	if err := saga.Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	eng, err := saga.New(pool, saga.Options{
		LeaseTTL:         time.Second,      // tiếp quản nhanh trong test crash
		RecoveryInterval: 200 * time.Millisecond,
		Logger:           slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Start(context.Background()); err != nil { // cần cho retry, Sleep, Await, recovery
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = eng.Stop(ctx)
	})
	return pool, eng
}
```

## Registry là toàn cục

Tên step và workflow có phạm vi toàn process và định nghĩa trùng sẽ panic. Hai hệ quả:

- Định nghĩa production nằm ở cấp package và được mọi test dùng chung: không sao, test chỉ việc dùng.
- Step và workflow **định nghĩa bên trong test** cần tên duy nhất, kể cả khi chạy `go test -count=N` (mỗi lần lặp lại định nghĩa lần nữa). Dùng bộ đếm, như `testdb.UniqueName(t, "wf")`.

## Kiểm thử những gì

### Luồng thành công và idempotency

```go
out, err := CompleteOrder.Run(ctx, eng, "k1", in)
// kiểm tra out, kiểm tra state của module
again, err := CompleteOrder.Run(ctx, eng, "k1", in)   // cùng key: kết quả đã lưu, không có hiệu ứng mới
```

### Compensation

Cho một step phía sau lỗi (module giả, một cờ, một input mà module thật từ chối) rồi kiểm tra các hiệu ứng trước đó đã được hoàn tác và run ở `compensated`:

```go
_, err := CompleteOrder.Run(ctx, eng, "k2", inThatPaymentDeclines)
var re *saga.RunError
if !errors.As(err, &re) || re.Status != saga.StatusCompensated {
	t.Fatalf("err = %v, want compensated", err)
}
// hàng đã được trả lại, không có đơn hàng nào
```

### Retry

Cho module lỗi tạm thời N lần (service payment trong modulith có `FailNext(n)`), chạy, rồi chờ kết quả. Retry làm run bị park, nên dùng handle và cho đủ thời gian (backoff làm tròn lên khoảng 5 s):

```go
_, err := wf.Run(ctx, eng, key, in)
if errors.Is(err, saga.ErrRunWaiting) {
	h, _ := wf.Lookup(ctx, eng, key)
	waitCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	out, err = h.Wait(waitCtx)
}
```

### Crash và resume

Ngắt run giữa một step rồi tiếp tục với cùng key:

```go
// 1. Một step bị chặn tới khi test cho đi tiếp, và đếm số lần chạy theo key.
// 2. Chạy trong goroutine với ctx có thể huỷ; huỷ nó khi step đang bị chặn.
// 3. Chạy lại với cùng key trên cùng engine hoặc engine khác (sau LeaseTTL,
//    hoặc ngay sau eng.Stop, vì Stop nhả lease).
// 4. Kiểm tra: step chạy lại với CÙNG key, các step đã xong không chạy lại,
//    kết quả cuối đúng, module chỉ áp dụng hiệu ứng một lần.
```

`Engine.Stop` với context đã hết hạn giả lập shutdown: run bị huỷ với `saga.ErrShutdown` và lease được nhả, nên engine thứ hai (hoặc cùng key) tiếp quản ngay. `test/chaos` của repository đi xa hơn với process con thật bị SIGKILL hoặc đóng băng bằng SIGSTOP.

### Tính tất định

Replay là lúc mất tính tất định lộ ra. Cách kiểm tra rẻ: chạy workflow, rồi ép resume (test crash ở trên) và kiểm tra nó đạt cùng kết quả. Khi review code, từ chối `time.Now`, số ngẫu nhiên, duyệt map và I/O giữa các step (chương 3).

### Signal và timer

```go
h, _ := wf.Start(ctx, eng, key, in)
// chờ tới khi nó park ở Await
for st, _ := h.Status(ctx); st != saga.StatusWaiting; st, _ = h.Status(ctx) {
	time.Sleep(50 * time.Millisecond)
}
_ = eng.Signal(ctx, h.ID(), "manager-approval", Approval{OK: true})
out, err := h.Wait(ctx)
```

Với timeout của `Sleep` và `Await`, giữ thời lượng ngắn trong test và chừa khoảng 5 s cho độ trễ của scheduler.

## Chạy test

```bash
go test -race ./...                 # cần Docker cho testcontainers
SAGA_TEST_DSN=postgres://... go test ./...   # dùng lại Postgres đang chạy (mỗi test vẫn có database riêng)
go test -race -count=10 ./...       # kiểm tra test chập chờn trước khi merge
```

Tránh `sleep` cố định để sắp thứ tự giữa các goroutine; hãy chờ một điều kiện (trạng thái, bộ đếm) có deadline.
