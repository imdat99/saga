# 1. Bắt đầu

## Cần có

- Go 1.27 trở lên.
- Postgres (repository chỉ được kiểm thử trên bản 16). Engine lưu mọi thứ ở đây: run, step, signal, lock và hàng đợi job (River).
- Redis 7, chỉ khi dùng event bus (chương 7).

```bash
go get github.com/imdat99/saga@latest   # các bản release có tag: vX.Y.Z
make up   # trong repository này: Postgres 16 và Redis 7 từ docker-compose.yml
```

## Mô hình trong một đoạn

Một **workflow** là một hàm Go gọi lần lượt các **step**. Mỗi step là một hàm thực hiện một hiệu ứng (giữ hàng, trừ tiền thẻ) và trả về output cùng dữ liệu cần để hoàn tác. Engine ghi kết quả của mỗi step vào Postgres trước khi đi tiếp. Nếu process chết, một instance khác nhận run, chạy lại hàm workflow từ đầu, nhận lại output đã ghi của các step đã xong mà không chạy lại chúng, rồi tiếp tục từ chỗ dừng. Nếu workflow trả về lỗi, engine chạy các **compensator** của những step đã xong theo thứ tự ngược. Một step có thể chạy nhiều lần (sau crash, khi retry), luôn với cùng một **key**, nên module thực hiện hiệu ứng phải khử trùng lặp theo key đó.

## Saga đầu tiên

Chương trình đầy đủ: [examples/quickstart/main.go](../../examples/quickstart/main.go). Nó chuyển tiền giữa hai tài khoản bằng hai step, trừ (debit) rồi cộng (credit); step credit có thể được cho lỗi để engine hoàn tác step debit.

### 1. Định nghĩa step

```go
debit := saga.DefineStep("quickstart.debit",
	// invoke: thực hiện hiệu ứng một lần cho mỗi key, trả output và dữ liệu compensation
	func(ctx context.Context, k saga.Key, in TransferIn) (string, string, error) {
		id, err := l.post(ctx, k, in.From, -in.Amount)
		return id, id, err
	},
	// compensate: hoàn tác, idempotent
	func(ctx context.Context, k saga.Key, _ string) error { return l.reverse(ctx, k) },
	saga.WithRetry(saga.Exponential(3, time.Second)))
```

`DefineStep[I, O, C]` suy ra kiểu input `I`, output `O` và dữ liệu compensation `C` từ các hàm truyền vào. Tên step là toàn cục và không được đổi khi đã có run dùng nó: khi khôi phục, engine tìm compensator theo tên.

### 2. Định nghĩa workflow

```go
transfer := saga.DefineWorkflow("quickstart.transfer", 1,
	func(c saga.Ctx, in TransferIn) (TransferOut, error) {
		debitID, err := debit.Run(c, in)
		if err != nil {
			return TransferOut{}, err
		}
		creditID, err := credit.Run(c, in)
		if err != nil {
			return TransferOut{}, err // engine sẽ compensate step debit
		}
		return TransferOut{DebitID: debitID, CreditID: creditID}, nil
	})
```

Quy tắc cho thân workflow (chi tiết ở chương 3): gọi step bằng `c` và tuần tự, trả lỗi của chúng nguyên vẹn, giữ code giữa các step tất định.

### 3. Tạo engine và chạy

```go
pool, _ := pgxpool.New(ctx, dsn)
if err := saga.Migrate(ctx, pool); err != nil { ... }   // tạo schema "saga" (bảng + River)

eng, err := saga.New(pool, saga.Options{})
if err := eng.Start(ctx); err != nil { ... }             // khôi phục, retry, timer, signal
defer eng.Stop(shutdownCtx)                              // luôn gọi: chờ run xong, đóng pool heartbeat

out, err := transfer.Run(ctx, eng, "transfer-42", TransferIn{From: "alice", To: "bob", Amount: 100})
```

Tham số thứ ba của `Run` là **idempotency key** của run. Gọi `Run` lại với cùng key không bao giờ tạo run thứ hai: nó trả kết quả đã lưu của run đã xong, tiếp tục run nếu run bị bỏ dở, hoặc báo run đang chạy.

### 4. Xử lý kết quả

```go
out, err := transfer.Run(ctx, eng, key, in)
if errors.Is(err, saga.ErrRunWaiting) || errors.Is(err, saga.ErrRunInProgress) {
	h, err2 := transfer.Lookup(ctx, eng, key) // đang park chờ retry, hoặc đang chạy ở nơi khác
	if err2 != nil { ... }
	out, err = h.Wait(ctx)
}
var re *saga.RunError
switch {
case errors.As(err, &re):
	// re.Status là compensated hoặc needs_attention, re.Cause cho biết lý do
case err != nil:
	// lỗi hạ tầng, mất lease, bị huỷ: gọi Run lại với cùng key sẽ tiếp tục run
	// (không áp dụng cho ErrIdemKeyConflict hay ErrEngineStopped, xem chương 4)
default:
	// done: out là output
}
```

### 5. Chạy thử

Chương trình kết nối tới `SAGA_DSN`, mặc định là database của docker compose (`postgres://saga:saga@localhost:5432/saga?sslmode=disable`).

```bash
go run ./examples/quickstart              # transfer done: debit ..., credit ...
go run ./examples/quickstart -fail        # run ... ended compensated: step quickstart.credit: credit refused
go run ./examples/quickstart -key demo-1  # done
go run ./examples/quickstart -key demo-1 -fail   # cùng key: trả kết quả đã lưu, không chạy lại gì
```

Xem engine đã ghi gì:

```sql
SELECT id, workflow, status, error FROM saga.run ORDER BY created_at DESC LIMIT 5;
SELECT run_id, seq, name, status, attempt, output, comp_data FROM saga.step ORDER BY run_id, seq;
SELECT * FROM ledger.entry;   -- run bị compensate không để lại bút toán nào
```

## Đọc tiếp

- Key và việc module phải làm với nó: [chương 6](06-module.md). Đây là phần quan trọng nhất: engine đảm bảo step chạy ít nhất một lần; hiệu ứng đúng một lần là trách nhiệm của module.
- Chờ đợi mà không giữ tài nguyên (retry, timer, chờ người duyệt): [chương 5](05-cho-doi.md).
- Trước khi lên production: [chương 9](09-production.md).
