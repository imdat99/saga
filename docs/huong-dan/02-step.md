# 2. Step

## Cấu trúc

```go
var ReserveStock = saga.DefineStep(
	"inventory.reserve",                              // tên: toàn cục, cố định mãi mãi
	func(ctx context.Context, k saga.Key, in ReserveIn) (string, string, error) {
		id, err := inventory.Reserve(ctx, string(k), in.SKU, in.Qty)
		return id, id, err                            // output O, dữ liệu compensation C, lỗi
	},
	func(ctx context.Context, k saga.Key, reservationID string) error {
		return inventory.Release(ctx, string(k))     // hoàn tác; compensator nil = không có gì để hoàn tác
	},
	saga.WithRetry(saga.Exponential(5, time.Second)),
	saga.WithTimeout(10*time.Second),
)
```

Định nghĩa step ở cấp package (hoặc một lần lúc khởi động). Định nghĩa trùng tên sẽ panic. Tên bắt đầu bằng `saga.` được dành cho các pseudo-step của engine.

## Key

Mỗi lần gọi `Step.Run` trong một run nhận một `saga.Key` dạng `<runID>:<vị trí>:<tên step>`. Key giống hệt nhau mỗi khi vị trí đó được thực thi: sau crash, khi retry, trên instance khác. Hãy truyền nó cho module làm idempotency key của request, và để module trả lại kết quả đầu tiên khi gặp một key đã xử lý (chương 6). Đó là cách biến "step có thể chạy nhiều lần" thành "hiệu ứng xảy ra một lần".

- `k.RunID()` trả về id của run.
- Key là chuỗi có thể lưu lại; nếu giữ key trong bộ nhớ lâu (ví dụ làm key của map cache), hãy lưu `strings.Clone(string(k))`: các key của cùng một lần thực thi dùng chung một buffer nhỏ.

## Output và dữ liệu compensation

Output (`O`) là thứ `Step.Run` trả cho workflow, bây giờ và ở mọi lần replay. Dữ liệu compensation (`C`) được đưa cho compensator. Cả hai được lưu dạng JSON trong `saga.step`:

- Giữ chúng nhỏ: ID, số tiền, mã tham chiếu. Không phải entity, không phải file.
- Phải đi qua JSON mà không mất gì: field exported, không channel hay function, không phụ thuộc state unexported.
- Chúng được replay: đổi kiểu không tương thích sẽ làm hỏng các run đang chạy (output đã ghi không decode được nữa thì run bị park ở `needs_attention`). Hãy tăng version workflow (chương 3).
- Step không có compensator thì không lưu dữ liệu compensation.

## Lỗi và retry

Mặc định một step chỉ được thử một lần: lỗi làm step thất bại, và workflow quyết định làm gì (thường là trả lỗi ra, dẫn tới compensate).

```go
saga.WithRetry(saga.RetryPolicy{
	MaxAttempts:    5,                // tổng số lần thử, tính cả lần đầu
	InitialBackoff: time.Second,
	MaxBackoff:     time.Minute,
	Multiplier:     2,
})
// hoặc
saga.WithRetry(saga.Exponential(5, time.Second)) // gấp đôi mỗi lần, tối đa 10 phút
```

Retry là bền vững: sau một lần thử lỗi, run được **park** (nhả lease, không giữ goroutine) và một job tiếp tục nó sau khoảng backoff, trên bất kỳ instance nào đã `Start`. Trong lúc đó `Step.Run` trả về lỗi bọc `saga.ErrRunWaiting`; workflow phải trả nó ra nguyên vẹn (xem chương 3). Độ chính xác của backoff khoảng 5 giây (do scheduler của River).

Đánh dấu những lỗi mà retry không sửa được bằng `saga.Permanent`: step thất bại ngay, bất kể policy.

```go
if errors.Is(err, payment.ErrDeclined) {
	err = saga.Permanent(err)
}
```

Dùng cho lỗi validate, nghiệp vụ từ chối, "không tìm thấy". Để nguyên các lỗi hạ tầng (timeout, connection refused, 5xx) là lỗi tạm thời.

## Timeout

`saga.WithTimeout(d)` huỷ context của step sau `d` (cause `saga.ErrStepTimeout`). Lần thử đó tính là lỗi tạm thời, nên được retry nếu policy cho phép. Một lần thử bị timeout vẫn có thể đã có hiệu lực (request đã tới phía bên kia); khi đó step được đánh dấu **uncertain** và sẽ được compensate dù cuối cùng nó thất bại. Hàm invoke phải tôn trọng `ctx`.

## Compensator

Compensator hoàn tác hiệu ứng của step. Engine gọi nó:

- theo thứ tự ngược các step, sau khi workflow trả lỗi, hoặc khi quá deadline của run;
- với cùng `Key` như step và dữ liệu compensation đã ghi;
- **ít nhất một lần**: crash giữa lúc compensate sẽ chạy lại nó. Compensator phải idempotent.

### Step không chắc chắn (uncertain)

Có lúc engine không biết step đã có hiệu lực hay chưa: process chết sau khi step bắt đầu và trước khi kết quả được ghi, đang chờ retry, hoặc một lần thử bị timeout. Step đó vẫn được compensate, với `saga.CompensationUncertain(ctx) == true` và dữ liệu là zero value (hoặc dữ liệu của một lần thử sau đó đã thành công). Compensator phải tự tra theo key xem đã làm gì, nếu có:

```go
func(ctx context.Context, k saga.Key, reservationID string) error {
	// Đúng dù reservationID có biết hay không: tra theo key.
	return inventory.Release(ctx, string(k))
}
```

Với package `idem`, `idem.Abort` làm đúng việc này và còn chặn một lần thử đến muộn có hiệu lực về sau (chương 6).

### Retry compensation và needs_attention

Compensator lỗi được retry bền vững:

```go
saga.WithCompensatePolicy(saga.Exponential(10, time.Second)) // mặc định: 10 lần, bắt đầu từ 1s
```

Khi hết policy (hoặc compensator trả `saga.Permanent`), run dừng ở trạng thái `needs_attention`, giữ business lock cho tới hết TTL, và event bus (nếu có) phát event `saga.needs_attention`. Sửa nguyên nhân rồi gọi `eng.RetryCompensation(ctx, runID)` (chương 4).

## Checklist cho một step

- [ ] Tên duy nhất, mô tả rõ (`module.hành_động`) và sẽ không bao giờ đổi.
- [ ] Module khử trùng lặp theo `Key` (hoặc hiệu ứng vốn idempotent).
- [ ] Output và dữ liệu compensation nhỏ, dễ chuyển JSON.
- [ ] Lỗi vĩnh viễn được bọc `saga.Permanent`; lỗi tạm thời được retry bằng policy.
- [ ] Hàm invoke tôn trọng `ctx` (timeout, shutdown, mất lease).
- [ ] Compensator idempotent và đúng cả khi `CompensationUncertain(ctx)` là true.
