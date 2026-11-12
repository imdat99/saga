# 3. Workflow

## Cấu trúc

```go
var CompleteOrder = saga.DefineWorkflow("order.complete", 1,
	func(c saga.Ctx, in CompleteIn) (Order, error) {
		if err := saga.Lock(c, "cart:"+in.CartID, 5*time.Minute); err != nil {
			return Order{}, err
		}
		resID, err := ReserveStock.Run(c, ReserveIn{SKU: in.SKU, Qty: in.Qty})
		if err != nil {
			return Order{}, err
		}
		payRef, err := Authorize.Run(c, in.Amount)
		if err != nil {
			return Order{}, err
		}
		return CreateOrder.Run(c, OrderIn{CartID: in.CartID, Reservation: resID, Payment: payRef})
	},
	saga.WithRunTimeout(time.Hour))
```

`DefineWorkflow(name, version, fn, opts...)` đăng ký hàm dưới tên `name@version`. Input `I` và output `O` được lưu dạng JSON như dữ liệu của step.

## Ba quy tắc cho thân workflow

### 1. Gọi step bằng `c`, lần lượt từng cái

`saga.Ctx` mang theo run. Step (cũng như `Sleep`, `Await`, `Lock`, `Now`, `Rand`) được khớp với lịch sử đã ghi **theo vị trí**, nên phải gọi tuần tự từ goroutine của workflow. Không gọi step từ goroutine sinh ra trong workflow. Việc song song thuộc về bên trong một step (một step có thể gọi song song nhiều nơi) hoặc tách thành nhiều run.

### 2. Trả lỗi của engine nguyên vẹn

`Step.Run`, `Sleep`, `Await` và `Lock` có thể trả lỗi bọc `saga.ErrRunWaiting` (run đã park) hoặc lỗi huỷ (mất lease, shutdown). Hãy trả chúng ra nguyên vẹn: engine nhận ra và không compensate. Thực tế: `if err != nil { return zero, err }`.

Không rẽ sang nhánh khác tuỳ theo **step trả về lỗi ứng dụng nào**. Step thất bại được replay từ bản ghi, bản ghi giữ lại thông điệp, các sentinel của engine (`ErrStepTimeout`, `ErrRunTimeout`, `ErrAwaitTimeout`, `ErrLockHeld`, `ErrLockLost`) và `saga.Permanent`, nhưng không giữ giá trị lỗi của bạn: `errors.Is(err, payment.ErrDeclined)` có thể đúng ở lần chạy đầu và sai khi replay, khiến run đi đường khác. Để rẽ nhánh theo kết quả nghiệp vụ, hãy đưa nó vào **output** của step thay vì trả lỗi:

```go
// Step báo bị từ chối bằng dữ liệu, không bằng lỗi.
res, err := ChargeCard.Run(c, in.Amount)  // res.Declined bool, res.Ref string
if err != nil {
	return Order{}, err                    // lỗi hạ tầng, ErrRunWaiting...: trả nguyên vẹn
}
if res.Declined {
	_, err := NotifyDeclined.Run(c, in.CartID) // tất định: res được replay đúng như đã ghi
	if err != nil {
		return Order{}, err
	}
	return Order{}, saga.Permanent(errors.New("payment declined"))
}
```

### 3. Tất định giữa các step

Run được tiếp tục sẽ chạy lại hàm từ đầu. Các step đã xong trả lại output đã ghi ngay lập tức, nên code giữa chúng chạy lại với cùng giá trị và phải đưa ra cùng quyết định. Chỉ rẽ nhánh theo input và output của step.

| Không làm | Làm thay |
| --- | --- |
| `time.Now()`, `time.Since(...)` | `now, err := saga.Now(c)` (giờ database, được ghi lại) |
| `rand.Int()`, `uuid.New()` | `n, err := saga.Rand(c)`, hoặc sinh ID bên trong một step |
| Đọc database, gọi API, đọc file hay biến môi trường | Làm trong một step và dùng output của nó |
| Duyệt map rồi gọi step theo thứ tự đó | Sắp xếp key trước |
| Đọc state toàn cục có thể thay đổi, feature flag | Truyền qua input, hoặc đọc trong một step |
| `go func() { step.Run(c, ...) }()` | Gọi step tuần tự; chạy song song bên trong một step |

Log và metric giữa các step thì được (hiệu ứng không gây hậu quả), nhưng chúng lặp lại ở mỗi lần replay.

## Mất tính tất định

Nếu run được tiếp tục gọi một step khác ở vị trí đã ghi, hoặc output đã ghi không decode được vào kiểu output, engine dừng run và park nó ở `needs_attention` (cause có `saga.ErrNonDeterministic`). Không chạy thêm gì. Sau khi sửa code, `RetryCompensation` sẽ compensate những gì đã làm.

## Versioning

Thay đổi workflow khi đang có run chạy dở là vấn đề deploy: run bắt đầu với code cũ sẽ được tiếp tục bằng code mới. Quy tắc:

- **An toàn, không cần version mới:** thay đổi không di chuyển hay đổi các step đã ghi: code sau step cuối, dòng log, thay đổi bên trong cài đặt của step mà vẫn giữ kiểu input/output tương thích.
- **Cần version mới:** thêm, bớt, đổi thứ tự step; đổi kiểu input/output của step không tương thích; đổi step nào chạy với cùng một input.

```go
// Giữ v1 đăng ký cho tới khi các run của nó xong; run mới dùng v2.
var completeV1 = saga.DefineWorkflow("order.complete", 1, completeOrderV1)
var CompleteOrder = saga.DefineWorkflow("order.complete", 2, completeOrderV2)
```

Mỗi run ghi lại version của nó và luôn được tiếp tục bằng hàm tương ứng. Instance không biết `name@version` của run sẽ để nó lại cho instance biết (rolling deploy). Gỡ version cũ khi không còn run nào dùng:

```sql
SELECT count(*) FROM saga.run
WHERE workflow = 'order.complete' AND version = 1
  AND status IN ('running', 'waiting', 'compensating');
```

Step cũng theo logic đó: không bao giờ đổi tên step đã có dữ liệu ghi; định nghĩa step mới với tên mới.

## Deadline của run

`saga.WithRunTimeout(d)` đặt deadline `d` tính từ lúc run được tạo. Nó được kiểm tra khi mỗi step bắt đầu và khi run đang chờ (run đang park sẽ thức dậy muộn nhất tại deadline). Quá deadline, step kế tiếp thất bại với `saga.ErrRunTimeout` và workflow compensate. Step đang chạy thì bị giới hạn bởi `WithTimeout` riêng của nó, không phải deadline của run.

## Giờ database và số ngẫu nhiên

```go
now, err := saga.Now(c)          // ghi ở lần chạy đầu, replay về sau
if err != nil {
	return Out{}, err
}
if now.Weekday() == time.Sunday { ... }   // rẽ nhánh tất định

n, err := saga.Rand(c)           // uint64 từ crypto/rand, được ghi lại
```

Mỗi lệnh tốn một pseudo-step được ghi.

## Run lồng nhau

Một step có thể khởi chạy workflow khác (`Child.Run(ctx, eng, key, in)` với `ctx` của step). Run con là run độc lập với key và lịch sử riêng; nó không kế thừa run id của run cha. Hãy tạo key của nó từ key của step cha (ví dụ `string(k) + ":child"`) để khi step cha chạy lại thì gặp lại đúng run con đó.
