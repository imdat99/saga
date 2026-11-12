# 6. Viết module với `idem`

Engine chạy mỗi step **ít nhất một lần**. Một step có thể bị chạy lại sau crash, khi bị instance khác tiếp quản lease, khi một lần thử bị timeout hoặc khi retry, luôn với cùng `saga.Key`. Hiệu ứng đúng một lần là việc của module: nó phải nhận ra key đã xử lý và trả lại kết quả đầu tiên. Package `idem` làm việc này ngay trong transaction Postgres của module.

## Thiết lập

Mỗi module có một bảng `processed_requests` trong schema riêng:

```go
var requests = idem.MustNew("inventory")          // bảng inventory.processed_requests

func (s *Service) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS inventory`); err != nil {
		return err
	}
	return requests.CreateTable(ctx, s.pool)       // hoặc đưa cùng DDL vào migration của bạn
}
```

## Thực hiện hiệu ứng một lần: `idem.Do`

```go
func (s *Service) Reserve(ctx context.Context, key, sku string, qty int) (string, error) {
	var id string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		id, err = idem.Do(ctx, tx, requests, key, func(ctx context.Context) (string, error) {
			tag, err := tx.Exec(ctx, `
				UPDATE inventory.stock SET available = available - $2
				WHERE sku = $1 AND available >= $2`, sku, qty)
			if err != nil {
				return "", err
			}
			if tag.RowsAffected() == 0 {
				return "", ErrOutOfStock
			}
			_, err = tx.Exec(ctx, `INSERT INTO inventory.reservation (key, sku, qty) VALUES ($1, $2, $3)`, key, sku, qty)
			return key, err
		})
		return err
	})
	return id, err
}
```

Cách hoạt động:

- `Do` giành key trong `tx`, rồi chạy `fn` trong một savepoint. Việc giành key, hiệu ứng và kết quả được lưu cùng commit hoặc cùng rollback.
- Lần gọi thứ hai với cùng key trả về kết quả đã lưu mà không chạy `fn`.
- Lần gọi đồng thời với cùng key bị chặn tới khi transaction đầu kết thúc, rồi trả kết quả của nó (hoặc tự chạy `fn` nếu transaction đầu rollback).
- Nếu `fn` lỗi, việc giành key bị huỷ và key vẫn trống cho lần thử sau.
- Kết quả được lưu dạng JSON: giữ nó nhỏ.

Ở mức `REPEATABLE READ` hoặc `SERIALIZABLE`, lần gọi trùng đồng thời thất bại với lỗi serialization (SQLSTATE 40001) thay vì chờ: hãy retry transaction.

Hiệu ứng nằm ngoài Postgres (gọi HTTP tới cổng thanh toán) không dùng chung transaction được. Hãy truyền key làm idempotency key của nhà cung cấp (`Idempotency-Key` của Stripe, mã end-to-end của ngân hàng); đa số nhà cung cấp khử trùng lặp theo nó.

## Compensate: `idem.Abort`

Compensator có thể được gọi cho một step chưa rõ kết quả (`saga.CompensationUncertain(ctx)`), và một lần thử cũ của step thậm chí có thể vẫn đang chạy trên một instance bị treo. `Abort` xử lý cả hai trường hợp:

```go
func (s *Service) Release(ctx context.Context, key string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, processed, err := idem.Abort[string](ctx, tx, requests, key)
		if err != nil || !processed {
			return err   // chưa làm gì, và từ giờ không thể làm gì nữa
		}
		// đã xử lý: hoàn tác (idempotent: bản thân compensator có thể chạy lại)
		...
	})
}
```

- Nếu request đã được xử lý, `Abort` trả kết quả đã lưu và `true`: hãy hoàn tác.
- Nếu chưa, nó ghi một **tombstone**: một lần `Do` đến muộn với key đó (lần thử sống lâu hơn lease của nó) sẽ lỗi `idem.ErrAborted` và không bao giờ có hiệu lực.
- Nếu có lần thử đang giữ key trong một transaction chưa kết thúc, `Abort` chờ nó xong trước.

Bản thân việc hoàn tác cũng phải idempotent (ví dụ `UPDATE ... SET released = true WHERE key = $1 AND NOT released`), vì compensator có thể chạy lại sau crash.

## Đọc và dọn dẹp

```go
res, ok, err := idem.Lookup[string](ctx, pool, requests, key)   // kết quả đã commit, không chờ
n, err := idem.Purge(ctx, pool, requests, 90*24*time.Hour, 10000) // xoá bản ghi cũ
```

Giữ tuổi purge **dài hơn** retention của saga và độ trễ resume hay retry dài nhất có thể: khi bản ghi đã bị xoá, một step chạy lại với key đó sẽ thực hiện hiệu ứng lần nữa. Purge cũng xoá tombstone.

## Ranh giới module

Example modulith minh hoạ cách tổ chức chặt chẽ:

- mỗi module có schema riêng và một role `NOLOGIN`, chạy transaction với `SET LOCAL ROLE`;
- module không bao giờ import lẫn nhau; chỉ package `flows` biết nhiều module và định nghĩa step, workflow;
- step là lớp chuyển tiếp mỏng: gọi một hàm của module với key và chuyển đổi lỗi (`saga.Permanent` cho từ chối nghiệp vụ).

## Checklist cho hàm module được step gọi

- [ ] Nhận key và khử trùng lặp theo nó (`idem.Do`, hoặc idempotency key của nhà cung cấp).
- [ ] Hàm hoàn tác dùng `idem.Abort` (hoặc tra cứu tương đương theo key) và idempotent.
- [ ] Trả lỗi riêng cho từ chối nghiệp vụ, để step đánh dấu `saga.Permanent`.
- [ ] Phát event (nếu có) trong cùng transaction, với `ctx` của step (chương 7).
