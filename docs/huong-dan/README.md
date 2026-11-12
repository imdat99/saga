# Hướng dẫn sử dụng

Hướng dẫn từng bước để xây dựng, chạy và vận hành saga với `github.com/imdat99/saga`. Bản tiếng Anh (bản chính, được cập nhật trước): [docs/guide](../guide/README.md).

Các tài liệu tham chiếu (tiếng Anh) giữ ngắn gọn và chính xác; hướng dẫn này giải thích cách dùng chúng:

| Tài liệu tham chiếu | Nội dung |
| --- | --- |
| [Concepts](../concepts.md) | Thuật ngữ: run, step, key, replay, lease, park, compensation |
| [Guarantees](../guarantees.md) | Những gì được đảm bảo khi crash, mất kết nối, deploy |
| [Operations](../operations.md) | Vòng đời, thời gian, cỡ pool, PgBouncer, retention |
| [Patterns](../patterns.md) | Công việc dài chạy bên ngoài, kích thước payload |
| [Benchmarks](../benchmarks.md) | Độ trễ, thông lượng, allocation |

## Các chương

1. [Bắt đầu](01-bat-dau.md): cài đặt, migrate, saga đầu tiên có compensation, chạy thử.
2. [Step](02-step.md): `DefineStep`, `Key`, output và dữ liệu compensation, retry, timeout, lỗi vĩnh viễn, compensator.
3. [Workflow](03-workflow.md): `DefineWorkflow`, tính tất định, `Now`/`Rand`, versioning, deadline của run, xử lý lỗi trong workflow.
4. [Khởi chạy và theo dõi run](04-run.md): idempotency key, `Run`, `Start`, `Lookup`, `Handle`, các kết quả, tích hợp HTTP, khôi phục thủ công.
5. [Chờ đợi: Sleep, Await, signal, lock](05-cho-doi.md): park, `Sleep`, `Await`, `Signal`, `SignalKey`, `Lock`.
6. [Viết module với `idem`](06-module.md): hiệu ứng đúng một lần, compensator hoạt động cả khi không chắc chắn.
7. [Event bus](07-su-kien.md): phát event từ step, subscriber, ngữ nghĩa giao nhận, event `needs_attention`.
8. [Kiểm thử](08-kiem-thu.md): integration test, giả lập lỗi, crash và resume, tính tất định.
9. [Triển khai production](09-production.md): cấu hình, định cỡ, shutdown, deploy, giám sát, retention.
10. [Xử lý sự cố](10-xu-ly-su-co.md): triệu chứng, nguyên nhân, cách sửa, và SQL để kiểm tra run.

Chương trình của chương 1 nằm ở [examples/quickstart](../../examples/quickstart/main.go); một ứng dụng đầy đủ với ba module tách biệt, HTTP API và nhiều instance nằm ở [examples/modulith](../../examples/modulith).

## Thuật ngữ

Hướng dẫn giữ nguyên các thuật ngữ tiếng Anh trùng với tên trong code:

| Thuật ngữ | Nghĩa |
| --- | --- |
| run | Một lần thực thi workflow cho một idempotency key |
| step | Một bước có hiệu ứng (side effect), có thể có compensator |
| key (`saga.Key`) | Khoá idempotency của một lần gọi step: `<runID>:<vị trí>:<tên step>` |
| replay | Chạy lại hàm workflow từ đầu, các step đã xong trả lại output đã ghi |
| compensate / compensator | Hoàn tác / hàm hoàn tác hiệu ứng của một step |
| lease | Quyền sở hữu tạm thời một run của một instance (owner + hạn + version) |
| park | Tạm dừng run trong Postgres, nhả lease, không giữ goroutine |
| needs_attention | Trạng thái cần người can thiệp |
