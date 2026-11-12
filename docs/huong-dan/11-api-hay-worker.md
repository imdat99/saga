# 11. Tầng API hay worker: workflow chạy ở đâu

## Thứ gì thực sự chạy ở đâu

Có ba thứ cần đặt chỗ, và chúng không giống nhau:

| Thứ | Chạy ở | Ghi chú |
| --- | --- | --- |
| **Khởi chạy run** (`Workflow.Run`, `Workflow.Start`), signal (`Engine.Signal`), trạng thái (`Handle.Status`), `RetryCompensation` | Bất cứ nơi nào bạn gọi: HTTP handler, consumer, CLI | Rẻ: một vài câu lệnh |
| **Thực thi run** (hàm workflow và các step) | Process đang giữ lease của run: nơi gọi `Workflow.Run` (inline), hoặc một instance đã gọi `eng.Start` (run từ hàng đợi, được đánh thức, được khôi phục) | Một run có thể bắt đầu ở process này và kết thúc ở process khác |
| **Chờ** (backoff của retry, `Sleep`, `Await`, lock bận) | Không ở đâu cả: run được park trong Postgres | Được một job đánh thức, rồi một instance đã `Start` thực thi tiếp |

Vì vậy bạn không đặt một *step* ở API hay ở worker: step chạy ở nơi run của nó đang được thực thi lúc đó. Thứ bạn chọn là **cách khởi chạy run** (inline hay qua hàng đợi) và **những process nào gọi `eng.Start`** (tức là thực thi các run từ hàng đợi, run được đánh thức và run được khôi phục).

Hai hệ quả luôn đúng, dù chọn cách nào:

1. **Mọi binary có thể thực thi một run phải đăng ký workflow và step của nó**, cùng các phụ thuộc mà step cần (service của module, client). Run được API chạy inline sẽ được worker tiếp tục sau backoff của retry; run do worker khởi chạy có thể được bất kỳ instance nào đã `Start` tiếp tục.
2. **Step không được phụ thuộc vào request.** Không `*http.Request`, không session người dùng, không giá trị lấy từ context của request (thông tin xác thực, trace id, deadline): sau khi resume, step chạy ở process khác với context khác. Mọi thứ step cần phải nằm trong input của workflow (user id, tenant, số tiền) hoặc trong output của các step.

## Inline ở API: `Workflow.Run`

```go
out, err := CompleteOrder.Run(ctx, eng, "checkout:"+idemKey, in) // trong HTTP handler
```

Run được thực thi trong goroutine của request. Đo trên database test cục bộ, một workflow 3 step chạy inline trả về sau khoảng 9 ms (p50), so với khoảng 27 ms (p50) và 70 ms (p90) khi đưa vào hàng đợi rồi chờ: thời gian nhận job từ hàng đợi cộng với chu kỳ poll của `Handle.Wait`.

**Dùng khi đủ tất cả các điều kiện sau:**

- Client cần kết quả ngay trong response (mã đơn hàng, xác nhận, lỗi validate).
- Workflow ngắn: mọi step đều nhanh, và cả run nằm gọn trong timeout HTTP ở p99.
- Bình thường không phải chờ: không `Sleep`, không `Await` người duyệt, retry chỉ cho lỗi tạm thời hiếm gặp.
- Lượng request vừa phải: mỗi request đang chạy giữ một goroutine và dùng connection cho các checkpoint.

**Cẩn thận với context của request.** Nếu nó bị huỷ (client ngắt kết nối, hết deadline), run bị gián đoạn giữa chừng và chỉ được recovery tiếp tục khi lease hết hạn (`LeaseTTL`, mặc định 30 s). Hãy tách run khỏi request và tự đặt giới hạn:

```go
ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 10*time.Second)
defer cancel()
out, err := CompleteOrder.Run(ctx, eng, key, in)
```

Và xử lý các kết quả "chưa xong" (`ErrRunWaiting`, `ErrRunInProgress`) bằng `202 Accepted` kèm URL trạng thái ([chương 4](04-run.md#tích-hợp-http)).

## Đưa vào hàng đợi cho worker: `Workflow.Start`

```go
h, err := ExportReport.Start(ctx, eng, "export:"+reqID, in) // trả về ngay khi run được enqueue
// trả 202 với h.ID(); client poll GET /runs/{id}, hoặc nhận event / webhook
```

Run được thực thi trên một instance đã gọi `eng.Start`, trong giới hạn `Options.MaxWorkers` của instance đó.

**Dùng khi có bất kỳ điều kiện nào sau:**

- **Step chậm:** lời gọi có timeout dài, xử lý file, tạo báo cáo, bất cứ thứ gì không vừa timeout HTTP.
- **Chờ là một phần của luồng:** duyệt (`Await`), hẹn giờ (`Sleep`), callback bên ngoài (`SignalKey`), policy retry dài. Chạy inline thì run cũng sẽ trả `ErrRunWaiting`.
- **Không có người dùng nào đang chờ:** consumer xử lý event, cron job, batch backfill, migration dữ liệu.
- **Đột biến tải và backpressure:** hàng đợi hấp thụ đỉnh tải; `MaxWorkers` giới hạn số run chạy cùng lúc trên mỗi instance, bảo vệ pool database và các dịch vụ phía sau.
- **Cô lập:** phụ thuộc nặng (CPU, bộ nhớ, SDK lớn) hoặc tích hợp rủi ro không nên chia process, pool và độ trễ với API.
- **Gửi đi rồi thôi:** client chỉ cần biết công việc đã được nhận.

## Kết hợp: đưa vào hàng đợi, chờ một chút

Khi kết quả thường nhanh nhưng đôi lúc chậm, hãy đưa run vào hàng đợi rồi chờ một lúc ngắn. Khác với run inline có deadline ngắn, việc hết thời gian *chờ* không làm gián đoạn run: nó vẫn tiếp tục chạy trên worker.

```go
h, err := CompleteOrder.Start(r.Context(), eng, key, in)
if err != nil { ... 503 }

wctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
defer cancel()
out, err := h.Wait(wctx)
var re *saga.RunError
switch {
case err == nil:
	writeJSON(w, http.StatusCreated, out)            // xong trong 2 s
case errors.As(err, &re):
	writeJSON(w, http.StatusConflict, re)            // đã compensate hoặc cần can thiệp
case wctx.Err() != nil:                           // hết thời gian chờ, không phải run lỗi
	w.Header().Set("Location", "/runs/"+h.ID())      // vẫn đang chạy trên worker
	w.WriteHeader(http.StatusAccepted)
default:
	http.Error(w, "try again", http.StatusServiceUnavailable)
}
```

Cái giá: chậm hơn inline vài chục mili giây, và API phụ thuộc vào worker đang chạy để trả lời nhanh.

## Những gì luôn nằm ở tầng API

Dù chọn mô hình thực thi nào, các lời gọi này thuộc về API và đều rẻ:

| Endpoint | Lời gọi |
| --- | --- |
| Khởi chạy | `Workflow.Run` hoặc `Workflow.Start` với idempotency key từ client |
| Trạng thái / kết quả | `Workflow.Handle(eng, id).Status(ctx)`, hoặc `Wait` với timeout ngắn |
| Quyết định của người, webhook | `Engine.Signal`, `Engine.SignalKey` |
| Vận hành | `Engine.RetryCompensation` (chỉ cho admin) |

Không lời gọi nào cần `eng.Start`: engine chưa từng gọi `Start` vẫn enqueue run, gửi signal và đọc trạng thái được.

## Mô hình triển khai

### Một binary, mọi instance làm cả hai việc (mặc định)

Mọi instance phục vụ HTTP, chạy run inline và gọi `eng.Start`. Đây là cách đơn giản nhất và là cách example modulith dùng. Hãy bắt đầu từ đây.

### Tách vai trò API và worker

Cùng codebase, hai vai trò (một flag hoặc hai binary trong `cmd/`), cả hai đều định nghĩa cùng các workflow và step:

```go
eng, err := saga.New(pool, saga.Options{MaxWorkers: cfg.MaxWorkers})
if cfg.Role == "worker" || cfg.Role == "all" {
	if err := eng.Start(ctx); err != nil { ... } // thực thi run từ hàng đợi, run được đánh thức, run được khôi phục
}
defer eng.Stop(shutdownCtx)                      // luôn gọi, ở mọi vai trò
```

| | Instance API | Instance worker |
| --- | --- | --- |
| `eng.Start` | Không (hoặc có, với `MaxWorkers` nhỏ) | Có |
| Thực thi | Run inline mà nó khởi chạy hoặc gặp lại | Run từ hàng đợi, run đang park được đánh thức, run được khôi phục |
| `MaxWorkers` | Không có tác dụng khi không `Start` | Định cỡ theo năng lực của các dịch vụ phía sau |
| Cỡ pool | Số checkpoint inline đồng thời + signal/trạng thái | Checkpoint của `MaxWorkers` run + River (một connection `LISTEN`) |
| Mở rộng theo | Lượng request | Độ sâu hàng đợi, thời gian chạy của run |

Quy tắc khi tách:

- **Luôn phải có ít nhất một worker chạy.** Không có instance nào đã `Start` thì run trong hàng đợi không bao giờ bắt đầu, run đang park không bao giờ thức dậy, run bị crash không bao giờ được khôi phục. Run inline vẫn chạy được, nên vấn đề dễ bị che khuất.
- **Worker cần mọi thứ step cần**, và API cũng cần nếu nó chạy workflow inline.
- **Thứ tự deploy:** version workflow hay step mới phải tới worker trước khi API bắt đầu dùng. Trong rolling deploy, run mà `name@version` không được một instance biết sẽ được để lại cho instance khác.
- **Nhiều nhóm worker** (ví dụ một deployment riêng cho export nặng) hiện cần database hoặc schema riêng: mọi engine đã `Start` trên cùng database đều nhận bất kỳ run nào. Thay vào đó, hãy giới hạn việc nặng bằng `MaxWorkers` và timeout của từng step.

## Bảng quyết định

| Tình huống | Lựa chọn |
| --- | --- |
| Checkout, đặt chỗ, chuyển tiền: vài step nhanh, người dùng chờ kết quả | `Run` inline ở API, context tách khỏi request, dự phòng bằng 202 |
| Như trên, nhưng cổng thanh toán đôi khi chậm | Kết hợp: `Start` + `Wait` 1-2 s, dự phòng bằng 202 |
| Duyệt, KYC, bất cứ thứ gì có `Await` hay `Sleep` | `Start`, 202, endpoint trạng thái hoặc event |
| Xuất báo cáo, xử lý media, import hàng loạt | `Start` trên worker, `MaxWorkers` định cỡ theo tải |
| Phản ứng với event hay message | Consumer gọi `Start` (hoặc `Run` nếu ngắn), key lấy từ id của event |
| Batch hằng đêm trên nhiều thực thể | Một job gọi `Start` cho từng thực thể với key tất định; worker xử lý dần hàng đợi |

## Checklist

- [ ] Workflow và step được đăng ký trong mọi binary có thể thực thi chúng.
- [ ] Step lấy mọi thứ từ input và output của step, không lấy gì từ request.
- [ ] Run inline dùng context tách khỏi request với timeout rõ ràng, và trả 202 khi run phải chờ.
- [ ] Luôn có ít nhất một instance gọi `eng.Start`; mọi instance gọi `eng.Stop`.
- [ ] `MaxWorkers` và cỡ pool phù hợp với từng vai trò.
- [ ] Client gửi idempotency key và có endpoint để poll trạng thái.
