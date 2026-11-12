# 4. Khởi chạy và theo dõi run

## Chọn idempotency key

Mỗi run có một idempotency key duy nhất. Khởi chạy workflow với key đã có run không bao giờ tạo run thứ hai. Chọn key thể hiện ý định nghiệp vụ, không phải lần thử:

| Tình huống | Key |
| --- | --- |
| HTTP request có header `Idempotency-Key` | `"checkout:" + header` |
| Mỗi thực thể nghiệp vụ một saga | `"order.complete:" + orderID` |
| Consumer xử lý message | `"invoice:" + event.ID` |
| Run con do một step khởi chạy | `string(k) + ":child"` (k là key của step) |

Thêm tiền tố workflow hoặc domain vào key: key đã được workflow khác dùng sẽ lỗi `saga.ErrIdemKeyConflict`. Khử trùng lặp kéo dài chừng nào run còn được giữ (`Options.Retention`); sau khi bị purge, cùng key sẽ tạo run mới.

Nên dùng cách nào trong HTTP API, consumer hay worker, và cách tách process API với worker: [chương 11](11-api-hay-worker.md).

## Ba cách khởi chạy

### `Workflow.Run`: chạy inline

```go
out, err := CompleteOrder.Run(ctx, eng, key, in)
```

Chạy trong goroutine của người gọi và trả kết quả, trừ khi run phải chờ. Độ trễ thấp nhất. Chạy được khi chưa gọi `eng.Start`, nhưng khi đó không có gì khôi phục run bị bỏ dở do crash, và run đã park sẽ không bao giờ chạy tiếp: hãy gọi `Start` trên ít nhất một instance (thường là tất cả).

`Run` trả về:

| Kết quả | Ý nghĩa | Cần làm |
| --- | --- | --- |
| `out, nil` | Run đã `done` (vừa xong hoặc từ trước) | Dùng `out` |
| `*saga.RunError` với `StatusCompensated` | Workflow lỗi, hiệu ứng đã hoàn tác | Báo `re.Cause` |
| `*saga.RunError` với `StatusNeedsAttention` | Compensation bỏ cuộc hoặc mất tính tất định | Cảnh báo; sửa; `RetryCompensation` |
| `saga.ErrRunWaiting` (được bọc) | Đang park: chờ retry, Sleep, Await, lock bận | `Lookup` + `Handle.Wait`, hoặc trả lời "đã nhận" |
| `saga.ErrRunInProgress` | Người gọi hoặc instance khác đang chạy nó | `Lookup` + `Handle.Wait` |
| `saga.ErrIdemKeyConflict` | Key thuộc workflow khác | Sửa cách đặt key |
| `saga.ErrWorkflowNotRegistered` | `name@version` của run không có ở binary này | Rolling deploy; instance khác sẽ tiếp tục |
| lỗi khác (DB, `ErrLeaseLost`, context) | Bị gián đoạn | Gọi lại `Run` với cùng key sau; recovery cũng sẽ tiếp tục nó |

`ctx` của người gọi giới hạn việc chạy inline: nếu nó bị huỷ (client ngắt kết nối, hết deadline), run bị gián đoạn và được recovery tiếp tục sau khi lease hết hạn (`LeaseTTL`). Nếu việc cần làm xong kể cả khi người gọi bỏ đi, hãy chạy tách khỏi context của request:

```go
runCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 30*time.Second)
defer cancel()
out, err := CompleteOrder.Run(runCtx, eng, key, in)
```

### `Workflow.Start`: đưa vào hàng đợi

```go
h, err := CompleteOrder.Start(ctx, eng, key, in) // tạo và enqueue trong cùng một transaction
fmt.Println(h.ID())
out, err := h.Wait(ctx)                           // tuỳ chọn
```

Bất kỳ instance nào đã `Start` sẽ thực thi nó (`Options.MaxWorkers` mỗi instance). Dùng cho việc không nên chạy trong goroutine của request, hoặc để dàn tải. Với key đã có run, `Start` trả handle tới run đó.

### `Workflow.Lookup` và `Workflow.Handle`

```go
h, err := CompleteOrder.Lookup(ctx, eng, key)   // theo idempotency key (ErrRunNotFound nếu không có)
h := CompleteOrder.Handle(eng, runID)           // theo run id (chưa kiểm tra cho tới khi dùng)

st, err := h.Status(ctx)   // running, waiting, compensating, done, compensated, needs_attention
out, err := h.Wait(ctx)    // chờ tới trạng thái kết thúc hoặc ctx kết thúc
```

`Wait` poll Postgres (20 ms, gấp đôi dần tới 1 s). Nhiều người chờ đồng thời tốn mỗi người một lần đọc theo khoá chính mỗi chu kỳ; với việc chờ lâu, nên phản ứng theo event (chương 7) hoặc tự poll `Status`.

## Trạng thái

```
running ──► done
   │  ▲
   ▼  │ (thức dậy, signal, retry)
waiting
   │
   ▼ (workflow lỗi, quá deadline)
compensating ──► compensated
   │
   ▼ (hết lượt compensation, mất tính tất định)
needs_attention ──(RetryCompensation)──► compensating
```

## Tích hợp HTTP

Một endpoint điển hình với header `Idempotency-Key` (example modulith cài đặt điều này trong `internal/api`):

```go
func (s *Server) postOrder(w http.ResponseWriter, r *http.Request) {
	key := "checkout:" + r.Header.Get("Idempotency-Key")
	var in flows.CompleteOrderInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil { ... 400 }

	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 20*time.Second)
	defer cancel()
	out, err := s.flows.CompleteOrder.Run(ctx, s.eng, key, in)

	var re *saga.RunError
	switch {
	case err == nil:
		writeJSON(w, http.StatusCreated, out)
	case errors.As(err, &re):
		writeJSON(w, http.StatusConflict, map[string]string{"run": re.RunID, "status": string(re.Status), "cause": re.Cause})
	case errors.Is(err, saga.ErrRunWaiting), errors.Is(err, saga.ErrRunInProgress):
		h, err := s.flows.CompleteOrder.Lookup(r.Context(), s.eng, key)
		if err != nil {
			http.Error(w, "try again", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Location", "/runs/"+h.ID())
		w.WriteHeader(http.StatusAccepted) // client poll GET /runs/{id}
	default:
		http.Error(w, "try again", http.StatusServiceUnavailable) // cùng key sẽ tiếp tục run
	}
}
```

Client gọi lại với cùng `Idempotency-Key` khi lỗi mạng hoặc 503: nó sẽ gặp lại đúng run đó.

## Khôi phục thủ công: needs_attention

Run ở `needs_attention` đã dừng vì compensation không thể hoàn tất (hoặc workflow mất tính tất định). Nó giữ lock tới hết TTL và không làm gì cho tới khi có người can thiệp:

1. Tìm nó: `SELECT id, workflow, error FROM saga.run WHERE status = 'needs_attention'`, hoặc subscribe event `saga.needs_attention` (chương 7).
2. Đọc nguyên nhân (`saga.run.error`, step lỗi trong `saga.step` có trạng thái `compensation_failed`).
3. Sửa nguyên nhân (deploy compensator đã sửa, sửa dữ liệu trong module, khôi phục dịch vụ bên ngoài).
4. `err := eng.RetryCompensation(ctx, runID)`: các compensator lỗi được cấp lại lượt thử và run được enqueue; `Handle.Wait` trả kết quả.

`RetryCompensation` trả `saga.ErrNotNeedsAttention` nếu run không ở trạng thái đó.
