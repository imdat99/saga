# Long-running workflow example

[Tiếng Việt bên dưới](#ví-dụ-workflow-chạy-dài)

A document publication saga that lives for minutes, hours or days, across processes:

```
create_draft ─► Await "approval" ─► start_render ─► Await <render key> ─► Sleep (embargo) ─► publish
                (a person, timeout)  (external job)  (renderer calls back)  (durable timer)
```

| Wait | Primitive | Woken by |
| --- | --- | --- |
| Reviewer decision | `saga.Await[Approval](c, "approval", in.ApprovalTimeout)` | `eng.Signal(ctx, runID, "approval", ...)` from any process |
| External render job | step returning its `saga.Key` + `saga.Await[Rendered](c, string(ref), ...)` | the renderer calling `eng.SignalKey(ctx, ref, ...)`, knowing only the reference |
| Embargo before publication | `saga.Sleep(c, in.Embargo)` | a River job at a database time |

While a run waits it holds **no goroutine, no connection and no lease**: it is a row in `saga.run`. Any process that called `eng.Start` resumes it, replaying the steps already done.

What the example shows on purpose:

- **Durations in the input.** `ApprovalTimeout`, `RenderTimeout` and `Embargo` are fields of `PublishIn`, not flags read by the workflow: code between steps must be deterministic, and the process resuming a run days later may have another configuration.
- **The step key as external reference.** `start_render` returns its own key; the renderer stores it and answers with `SignalKey`. Duplicated callbacks are ignored (the first signal wins).
- **Compensation of each wait:** rejected or timed-out approval discards the draft; a render timeout also cancels the render job; `WithRunTimeout(7 days)` bounds the whole run.
- **Compensators that work by key** (`created_by` column, `idem.Abort`), so they are correct when the step's outcome is uncertain.

## Run it

```bash
make up                                           # Postgres from docker-compose.yml (or set SAGA_DSN)
go run ./examples/longrunning demo                # everything in one process: published ...
go run ./examples/longrunning demo -reject        # run ended compensated: rejected by alice
```

Across processes, as in production:

```bash
# terminal 1: a worker (resumes runs, runs the fake renderer); start several to see them share the work
go run ./examples/longrunning worker

# terminal 2
RUN=$(go run ./examples/longrunning start -doc doc-1 -embargo 1m)   # prints the run id, exits
go run ./examples/longrunning status -run $RUN                       # waiting, on saga.await:approval
go run ./examples/longrunning approve -run $RUN -by bob              # any time before -approval (10m)
go run ./examples/longrunning status -run $RUN                       # ... saga.sleep waiting, then done
```

Things to try:

- Kill the worker with `kill -9` while the run sleeps, start another one: it finishes the run (the sleep is not restarted; its wake-up time is recorded).
- Approve before the worker even started: the signal is kept and consumed when the run reaches `Await`.
- `start -approval 5s` and do not approve: the run is compensated with `saga.ErrAwaitTimeout` and the draft is discarded.
- `approve -reject -note "typo in title"`: compensated, `rejected by ...`.

Inspect what is stored:

```sql
SELECT id, status, wake_at, await_name FROM saga.run WHERE workflow = 'longrun.publish_document';
SELECT * FROM longrun.document;
SELECT * FROM longrun.render_job;
```

See also: [guide, chapter 5](../../docs/guide/05-waiting.md) and [patterns](../../docs/patterns.md).

---

# Ví dụ workflow chạy dài

Một saga xuất bản tài liệu kéo dài vài phút, vài giờ hay vài ngày, qua nhiều process:

```
create_draft ─► Await "approval" ─► start_render ─► Await <key render> ─► Sleep (embargo) ─► publish
                (người duyệt, timeout) (job bên ngoài) (renderer gọi lại)   (timer bền vững)
```

| Chờ | Công cụ | Được đánh thức bởi |
| --- | --- | --- |
| Quyết định của người duyệt | `saga.Await[Approval](c, "approval", in.ApprovalTimeout)` | `eng.Signal(ctx, runID, "approval", ...)` từ bất kỳ process nào |
| Job render bên ngoài | step trả về chính `saga.Key` của nó + `saga.Await[Rendered](c, string(ref), ...)` | renderer gọi `eng.SignalKey(ctx, ref, ...)`, chỉ cần biết mã tham chiếu |
| Thời gian chờ trước khi công bố | `saga.Sleep(c, in.Embargo)` | job River hẹn theo giờ database |

Trong lúc chờ, run **không giữ goroutine, connection hay lease**: nó chỉ là một dòng trong `saga.run`. Bất kỳ process nào đã gọi `eng.Start` cũng tiếp tục được nó, replay các step đã xong.

Những điều ví dụ cố ý thể hiện:

- **Thời lượng nằm trong input.** `ApprovalTimeout`, `RenderTimeout` và `Embargo` là field của `PublishIn`, không phải flag workflow tự đọc: code giữa các step phải tất định, và process tiếp tục run vài ngày sau có thể có cấu hình khác.
- **Key của step làm mã tham chiếu bên ngoài.** `start_render` trả về key của chính nó; renderer lưu lại và trả lời bằng `SignalKey`. Callback trùng lặp bị bỏ qua (signal đầu tiên thắng).
- **Compensation cho từng kiểu chờ:** bị từ chối hoặc hết hạn duyệt thì bản nháp bị huỷ; hết hạn render thì job render cũng bị huỷ; `WithRunTimeout(7 ngày)` giới hạn cả run.
- **Compensator làm việc theo key** (cột `created_by`, `idem.Abort`), nên vẫn đúng khi kết quả của step không chắc chắn.

## Chạy

```bash
make up                                           # Postgres từ docker-compose.yml (hoặc đặt SAGA_DSN)
go run ./examples/longrunning demo                # tất cả trong một process: published ...
go run ./examples/longrunning demo -reject        # run ended compensated: rejected by alice
```

Qua nhiều process, như trên production:

```bash
# terminal 1: một worker (tiếp tục run, chạy renderer giả); chạy vài worker để thấy chúng chia việc
go run ./examples/longrunning worker

# terminal 2
RUN=$(go run ./examples/longrunning start -doc doc-1 -embargo 1m)   # in run id rồi thoát
go run ./examples/longrunning status -run $RUN                       # waiting, ở saga.await:approval
go run ./examples/longrunning approve -run $RUN -by bob              # bất kỳ lúc nào trước -approval (10m)
go run ./examples/longrunning status -run $RUN                       # ... saga.sleep waiting, rồi done
```

Thử thêm:

- `kill -9` worker khi run đang sleep, rồi chạy worker khác: nó hoàn tất run (không ngủ lại từ đầu; thời điểm thức dậy đã được ghi).
- Duyệt trước cả khi worker chạy: signal được giữ lại và dùng khi run tới `Await`.
- `start -approval 5s` và không duyệt: run được compensate với `saga.ErrAwaitTimeout`, bản nháp bị huỷ.
- `approve -reject -note "sai tiêu đề"`: compensated, `rejected by ...`.

Xem dữ liệu được lưu:

```sql
SELECT id, status, wake_at, await_name FROM saga.run WHERE workflow = 'longrun.publish_document';
SELECT * FROM longrun.document;
SELECT * FROM longrun.render_job;
```

Xem thêm: [hướng dẫn, chương 5](../../docs/huong-dan/05-cho-doi.md) và [patterns](../../docs/patterns.md).
