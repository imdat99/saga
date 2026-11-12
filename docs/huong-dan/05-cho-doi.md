# 5. Chờ đợi: Sleep, Await, signal, lock

## Park

Mỗi khi run phải chờ (backoff của retry, timer, signal, lock bận, backoff của compensation), engine **park** nó: trạng thái `waiting` (với backoff của compensation thì là `compensating` không có owner), nhả lease, không giữ goroutine hay connection. Một job River hẹn giờ theo giờ database (hoặc chính signal) đánh thức nó trên bất kỳ instance nào đã `Start`; instance đó chạy lại workflow từ đầu với replay. Một run đang park tốn một dòng trong `saga.run`, cộng một dòng trong `saga.river_job` khi có thời điểm thức dậy (`Await` không timeout trên run không có deadline thì không có: chỉ signal mới đánh thức nó), nên hàng triệu run đang chờ vẫn ổn.

Bên trong workflow, mọi lệnh chờ trả lỗi bọc `saga.ErrRunWaiting` ở lần đầu; hãy trả nó nguyên vẹn. Bên ngoài, `Workflow.Run` trả `ErrRunWaiting`; dùng `Handle.Wait` để lấy kết quả. Các khoảng chờ bền vững có độ chính xác khoảng 5 giây (scheduler của River).

## Sleep

```go
if err := saga.Sleep(c, 24*time.Hour); err != nil {
	return Out{}, err
}
// một ngày sau, có thể trên instance khác
_, err := SendReminder.Run(c, in.UserID)
```

Thời điểm thức dậy được ghi lại (giờ database), nên replay không ngủ lại. `d <= 0` trả về ngay. `d` phải được tính tất định (từ input, output của step, hoặc `saga.Now`).

## Await và signal

`Await[T]` chờ một signal có tên và trả về payload của nó:

```go
approval, err := saga.Await[Approval](c, "manager-approval", 72*time.Hour)
if errors.Is(err, saga.ErrAwaitTimeout) {
	return Out{}, err          // 72 giờ không có trả lời: compensate
}
if err != nil {
	return Out{}, err          // gồm cả ErrRunWaiting
}
if !approval.OK {
	return Out{}, saga.Permanent(errors.New("rejected"))
}
```

Ai đó gửi signal kèm run id:

```go
err := eng.Signal(ctx, runID, "manager-approval", Approval{OK: true, By: "alice"})
```

- Signal gửi **trước** khi run tới `Await` được giữ lại và dùng ngay khi tới.
- Signal **đầu tiên** của một tên thắng; các signal sau bị bỏ qua, nên bên gửi được phép gửi lại thoải mái.
- Await cùng một tên hai lần sẽ trả lại payload đầu tiên. Để chờ nhiều câu trả lời, dùng nhiều tên (`"approval:1"`, `"approval:2"`).
- Payload không decode được vào `T` làm `Await` thất bại vĩnh viễn.
- `timeout <= 0` chờ mãi (tới deadline của run, nếu có).

### SignalKey: phản hồi từ hệ thống bên ngoài

Với việc chạy bên ngoài process (dịch vụ render, ngân hàng, API đối tác có callback), hãy đưa **key của step** cho hệ thống bên ngoài làm mã tham chiếu và để nó trả lời bằng `SignalKey`; nó không cần biết gì khác:

```go
var StartRender = saga.DefineStep("media.render.start",
	func(ctx context.Context, k saga.Key, in RenderIn) (saga.Key, string, error) {
		jobID, err := renderer.Submit(ctx, string(k), in) // mã tham chiếu = key
		return k, jobID, err
	},
	func(ctx context.Context, k saga.Key, _ string) error { return renderer.Cancel(ctx, string(k)) })

// workflow
ref, err := StartRender.Run(c, in)
if err != nil {
	return Out{}, err
}
res, err := saga.Await[RenderResult](c, string(ref), 2*time.Hour)

// webhook handler
err := eng.SignalKey(ctx, saga.Key(req.Reference), RenderResult{URL: req.URL})
```

[Patterns](../patterns.md) có vòng lặp timeout-rồi-thử-lại cho trường hợp này.

## Business lock

`saga.Lock` cho run quyền dùng độc quyền một khoá nghiệp vụ (giỏ hàng, tài khoản) trong phần còn lại của run:

```go
if err := saga.Lock(c, "cart:"+in.CartID, 5*time.Minute); err != nil {
	return Out{}, err  // ErrLockHeld: một checkout khác của giỏ này đang chạy
}
```

- Được nhả khi run `done` hoặc `compensated`.
- Được heartbeat của lease gia hạn khi run đang chạy.
- Khi run đang park hoặc đã chết, lock hết hạn sau `ttl`: `ttl` giới hạn thời gian một run đang chờ có thể chặn người khác. Nếu run khác lấy lock trong lúc đó, run đầu thất bại với `saga.ErrLockLost` khi chạy tiếp, và compensate.
- `ttl` được nâng lên ít nhất bằng `Options.LeaseTTL`.

Mặc định lock bận làm step thất bại ngay với `saga.ErrLockHeld` (vĩnh viễn). Để chờ thay vì thất bại:

```go
err := saga.Lock(c, "account:"+in.AccountID, time.Minute,
	saga.LockWait(saga.Exponential(10, time.Second)))  // park và thử lại cho tới khi hết policy
```

Lock được poll, không xếp hàng: không công bằng và không phát hiện deadlock. Khi một run cần nhiều lock, hãy lấy theo thứ tự cố định (ví dụ key đã sắp xếp).

## Chọn công cụ

| Nhu cầu | Dùng |
| --- | --- |
| Thử lại một lời gọi lỗi sau | `WithRetry` trên step |
| Chờ một khoảng cố định | `Sleep` |
| Chờ người hoặc dịch vụ khác | `Await` + `Signal` |
| Chờ job bên ngoài có callback | step khởi động job + `Await` trên key của step + `SignalKey` |
| Không cho hai run cùng đụng một thực thể | `Lock` (thất bại ngay hoặc `LockWait`) |
| Bỏ cuộc sau tổng thời gian nhất định | `WithRunTimeout` trên workflow |
