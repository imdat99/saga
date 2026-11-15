# 7. Event và hook khi run kết thúc

Step gọi thẳng vào module. Các phần khác của hệ thống (gửi mail, xuất hoá đơn ở service khác, thống kê) thường cần **phản ứng sau khi saga thành công**: "đơn hàng đã đặt". Engine không tự giao event; nó cung cấp đúng một hook giúp việc phát event trở nên đúng đắn.

## Vì sao không phát event từ step

Step chạy **ít nhất một lần**, và một run đã xong một step vẫn có thể bị **compensate** về sau. Vì vậy một event phát từ bên trong step có thể:

- bị phát hai lần (step chạy lại sau crash hoặc retry);
- được phát cho một đơn hàng sau đó bị hoàn tác (một step sau lỗi, run bị compensate).

Phát sau khi `Run` trả về cũng không giải quyết được: crash giữa lúc commit và lúc phát làm mất event, và các run kết thúc trên instance khác (sau retry hoặc takeover) không bao giờ quay lại chỗ gọi.

## Cách làm: giữ lại trong step, thả khi run done

1. **Giữ lại (stage).** Bên trong step, module ghi event trong **transaction của chính nó**, cùng với thay đổi nghiệp vụ, vào một bảng staging, gắn run id (`saga.RunIDFrom(ctx)`). Đặt id tất định cho event, suy ra từ key của step, để step chạy lại không ghi event hai lần.
2. **Thả hoặc bỏ.** Cài đặt `saga.RunHook`. `RunEnded` được gọi **bên trong transaction ghi trạng thái cuối của run**, dưới lease của run: run `done` chuyển các event đã giữ sang outbox, run `compensated` xoá chúng. Cả hai commit nguyên tử cùng trạng thái, nên run bị hoàn tác không bao giờ phát gì.
3. **Relay.** Một vòng lặp đọc outbox và phát sang broker của bạn (ít nhất một lần: consumer khử trùng lặp theo id của event), rồi đánh dấu các dòng đã gửi.

Bên ngoài saga (`RunIDFrom` trả false), module ghi thẳng vào outbox trong transaction của nó. Compensator cũng nằm ngoài: event nó phát ("order cancelled") vẫn được phát dù run bị compensate.

Phần tích hợp đầy đủ (cam kết của engine, schema, hook, relay, lưu ý theo broker, consumer, vận hành, kiểm thử) nằm ở [chương 12](12-tich-hop-event-bus.md); code của nó, được kiểm thử trong CI, nằm ở [examples/outbox](../../examples/outbox).

Quy tắc cho hook:

- `RunEnded` nằm trên đường nóng của engine, trong transaction cuối: giữ ở mức vài câu lệnh có index. Có bất kỳ hook nào thì kết thúc một run tốn một transaction thay vì một câu lệnh.
- Trả lỗi sẽ rollback bước chuyển trạng thái cuối, cùng mọi thứ hook đã ghi; run được tiếp tục sau và hook được gọi lại từ trạng thái sạch.
- Không gọi mạng trong hook (không broker, không HTTP): nó đang giữ row lock của run. Phát event là việc của relay.
- Run `needs_attention` là chỗ tốt để phát một event cảnh báo (`run.Error` chứa nguyên nhân).

## Chọn broker

Engine không biết broker nào: relay là của bạn, và lựa chọn cũng vậy (Kafka, NATS, RabbitMQ, Redis stream hoặc queue). Dù là gì, hãy giữ hai tính chất ở trên: event được ghi trong transaction nghiệp vụ (không bao giờ phát thẳng từ step), và được hook thả trong transaction cuối của run. Đổi broker về sau chỉ đổi phần relay.
