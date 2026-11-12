# Multi-workflow example

[Tiếng Việt bên dưới](#ví-dụ-nhiều-workflow)

Several workflows working together: an order (parent) fans out one shipment (child) per warehouse; the children run in parallel, each as its own run; parent and children talk with signals in both directions, in two phases.

```
multi.order (parent)                          multi.shipment (child, one run per warehouse)
 ├─ start_shipment ×N  ───Start─────────────► pick (reversible)
 ├─ Await "ready:<wh>" ×N ◄──signal────────── signal parent "ready:<wh>"
 ├─ charge (reversible)                        Await "decision"
 ├─ signal "decision" ×N  ──────────────────► dispatch (irreversible, only after commit)
 └─ Await "shipped:<wh>" ×N ◄──signal──────── signal parent "shipped:<wh>"
```

## Techniques shown

| Technique | Where |
| --- | --- |
| **Fan-out**: start child runs from a step | `multi.start_shipment` calls `shipment.Start`, with the **step key as the child's idempotency key**: a re-executed step joins the same child instead of starting a second one |
| **Parallel children** | each child is its own run, executed by any started engine; the parent only waits |
| **Fan-in**: wait for every child | the parent `Await`s `ready:<wh>` and `shipped:<wh>` in a sorted order; signals that arrive before their `Await` are kept |
| **Child to parent** | the child gets the parent's run id in its input (`c.RunID()` of the parent) and signals it |
| **Parent to child** | the parent signals `decision` to each child run id (the output of `start_shipment`) |
| **Signals as steps** | `multi.signal` sends a signal from a step: recorded, so not repeated on replay |
| **Two-phase protocol** | children only do reversible work (`pick`) until the parent commits; the parent's last reversible step (`charge`) runs before the commit |
| **Cascading compensation** | compensating `start_shipment` looks the child up by key and signals it to abort; the child then compensates its own `pick` |
| **Failure reporting** | a child reports a recorded step failure (`*saga.StepError`, identical on replay) to the parent before compensating; parking and lease loss are not reported |
| **Determinism** | lines are grouped in a map, but warehouses are **sorted** before steps are called; all timeouts are in the input |

Timeouts close every gap: the parent gives up if a child is not ready in time (`ReadyTimeout`), a child gives up if no decision comes (`DecisionTimeout`, set longer than the parent's `ReadyTimeout`), and both have a run deadline.

The commit is the point of no return: after it, a failing child retries `dispatch` until its policy runs out and then needs attention; nothing is compensated. That is the usual two-phase trade-off: keep everything before the commit reversible.

## Run it

```bash
make up
go run ./examples/multiworkflow demo                      # order done, tracking for wh-a, wh-b, wh-c
go run ./examples/multiworkflow demo -out-of-stock wh-b   # wh-b fails: parent and all children compensated
go run ./examples/multiworkflow demo -decline             # all ready, payment declined: all children aborted
```

The demo prints every run of the order (the parent and its children), the stock left and the payment. With `-out-of-stock wh-b`:

```
order compensated: warehouse wh-b: step multi.pick: out of stock: sku-... at wh-b
runs:
  multi.order          compensated  warehouse wh-b: step multi.pick: out of stock ...
  multi.shipment wh-a  compensated  aborted by the order: order failed
  multi.shipment wh-b  compensated  step multi.pick: out of stock ...
  multi.shipment wh-c  compensated  aborted by the order: order failed
stock left (started with 10, or 0 for an out-of-stock warehouse):
  wh-a 10
  wh-b 0
  wh-c 10
```

Find the children of a parent in SQL (their keys are the parent's step keys):

```sql
SELECT workflow, status, input->>'warehouse', error FROM saga.run
WHERE id = $1 OR idem_key LIKE $1 || ':%';
```

## When to use child workflows

- **Use them** for parallel branches that each have several steps, their own waits or their own retries, or that are useful on their own (a shipment can be retried or inspected separately).
- **Do not** for a single call: a step is simpler. Parallel calls without their own lifecycle can be made inside one step.
- A child started with `Start` must report back by signal; a child run inline with `Run` from a step blocks that step and returns `ErrRunWaiting` if it parks, so only do that for children that never wait.

See also: [guide, chapter 3 (nested runs)](../../docs/guide/03-workflows.md#nested-runs), [chapter 5 (signals)](../../docs/guide/05-waiting.md).

---

# Ví dụ nhiều workflow

Nhiều workflow phối hợp với nhau: một đơn hàng (workflow cha) tách thành một lô giao hàng (workflow con) cho mỗi kho; các workflow con chạy song song, mỗi cái là một run riêng; cha và con trao đổi signal theo cả hai chiều, qua hai pha.

```
multi.order (cha)                             multi.shipment (con, một run cho mỗi kho)
 ├─ start_shipment ×N  ───Start─────────────► pick (hoàn tác được)
 ├─ Await "ready:<kho>" ×N ◄──signal───────── signal cha "ready:<kho>"
 ├─ charge (hoàn tác được)                     Await "decision"
 ├─ signal "decision" ×N  ──────────────────► dispatch (không hoàn tác được, chỉ sau commit)
 └─ Await "shipped:<kho>" ×N ◄──signal─────── signal cha "shipped:<kho>"
```

## Các kỹ thuật được minh hoạ

| Kỹ thuật | Ở đâu |
| --- | --- |
| **Fan-out**: khởi chạy run con từ một step | `multi.start_shipment` gọi `shipment.Start`, dùng **key của step làm idempotency key của run con**: step chạy lại sẽ gặp lại đúng run con đó thay vì tạo run thứ hai |
| **Run con song song** | mỗi run con là một run riêng, do bất kỳ engine nào đã `Start` thực thi; cha chỉ chờ |
| **Fan-in**: chờ mọi run con | cha `Await` `ready:<kho>` và `shipped:<kho>` theo thứ tự đã sắp xếp; signal tới trước `Await` tương ứng được giữ lại |
| **Con báo cha** | run con nhận run id của cha trong input (`c.RunID()` của cha) và gửi signal cho nó |
| **Cha báo con** | cha gửi signal `decision` tới run id của từng con (output của `start_shipment`) |
| **Gửi signal bằng step** | `multi.signal` gửi signal từ một step: được ghi lại, nên không lặp lại khi replay |
| **Giao thức hai pha** | run con chỉ làm việc hoàn tác được (`pick`) cho tới khi cha commit; step hoàn tác được cuối cùng của cha (`charge`) chạy trước commit |
| **Compensation lan truyền** | compensate `start_shipment` sẽ tìm run con theo key và gửi signal huỷ; run con sau đó tự compensate `pick` của nó |
| **Báo lỗi** | run con báo cho cha một lỗi step đã được ghi (`*saga.StepError`, giống hệt khi replay) trước khi compensate; park và mất lease thì không báo |
| **Tính tất định** | các dòng hàng được gom trong map, nhưng danh sách kho được **sắp xếp** trước khi gọi step; mọi timeout nằm trong input |

Timeout lấp mọi khoảng trống: cha bỏ cuộc nếu một run con không sẵn sàng kịp (`ReadyTimeout`), run con bỏ cuộc nếu không nhận được quyết định (`DecisionTimeout`, đặt dài hơn `ReadyTimeout` của cha), và cả hai đều có deadline cho run.

Commit là điểm không quay lại: sau đó, run con lỗi sẽ retry `dispatch` tới hết policy rồi chuyển sang needs_attention; không có gì bị compensate. Đây là đánh đổi thường thấy của hai pha: mọi thứ trước commit phải hoàn tác được.

## Chạy

```bash
make up
go run ./examples/multiworkflow demo                      # đơn hoàn tất, có mã vận đơn cho wh-a, wh-b, wh-c
go run ./examples/multiworkflow demo -out-of-stock wh-b   # wh-b lỗi: cha và mọi run con được compensate
go run ./examples/multiworkflow demo -decline             # mọi kho sẵn sàng, thanh toán bị từ chối: mọi run con bị huỷ
```

Demo in ra mọi run của đơn hàng (cha và các con), tồn kho còn lại và trạng thái thanh toán. Xem các run con của một run cha bằng SQL (key của chúng là key step của cha):

```sql
SELECT workflow, status, input->>'warehouse', error FROM saga.run
WHERE id = $1 OR idem_key LIKE $1 || ':%';
```

## Khi nào dùng workflow con

- **Nên dùng** cho các nhánh song song có nhiều step, có kiểu chờ hay retry riêng, hoặc có ý nghĩa riêng (một lô giao hàng có thể được thử lại hay kiểm tra riêng).
- **Không nên** cho một lời gọi đơn lẻ: một step đơn giản hơn. Các lời gọi song song không có vòng đời riêng có thể thực hiện bên trong một step.
- Run con khởi chạy bằng `Start` phải báo lại bằng signal; run con chạy inline bằng `Run` từ một step sẽ chặn step đó và trả `ErrRunWaiting` nếu nó park, nên chỉ làm vậy với run con không bao giờ chờ.

Xem thêm: [hướng dẫn, chương 3 (run lồng nhau)](../../docs/huong-dan/03-workflow.md#run-lồng-nhau), [chương 5 (signal)](../../docs/huong-dan/05-cho-doi.md).
