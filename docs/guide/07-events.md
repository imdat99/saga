# 7. Events and run-end hooks

Steps call modules directly. Other parts of the system (mail, invoicing in another service, analytics) often need to **react after a saga succeeded**: "order placed". The engine does not deliver events itself; it gives you the one hook that makes publishing them correct.

## Why not publish from a step

A step runs **at least once**, and a run that completed a step can still be **compensated** later. An event published from inside a step can therefore:

- go out twice (the step is re-executed after a crash or retry);
- go out for an order that was then rolled back (a later step failed, the run compensated).

Publishing after `Run` returns does not help either: a crash between the commit and the publish loses the event, and runs that finish on another instance (after a retry or takeover) never return to your caller.

## The pattern: stage in the step, release when the run is done

1. **Stage.** Inside the step, the module writes the event in **its own transaction**, together with its business change, into a staging table, tagged with the run id (`saga.RunIDFrom(ctx)`). Give the event a deterministic id derived from the step key, so a re-executed step does not stage it twice.
2. **Release or drop.** Implement `saga.RunHook`. `RunEnded` is called **inside the transaction that records the run's terminal status**, under the run's lease: a `done` run moves its staged events to an outbox, a `compensated` run deletes them. Both commit atomically with the status, so a rolled-back run never publishes anything.
3. **Relay.** A loop reads the outbox and publishes to your broker (at least once: consumers deduplicate by event id), then marks the rows sent.

Outside a saga (`RunIDFrom` returns false), the module writes straight to the outbox in its transaction. Compensators are outside too: what they emit ("order cancelled") is published even though the run is compensated.

The complete integration (engine contract, schema, hook, relay, broker notes, consumers, operations, tests) is in [chapter 12](12-event-integration.md); its code, tested in CI, is in [examples/outbox](../../examples/outbox).

Rules for hooks:

- `RunEnded` runs on the engine's hot path, in the terminal transaction: keep it to a few indexed statements. With any hook, ending a run costs a transaction instead of one statement.
- Returning an error rolls the terminal transition back, with everything the hook wrote; the run is resumed later and the hook called again from a clean state.
- Never call the network from a hook (no broker, no HTTP): it holds the run's row lock. Publishing is the relay's job.
- A `needs_attention` run is a good place to emit an alert event (`run.Error` holds the cause).

## Choosing the broker

The engine knows no broker: the relay is yours, and so is the choice (Kafka, NATS, RabbitMQ, Redis streams or queues). Whatever it is, keep the two properties above: events are written in the business transaction (never published directly from a step), and released by the hook in the run's terminal transaction. Changing broker later only changes the relay.
