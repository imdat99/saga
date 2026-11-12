# Concepts

## Run

A run is one execution of a workflow for one idempotency key (`saga.run`, unique `idem_key`). Calling `Workflow.Run` or `Workflow.Start` again with the same key joins the existing run: a finished run returns its stored outcome, an abandoned one is resumed, a live one reports `ErrRunInProgress`.

Statuses: `running`, `waiting` (parked), `compensating`, and the terminal `done`, `compensated`, `needs_attention`.

## Step and Key

A step is declared once (`DefineStep`) with an invoke function and an optional compensator. Inside a workflow, `Step.Run` either executes it or, when the run is replayed, returns the recorded output.

Each invocation gets a `saga.Key` = `runID:seq:stepName`. The key is the same on every re-execution of that position (crash, retry), so the module that performs the effect deduplicates on it (`idem.Do`). Steps run **at least once**; with the key, effects happen **once**.

Outputs and compensation data are stored as JSON. Keep them to IDs.

## Replay and determinism

A resumed run executes the workflow function again from the top. Completed steps return their recorded outputs without running, so the function reaches the point where it stopped. Therefore the code between steps must be deterministic: branch only on inputs and step outputs. Use `saga.Now(c)` and `saga.Rand(c)` (checkpointed) instead of `time.Now()` and random numbers. Calling a different step at a recorded position is non-determinism: the run is parked as `needs_attention`.

Change the sequence of steps only with a new workflow version (`DefineWorkflow(name, version, ...)`), and keep the old version registered until its runs finish.

## Lease and fencing

The instance executing a run holds its lease (`owner_id`, `lease_until`, `lease_version`). A heartbeat renews the leases of all of an instance's runs every `LeaseTTL/3` in one statement. Every checkpoint locks the run row and checks owner and version (`store.Fenced`, or a single statement doing the same on the step hot path), so an instance that lost its lease (pause, partition) cannot write anymore; its run context is cancelled with `ErrLeaseLost`.

## Parking

A run that has to wait (retry backoff, `Sleep`, `Await`, a busy `Lock` with `LockWait`, compensation backoff) is parked: it releases its lease and a River job scheduled at a database time resumes it on any instance. Parked runs cost one row, no goroutine. `Workflow.Run` returns `ErrRunWaiting` for them; `Handle.Wait` waits for the outcome.

## Compensation

When the workflow function returns an error, the run becomes `compensating` and the compensators of completed steps run in reverse order, with their recorded compensation data and the original `Key`. A step whose outcome is unknown (crash mid-step, timed-out attempt) is compensated too, with `CompensationUncertain(ctx) == true`; the module checks by key (`idem.Abort`). Compensators retry durably (`WithCompensatePolicy`); when they give up, the run is `needs_attention`, an event is emitted, and `Engine.RetryCompensation` resumes it after a fix.

Compensators are found by step name, so recovery can compensate without the workflow function.

## Signals

`Engine.Signal(runID, name, payload)` stores a signal (first one wins) and wakes a run parked in `Await(c, name, timeout)`. A signal sent before the run reaches `Await` is kept. `Engine.SignalKey(key, payload)` addresses the run by a step `Key`, for external systems that only know the reference a step gave them.

## Locks

`saga.Lock(c, key, ttl)` takes a business lock for the rest of the run, released when the run is done or compensated. It is kept alive while the run executes and lapses after `ttl` while the run is parked or dead; a resumed run that lost it fails with `ErrLockLost`.

## Events

Modules emit events with `eventbus.Bus.EmitTx` in their own transaction. From inside a step, events are staged and only published (moved to the outbox) in the transaction that marks the run `done`; a compensated run publishes nothing. The relay pushes the outbox to BullMQ, one job per subscriber, deduplicated by job id.
