# saga - agent guide

Saga orchestrator library for Go modular monoliths that run as several instances. Postgres is the source of truth; River (Postgres queue) drives saga timers, retries and resumes. Event delivery is not part of this repository: applications plug their own outbox and broker (Kafka, BullMQ, ...) in through `RunHook`; the engine knows no broker.

## Start here

1. Read [`docs/PLAN.md`](docs/PLAN.md): goals, architecture, data model, algorithms, milestones, risks, decision log. The plan is in Vietnamese; the code and code docs are in English.
2. Read [`TASKS.md`](TASKS.md): what is done and what is next. Work on the first unchecked milestone unless told otherwise.
3. Read the code of the current milestone before changing it. The core is small: `engine.go`, `step.go`, `workflow.go`, `context.go`, `registry.go`, `internal/store/store.go`, `migrations/`.

## Workflow rules

- Update `TASKS.md` in the same change as the code: tick items, mark `[~]` for partial work, add items you discover, and keep "Known limitations" true.
- When you deviate from `docs/PLAN.md` or make a design choice it does not cover, add a dated line to its "Nhật ký quyết định" (decision log) with the reason.
- A milestone is done only when its tests listed in the plan pass with `go test -race -count=10`.
- Do not commit or push unless the user asks.
- The user wants risks called out unprompted: memory growth, latency (round trips per step), concurrency across instances.

## Commands

```bash
make up / make down            # docker compose Postgres: the tests need it (make up first)
make test                      # go test -race -count=1 ./...
go test -race -count=10 .      # flakiness check before ticking a milestone
make test-chaos                # chaos tests (build tag chaos), from M2 on
make lint
SAGA_TEST_DSN=postgres://... make test   # another server
```

## Architecture invariants (do not break)

- **Fencing:** every write on behalf of a run goes through `store.Fenced`, which locks the run row and checks `owner_id` + `lease_version`, or through the single-statement fenced checkpoints (`StartStepFenced`, `FinishStepFenced`) whose CTE takes the same lock and check. Never write `saga.run` / `saga.step` for a running saga outside them.
- **Lease:** `AcquireLease` only takes expired leases, never re-grants a live one (not even to the same owner).
- **Idempotency:** steps run at least once; the step `Key` (`runID:seq:name`) is stable across re-executions and is the module's dedup key. Compensators are at-least-once and idempotent.
- **Replay:** workflow functions are re-executed on resume; recorded steps return stored outputs; a different step at the same position is non-determinism and parks the run as `needs_attention`.
- **Compensation by name:** compensators are looked up in the registry by step name with persisted `comp_data`, so recovery never needs the workflow function.
- **Same-transaction enqueue:** any River job that continues a run (resume, retry, timer) must be inserted in the same transaction as the checkpoint that requires it (`InsertTx`). No dual writes: whatever must happen with a checkpoint commits in its transaction (e.g. `RunHook` in the terminal one).
- **Time:** leases and timers use Postgres `now()`, never the instance clock.
- **Small checkpoints:** outputs and `comp_data` hold IDs, not entities.

## Code conventions

- Go 1.27, `pgx/v5` (no `database/sql` except for goose), goose migrations embedded in `migrations/` (new file per change, never edit an applied migration), River for jobs.
- Public API in the root package `saga`; storage and internals under `internal/`.
- Direct dependencies are an allowlist (CI `deps` check in `.github/workflows/ci.yml`): pgx, goose, River, uuid. Adding one is a design decision (decision log): Go modules have no test-only dependencies, so any import, tests included, reaches every user's module graph (that is why there is no testcontainers). No broker or event library: event delivery belongs to applications, through `RunHook` (guide chapter 7).
- `RunHook`, `RunInfo` and `RunIDFrom` are public extension points that applications build outboxes on: treat changes to them as breaking.
- Errors: wrap with `%w` and context; exported sentinels in `errors.go`.
- Tests are integration tests against real Postgres via `internal/testdb` (`testdb.New(t)` gives a fresh database per test). Name steps/workflows defined in tests with `testdb.UniqueName(t, ...)`: the registry is global and rejects duplicates.
- No emojis.

## Skills

Before any Go coding, review, debugging, troubleshooting, or setup task, load the `samber/cc-skills-golang@golang-how-to` skill first — it routes to whichever other Go skills the task needs.

Go skills are installed in `.agents/skills/` (see `skills-lock.json`). Load the relevant one before the matching work, e.g. `golang-database` for store/migrations, `golang-concurrency` and `golang-context` for heartbeat, workers and shutdown, `golang-benchmark` / `golang-performance` for M6, `golang-lint` when adding `.golangci.yml`.
