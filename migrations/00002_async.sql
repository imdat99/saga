-- +goose Up

-- wake_at: when a waiting run (retry backoff, Sleep, Await timeout) is due.
-- await_name: the signal a waiting run is parked on, if any.
-- deadline: run timeout, checked at step boundaries and while waiting.
ALTER TABLE saga.run
    ADD COLUMN IF NOT EXISTS wake_at    timestamptz,
    ADD COLUMN IF NOT EXISTS await_name text,
    ADD COLUMN IF NOT EXISTS deadline   timestamptz;

-- retrying: an attempt failed with a transient error, the next one is scheduled.
-- waiting: a Sleep or Await pseudo-step parked the run.
ALTER TABLE saga.step DROP CONSTRAINT IF EXISTS step_status_check;
ALTER TABLE saga.step ADD CONSTRAINT step_status_check CHECK (status IN
    ('started', 'retrying', 'waiting', 'done', 'failed', 'compensated', 'compensation_failed'));

-- comp_attempt counts failed compensation attempts of the step (durable
-- CompensatePolicy).
ALTER TABLE saga.step ADD COLUMN IF NOT EXISTS comp_attempt int NOT NULL DEFAULT 0;

-- Recovery also picks up waiting runs whose wake-up job went missing: a parked
-- run keeps lease_until = wake_at + grace with no owner.
DROP INDEX IF EXISTS saga.run_recoverable_idx;
CREATE INDEX IF NOT EXISTS run_recoverable_idx
    ON saga.run (lease_until)
    WHERE status IN ('running', 'waiting', 'compensating');

-- Signals delivered to a run (Engine.Signal). The first signal of a name wins.
CREATE TABLE IF NOT EXISTS saga.signal (
    run_id     uuid        NOT NULL REFERENCES saga.run (id) ON DELETE CASCADE,
    name       text        NOT NULL,
    payload    jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, name)
);

-- +goose Down
DROP TABLE IF EXISTS saga.signal;
DROP INDEX IF EXISTS saga.run_recoverable_idx;
CREATE INDEX IF NOT EXISTS run_recoverable_idx
    ON saga.run (lease_until)
    WHERE status IN ('running', 'compensating');
ALTER TABLE saga.step DROP COLUMN IF EXISTS comp_attempt;
ALTER TABLE saga.step DROP CONSTRAINT IF EXISTS step_status_check;
ALTER TABLE saga.step ADD CONSTRAINT step_status_check CHECK (status IN
    ('started', 'done', 'failed', 'compensated', 'compensation_failed'));
ALTER TABLE saga.run
    DROP COLUMN IF EXISTS deadline,
    DROP COLUMN IF EXISTS await_name,
    DROP COLUMN IF EXISTS wake_at;
