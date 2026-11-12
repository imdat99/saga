-- +goose Up

-- error_kind keeps the class of a recorded failure (permanent, step_timeout,
-- run_timeout, await_timeout, ...) so replay rebuilds an error that still
-- matches the engine's sentinels with errors.Is.
-- uncertain marks a step that may have taken effect even though it did not
-- record success (an attempt timed out): compensation must still run for it.
ALTER TABLE saga.step
    ADD COLUMN IF NOT EXISTS error_kind text,
    ADD COLUMN IF NOT EXISTS uncertain  boolean NOT NULL DEFAULT false;

-- +goose Down
ALTER TABLE saga.step
    DROP COLUMN IF EXISTS uncertain,
    DROP COLUMN IF EXISTS error_kind;
