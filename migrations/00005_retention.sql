-- +goose Up

-- Retention deletes ended runs by age (Engine.PurgeRuns, Options.Retention).
CREATE INDEX IF NOT EXISTS run_ended_idx
    ON saga.run (updated_at)
    WHERE status IN ('done', 'compensated');

-- +goose Down
DROP INDEX IF EXISTS saga.run_ended_idx;
