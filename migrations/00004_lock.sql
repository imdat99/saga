-- +goose Up

-- Business locks held by runs across steps (saga.Lock). A lock is free when
-- its holder released it (run done or compensated) or when until passed (the
-- holder died or overran its TTL).
CREATE TABLE IF NOT EXISTS saga.lock (
    key        text        PRIMARY KEY,
    run_id     uuid        NOT NULL REFERENCES saga.run (id) ON DELETE CASCADE,
    until      timestamptz NOT NULL,
    -- ttl_us is the TTL the holder asked for: the lease heartbeat extends
    -- the lock by it while the holder executes.
    ttl_us     bigint      NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Release on run end deletes by run.
CREATE INDEX IF NOT EXISTS lock_run_idx ON saga.lock (run_id);

-- +goose Down
DROP TABLE IF EXISTS saga.lock;
