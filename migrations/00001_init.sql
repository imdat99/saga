-- +goose Up
CREATE TABLE IF NOT EXISTS saga.run (
    id            uuid PRIMARY KEY,
    workflow      text        NOT NULL,
    version       int         NOT NULL,
    idem_key      text        NOT NULL UNIQUE,
    status        text        NOT NULL,
    input         jsonb       NOT NULL,
    output        jsonb,
    error         text,
    owner_id      text,
    lease_until   timestamptz,
    lease_version bigint      NOT NULL DEFAULT 0,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT run_status_check CHECK (status IN
        ('running', 'waiting', 'compensating', 'done', 'compensated', 'needs_attention'))
);

-- Recovery scans runs whose lease has expired.
CREATE INDEX IF NOT EXISTS run_recoverable_idx
    ON saga.run (lease_until)
    WHERE status IN ('running', 'compensating');

-- One row per step invocation. Rows are inserted once and then only change status;
-- the whole flow is never rewritten (unlike Medusa's checkpoint).
CREATE TABLE IF NOT EXISTS saga.step (
    run_id       uuid        NOT NULL REFERENCES saga.run (id) ON DELETE CASCADE,
    seq          int         NOT NULL,
    name         text        NOT NULL,
    status       text        NOT NULL,
    attempt      int         NOT NULL DEFAULT 1,
    output       jsonb,
    comp_data    jsonb,
    error        text,
    started_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    PRIMARY KEY (run_id, seq),
    CONSTRAINT step_status_check CHECK (status IN
        ('started', 'done', 'failed', 'compensated', 'compensation_failed'))
);

-- +goose Down
DROP TABLE IF EXISTS saga.step;
DROP TABLE IF EXISTS saga.run;
