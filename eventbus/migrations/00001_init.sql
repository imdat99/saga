-- +goose Up

-- Events emitted from inside a saga step wait here until their run ends:
-- moved to the outbox when it is done, deleted when it is compensated.
CREATE TABLE IF NOT EXISTS eventbus.staging (
    id         uuid        PRIMARY KEY,
    run_id     uuid        NOT NULL,
    name       text        NOT NULL,
    payload    jsonb       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS staging_run_idx ON eventbus.staging (run_id);

-- Events to deliver. The relay pushes unsent rows to BullMQ and sets sent_at.
CREATE TABLE IF NOT EXISTS eventbus.outbox (
    id         uuid        PRIMARY KEY,
    name       text        NOT NULL,
    payload    jsonb       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    sent_at    timestamptz
);

-- The relay scans unsent rows in id (UUIDv7, time) order.
CREATE INDEX IF NOT EXISTS outbox_unsent_idx ON eventbus.outbox (id) WHERE sent_at IS NULL;
-- Retention deletes sent rows by age.
CREATE INDEX IF NOT EXISTS outbox_sent_idx ON eventbus.outbox (sent_at) WHERE sent_at IS NOT NULL;

-- +goose Down
DROP TABLE IF EXISTS eventbus.outbox;
DROP TABLE IF EXISTS eventbus.staging;
