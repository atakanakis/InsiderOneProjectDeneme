-- +goose Up
CREATE TABLE notifications (
    id                  UUID PRIMARY KEY,
    batch_id            UUID,
    -- Plain UNIQUE: Postgres allows any number of NULLs, so the constraint
    -- only applies to requests that actually sent a key.
    idempotency_key     TEXT UNIQUE,
    recipient           TEXT        NOT NULL,
    channel             TEXT        NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    content             TEXT        NOT NULL,
    -- 0 = high, 1 = normal, 2 = low, so ORDER BY priority is "most urgent first".
    priority            SMALLINT    NOT NULL CHECK (priority BETWEEN 0 AND 2),
    status              TEXT        NOT NULL CHECK (status IN ('pending', 'processing', 'retrying', 'sent', 'failed', 'cancelled')),
    attempts            INT         NOT NULL DEFAULT 0,
    max_attempts        INT         NOT NULL,
    scheduled_at        TIMESTAMPTZ,
    -- When the notification becomes eligible for (re)delivery. Doubles as the
    -- schedule time for new notifications and the backoff time for retries.
    next_attempt_at     TIMESTAMPTZ NOT NULL,
    -- Lease: a worker owns a "processing" row until this time. If the worker
    -- dies, the reaper hands the row back to the queue after it expires.
    locked_until        TIMESTAMPTZ,
    provider_message_id TEXT,
    last_error          TEXT,
    correlation_id      TEXT        NOT NULL DEFAULT '',
    trace_parent        TEXT        NOT NULL DEFAULT '',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at             TIMESTAMPTZ
);

-- The queue: only rows waiting for delivery are indexed, so the index stays
-- small no matter how many millions of sent notifications pile up.
CREATE INDEX idx_notifications_dispatch
    ON notifications (channel, priority, next_attempt_at)
    WHERE status IN ('pending', 'retrying');

-- Lets the reaper find expired leases without scanning the table.
CREATE INDEX idx_notifications_lease
    ON notifications (locked_until)
    WHERE status = 'processing';

CREATE INDEX idx_notifications_batch ON notifications (batch_id) WHERE batch_id IS NOT NULL;

-- Listing endpoint: newest first, keyset pagination on (created_at, id).
CREATE INDEX idx_notifications_created ON notifications (created_at DESC, id DESC);
CREATE INDEX idx_notifications_status_created ON notifications (status, created_at DESC, id DESC);

-- End-to-end latency on /v1/metrics looks at recently sent rows only.
CREATE INDEX idx_notifications_sent_at ON notifications (sent_at) WHERE status = 'sent';

-- +goose Down
DROP TABLE notifications;
