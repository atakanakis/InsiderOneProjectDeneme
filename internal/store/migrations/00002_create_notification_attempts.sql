-- +goose Up
-- One row per call to the external provider: the audit trail behind retries
-- and the source of the success-rate / latency numbers on /v1/metrics.
CREATE TABLE notification_attempts (
    id              BIGSERIAL PRIMARY KEY,
    notification_id UUID        NOT NULL REFERENCES notifications (id) ON DELETE CASCADE,
    attempt_number  INT         NOT NULL,
    channel         TEXT        NOT NULL,
    outcome         TEXT        NOT NULL CHECK (outcome IN ('delivered', 'retryable', 'permanent')),
    http_status     INT,
    error           TEXT,
    duration_ms     BIGINT      NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_attempts_notification ON notification_attempts (notification_id, attempt_number);
CREATE INDEX idx_attempts_created ON notification_attempts (created_at);

-- +goose Down
DROP TABLE notification_attempts;
