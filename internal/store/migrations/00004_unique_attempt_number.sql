-- +goose Up
-- A worker retries recording an attempt when the database write fails. If
-- the first write did commit (the reply was lost), the retry must not add a
-- second audit row for the same attempt.
CREATE UNIQUE INDEX idx_attempts_notification_number ON notification_attempts (notification_id, attempt_number);
DROP INDEX idx_attempts_notification;

-- +goose Down
CREATE INDEX idx_attempts_notification ON notification_attempts (notification_id, attempt_number);
DROP INDEX idx_attempts_notification_number;
