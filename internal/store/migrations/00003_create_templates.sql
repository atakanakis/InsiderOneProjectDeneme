-- +goose Up
CREATE TABLE templates (
    id         UUID PRIMARY KEY,
    name       TEXT        NOT NULL UNIQUE,
    channel    TEXT        NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    body       TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Remember which template produced a notification. The rendered text is
-- still stored in notifications.content, so editing a template never changes
-- what was already queued.
ALTER TABLE notifications
    ADD COLUMN template_id UUID REFERENCES templates (id) ON DELETE SET NULL;

-- +goose Down
ALTER TABLE notifications DROP COLUMN template_id;
DROP TABLE templates;
