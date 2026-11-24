ALTER TABLE webhooks ADD COLUMN description text;
ALTER TABLE webhooks ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE webhooks ADD COLUMN deleted_at timestamptz;
ALTER TABLE webhooks ADD COLUMN last_status int;
ALTER TABLE webhooks ADD COLUMN last_attempt_at timestamptz;
