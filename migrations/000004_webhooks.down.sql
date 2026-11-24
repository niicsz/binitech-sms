ALTER TABLE webhooks DROP COLUMN IF EXISTS last_attempt_at;
ALTER TABLE webhooks DROP COLUMN IF EXISTS last_status;
ALTER TABLE webhooks DROP COLUMN IF EXISTS deleted_at;
ALTER TABLE webhooks DROP COLUMN IF EXISTS updated_at;
ALTER TABLE webhooks DROP COLUMN IF EXISTS description;
