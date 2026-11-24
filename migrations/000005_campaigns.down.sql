DROP INDEX IF EXISTS messages_account_cursor;
DROP TABLE IF EXISTS campaign_recipients;
DROP INDEX IF EXISTS campaigns_due;
DROP INDEX IF EXISTS campaigns_idempotency;
ALTER TABLE campaigns DROP COLUMN IF EXISTS idempotency_key, DROP COLUMN IF EXISTS finished_at, DROP COLUMN IF EXISTS started_at,
  DROP COLUMN IF EXISTS rejected, DROP COLUMN IF EXISTS submitted, DROP COLUMN IF EXISTS test, DROP COLUMN IF EXISTS time_zone,
  DROP COLUMN IF EXISTS validity_minutes, DROP COLUMN IF EXISTS normalize, DROP COLUMN IF EXISTS body, DROP COLUMN IF EXISTS sender;
