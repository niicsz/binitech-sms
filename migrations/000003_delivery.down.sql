UPDATE messages SET route_id = NULL WHERE route_id = 'sandbox';
DELETE FROM routes WHERE id = 'sandbox';
ALTER TABLE routes DROP CONSTRAINT routes_kind_check;
ALTER TABLE routes ADD CONSTRAINT routes_kind_check CHECK (kind IN ('smpp','gsm','http'));
ALTER TABLE messages DROP COLUMN IF EXISTS done_at;
ALTER TABLE messages DROP COLUMN IF EXISTS submitted_at;
DROP TABLE IF EXISTS route_health;
DROP TABLE IF EXISTS message_parts;
