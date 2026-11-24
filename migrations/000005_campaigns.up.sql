ALTER TABLE campaigns ADD COLUMN sender varchar(16) NOT NULL DEFAULT '';
ALTER TABLE campaigns ADD COLUMN body text NOT NULL DEFAULT '';
ALTER TABLE campaigns ADD COLUMN normalize boolean NOT NULL DEFAULT false;
ALTER TABLE campaigns ADD COLUMN validity_minutes int NOT NULL DEFAULT 1440;
ALTER TABLE campaigns ADD COLUMN time_zone text NOT NULL DEFAULT 'America/Sao_Paulo';
ALTER TABLE campaigns ADD COLUMN test boolean NOT NULL DEFAULT false;
ALTER TABLE campaigns ADD COLUMN submitted int NOT NULL DEFAULT 0;
ALTER TABLE campaigns ADD COLUMN rejected int NOT NULL DEFAULT 0;
ALTER TABLE campaigns ADD COLUMN started_at timestamptz;
ALTER TABLE campaigns ADD COLUMN finished_at timestamptz;
ALTER TABLE campaigns ADD COLUMN idempotency_key varchar(64);
CREATE UNIQUE INDEX campaigns_idempotency ON campaigns (account_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX campaigns_due ON campaigns (scheduled_at) WHERE status IN ('scheduled', 'running');

CREATE TABLE campaign_recipients (
  campaign_id  text NOT NULL REFERENCES campaigns(id),
  position     int NOT NULL,
  to_msisdn    varchar(16) NOT NULL,
  vars         jsonb NOT NULL DEFAULT '{}',
  status       text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'submitted', 'rejected')),
  message_id   text,
  error        text,
  PRIMARY KEY (campaign_id, position)
);
CREATE INDEX campaign_recipients_pending ON campaign_recipients (campaign_id, position) WHERE status = 'pending';
CREATE INDEX messages_account_cursor ON messages (account_id, created_at DESC, id DESC);
