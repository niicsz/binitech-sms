CREATE TABLE accounts (
  id            text PRIMARY KEY,
  name          text NOT NULL,
  status        text NOT NULL CHECK (status IN ('active','suspended','closed')),
  high_volume   boolean NOT NULL DEFAULT false,
  currency      char(3) NOT NULL DEFAULT 'BRL',
  created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
  id            text PRIMARY KEY,
  account_id    text NOT NULL REFERENCES accounts(id),
  environment   text NOT NULL CHECK (environment IN ('live','test')),
  secret_hash   bytea NOT NULL,
  salt          bytea NOT NULL,
  label         text,
  last_used_at  timestamptz,
  revoked_at    timestamptz,
  created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX api_keys_active ON api_keys (account_id, environment) WHERE revoked_at IS NULL;

CREATE TABLE balances (
  account_id    text PRIMARY KEY REFERENCES accounts(id),
  available     numeric(14,4) NOT NULL DEFAULT 0 CHECK (available >= 0),
  reserved      numeric(14,4) NOT NULL DEFAULT 0 CHECK (reserved >= 0),
  version       bigint NOT NULL DEFAULT 0,
  updated_at    timestamptz NOT NULL DEFAULT now(),
  CHECK (reserved <= available)
);

CREATE TABLE ledger_entries (
  id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  account_id    text NOT NULL REFERENCES accounts(id),
  kind          text NOT NULL CHECK (kind IN ('credit','reserve','release','debit','refund','adjust')),
  amount        numeric(14,4) NOT NULL,
  message_id    text,
  op_id         uuid NOT NULL UNIQUE,
  balance_after numeric(14,4) NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ledger_account_time ON ledger_entries (account_id, created_at DESC);

CREATE FUNCTION ledger_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'ledger_entries é somente inserção';
END
$$;
CREATE TRIGGER ledger_no_update BEFORE UPDATE OR DELETE ON ledger_entries
  FOR EACH ROW EXECUTE FUNCTION ledger_immutable();

CREATE TABLE tariffs (
  id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  plan          text NOT NULL,
  mcc           char(3) NOT NULL,
  mnc           varchar(3),
  price         numeric(10,4) NOT NULL CHECK (price >= 0),
  valid_from    timestamptz NOT NULL,
  valid_to      timestamptz,
  UNIQUE NULLS NOT DISTINCT (plan, mcc, mnc, valid_from)
);

CREATE TABLE alphanames (
  id            text PRIMARY KEY,
  account_id    text NOT NULL REFERENCES accounts(id),
  sender        varchar(11) NOT NULL,
  kind          text NOT NULL CHECK (kind IN ('alphanumeric','shortcode','longcode')),
  countries     char(2)[] NOT NULL,
  status        text NOT NULL CHECK (status IN ('pending','approved','rejected')),
  created_at    timestamptz NOT NULL DEFAULT now(),
  UNIQUE (account_id, sender)
);

CREATE TABLE routes (
  id            text PRIMARY KEY,
  kind          text NOT NULL CHECK (kind IN ('smpp','gsm','http')),
  config        jsonb NOT NULL,
  mcc_mnc       text[] NOT NULL,
  cost          numeric(10,4) NOT NULL,
  priority      int NOT NULL DEFAULT 100,
  weight        int NOT NULL DEFAULT 1 CHECK (weight > 0),
  enabled       boolean NOT NULL DEFAULT true,
  updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE FUNCTION notify_routes_changed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_notify('routes_changed', COALESCE(NEW.id, OLD.id));
  RETURN NULL;
END
$$;
CREATE TRIGGER routes_changed AFTER INSERT OR UPDATE OR DELETE ON routes
  FOR EACH ROW EXECUTE FUNCTION notify_routes_changed();

CREATE TABLE campaigns (
  id            text PRIMARY KEY,
  account_id    text NOT NULL REFERENCES accounts(id),
  name          text NOT NULL,
  status        text NOT NULL CHECK (status IN ('scheduled','running','paused','done','cancelled')),
  scheduled_at  timestamptz NOT NULL,
  window_start  time,
  window_end    time,
  total         int NOT NULL DEFAULT 0,
  created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE messages (
  id               text PRIMARY KEY,
  account_id       text NOT NULL REFERENCES accounts(id),
  campaign_id      text REFERENCES campaigns(id),
  idempotency_key  varchar(64),
  to_msisdn        varchar(16) NOT NULL,
  sender           varchar(16) NOT NULL,
  body             text NOT NULL,
  encoding         text NOT NULL CHECK (encoding IN ('GSM7','UCS2')),
  segments         smallint NOT NULL CHECK (segments BETWEEN 1 AND 10),
  mcc              char(3),
  mnc              varchar(3),
  unit_price       numeric(10,4) NOT NULL,
  cost             numeric(12,4) NOT NULL,
  status           text NOT NULL CHECK (status IN ('accepted','routed','submitted','delivered','undelivered','expired','rejected','failed')),
  route_id         text REFERENCES routes(id),
  provider_msg_id  text,
  error_code       text,
  kafka_partition  int,
  kafka_offset     bigint,
  webhook_ref      varchar(64),
  expires_at       timestamptz NOT NULL,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  UNIQUE (account_id, idempotency_key)
);
CREATE INDEX messages_account_time ON messages (account_id, created_at DESC);
CREATE INDEX messages_provider ON messages (route_id, provider_msg_id) WHERE provider_msg_id IS NOT NULL;
CREATE INDEX messages_campaign ON messages (campaign_id) WHERE campaign_id IS NOT NULL;

CREATE TABLE dlr_events (
  id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  message_id       text NOT NULL REFERENCES messages(id),
  route_id         text NOT NULL,
  provider_status  text NOT NULL,
  mapped_status    text NOT NULL,
  error_code       text,
  raw              jsonb NOT NULL,
  op_id            uuid NOT NULL UNIQUE,
  received_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX dlr_events_message ON dlr_events (message_id);

CREATE TABLE webhooks (
  id            text PRIMARY KEY,
  account_id    text NOT NULL REFERENCES accounts(id),
  url           text NOT NULL CHECK (url LIKE 'https://%'),
  events        text[] NOT NULL,
  secret        bytea NOT NULL,
  failures      int NOT NULL DEFAULT 0,
  disabled_at   timestamptz,
  created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX webhooks_account ON webhooks (account_id) WHERE disabled_at IS NULL;

CREATE TABLE idempotency_requests (
  account_id    text NOT NULL,
  key           varchar(64) NOT NULL,
  request_hash  bytea NOT NULL,
  response      jsonb NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (account_id, key)
);
CREATE INDEX idempotency_requests_created ON idempotency_requests (created_at);

CREATE TABLE outbox (
  id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  topic         text NOT NULL,
  key           text NOT NULL,
  headers       jsonb NOT NULL,
  payload       bytea NOT NULL,
  published_at  timestamptz,
  created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX outbox_pending ON outbox (id) WHERE published_at IS NULL;

CREATE TABLE processed_ops (
  consumer      text NOT NULL,
  op_id         uuid NOT NULL,
  processed_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, op_id)
);
