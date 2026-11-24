CREATE TABLE otp_challenges (
  id            text PRIMARY KEY,
  account_id    text NOT NULL REFERENCES accounts(id),
  to_msisdn     varchar(16) NOT NULL,
  code_hmac     bytea NOT NULL,
  message_id    text REFERENCES messages(id),
  status        text NOT NULL CHECK (status IN ('pending', 'verified', 'blocked')),
  attempts      int NOT NULL DEFAULT 0,
  max_attempts  int NOT NULL,
  expires_at    timestamptz NOT NULL,
  verified_at   timestamptz,
  idempotency_key varchar(64),
  created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX otp_recent ON otp_challenges (account_id, to_msisdn, created_at DESC);
CREATE UNIQUE INDEX otp_idempotency ON otp_challenges (account_id, idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE TABLE short_links (
  id           text PRIMARY KEY,
  account_id   text NOT NULL REFERENCES accounts(id),
  code         varchar(16) NOT NULL UNIQUE,
  url          text NOT NULL,
  campaign_id  text REFERENCES campaigns(id),
  message_id   text,
  clicks       bigint NOT NULL DEFAULT 0,
  created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX short_links_account ON short_links (account_id, created_at DESC);

CREATE TABLE link_clicks (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  link_id     text NOT NULL REFERENCES short_links(id),
  ip_hash     bytea,
  user_agent  text,
  clicked_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX link_clicks_link ON link_clicks (link_id, clicked_at DESC);

CREATE TABLE opt_outs (
  account_id  text NOT NULL REFERENCES accounts(id),
  msisdn      varchar(16) NOT NULL,
  sender      varchar(16) NOT NULL,
  keyword     text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (account_id, msisdn)
);
