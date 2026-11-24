CREATE TABLE message_parts (
  message_id       text NOT NULL REFERENCES messages(id),
  part             smallint NOT NULL,
  route_id         text NOT NULL,
  provider_msg_id  text NOT NULL,
  status           text NOT NULL DEFAULT 'submitted' CHECK (status IN ('submitted','delivered','undelivered','expired')),
  error_code       text,
  updated_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (message_id, part),
  UNIQUE (route_id, provider_msg_id)
);

CREATE TABLE route_health (
  route_id    text PRIMARY KEY,
  healthy     boolean NOT NULL,
  detail      text,
  checked_at  timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE messages ADD COLUMN submitted_at timestamptz;
ALTER TABLE messages ADD COLUMN done_at timestamptz;

ALTER TABLE routes DROP CONSTRAINT routes_kind_check;
ALTER TABLE routes ADD CONSTRAINT routes_kind_check CHECK (kind IN ('smpp','gsm','http','sandbox'));
INSERT INTO routes (id, kind, config, mcc_mnc, cost, priority, enabled)
VALUES ('sandbox', 'sandbox', '{}', '{}', 0, 1000, true)
ON CONFLICT (id) DO NOTHING;
