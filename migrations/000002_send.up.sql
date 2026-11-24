ALTER TABLE accounts ADD COLUMN plan text NOT NULL DEFAULT 'default';
ALTER TABLE messages ADD COLUMN test boolean NOT NULL DEFAULT false;
CREATE INDEX alphanames_approved ON alphanames (account_id, sender) WHERE status = 'approved';
