package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/contract/events"
)

func (r *OutboxRepository) EnqueueMany(ctx context.Context, envelopes []events.Envelope) ([]int64, error) {
	topics := make([]string, len(envelopes))
	keys := make([]string, len(envelopes))
	headers := make([]string, len(envelopes))
	payloads := make([][]byte, len(envelopes))
	for i, e := range envelopes {
		h, err := json.Marshal(e.Headers)
		if err != nil {
			return nil, err
		}
		topics[i], keys[i], headers[i], payloads[i] = e.Topic, e.Key, string(h), e.Payload
	}
	rows, err := conn(ctx, r.pool).Query(ctx,
		`INSERT INTO outbox (topic, key, headers, payload)
		 SELECT t, k, h::jsonb, p FROM unnest($1::text[], $2::text[], $3::text[], $4::bytea[]) WITH ORDINALITY AS x(t, k, h, p, n)
		 ORDER BY n
		 RETURNING id`, topics, keys, headers, payloads)
	if err != nil {
		return nil, fmt.Errorf("outbox: insert em massa: %w", err)
	}
	defer rows.Close()
	ids := make([]int64, 0, len(envelopes))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (r *Balances) AppendMany(ctx context.Context, entries []out.LedgerEntry) error {
	if len(entries) == 0 {
		return nil
	}
	accounts := make([]string, len(entries))
	kinds := make([]string, len(entries))
	amounts := make([]string, len(entries))
	messages := make([]*string, len(entries))
	ops := make([]string, len(entries))
	after := make([]string, len(entries))
	for i, e := range entries {
		accounts[i], kinds[i], amounts[i], ops[i], after[i] = e.AccountID, e.Kind, e.Amount.String(), e.OpID, e.BalanceAfter.String()
		if e.MessageID != "" {
			id := e.MessageID
			messages[i] = &id
		}
	}
	_, err := conn(ctx, r.pool).Exec(ctx,
		`INSERT INTO ledger_entries (account_id, kind, amount, message_id, op_id, balance_after)
		 SELECT a, k, m::numeric, msg, o::uuid, b::numeric
		   FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[]) AS x(a, k, m, msg, o, b)`,
		accounts, kinds, amounts, messages, ops, after)
	if err != nil {
		return fmt.Errorf("ledger: insert em massa: %w", err)
	}
	return nil
}

func (r *Messages) InsertMany(ctx context.Context, msgs []out.StoredMessage) error {
	if len(msgs) == 0 {
		return nil
	}
	n := len(msgs)
	ids, accounts, tos, senders, bodies, encodings := make([]string, n), make([]string, n), make([]string, n), make([]string, n), make([]string, n), make([]string, n)
	segments := make([]int32, n)
	mccs, mncs, webhooks, campaigns := make([]*string, n), make([]*string, n), make([]*string, n), make([]*string, n)
	units, costs, statuses := make([]string, n), make([]string, n), make([]string, n)
	tests := make([]bool, n)
	expires, created := make([]time.Time, n), make([]time.Time, n)
	for i, m := range msgs {
		ids[i], accounts[i], tos[i], senders[i], bodies[i], encodings[i] = m.ID, m.AccountID, m.To.String(), m.Sender, m.Body, string(m.Encoding)
		segments[i] = int32(m.Segments) //nolint:gosec
		mccs[i], mncs[i], webhooks[i], campaigns[i] = ptr(m.MCC), ptr(m.MNC), ptr(m.WebhookRef), ptr(m.CampaignID)
		units[i], costs[i], statuses[i] = m.UnitPrice.String(), m.Cost.String(), string(m.Status)
		tests[i] = m.Test
		expires[i], created[i] = m.ExpiresAt, m.CreatedAt
	}
	_, err := conn(ctx, r.pool).Exec(ctx,
		`INSERT INTO messages (id, account_id, to_msisdn, sender, body, encoding, segments, mcc, mnc, webhook_ref, campaign_id,
		                       unit_price, cost, status, test, expires_at, created_at, updated_at)
		 SELECT id, acc, t, s, b, e, seg, mcc, mnc, wh, camp, u::numeric, c::numeric, st, tst, exp, cr, cr
		   FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::int[], $8::text[], $9::text[],
		               $10::text[], $11::text[], $12::text[], $13::text[], $14::text[], $15::bool[], $16::timestamptz[], $17::timestamptz[])
		     AS x(id, acc, t, s, b, e, seg, mcc, mnc, wh, camp, u, c, st, tst, exp, cr)`,
		ids, accounts, tos, senders, bodies, encodings, segments, mccs, mncs, webhooks, campaigns,
		units, costs, statuses, tests, expires, created)
	if err != nil {
		return fmt.Errorf("messages: insert em massa: %w", err)
	}
	return nil
}

func ptr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
