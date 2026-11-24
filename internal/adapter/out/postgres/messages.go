package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/domain/message"
	"github.com/niicsz/binitech-sms/internal/domain/money"
	"github.com/niicsz/binitech-sms/internal/domain/phone"
	"github.com/niicsz/binitech-sms/internal/domain/segment"
)

const uniqueViolation = "23505"

type Messages struct {
	pool *pgxpool.Pool
}

type Idempotency struct {
	pool *pgxpool.Pool
}

var (
	_ out.MessageRepository = (*Messages)(nil)
	_ out.IdempotencyStore  = (*Idempotency)(nil)
)

func NewMessages(pool *pgxpool.Pool) *Messages {
	return &Messages{pool: pool}
}

func NewIdempotency(pool *pgxpool.Pool) *Idempotency {
	return &Idempotency{pool: pool}
}

func (r *Messages) Insert(ctx context.Context, m out.StoredMessage) error {
	_, err := conn(ctx, r.pool).Exec(ctx,
		`INSERT INTO messages (id, account_id, idempotency_key, to_msisdn, sender, body, encoding, segments,
		                       mcc, mnc, unit_price, cost, status, webhook_ref, test, expires_at, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::numeric, $12::numeric, $13, $14, $15, $16, $17, $17)`,
		m.ID, m.AccountID, nullable(m.IdemKey), m.To.String(), m.Sender, m.Body, string(m.Encoding), m.Segments,
		nullable(m.MCC), nullable(m.MNC), m.UnitPrice.String(), m.Cost.String(), string(m.Status),
		nullable(m.WebhookRef), m.Test, m.ExpiresAt, m.CreatedAt,
	)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		return fmt.Errorf("messages: duplicada: %w", err)
	}
	if err != nil {
		return fmt.Errorf("messages: insert: %w", err)
	}
	return nil
}

const messageColumns = `id, account_id, to_msisdn, sender, body, encoding, segments, mcc, mnc,
	unit_price::text, cost::text, status, webhook_ref, error_code, test, expires_at, created_at, updated_at`

func (r *Messages) FindByID(ctx context.Context, accountID, id string) (out.StoredMessage, error) {
	return scanMessage(conn(ctx, r.pool).QueryRow(ctx,
		`SELECT `+messageColumns+` FROM messages WHERE id = $1 AND account_id = $2`, id, accountID))
}

func scanMessage(row pgx.Row) (out.StoredMessage, error) {
	var (
		m                          out.StoredMessage
		to, encoding, status       string
		unit, cost                 string
		mcc, mnc, webhook, errCode *string
	)
	err := row.Scan(&m.ID, &m.AccountID, &to, &m.Sender, &m.Body, &encoding, &m.Segments, &mcc, &mnc,
		&unit, &cost, &status, &webhook, &errCode, &m.Test, &m.ExpiresAt, &m.CreatedAt, &m.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return out.StoredMessage{}, out.ErrNotFound
	}
	if err != nil {
		return out.StoredMessage{}, fmt.Errorf("messages: scan: %w", err)
	}
	m.To = phone.Number(to)
	m.Encoding = segment.Encoding(encoding)
	m.Status = message.Status(status)
	m.MCC, m.MNC, m.WebhookRef, m.ErrorCode = deref(mcc), deref(mnc), deref(webhook), deref(errCode)
	if m.UnitPrice, err = money.Parse(unit); err != nil {
		return out.StoredMessage{}, err
	}
	if m.Cost, err = money.Parse(cost); err != nil {
		return out.StoredMessage{}, err
	}
	return m, nil
}

func (r *Idempotency) Begin(ctx context.Context, accountID, key string, requestHash []byte) (*out.IdempotencyRecord, error) {
	tag, err := conn(ctx, r.pool).Exec(ctx,
		`INSERT INTO idempotency_requests (account_id, key, request_hash, response)
		 VALUES ($1, $2, $3, 'null'::jsonb) ON CONFLICT DO NOTHING`,
		accountID, key, requestHash)
	if err != nil {
		return nil, fmt.Errorf("idempotency: begin: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil, nil
	}
	var rec out.IdempotencyRecord
	err = conn(ctx, r.pool).QueryRow(ctx,
		`SELECT request_hash, response::text, response <> 'null'::jsonb
		   FROM idempotency_requests WHERE account_id = $1 AND key = $2`,
		accountID, key,
	).Scan(&rec.RequestHash, &rec.Response, &rec.Completed)
	if err != nil {
		return nil, fmt.Errorf("idempotency: read: %w", err)
	}
	return &rec, nil
}

func (r *Idempotency) Complete(ctx context.Context, accountID, key string, response []byte) error {
	_, err := conn(ctx, r.pool).Exec(ctx,
		`UPDATE idempotency_requests SET response = $3::jsonb WHERE account_id = $1 AND key = $2`,
		accountID, key, string(response))
	if err != nil {
		return fmt.Errorf("idempotency: complete: %w", err)
	}
	return nil
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
