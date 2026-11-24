package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
)

type Webhooks struct {
	pool *pgxpool.Pool
}

var _ out.WebhookStore = (*Webhooks)(nil)

func NewWebhooks(pool *pgxpool.Pool) *Webhooks {
	return &Webhooks{pool: pool}
}

const webhookColumns = `id, account_id, url, events, COALESCE(description, ''), secret, failures,
	disabled_at IS NOT NULL, COALESCE(last_status, 0), created_at, updated_at`

func scanWebhook(row pgx.Row) (out.Webhook, error) {
	var w out.Webhook
	err := row.Scan(&w.ID, &w.AccountID, &w.URL, &w.Events, &w.Description, &w.Secret, &w.Failures,
		&w.Disabled, &w.LastStatus, &w.CreatedAt, &w.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return out.Webhook{}, out.ErrNotFound
	}
	if err != nil {
		return out.Webhook{}, fmt.Errorf("webhooks: %w", err)
	}
	return w, nil
}

func (r *Webhooks) Create(ctx context.Context, w out.Webhook) error {
	_, err := conn(ctx, r.pool).Exec(ctx,
		`INSERT INTO webhooks (id, account_id, url, events, description, secret, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, NULLIF($5, ''), $6, $7, $7)`,
		w.ID, w.AccountID, w.URL, w.Events, w.Description, w.Secret, w.CreatedAt)
	if err != nil {
		return fmt.Errorf("webhooks: criar: %w", err)
	}
	return nil
}

func (r *Webhooks) List(ctx context.Context, accountID string) ([]out.Webhook, error) {
	return r.query(ctx, `SELECT `+webhookColumns+` FROM webhooks WHERE account_id = $1 AND deleted_at IS NULL ORDER BY created_at`, accountID)
}

func (r *Webhooks) Get(ctx context.Context, accountID, id string) (out.Webhook, error) {
	return scanWebhook(conn(ctx, r.pool).QueryRow(ctx,
		`SELECT `+webhookColumns+` FROM webhooks WHERE id = $1 AND account_id = $2 AND deleted_at IS NULL`, id, accountID))
}

func (r *Webhooks) GetActive(ctx context.Context, id string) (out.Webhook, error) {
	return scanWebhook(conn(ctx, r.pool).QueryRow(ctx,
		`SELECT `+webhookColumns+` FROM webhooks WHERE id = $1 AND deleted_at IS NULL AND disabled_at IS NULL`, id))
}

func (r *Webhooks) Update(ctx context.Context, w out.Webhook) error {
	_, err := conn(ctx, r.pool).Exec(ctx,
		`UPDATE webhooks SET url = $3, events = $4, description = NULLIF($5, ''), updated_at = $6,
		        failures = $7, disabled_at = CASE WHEN $8 THEN COALESCE(disabled_at, now()) ELSE NULL END
		  WHERE id = $1 AND account_id = $2 AND deleted_at IS NULL`,
		w.ID, w.AccountID, w.URL, w.Events, w.Description, w.UpdatedAt, w.Failures, w.Disabled)
	if err != nil {
		return fmt.Errorf("webhooks: atualizar: %w", err)
	}
	return nil
}

func (r *Webhooks) Delete(ctx context.Context, accountID, id string) error {
	_, err := conn(ctx, r.pool).Exec(ctx,
		`UPDATE webhooks SET deleted_at = now() WHERE id = $1 AND account_id = $2 AND deleted_at IS NULL`, id, accountID)
	if err != nil {
		return fmt.Errorf("webhooks: remover: %w", err)
	}
	return nil
}

func (r *Webhooks) Count(ctx context.Context, accountID string) (int, error) {
	var n int
	err := conn(ctx, r.pool).QueryRow(ctx,
		`SELECT count(*) FROM webhooks WHERE account_id = $1 AND deleted_at IS NULL`, accountID).Scan(&n)
	return n, err
}

func (r *Webhooks) Subscribed(ctx context.Context, accountID, event string) ([]out.Webhook, error) {
	return r.query(ctx, `SELECT `+webhookColumns+` FROM webhooks
		WHERE account_id = $1 AND $2 = ANY(events) AND deleted_at IS NULL AND disabled_at IS NULL`, accountID, event)
}

func (r *Webhooks) RecordAttempt(ctx context.Context, id string, status int, ok bool, disableAfter int) (bool, error) {
	var disabled bool
	err := conn(ctx, r.pool).QueryRow(ctx,
		`UPDATE webhooks
		    SET failures = CASE WHEN $3 THEN 0 ELSE failures + 1 END,
		        last_status = NULLIF($2, 0),
		        last_attempt_at = now(),
		        disabled_at = CASE WHEN NOT $3 AND failures + 1 >= $4 THEN now() ELSE disabled_at END
		  WHERE id = $1
		  RETURNING disabled_at IS NOT NULL`, id, status, ok, disableAfter).Scan(&disabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("webhooks: tentativa: %w", err)
	}
	return disabled, nil
}

func (r *Webhooks) query(ctx context.Context, sql string, args ...any) ([]out.Webhook, error) {
	rows, err := conn(ctx, r.pool).Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("webhooks: %w", err)
	}
	defer rows.Close()
	list := []out.Webhook{}
	for rows.Next() {
		w, err := scanWebhook(rows)
		if err != nil {
			return nil, err
		}
		list = append(list, w)
	}
	return list, rows.Err()
}
