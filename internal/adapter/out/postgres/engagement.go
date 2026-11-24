package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
)

type Engagement struct {
	pool *pgxpool.Pool
}

var (
	_ out.OTPStore    = (*Engagement)(nil)
	_ out.LinkStore   = (*Engagement)(nil)
	_ out.OptOutStore = (*Engagement)(nil)
)

func NewEngagement(pool *pgxpool.Pool) *Engagement {
	return &Engagement{pool: pool}
}

const otpColumns = `id, account_id, to_msisdn, code_hmac, COALESCE(message_id, ''), status, attempts, max_attempts,
	expires_at, created_at, COALESCE(idempotency_key, '')`

func scanOTP(row pgx.Row) (out.OTPChallenge, error) {
	var c out.OTPChallenge
	err := row.Scan(&c.ID, &c.AccountID, &c.To, &c.CodeHMAC, &c.MessageID, &c.Status, &c.Attempts, &c.MaxAttempts,
		&c.ExpiresAt, &c.CreatedAt, &c.IdemKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return out.OTPChallenge{}, out.ErrNotFound
	}
	if err != nil {
		return out.OTPChallenge{}, fmt.Errorf("otp: %w", err)
	}
	return c, nil
}

func (r *Engagement) ByIdempotency(ctx context.Context, accountID, key string) (out.OTPChallenge, error) {
	return scanOTP(conn(ctx, r.pool).QueryRow(ctx,
		`SELECT `+otpColumns+` FROM otp_challenges WHERE account_id = $1 AND idempotency_key = $2`, accountID, key))
}

func (r *Engagement) Create(ctx context.Context, c out.OTPChallenge) error {
	_, err := conn(ctx, r.pool).Exec(ctx,
		`INSERT INTO otp_challenges (id, account_id, to_msisdn, code_hmac, status, max_attempts, expires_at, created_at, idempotency_key)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, ''))`,
		c.ID, c.AccountID, c.To, c.CodeHMAC, c.Status, c.MaxAttempts, c.ExpiresAt, c.CreatedAt, c.IdemKey)
	if err != nil {
		return fmt.Errorf("otp: criar: %w", err)
	}
	return nil
}

func (r *Engagement) AttachMessage(ctx context.Context, id, messageID string) error {
	_, err := conn(ctx, r.pool).Exec(ctx, `UPDATE otp_challenges SET message_id = $2 WHERE id = $1`, id, messageID)
	return err
}

func (r *Engagement) RecentFor(ctx context.Context, accountID, to string, since time.Time) (int, time.Time, error) {
	var (
		count int
		last  *time.Time
	)
	err := conn(ctx, r.pool).QueryRow(ctx,
		`SELECT count(*), max(created_at) FROM otp_challenges WHERE account_id = $1 AND to_msisdn = $2 AND created_at >= $3`,
		accountID, to, since).Scan(&count, &last)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("otp: recentes: %w", err)
	}
	if last == nil {
		return count, time.Time{}, nil
	}
	return count, *last, nil
}

func (r *Engagement) ForUpdate(ctx context.Context, accountID, id string) (out.OTPChallenge, error) {
	return scanOTP(conn(ctx, r.pool).QueryRow(ctx,
		`SELECT `+otpColumns+` FROM otp_challenges WHERE id = $1 AND account_id = $2 FOR UPDATE`, id, accountID))
}

func (r *Engagement) SaveAttempt(ctx context.Context, id string, attempts int, status string, at time.Time) error {
	_, err := conn(ctx, r.pool).Exec(ctx,
		`UPDATE otp_challenges SET attempts = $2, status = $3,
		        verified_at = CASE WHEN $3 = 'verified' THEN $4 ELSE verified_at END
		  WHERE id = $1`, id, attempts, status, at)
	return err
}

func (r *Engagement) CreateLink(ctx context.Context, l out.Link) error {
	_, err := conn(ctx, r.pool).Exec(ctx,
		`INSERT INTO short_links (id, account_id, code, url, campaign_id, message_id, created_at)
		 VALUES ($1, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''), $7)`,
		l.ID, l.AccountID, l.Code, l.URL, l.CampaignID, l.MessageID, l.CreatedAt)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		return out.ErrConflict
	}
	if err != nil {
		return fmt.Errorf("links: criar: %w", err)
	}
	return nil
}

func (r *Engagement) ByCode(ctx context.Context, code string) (out.Link, error) {
	var l out.Link
	err := conn(ctx, r.pool).QueryRow(ctx,
		`SELECT id, account_id, code, url, COALESCE(campaign_id, ''), COALESCE(message_id, ''), created_at
		   FROM short_links WHERE code = $1`, code,
	).Scan(&l.ID, &l.AccountID, &l.Code, &l.URL, &l.CampaignID, &l.MessageID, &l.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return out.Link{}, out.ErrNotFound
	}
	if err != nil {
		return out.Link{}, fmt.Errorf("links: %w", err)
	}
	return l, nil
}

func (r *Engagement) RecordClick(ctx context.Context, linkID string, ipHash []byte, userAgent string, at time.Time) error {
	return NewTxManager(r.pool).WithinTx(ctx, func(ctx context.Context) error {
		if _, err := conn(ctx, r.pool).Exec(ctx,
			`INSERT INTO link_clicks (link_id, ip_hash, user_agent, clicked_at) VALUES ($1, $2, NULLIF($3, ''), $4)`,
			linkID, ipHash, userAgent, at); err != nil {
			return fmt.Errorf("links: clique: %w", err)
		}
		_, err := conn(ctx, r.pool).Exec(ctx, `UPDATE short_links SET clicks = clicks + 1 WHERE id = $1`, linkID)
		return err
	})
}

func (r *Engagement) IsOptedOut(ctx context.Context, accountID, msisdn string) (bool, error) {
	var ok bool
	err := conn(ctx, r.pool).QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM opt_outs WHERE account_id = $1 AND msisdn = $2)`, accountID, msisdn).Scan(&ok)
	return ok, err
}

func (r *Engagement) OptOut(ctx context.Context, sender, msisdn, keyword string) (int, error) {
	tag, err := conn(ctx, r.pool).Exec(ctx,
		`INSERT INTO opt_outs (account_id, msisdn, sender, keyword)
		 SELECT account_id, $2::text, $1::text, $3::text FROM alphanames WHERE sender = $1::text AND status = 'approved'
		 ON CONFLICT (account_id, msisdn) DO NOTHING`, sender, msisdn, keyword)
	if err != nil {
		return 0, fmt.Errorf("opt-out: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
