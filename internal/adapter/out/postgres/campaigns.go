package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/domain/money"
)

const schedulerLockKey = 7240001

var (
	_ out.MessageQuery        = (*Messages)(nil)
	_ out.AlphanameRepository = (*Catalog)(nil)
	_ out.CampaignStore       = (*Campaigns)(nil)
)

func (r *Messages) List(ctx context.Context, accountID string, f out.MessageFilter) ([]out.StoredMessage, error) {
	where := []string{"account_id = $1"}
	args := []any{accountID}
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, strings.ReplaceAll(cond, "?", fmt.Sprintf("$%d", len(args))))
	}
	if f.From != nil {
		add("created_at >= ?", *f.From)
	}
	if f.To != nil {
		add("created_at < ?", *f.To)
	}
	if f.Status != "" {
		add("status = ?", f.Status)
	}
	if f.CampaignID != "" {
		add("campaign_id = ?", f.CampaignID)
	}
	if f.AfterCreated != nil {
		args = append(args, *f.AfterCreated, f.AfterID)
		where = append(where, fmt.Sprintf("(created_at, id) < ($%d, $%d)", len(args)-1, len(args)))
	}
	args = append(args, f.Limit)
	sql := `SELECT ` + messageColumns + ` FROM messages WHERE ` + strings.Join(where, " AND ") +
		fmt.Sprintf(` ORDER BY created_at DESC, id DESC LIMIT $%d`, len(args))
	rows, err := conn(ctx, r.pool).Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("messages: listar: %w", err)
	}
	defer rows.Close()
	var list []out.StoredMessage
	for rows.Next() {
		m, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		list = append(list, m)
	}
	return list, rows.Err()
}

func (r *Catalog) ListAlphanames(ctx context.Context, accountID string) ([]out.Alphaname, error) {
	rows, err := conn(ctx, r.pool).Query(ctx,
		`SELECT sender, kind, countries::text[], status FROM alphanames WHERE account_id = $1 ORDER BY sender`, accountID)
	if err != nil {
		return nil, fmt.Errorf("alphanames: %w", err)
	}
	defer rows.Close()
	list := []out.Alphaname{}
	for rows.Next() {
		var a out.Alphaname
		if err := rows.Scan(&a.Sender, &a.Kind, &a.Countries, &a.Status); err != nil {
			return nil, err
		}
		for i := range a.Countries {
			a.Countries[i] = strings.TrimSpace(a.Countries[i])
		}
		list = append(list, a)
	}
	return list, rows.Err()
}

type Campaigns struct {
	pool *pgxpool.Pool
}

func NewCampaigns(pool *pgxpool.Pool) *Campaigns {
	return &Campaigns{pool: pool}
}

const campaignColumns = `id, account_id, name, sender, body, normalize, validity_minutes, scheduled_at,
	COALESCE(to_char(window_start, 'HH24:MI'), ''), COALESCE(to_char(window_end, 'HH24:MI'), ''), time_zone, test, status, total,
	COALESCE(idempotency_key, ''), started_at, finished_at, created_at`

func scanCampaign(row pgx.Row) (out.Campaign, error) {
	var c out.Campaign
	err := row.Scan(&c.ID, &c.AccountID, &c.Name, &c.Sender, &c.Body, &c.Normalize, &c.ValidityMinutes, &c.ScheduledAt,
		&c.WindowStart, &c.WindowEnd, &c.TimeZone, &c.Test, &c.Status, &c.Total, &c.IdempotencyKey, &c.StartedAt, &c.FinishedAt, &c.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return out.Campaign{}, out.ErrNotFound
	}
	if err != nil {
		return out.Campaign{}, fmt.Errorf("campaigns: %w", err)
	}
	return c, nil
}

func (r *Campaigns) Create(ctx context.Context, c out.Campaign, recipients []out.Recipient) (string, error) {
	var id string
	if c.IdempotencyKey != "" {
		err := conn(ctx, r.pool).QueryRow(ctx,
			`SELECT id FROM campaigns WHERE account_id = $1 AND idempotency_key = $2`, c.AccountID, c.IdempotencyKey).Scan(&id)
		if err == nil {
			return id, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("campaigns: idempotência: %w", err)
		}
	}
	err := NewTxManager(r.pool).WithinTx(ctx, func(ctx context.Context) error {
		_, err := conn(ctx, r.pool).Exec(ctx,
			`INSERT INTO campaigns (id, account_id, name, status, scheduled_at, window_start, window_end, total, created_at,
			                        sender, body, normalize, validity_minutes, time_zone, test, idempotency_key)
			 VALUES ($1, $2, $3, 'scheduled', $4, NULLIF($5, '')::time, NULLIF($6, '')::time, $7, $8, $9, $10, $11, $12, $13, $14, NULLIF($15, ''))`,
			c.ID, c.AccountID, c.Name, c.ScheduledAt, c.WindowStart, c.WindowEnd, c.Total, c.CreatedAt,
			c.Sender, c.Body, c.Normalize, max(c.ValidityMinutes, 0), c.TimeZone, c.Test, c.IdempotencyKey)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation && c.IdempotencyKey != "" {
			return errCampaignRace
		}
		if err != nil {
			return fmt.Errorf("campaigns: criar: %w", err)
		}
		id = c.ID
		positions := make([]int32, len(recipients))
		tos := make([]string, len(recipients))
		vars := make([]string, len(recipients))
		for i, rc := range recipients {
			raw, err := json.Marshal(rc.Vars)
			if err != nil {
				return err
			}
			positions[i], tos[i], vars[i] = int32(rc.Position), rc.To, string(raw) //nolint:gosec
		}
		_, err = conn(ctx, r.pool).Exec(ctx,
			`INSERT INTO campaign_recipients (campaign_id, position, to_msisdn, vars)
			 SELECT $1, p, t, COALESCE(NULLIF(v, 'null'), '{}')::jsonb FROM unnest($2::int[], $3::text[], $4::text[]) AS x(p, t, v)`,
			c.ID, positions, tos, vars)
		if err != nil {
			return fmt.Errorf("campaigns: destinatários: %w", err)
		}
		return nil
	})
	if errors.Is(err, errCampaignRace) {
		err = conn(ctx, r.pool).QueryRow(ctx,
			`SELECT id FROM campaigns WHERE account_id = $1 AND idempotency_key = $2`, c.AccountID, c.IdempotencyKey).Scan(&id)
	}
	return id, err
}

var errCampaignRace = errors.New("campaigns: criada em paralelo com a mesma chave")

func (r *Campaigns) Get(ctx context.Context, accountID, id string) (out.Campaign, error) {
	return scanCampaign(conn(ctx, r.pool).QueryRow(ctx,
		`SELECT `+campaignColumns+` FROM campaigns WHERE id = $1 AND account_id = $2`, id, accountID))
}

func (r *Campaigns) Stats(ctx context.Context, id string) (out.CampaignStats, error) {
	var s out.CampaignStats
	var cost string
	err := conn(ctx, r.pool).QueryRow(ctx,
		`SELECT
		   (SELECT count(*) FROM campaign_recipients WHERE campaign_id = $1 AND status = 'pending'),
		   (SELECT count(*) FROM campaign_recipients WHERE campaign_id = $1 AND status = 'submitted'),
		   (SELECT count(*) FROM campaign_recipients WHERE campaign_id = $1 AND status = 'rejected'),
		   count(*) FILTER (WHERE m.status = 'delivered'),
		   count(*) FILTER (WHERE m.status IN ('undelivered', 'rejected', 'expired', 'failed')),
		   count(*) FILTER (WHERE m.status IN ('accepted', 'routed', 'submitted')),
		   COALESCE(sum(m.cost) FILTER (WHERE m.status NOT IN ('rejected', 'expired') OR m.submitted_at IS NOT NULL), 0)::text
		 FROM messages m WHERE m.campaign_id = $1`, id,
	).Scan(&s.Pending, &s.Submitted, &s.Rejected, &s.Delivered, &s.Failed, &s.InFlight, &cost)
	if err != nil {
		return s, fmt.Errorf("campaigns: estatísticas: %w", err)
	}
	s.Cost, err = money.Parse(cost)
	return s, err
}

func (r *Campaigns) TryLock(ctx context.Context) (func(), bool, error) {
	c, err := r.pool.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	var ok bool
	if err := c.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, schedulerLockKey).Scan(&ok); err != nil || !ok {
		c.Release()
		return nil, false, err
	}
	return func() {
		_, _ = c.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, schedulerLockKey)
		c.Release()
	}, true, nil
}

func (r *Campaigns) NextDue(ctx context.Context, now time.Time, skip []string) (out.Campaign, error) {
	if skip == nil {
		skip = []string{}
	}
	return scanCampaign(conn(ctx, r.pool).QueryRow(ctx,
		`SELECT `+campaignColumns+` FROM campaigns
		  WHERE status IN ('scheduled', 'running') AND scheduled_at <= $1 AND NOT (id = ANY($2))
		  ORDER BY scheduled_at LIMIT 1`, now, skip))
}

func (r *Campaigns) MarkStatus(ctx context.Context, id, status string, at time.Time) error {
	_, err := conn(ctx, r.pool).Exec(ctx,
		`UPDATE campaigns SET status = $2,
		        started_at = CASE WHEN $2 = 'running' THEN COALESCE(started_at, $3) ELSE started_at END,
		        finished_at = CASE WHEN $2 = 'done' THEN $3 ELSE finished_at END
		  WHERE id = $1`, id, status, at)
	if err != nil {
		return fmt.Errorf("campaigns: status: %w", err)
	}
	return nil
}

func (r *Campaigns) PendingChunk(ctx context.Context, id string, limit int) ([]out.Recipient, error) {
	rows, err := conn(ctx, r.pool).Query(ctx,
		`SELECT position, to_msisdn, vars FROM campaign_recipients
		  WHERE campaign_id = $1 AND status = 'pending' ORDER BY position LIMIT $2`, id, limit)
	if err != nil {
		return nil, fmt.Errorf("campaigns: pendentes: %w", err)
	}
	defer rows.Close()
	var list []out.Recipient
	for rows.Next() {
		var (
			rc  out.Recipient
			raw []byte
		)
		if err := rows.Scan(&rc.Position, &rc.To, &raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &rc.Vars); err != nil {
			return nil, err
		}
		list = append(list, rc)
	}
	return list, rows.Err()
}

func (r *Campaigns) RecordChunk(ctx context.Context, id string, result out.ChunkResult) error {
	return NewTxManager(r.pool).WithinTx(ctx, func(ctx context.Context) error {
		for pos, msg := range result.Accepted {
			if _, err := conn(ctx, r.pool).Exec(ctx,
				`UPDATE campaign_recipients SET status = 'submitted', message_id = $3 WHERE campaign_id = $1 AND position = $2`,
				id, pos, msg); err != nil {
				return err
			}
		}
		for pos, reason := range result.Rejected {
			if _, err := conn(ctx, r.pool).Exec(ctx,
				`UPDATE campaign_recipients SET status = 'rejected', error = $3 WHERE campaign_id = $1 AND position = $2`,
				id, pos, reason); err != nil {
				return err
			}
		}
		_, err := conn(ctx, r.pool).Exec(ctx,
			`UPDATE campaigns SET submitted = submitted + $2, rejected = rejected + $3 WHERE id = $1`,
			id, len(result.Accepted), len(result.Rejected))
		return err
	})
}
