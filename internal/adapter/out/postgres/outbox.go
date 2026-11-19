package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/contract/events"
)

type OutboxRepository struct {
	pool *pgxpool.Pool
}

var _ out.Outbox = (*OutboxRepository)(nil)

func NewOutboxRepository(pool *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{pool: pool}
}

func (r *OutboxRepository) Enqueue(ctx context.Context, envelope events.Envelope) (int64, error) {
	headers, err := json.Marshal(envelope.Headers)
	if err != nil {
		return 0, fmt.Errorf("outbox: headers: %w", err)
	}
	var id int64
	err = conn(ctx, r.pool).QueryRow(ctx,
		`INSERT INTO outbox (topic, key, headers, payload) VALUES ($1, $2, $3, $4) RETURNING id`,
		envelope.Topic, envelope.Key, headers, envelope.Payload,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("outbox: insert: %w", err)
	}
	return id, nil
}

func (r *OutboxRepository) ClaimPending(ctx context.Context, olderThan time.Duration, limit int) ([]out.OutboxRecord, error) {
	rows, err := conn(ctx, r.pool).Query(ctx,
		`SELECT id, topic, key, headers, payload
		   FROM outbox
		  WHERE published_at IS NULL AND created_at <= now() - make_interval(secs => $1)
		  ORDER BY id
		  LIMIT $2
		  FOR UPDATE SKIP LOCKED`,
		olderThan.Seconds(), limit,
	)
	if err != nil {
		return nil, fmt.Errorf("outbox: claim: %w", err)
	}
	defer rows.Close()
	var records []out.OutboxRecord
	for rows.Next() {
		var (
			record  out.OutboxRecord
			headers []byte
		)
		if err := rows.Scan(&record.ID, &record.Envelope.Topic, &record.Envelope.Key, &headers, &record.Envelope.Payload); err != nil {
			return nil, fmt.Errorf("outbox: scan: %w", err)
		}
		if err := json.Unmarshal(headers, &record.Envelope.Headers); err != nil {
			return nil, fmt.Errorf("outbox: headers da linha %d: %w", record.ID, err)
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (r *OutboxRepository) MarkPublished(ctx context.Context, ids []int64, at time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := conn(ctx, r.pool).Exec(ctx,
		`UPDATE outbox SET published_at = $2 WHERE id = ANY($1)`, ids, at)
	if err != nil {
		return fmt.Errorf("outbox: mark: %w", err)
	}
	return nil
}
