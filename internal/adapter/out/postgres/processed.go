package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
)

type ProcessedOps struct {
	pool *pgxpool.Pool
}

var _ out.ProcessedOps = (*ProcessedOps)(nil)

func NewProcessedOps(pool *pgxpool.Pool) *ProcessedOps {
	return &ProcessedOps{pool: pool}
}

func (r *ProcessedOps) MarkOnce(ctx context.Context, consumer, opID string) (bool, error) {
	tag, err := conn(ctx, r.pool).Exec(ctx,
		`INSERT INTO processed_ops (consumer, op_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, consumer, opID)
	if err != nil {
		return false, fmt.Errorf("processed_ops: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
