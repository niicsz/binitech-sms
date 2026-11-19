package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
)

type HealthChecker struct {
	pool *pgxpool.Pool
}

var _ out.HealthChecker = (*HealthChecker)(nil)

func NewHealthChecker(pool *pgxpool.Pool) *HealthChecker {
	return &HealthChecker{pool: pool}
}

func (h *HealthChecker) Name() string { return "postgres" }

func (h *HealthChecker) Check(ctx context.Context) error {
	return h.pool.Ping(ctx)
}
