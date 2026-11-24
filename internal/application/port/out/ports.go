package out

import (
	"context"
	"time"

	"github.com/niicsz/binitech-sms/internal/contract/events"
)

type Clock interface {
	Now() time.Time
}

type IDGenerator interface {
	MessageID() string
	OpID() string
}

type TxManager interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

type OutboxRecord struct {
	ID       int64
	Envelope events.Envelope
}

type Outbox interface {
	Enqueue(ctx context.Context, envelope events.Envelope) (int64, error)
	EnqueueMany(ctx context.Context, envelopes []events.Envelope) ([]int64, error)
	ClaimPending(ctx context.Context, olderThan time.Duration, limit int) ([]OutboxRecord, error)
	MarkPublished(ctx context.Context, ids []int64, at time.Time) error
}

type EventPublisher interface {
	Publish(ctx context.Context, envelopes ...events.Envelope) error
}

type HealthChecker interface {
	Name() string
	Check(ctx context.Context) error
}

type ProcessedOps interface {
	MarkOnce(ctx context.Context, consumer, opID string) (bool, error)
}
