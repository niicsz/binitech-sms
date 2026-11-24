package out

import (
	"context"
	"time"

	"github.com/niicsz/binitech-sms/internal/domain/money"
)

type MessageFilter struct {
	From         *time.Time
	To           *time.Time
	Status       string
	CampaignID   string
	Limit        int
	AfterCreated *time.Time
	AfterID      string
}

type MessageQuery interface {
	List(ctx context.Context, accountID string, filter MessageFilter) ([]StoredMessage, error)
}

type Alphaname struct {
	Sender    string
	Kind      string
	Countries []string
	Status    string
}

type AlphanameRepository interface {
	ListAlphanames(ctx context.Context, accountID string) ([]Alphaname, error)
}

type Campaign struct {
	ID              string
	AccountID       string
	Name            string
	Sender          string
	Body            string
	Normalize       bool
	ValidityMinutes int
	ScheduledAt     time.Time
	WindowStart     string
	WindowEnd       string
	TimeZone        string
	Test            bool
	Status          string
	Total           int
	IdempotencyKey  string
	StartedAt       *time.Time
	FinishedAt      *time.Time
	CreatedAt       time.Time
}

type Recipient struct {
	Position int
	To       string
	Vars     map[string]string
}

type CampaignStats struct {
	Pending   int
	Submitted int
	Rejected  int
	Delivered int
	Failed    int
	InFlight  int
	Cost      money.Amount
}

type ChunkResult struct {
	Accepted map[int]string
	Rejected map[int]string
}

type CampaignStore interface {
	Create(ctx context.Context, c Campaign, recipients []Recipient) (string, error)
	Get(ctx context.Context, accountID, id string) (Campaign, error)
	Stats(ctx context.Context, id string) (CampaignStats, error)
	TryLock(ctx context.Context) (func(), bool, error)
	NextDue(ctx context.Context, now time.Time, skip []string) (Campaign, error)
	MarkStatus(ctx context.Context, id, status string, at time.Time) error
	PendingChunk(ctx context.Context, id string, limit int) ([]Recipient, error)
	RecordChunk(ctx context.Context, id string, result ChunkResult) error
}
