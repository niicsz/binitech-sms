package in

import (
	"context"
	"time"

	"github.com/niicsz/binitech-sms/internal/domain/money"
)

type MessageFilter struct {
	From       *time.Time
	To         *time.Time
	Status     string
	CampaignID string
	Limit      int
	Cursor     string
}

type MessagePage struct {
	Items      []MessageView
	NextCursor string
}

type AlphanameView struct {
	Sender    string
	Kind      string
	Countries []string
	Status    string
}

type Listings interface {
	Messages(ctx context.Context, p Principal, filter MessageFilter) (MessagePage, error)
	Alphanames(ctx context.Context, p Principal) ([]AlphanameView, error)
}

type CampaignRecipientInput struct {
	To   string
	Vars map[string]string
}

type CampaignInput struct {
	IdempotencyKey  string
	Name            string
	From            string
	Text            string
	Normalize       bool
	ValidityMinutes int
	ScheduledAt     time.Time
	WindowStart     string
	WindowEnd       string
	TimeZone        string
	Recipients      []CampaignRecipientInput
}

type CampaignSummary struct {
	ID          string
	Name        string
	Status      string
	ScheduledAt time.Time
	StartedAt   *time.Time
	FinishedAt  *time.Time
	Total       int
	Pending     int
	Submitted   int
	Rejected    int
	Delivered   int
	Failed      int
	InFlight    int
	Cost        money.Amount
	Currency    string
	Test        bool
}

type CampaignManager interface {
	Create(ctx context.Context, p Principal, input CampaignInput) (CampaignSummary, error)
	Summary(ctx context.Context, p Principal, id string) (CampaignSummary, error)
}

type CampaignRunner interface {
	Tick(ctx context.Context) (bool, error)
}
