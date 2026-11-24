package in

import (
	"context"
	"time"

	"github.com/niicsz/binitech-sms/internal/domain/apikey"
	"github.com/niicsz/binitech-sms/internal/domain/money"
)

type Principal struct {
	AccountID string
	KeyID     string
	Env       apikey.Environment
}

type Authenticator interface {
	Authenticate(ctx context.Context, rawKey string) (Principal, error)
}

type RateLimit struct {
	Limit      int
	Remaining  int
	ResetAfter time.Duration
}

type RateLimiter interface {
	Allow(ctx context.Context, principal Principal) (RateLimit, error)
}

type SendCommand struct {
	Principal       Principal
	IdempotencyKey  string
	To              string
	From            string
	Text            string
	Normalize       bool
	ValidityMinutes int
	WebhookRef      string
}

type SendResult struct {
	MessageID string       `json:"message_id"`
	Status    string       `json:"status"`
	Segments  int          `json:"segments"`
	Encoding  string       `json:"encoding"`
	Cost      money.Amount `json:"cost"`
	Currency  string       `json:"currency"`
	Test      bool         `json:"test"`
}

type MessageSender interface {
	Send(ctx context.Context, cmd SendCommand) (SendResult, error)
}

type MessageView struct {
	ID         string
	To         string
	From       string
	Status     string
	Segments   int
	Encoding   string
	Cost       money.Amount
	Currency   string
	ErrorCode  string
	WebhookRef string
	Test       bool
	CreatedAt  time.Time
	UpdatedAt  time.Time
	ExpiresAt  time.Time
}

type BalanceView struct {
	Available money.Amount
	Reserved  money.Amount
	Spendable money.Amount
	Currency  string
}

type AccountQueries interface {
	Message(ctx context.Context, principal Principal, id string) (MessageView, error)
	Balance(ctx context.Context, principal Principal) (BalanceView, error)
}

type BulkItem struct {
	To         string
	Text       string
	WebhookRef string
}

type BulkCommand struct {
	Principal       Principal
	IdempotencyKey  string
	From            string
	Text            string
	Normalize       bool
	ValidityMinutes int
	Items           []BulkItem
	CampaignID      string
}

type BulkAccepted struct {
	Index     int          `json:"index"`
	MessageID string       `json:"message_id"`
	Segments  int          `json:"segments"`
	Cost      money.Amount `json:"cost"`
}

type BulkRejected struct {
	Index   int    `json:"index"`
	Field   string `json:"field"`
	Message string `json:"message"`
}

type BulkResult struct {
	Accepted  []BulkAccepted `json:"accepted"`
	Rejected  []BulkRejected `json:"rejected"`
	TotalCost money.Amount   `json:"total_cost"`
	Currency  string         `json:"currency"`
	Test      bool           `json:"test"`
}

type BulkSender interface {
	SendBulk(ctx context.Context, cmd BulkCommand) (BulkResult, error)
}
