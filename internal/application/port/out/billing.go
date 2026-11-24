package out

import (
	"context"
	"errors"
	"time"

	"github.com/niicsz/binitech-sms/internal/domain/money"
)

var ErrLedgerInconsistent = errors.New("ledger: reserva menor que o lançamento")

type LedgerStore interface {
	Debit(ctx context.Context, accountID string, amount money.Amount) (Balance, error)
	Release(ctx context.Context, accountID string, amount money.Amount) (Balance, error)
	Append(ctx context.Context, entry LedgerEntry) error
}

type Webhook struct {
	ID          string
	AccountID   string
	URL         string
	Events      []string
	Description string
	Secret      []byte
	Failures    int
	Disabled    bool
	LastStatus  int
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type WebhookStore interface {
	Create(ctx context.Context, w Webhook) error
	List(ctx context.Context, accountID string) ([]Webhook, error)
	Get(ctx context.Context, accountID, id string) (Webhook, error)
	Update(ctx context.Context, w Webhook) error
	Delete(ctx context.Context, accountID, id string) error
	Count(ctx context.Context, accountID string) (int, error)
	Subscribed(ctx context.Context, accountID, event string) ([]Webhook, error)
	GetActive(ctx context.Context, id string) (Webhook, error)
	RecordAttempt(ctx context.Context, id string, status int, ok bool, disableAfter int) (bool, error)
}

type SecretBox interface {
	Seal(plain []byte) ([]byte, error)
	Open(sealed []byte) ([]byte, error)
}

type WebhookSender interface {
	Post(ctx context.Context, url string, body []byte, headers map[string]string) (int, error)
}
