package out

import (
	"context"
	"time"

	"github.com/niicsz/binitech-sms/internal/domain/money"
)

type NewAccount struct {
	ID       string
	Name     string
	Plan     string
	Currency string
}

type NewAPIKey struct {
	StoredKey
	Label string
}

type NewTariff struct {
	Plan      string
	MCC       string
	MNC       string
	Price     money.Amount
	ValidFrom time.Time
}

type NewSender struct {
	ID        string
	AccountID string
	Sender    string
	Kind      string
	Countries []string
}

type AdminStore interface {
	InsertAccount(ctx context.Context, a NewAccount) error
	InsertKey(ctx context.Context, k NewAPIKey) error
	RevokeKey(ctx context.Context, accountID, keyID string, at time.Time) error
	Credit(ctx context.Context, accountID string, amount money.Amount) (Balance, error)
	InsertTariff(ctx context.Context, t NewTariff) error
	ApproveSender(ctx context.Context, s NewSender) error
}
