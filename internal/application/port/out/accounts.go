package out

import (
	"context"
	"errors"
	"time"

	"github.com/niicsz/binitech-sms/internal/domain/apikey"
	"github.com/niicsz/binitech-sms/internal/domain/message"
	"github.com/niicsz/binitech-sms/internal/domain/money"
	"github.com/niicsz/binitech-sms/internal/domain/phone"
)

var (
	ErrNotFound            = errors.New("não encontrado")
	ErrInsufficientBalance = errors.New("saldo insuficiente")
)

type StoredKey struct {
	ID            string
	AccountID     string
	Env           apikey.Environment
	Hash          []byte
	Salt          []byte
	AccountStatus string
}

type APIKeyStore interface {
	FindActive(ctx context.Context, id string) (StoredKey, error)
}

type SecretHasher interface {
	Hash(secret string) (hash, salt []byte, err error)
	Verify(secret string, hash, salt []byte) bool
}

type Account struct {
	ID         string
	Plan       string
	Status     string
	Currency   string
	HighVolume bool
}

type AccountRepository interface {
	Get(ctx context.Context, id string) (Account, error)
}

type Balance struct {
	Available money.Amount
	Reserved  money.Amount
}

type BalanceRepository interface {
	Get(ctx context.Context, accountID string) (Balance, error)
	Reserve(ctx context.Context, accountID string, amount money.Amount) (Balance, error)
}

type LedgerEntry struct {
	AccountID    string
	Kind         string
	Amount       money.Amount
	MessageID    string
	OpID         string
	BalanceAfter money.Amount
}

type LedgerRepository interface {
	Append(ctx context.Context, entry LedgerEntry) error
	AppendMany(ctx context.Context, entries []LedgerEntry) error
}

type Destination struct {
	MCC     string
	MNC     string
	Country string
}

type DestinationResolver interface {
	Resolve(ctx context.Context, number phone.Number) (Destination, error)
}

type TariffRepository interface {
	UnitPrice(ctx context.Context, plan, mcc, mnc string, at time.Time) (money.Amount, error)
}

type SenderRepository interface {
	IsApproved(ctx context.Context, accountID, sender, country string) (bool, error)
}

type StoredMessage struct {
	message.Message
	MCC        string
	MNC        string
	Test       bool
	ErrorCode  string
	UpdatedAt  time.Time
	IdemKey    string
	CampaignID string
}

type MessageRepository interface {
	Insert(ctx context.Context, msg StoredMessage) error
	InsertMany(ctx context.Context, msgs []StoredMessage) error
	FindByID(ctx context.Context, accountID, id string) (StoredMessage, error)
}

type IdempotencyRecord struct {
	RequestHash []byte
	Response    []byte
	Completed   bool
}

type IdempotencyStore interface {
	Begin(ctx context.Context, accountID, key string, requestHash []byte) (existing *IdempotencyRecord, err error)
	Complete(ctx context.Context, accountID, key string, response []byte) error
}

type RateDecision struct {
	Allowed    bool
	Limit      int
	Remaining  int
	RetryAfter time.Duration
	ResetAfter time.Duration
}

type RateLimiter interface {
	Allow(ctx context.Context, key string) (RateDecision, error)
}
