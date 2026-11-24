package out

import (
	"context"
	"errors"
	"time"
)

var (
	ErrOptedOut = errors.New("destino pediu para não receber mensagens")
	ErrConflict = errors.New("registro já existe")
)

type OTPChallenge struct {
	ID          string
	AccountID   string
	To          string
	CodeHMAC    []byte
	MessageID   string
	Status      string
	Attempts    int
	MaxAttempts int
	ExpiresAt   time.Time
	CreatedAt   time.Time
	IdemKey     string
}

type OTPStore interface {
	ByIdempotency(ctx context.Context, accountID, key string) (OTPChallenge, error)
	Create(ctx context.Context, c OTPChallenge) error
	AttachMessage(ctx context.Context, id, messageID string) error
	RecentFor(ctx context.Context, accountID, to string, since time.Time) (count int, last time.Time, err error)
	ForUpdate(ctx context.Context, accountID, id string) (OTPChallenge, error)
	SaveAttempt(ctx context.Context, id string, attempts int, status string, at time.Time) error
}

type Link struct {
	ID         string
	AccountID  string
	Code       string
	URL        string
	CampaignID string
	MessageID  string
	CreatedAt  time.Time
}

type LinkStore interface {
	CreateLink(ctx context.Context, l Link) error
	ByCode(ctx context.Context, code string) (Link, error)
	RecordClick(ctx context.Context, linkID string, ipHash []byte, userAgent string, at time.Time) error
}

type OptOutStore interface {
	IsOptedOut(ctx context.Context, accountID, msisdn string) (bool, error)
	OptOut(ctx context.Context, sender, msisdn, keyword string) (int, error)
}
