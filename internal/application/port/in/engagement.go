package in

import (
	"context"
	"time"
)

type OTPSendCommand struct {
	Principal      Principal
	IdempotencyKey string
	To             string
	From           string
	Template       string
	Length         int
	TTLSeconds     int
	MaxAttempts    int
}

type OTPSent struct {
	OTPID     string
	MessageID string
	ExpiresAt time.Time
	TestCode  string
}

type OTPVerification struct {
	Status       string
	AttemptsLeft int
}

type OTPService interface {
	Send(ctx context.Context, cmd OTPSendCommand) (OTPSent, error)
	Verify(ctx context.Context, p Principal, otpID, code string) (OTPVerification, error)
}

type ShortLink struct {
	ID       string
	Code     string
	ShortURL string
	URL      string
	Clicks   int64
}

type LinkShortener interface {
	Shorten(ctx context.Context, p Principal, url, campaignID, messageID string) (ShortLink, error)
}

type Click struct {
	Code      string
	IP        string
	UserAgent string
}

type LinkResolver interface {
	Resolve(ctx context.Context, click Click) (string, error)
}
