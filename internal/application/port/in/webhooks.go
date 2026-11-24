package in

import (
	"context"
	"time"
)

type WebhookInput struct {
	URL         string
	Events      []string
	Description string
}

type WebhookView struct {
	ID          string
	URL         string
	Events      []string
	Description string
	Enabled     bool
	Failures    int
	LastStatus  int
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type WebhookManager interface {
	Create(ctx context.Context, p Principal, input WebhookInput) (WebhookView, string, error)
	List(ctx context.Context, p Principal) ([]WebhookView, error)
	Get(ctx context.Context, p Principal, id string) (WebhookView, error)
	Update(ctx context.Context, p Principal, id string, input WebhookInput, enable bool) (WebhookView, error)
	Delete(ctx context.Context, p Principal, id string) error
}
