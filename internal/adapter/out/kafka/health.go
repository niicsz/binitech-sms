package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
)

type HealthChecker struct {
	client *kgo.Client
}

var _ out.HealthChecker = (*HealthChecker)(nil)

func NewHealthChecker(client *kgo.Client) *HealthChecker {
	return &HealthChecker{client: client}
}

func (h *HealthChecker) Name() string { return "kafka" }

func (h *HealthChecker) Check(ctx context.Context) error {
	return h.client.Ping(ctx)
}
