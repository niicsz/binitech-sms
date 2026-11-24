package main

import (
	"context"
	"fmt"
	"time"

	"github.com/niicsz/binitech-sms/internal/adapter/out/secretbox"
	"github.com/niicsz/binitech-sms/internal/adapter/out/webhookhttp"
	"github.com/niicsz/binitech-sms/internal/bootstrap"
	"github.com/niicsz/binitech-sms/internal/contract/events"
)

func main() {
	bootstrap.Main("webhook-dispatcher", func(ctx context.Context, p bootstrap.Process) error {
		box, err := secretbox.FromBase64(p.Config.WebhookKey)
		if err != nil {
			return fmt.Errorf("WEBHOOK_SECRET_KEY: %w", err)
		}
		return bootstrap.Worker{
			Name:        "webhook-dispatcher",
			Brokers:     p.Config.KafkaBrokers,
			Topics:      []string{events.TopicWebhookOutbound},
			Handler:     bootstrap.WebhookDispatcher(p.Pool, p.Producer, box, webhookhttp.New(10*time.Second, false)),
			Log:         p.Log,
			Registry:    p.Registry,
			MetricsAddr: p.Config.MetricsAddr,
		}.Run(ctx)
	})
}
