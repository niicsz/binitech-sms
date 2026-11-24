package main

import (
	"context"

	"github.com/niicsz/binitech-sms/internal/bootstrap"
	"github.com/niicsz/binitech-sms/internal/contract/events"
)

func main() {
	bootstrap.Main("status-engine", func(ctx context.Context, p bootstrap.Process) error {
		return bootstrap.Worker{
			Name:        "status-engine",
			Brokers:     p.Config.KafkaBrokers,
			Topics:      []string{events.TopicStatusDLR},
			Handler:     bootstrap.StatusEngine(p.Pool, p.Producer),
			Log:         p.Log,
			Registry:    p.Registry,
			MetricsAddr: p.Config.MetricsAddr,
		}.Run(ctx)
	})
}
