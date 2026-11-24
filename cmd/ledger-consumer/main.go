package main

import (
	"context"

	"github.com/niicsz/binitech-sms/internal/bootstrap"
	"github.com/niicsz/binitech-sms/internal/contract/events"
)

func main() {
	bootstrap.Main("ledger-consumer", func(ctx context.Context, p bootstrap.Process) error {
		return bootstrap.Worker{
			Name:        "ledger-consumer",
			Brokers:     p.Config.KafkaBrokers,
			Topics:      []string{events.TopicLedgerEvents},
			Handler:     bootstrap.LedgerConsumer(p.Pool),
			Log:         p.Log,
			Registry:    p.Registry,
			MetricsAddr: p.Config.MetricsAddr,
		}.Run(ctx)
	})
}
