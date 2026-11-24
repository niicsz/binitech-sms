package main

import (
	"context"
	"time"

	"github.com/niicsz/binitech-sms/internal/adapter/out/postgres"
	"github.com/niicsz/binitech-sms/internal/bootstrap"
	"github.com/niicsz/binitech-sms/internal/contract/events"
)

func main() {
	bootstrap.Main("router-worker", func(ctx context.Context, p bootstrap.Process) error {
		catalog := postgres.NewRouteCatalog(p.Pool, 30*time.Second, time.Minute)
		go catalog.Listen(ctx, func(err error) { p.Log.Warn("router: LISTEN das rotas caiu, tentando de novo", "error", err) })
		return bootstrap.Worker{
			Name:        "router",
			Brokers:     p.Config.KafkaBrokers,
			Topics:      []string{events.TopicSubmit(p.Config.Env)},
			Handler:     bootstrap.Router(p.Pool, p.Producer, catalog),
			Log:         p.Log,
			Registry:    p.Registry,
			MetricsAddr: p.Config.MetricsAddr,
		}.Run(ctx)
	})
}
