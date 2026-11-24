package main

import (
	"context"
	"errors"
	"os"

	"github.com/niicsz/binitech-sms/internal/bootstrap"
)

func main() {
	bootstrap.Main("route-adapter", func(ctx context.Context, p bootstrap.Process) error {
		routeID := os.Getenv("ROUTE_ID")
		if routeID == "" {
			return errors.New("defina ROUTE_ID com o id da rota (ex.: sandbox, http-wholesale-1)")
		}
		return bootstrap.RouteAdapter{
			RouteID:      routeID,
			Pool:         p.Pool,
			Producer:     p.Producer,
			Brokers:      p.Config.KafkaBrokers,
			Log:          p.Log,
			Registry:     p.Registry,
			MetricsAddr:  p.Config.MetricsAddr,
			CallbackAddr: os.Getenv("CALLBACK_ADDR"),
		}.Run(ctx)
	})
}
