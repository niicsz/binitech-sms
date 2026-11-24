package bootstrap

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/niicsz/binitech-sms/internal/adapter/out/postgres"
	"github.com/niicsz/binitech-sms/internal/application/usecase/delivery"
	"github.com/niicsz/binitech-sms/internal/contract/events"
	"github.com/niicsz/binitech-sms/internal/platform/httpserver"
)

type RouteAdapter struct {
	RouteID      string
	Pool         *pgxpool.Pool
	Producer     *kgo.Client
	Brokers      []string
	Log          *slog.Logger
	Registry     *prometheus.Registry
	MetricsAddr  string
	CallbackAddr string
	Env          func(string) string
	SandboxDelay time.Duration
	ShortDelay   time.Duration
	LongDelay    time.Duration
	HealthEvery  time.Duration
}

func (a RouteAdapter) Run(ctx context.Context) error {
	if a.Env == nil {
		a.Env = os.Getenv
	}
	if a.HealthEvery == 0 {
		a.HealthEvery = 15 * time.Second
	}
	settings, err := LoadRoute(ctx, a.Pool, a.RouteID)
	if err != nil {
		return err
	}
	built, err := BuildCarrier(a.RouteID, settings, a.Env, a.SandboxDelay)
	if err != nil {
		return err
	}
	defer func() { _ = built.Carrier.Close() }()
	emitter := Emitter(a.Pool, a.Producer)
	log := a.Log.With("route", a.RouteID)
	onError := func(err error) { log.Warn("route-adapter: erro", "error", err) }

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		delivery.PumpReports(ctx, built.Carrier, emitter, onError)
		return nil
	})
	g.Go(func() error {
		delivery.WatchHealth(ctx, built.Carrier, postgres.NewDelivery(a.Pool), a.HealthEvery, onError)
		return nil
	})
	if built.Callback != nil && a.CallbackAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/callbacks/"+a.RouteID, built.Callback.CallbackHandler())
		g.Go(func() error { return httpserver.Run(ctx, log, a.CallbackAddr, mux, 10*time.Second) })
	}
	g.Go(func() error {
		return Worker{
			Name:        "route-" + a.RouteID,
			Brokers:     a.Brokers,
			Topics:      []string{events.TopicDispatch(a.RouteID)},
			Handler:     delivery.New(built.Carrier, emitter, built.Throttle),
			Log:         log,
			Registry:    a.Registry,
			MetricsAddr: a.MetricsAddr,
			ShortDelay:  a.ShortDelay,
			LongDelay:   a.LongDelay,
		}.Run(ctx)
	})
	return g.Wait()
}
