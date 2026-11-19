package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	kafkaadapter "github.com/niicsz/binitech-sms/internal/adapter/out/kafka"
	"github.com/niicsz/binitech-sms/internal/adapter/out/postgres"
	"github.com/niicsz/binitech-sms/internal/adapter/out/system"
	"github.com/niicsz/binitech-sms/internal/application/port/in"
	"github.com/niicsz/binitech-sms/internal/application/usecase/outboxrelay"
	"github.com/niicsz/binitech-sms/internal/platform/config"
	"github.com/niicsz/binitech-sms/internal/platform/httpserver"
	"github.com/niicsz/binitech-sms/internal/platform/kafkax"
	"github.com/niicsz/binitech-sms/internal/platform/logging"
	"github.com/niicsz/binitech-sms/internal/platform/postgresx"
)

func main() {
	if err := run(); err != nil {
		slog.Error("outbox-relay: encerrado com erro", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("outbox-relay")
	if err != nil {
		return err
	}
	log := logging.New(cfg.Service, cfg.Env, cfg.LogLevel)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgresx.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	kafka, err := kafkax.NewClient(kafkax.ProducerOptions(cfg.KafkaBrokers, cfg.KafkaClientID)...)
	if err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	defer kafka.Close()

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	published := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "outbox_relay_published_total",
		Help: "Eventos do outbox publicados no Kafka pelo relay.",
	})
	failures := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "outbox_relay_failures_total",
		Help: "Rodadas do relay que terminaram com erro.",
	})
	registry.MustRegister(published, failures)

	relay := outboxrelay.New(
		postgres.NewTxManager(pool),
		postgres.NewOutboxRepository(pool),
		kafkaadapter.NewPublisher(kafka),
		system.Clock{},
		outboxrelay.Options{BatchSize: cfg.OutboxBatch, Grace: cfg.OutboxGrace},
	)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{Registry: registry}))
	go func() {
		if err := httpserver.Run(ctx, log, cfg.MetricsAddr, mux, cfg.ShutdownTimeout); err != nil {
			log.Error("metrics: servidor parou", "error", err)
		}
	}()

	loop(ctx, log, relay, cfg.OutboxInterval, published, failures)
	return nil
}

func loop(ctx context.Context, log *slog.Logger, relay in.OutboxRelay, interval time.Duration, published, failures prometheus.Counter) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		result, err := relay.RelayOnce(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			failures.Inc()
			log.Warn("outbox-relay: rodada falhou", "error", err)
		case result.Published > 0:
			published.Add(float64(result.Published))
			log.Debug("outbox-relay: publicados", "count", result.Published)
			continue
		}
		select {
		case <-ctx.Done():
			log.Info("outbox-relay: encerrando")
			return
		case <-ticker.C:
		}
	}
}
