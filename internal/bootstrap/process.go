package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/niicsz/binitech-sms/internal/platform/config"
	"github.com/niicsz/binitech-sms/internal/platform/kafkax"
	"github.com/niicsz/binitech-sms/internal/platform/logging"
	"github.com/niicsz/binitech-sms/internal/platform/postgresx"
)

type Process struct {
	Config   config.Config
	Log      *slog.Logger
	Pool     *pgxpool.Pool
	Producer *kgo.Client
	Registry *prometheus.Registry
}

func Main(service string, run func(ctx context.Context, p Process) error) {
	if err := mainErr(service, run); err != nil {
		slog.Error(service+": encerrado com erro", "error", err)
		os.Exit(1)
	}
}

func mainErr(service string, run func(ctx context.Context, p Process) error) error {
	cfg, err := config.Load(service)
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
	producer, err := kafkax.NewClient(kafkax.ProducerOptions(cfg.KafkaBrokers, cfg.KafkaClientID)...)
	if err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	defer producer.Close()
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return run(ctx, Process{Config: cfg, Log: log, Pool: pool, Producer: producer, Registry: registry})
}

func Loop(ctx context.Context, log *slog.Logger, name string, every time.Duration, tick func(context.Context) (bool, error)) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		busy, err := tick(ctx)
		if err != nil && ctx.Err() == nil {
			log.Warn(name+": rodada falhou", "error", err)
		}
		if busy && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
