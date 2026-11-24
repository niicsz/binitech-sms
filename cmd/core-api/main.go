package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/redis/go-redis/v9"

	"github.com/niicsz/binitech-sms/internal/bootstrap"
	"github.com/niicsz/binitech-sms/internal/platform/config"
	"github.com/niicsz/binitech-sms/internal/platform/httpserver"
	"github.com/niicsz/binitech-sms/internal/platform/kafkax"
	"github.com/niicsz/binitech-sms/internal/platform/logging"
	"github.com/niicsz/binitech-sms/internal/platform/postgresx"
)

func main() {
	if err := run(); err != nil {
		slog.Error("core-api: encerrado com erro", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("core-api")
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

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPassword})
	defer func() { _ = rdb.Close() }()

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	handler := bootstrap.CoreAPI{
		Log:        log,
		Env:        cfg.Env,
		Pool:       pool,
		Kafka:      kafka,
		Redis:      rdb,
		Registry:   registry,
		RatePerSec: cfg.RatePerSecond,
		RateBurst:  cfg.RateBurst,
	}.Handler()
	return httpserver.Run(ctx, log, cfg.HTTPAddr, handler, cfg.ShutdownTimeout)
}
