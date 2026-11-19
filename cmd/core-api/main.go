package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	httpadapter "github.com/niicsz/binitech-sms/internal/adapter/in/http"
	kafkaadapter "github.com/niicsz/binitech-sms/internal/adapter/out/kafka"
	"github.com/niicsz/binitech-sms/internal/adapter/out/postgres"
	"github.com/niicsz/binitech-sms/internal/application/usecase/readiness"
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

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	ready := readiness.New(2*time.Second,
		postgres.NewHealthChecker(pool),
		kafkaadapter.NewHealthChecker(kafka),
	)
	router := httpadapter.NewRouter(httpadapter.Deps{Log: log, Readiness: ready, Registry: registry})
	return httpserver.Run(ctx, log, cfg.HTTPAddr, router, cfg.ShutdownTimeout)
}
