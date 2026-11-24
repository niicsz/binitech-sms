package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Service         string
	Env             string
	LogLevel        string
	HTTPAddr        string
	MetricsAddr     string
	DatabaseURL     string
	KafkaBrokers    []string
	KafkaClientID   string
	RedisAddr       string
	RedisPassword   string
	RatePerSecond   int
	RateBurst       int
	OutboxBatch     int
	OutboxInterval  time.Duration
	OutboxGrace     time.Duration
	ShutdownTimeout time.Duration
}

func Load(service string) (Config, error) {
	cfg := Config{
		Service:         service,
		Env:             get("APP_ENV", "local"),
		LogLevel:        get("LOG_LEVEL", "info"),
		HTTPAddr:        get("HTTP_ADDR", ":8080"),
		MetricsAddr:     get("METRICS_ADDR", ":9101"),
		DatabaseURL:     os.Getenv("DATABASE_URL"),
		KafkaBrokers:    split(get("KAFKA_BROKERS", "127.0.0.1:29092")),
		KafkaClientID:   get("KAFKA_CLIENT_ID", service),
		RedisAddr:       get("REDIS_ADDR", "127.0.0.1:6380"),
		RedisPassword:   os.Getenv("REDIS_PASSWORD"),
		ShutdownTimeout: 15 * time.Second,
	}
	var err error
	if cfg.RatePerSecond, err = intValue("RATE_LIMIT_PER_SECOND", 50); err != nil {
		return cfg, err
	}
	if cfg.RateBurst, err = intValue("RATE_LIMIT_BURST", 200); err != nil {
		return cfg, err
	}
	if cfg.OutboxBatch, err = intValue("OUTBOX_BATCH", 500); err != nil {
		return cfg, err
	}
	if cfg.OutboxInterval, err = duration("OUTBOX_INTERVAL", 500*time.Millisecond); err != nil {
		return cfg, err
	}
	if cfg.OutboxGrace, err = duration("OUTBOX_GRACE", 2*time.Second); err != nil {
		return cfg, err
	}
	if cfg.DatabaseURL == "" {
		return cfg, fmt.Errorf("config: DATABASE_URL é obrigatória")
	}
	if len(cfg.KafkaBrokers) == 0 {
		return cfg, fmt.Errorf("config: KAFKA_BROKERS é obrigatória")
	}
	return cfg, nil
}

func get(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

func split(value string) []string {
	var parts []string
	for _, part := range strings.Split(value, ",") {
		if part = strings.TrimSpace(part); part != "" {
			parts = append(parts, part)
		}
	}
	return parts
}

func intValue(key string, fallback int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("config: %s precisa ser inteiro positivo", key)
	}
	return value, nil
}

func duration(key string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("config: %s precisa ser uma duração como 500ms ou 2s", key)
	}
	return value, nil
}
