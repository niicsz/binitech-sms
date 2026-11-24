package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	kafkain "github.com/niicsz/binitech-sms/internal/adapter/in/kafka"
	"github.com/niicsz/binitech-sms/internal/application/port/in"
	"github.com/niicsz/binitech-sms/internal/contract/events"
	"github.com/niicsz/binitech-sms/internal/platform/httpserver"
	"github.com/niicsz/binitech-sms/internal/platform/kafkax"
)

type Worker struct {
	Name        string
	Brokers     []string
	Topics      []string
	Handler     in.EventHandler
	Log         *slog.Logger
	Registry    *prometheus.Registry
	MetricsAddr string
	ShortDelay  time.Duration
	LongDelay   time.Duration
	ExtraOpts   []kgo.Opt
}

func (w Worker) Run(ctx context.Context) error {
	if w.ShortDelay == 0 {
		w.ShortDelay = kafkain.ShortDelay
	}
	if w.LongDelay == 0 {
		w.LongDelay = kafkain.LongDelay
	}
	producer, err := kafkax.NewClient(kafkax.ProducerOptions(w.Brokers, w.Name+"-retry")...)
	if err != nil {
		return fmt.Errorf("worker: produtor: %w", err)
	}
	defer producer.Close()
	metrics := kafkain.NewMetrics(w.Registry)

	var short, long []string
	for _, topic := range w.Topics {
		short = append(short, events.RetryTopic(topic, events.RetryShort))
		long = append(long, events.RetryTopic(topic, events.RetryLong))
	}
	stages := []struct {
		group  string
		topics []string
		delay  time.Duration
	}{
		{w.Name, w.Topics, 0},
		{w.Name + "-" + events.RetryShort, short, w.ShortDelay},
		{w.Name + "-" + events.RetryLong, long, w.LongDelay},
	}

	g, ctx := errgroup.WithContext(ctx)
	for _, stage := range stages {
		opts := kafkax.ConsumerOptions(w.Brokers, stage.group, stage.group, stage.topics...)
		if stage.delay > 0 {
			opts = kafkax.DelayedConsumerOptions(w.Brokers, stage.group, stage.group, stage.topics...)
		}
		consumer, err := kafkax.NewClient(append(opts, w.ExtraOpts...)...)
		if err != nil {
			return fmt.Errorf("worker: consumidor %s: %w", stage.group, err)
		}
		runner := kafkain.NewRunner(kafkain.Config{
			Group: stage.group, Consumer: consumer, Producer: producer, Handler: w.Handler,
			Log: w.Log, Metrics: metrics, Delay: stage.delay,
		})
		g.Go(func() error {
			defer consumer.Close()
			return runner.Run(ctx)
		})
	}
	if w.MetricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(w.Registry, promhttp.HandlerOpts{Registry: w.Registry}))
		g.Go(func() error { return httpserver.Run(ctx, w.Log, w.MetricsAddr, mux, 10*time.Second) })
	}
	w.Log.Info("worker: consumindo", "topics", w.Topics)
	return g.Wait()
}
