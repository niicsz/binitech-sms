package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/niicsz/binitech-sms/internal/application/port/in"
	"github.com/niicsz/binitech-sms/internal/contract/events"
)

const (
	ShortDelay     = 5 * time.Minute
	LongDelay      = time.Hour
	headerOrigin   = "origin_topic"
	shortAttempts  = 3
	longAttempts   = 6
	publishTimeout = 10 * time.Second
)

type Metrics struct {
	processed *prometheus.CounterVec
	retried   *prometheus.CounterVec
	dead      *prometheus.CounterVec
}

func NewMetrics(registry *prometheus.Registry) *Metrics {
	m := &Metrics{
		processed: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "consumer_processed_total", Help: "Eventos processados com sucesso."}, []string{"group", "topic"}),
		retried:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "consumer_retried_total", Help: "Eventos enviados para retry."}, []string{"group", "tier"}),
		dead:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "consumer_dead_letter_total", Help: "Eventos enviados para a DLQ."}, []string{"group", "reason"}),
	}
	registry.MustRegister(m.processed, m.retried, m.dead)
	return m
}

type Runner struct {
	group     string
	client    *kgo.Client
	producer  *kgo.Client
	handler   in.EventHandler
	log       *slog.Logger
	metrics   *Metrics
	delayed   time.Duration
	clock     func() time.Time
	sleepFunc func(ctx context.Context, d time.Duration) error
}

type Config struct {
	Group    string
	Consumer *kgo.Client
	Producer *kgo.Client
	Handler  in.EventHandler
	Log      *slog.Logger
	Metrics  *Metrics
	Delay    time.Duration
}

func NewRunner(cfg Config) *Runner {
	return &Runner{
		group: cfg.Group, client: cfg.Consumer, producer: cfg.Producer, handler: cfg.Handler,
		log: cfg.Log.With("group", cfg.Group), metrics: cfg.Metrics, delayed: cfg.Delay,
		clock: time.Now, sleepFunc: sleep,
	}
}

func (r *Runner) Run(ctx context.Context) error {
	defer r.client.AllowRebalance()
	for {
		fetches := r.client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			r.log.Warn("kafka: erro de fetch", "topic", topic, "partition", partition, "error", err)
		})
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			commits []*kgo.Record
		)
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return
			}
			wg.Go(func() {
				done := r.processPartition(ctx, p.Records)
				mu.Lock()
				commits = append(commits, done...)
				mu.Unlock()
			})
		})
		wg.Wait()
		if len(commits) > 0 {
			if err := r.client.CommitRecords(context.WithoutCancel(ctx), commits...); err != nil {
				r.log.Warn("kafka: commit falhou, eventos podem ser reprocessados", "error", err)
			}
		}
		r.client.AllowRebalance()
	}
}

func (r *Runner) processPartition(ctx context.Context, records []*kgo.Record) []*kgo.Record {
	var done []*kgo.Record
	for _, record := range records {
		if r.delayed > 0 {
			if wait := record.Timestamp.Add(r.delayed).Sub(r.clock()); wait > 0 {
				if err := r.sleepFunc(ctx, wait); err != nil {
					return done
				}
			}
		}
		if err := r.process(ctx, record); err != nil {
			r.log.Error("kafka: evento não confirmado, vai ser reprocessado", "topic", record.Topic, "offset", record.Offset, "error", err)
			return done
		}
		done = append(done, record)
	}
	return done
}

func (r *Runner) process(ctx context.Context, record *kgo.Record) error {
	event := toEvent(record)
	err := r.handler.Handle(ctx, event)
	if err == nil {
		r.count(r.metrics.processedVec(), r.group, record.Topic)
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	attempt, _ := strconv.Atoi(event.Headers[events.HeaderAttempt])
	attempt++
	origin := event.Headers[headerOrigin]
	if origin == "" {
		origin = record.Topic
	}
	target, tier := r.next(origin, attempt, err)
	r.log.Warn("kafka: falha no handler", "topic", record.Topic, "offset", record.Offset, "attempt", attempt, "destino", target, "error", err)
	headers := cloneHeaders(record.Headers)
	setHeader(&headers, events.HeaderAttempt, strconv.Itoa(attempt))
	setHeader(&headers, headerOrigin, origin)
	setHeader(&headers, events.HeaderError, truncate(err.Error(), 500))
	out := &kgo.Record{Topic: target, Key: record.Key, Value: record.Value, Headers: headers}
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishTimeout)
	defer cancel()
	if perr := r.producer.ProduceSync(pctx, out).FirstErr(); perr != nil {
		return fmt.Errorf("encaminhar para %s: %w", target, errors.Join(err, perr))
	}
	if target == events.TopicDLQ {
		reason := "esgotado"
		if in.IsPermanent(err) {
			reason = "permanente"
		}
		r.count(r.metrics.deadVec(), r.group, reason)
	} else {
		r.count(r.metrics.retriedVec(), r.group, tier)
	}
	return nil
}

func (r *Runner) next(origin string, attempt int, err error) (string, string) {
	switch {
	case in.IsPermanent(err):
		return events.TopicDLQ, ""
	case attempt <= shortAttempts:
		return events.RetryTopic(origin, events.RetryShort), events.RetryShort
	case attempt <= shortAttempts+longAttempts:
		return events.RetryTopic(origin, events.RetryLong), events.RetryLong
	default:
		return events.TopicDLQ, ""
	}
}

func (r *Runner) count(vec *prometheus.CounterVec, labels ...string) {
	if vec != nil {
		vec.WithLabelValues(labels...).Inc()
	}
}

func (m *Metrics) processedVec() *prometheus.CounterVec {
	if m == nil {
		return nil
	}
	return m.processed
}

func (m *Metrics) retriedVec() *prometheus.CounterVec {
	if m == nil {
		return nil
	}
	return m.retried
}

func (m *Metrics) deadVec() *prometheus.CounterVec {
	if m == nil {
		return nil
	}
	return m.dead
}

func RetryTopicsFor(topic string) []string {
	return []string{events.RetryTopic(topic, events.RetryShort), events.RetryTopic(topic, events.RetryLong)}
}

func IsRetryTopic(topic string) bool {
	return strings.HasSuffix(topic, "."+events.RetryShort) || strings.HasSuffix(topic, "."+events.RetryLong)
}

func toEvent(record *kgo.Record) in.Event {
	headers := make(map[string]string, len(record.Headers))
	for _, h := range record.Headers {
		headers[h.Key] = string(h.Value)
	}
	return in.Event{
		Topic: record.Topic, Key: string(record.Key), Headers: headers, Payload: record.Value,
		Partition: record.Partition, Offset: record.Offset, Timestamp: record.Timestamp,
	}
}

func cloneHeaders(headers []kgo.RecordHeader) []kgo.RecordHeader {
	out := make([]kgo.RecordHeader, len(headers))
	copy(out, headers)
	return out
}

func setHeader(headers *[]kgo.RecordHeader, key, value string) {
	for i, h := range *headers {
		if h.Key == key {
			(*headers)[i].Value = []byte(value)
			return
		}
	}
	*headers = append(*headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
