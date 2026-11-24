package kafka_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	kafkain "github.com/niicsz/binitech-sms/internal/adapter/in/kafka"
	"github.com/niicsz/binitech-sms/internal/application/port/in"
	"github.com/niicsz/binitech-sms/internal/contract/events"
	"github.com/niicsz/binitech-sms/internal/platform/kafkax"
	"github.com/niicsz/binitech-sms/internal/testsupport/kafkatest"
)

const topic = "work"

var allTopics = append([]string{topic, events.TopicDLQ}, kafkain.RetryTopicsFor(topic)...)

type recorder struct {
	mu       sync.Mutex
	seen     map[string]int
	failFor  map[string]int
	permFor  map[string]bool
	attempts map[string][]string
}

func newRecorder() *recorder {
	return &recorder{seen: map[string]int{}, failFor: map[string]int{}, permFor: map[string]bool{}, attempts: map[string][]string{}}
}

func (r *recorder) Handle(_ context.Context, e in.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := string(e.Payload)
	r.seen[key]++
	r.attempts[key] = append(r.attempts[key], e.Topic+"#"+e.Headers[events.HeaderAttempt])
	if r.permFor[key] {
		return in.Permanent(errors.New("payload inválido"))
	}
	if r.seen[key] <= r.failFor[key] {
		return errors.New("falha transitória")
	}
	return nil
}

func (r *recorder) count(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen[key]
}

func start(t *testing.T, cluster *kafkatest.Cluster, group string, handler in.EventHandler, topics []string, delay time.Duration) {
	t.Helper()
	opts := kafkax.ConsumerOptions(cluster.Brokers(), group, group, topics...)
	if delay > 0 {
		opts = kafkax.DelayedConsumerOptions(cluster.Brokers(), group, group, topics...)
	}
	opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	consumer := cluster.Client(t, opts...)
	producer := cluster.Client(t, kafkax.ProducerOptions(cluster.Brokers(), group+"-p")...)
	runner := kafkain.NewRunner(kafkain.Config{
		Group: group, Consumer: consumer, Producer: producer, Handler: handler,
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics: kafkain.NewMetrics(prometheus.NewRegistry()),
		Delay:   delay,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = runner.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
}

func startAll(t *testing.T, cluster *kafkatest.Cluster, handler in.EventHandler) {
	start(t, cluster, "main", handler, []string{topic}, 0)
	retries := kafkain.RetryTopicsFor(topic)
	start(t, cluster, "retry-short", handler, retries[:1], 20*time.Millisecond)
	start(t, cluster, "retry-long", handler, retries[1:], 40*time.Millisecond)
}

func produce(t *testing.T, cluster *kafkatest.Cluster, payloads ...string) {
	t.Helper()
	client := cluster.Client(t)
	for _, p := range payloads {
		if err := client.ProduceSync(context.Background(), &kgo.Record{Topic: topic, Key: []byte("acc"), Value: []byte(p)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("tempo esgotado esperando: %s", what)
}

func TestProcessesAndCommits(t *testing.T) {
	cluster := kafkatest.New(t, allTopics...)
	rec := newRecorder()
	startAll(t, cluster, rec)
	produce(t, cluster, "a", "b", "c")
	eventually(t, "3 eventos processados", func() bool { return rec.count("a") == 1 && rec.count("b") == 1 && rec.count("c") == 1 })
}

func TestTransientFailureGoesThroughRetryTiers(t *testing.T) {
	cluster := kafkatest.New(t, allTopics...)
	rec := newRecorder()
	rec.failFor["x"] = 4
	startAll(t, cluster, rec)
	produce(t, cluster, "x")
	eventually(t, "sucesso na quinta tentativa", func() bool { return rec.count("x") == 5 })
	rec.mu.Lock()
	got := rec.attempts["x"]
	rec.mu.Unlock()
	want := []string{"work#", "work.retry.5m#1", "work.retry.5m#2", "work.retry.5m#3", "work.retry.1h#4"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("caminho das tentativas: got %v want %v", got, want)
		}
	}
}

func TestPermanentErrorGoesStraightToDLQ(t *testing.T) {
	cluster := kafkatest.New(t, allTopics...)
	rec := newRecorder()
	rec.permFor["bad"] = true
	startAll(t, cluster, rec)
	produce(t, cluster, "bad")
	records := cluster.Consume(t, events.TopicDLQ, 1)
	headers := map[string]string{}
	for _, h := range records[0].Headers {
		headers[h.Key] = string(h.Value)
	}
	if headers["origin_topic"] != topic || headers[events.HeaderAttempt] != "1" || headers[events.HeaderError] == "" {
		t.Fatalf("headers na DLQ: %v", headers)
	}
	if rec.count("bad") != 1 {
		t.Fatalf("erro permanente não deve ser repetido: %d", rec.count("bad"))
	}
}

func TestExhaustedRetriesGoToDLQ(t *testing.T) {
	cluster := kafkatest.New(t, allTopics...)
	rec := newRecorder()
	rec.failFor["never"] = 1000
	startAll(t, cluster, rec)
	produce(t, cluster, "never")
	records := cluster.Consume(t, events.TopicDLQ, 1)
	attempt := ""
	for _, h := range records[0].Headers {
		if h.Key == events.HeaderAttempt {
			attempt = string(h.Value)
		}
	}
	if n, _ := strconv.Atoi(attempt); n != 10 || rec.count("never") != 10 {
		t.Fatalf("esperava 10 tentativas antes da DLQ: header=%s chamadas=%d", attempt, rec.count("never"))
	}
}

func TestDoesNotReprocessAfterRestart(t *testing.T) {
	cluster := kafkatest.New(t, allTopics...)
	first := newRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	opts := append(kafkax.ConsumerOptions(cluster.Brokers(), "g", "g", topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	consumer, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	producer := cluster.Client(t, kafkax.ProducerOptions(cluster.Brokers(), "p")...)
	runner := kafkain.NewRunner(kafkain.Config{Group: "g", Consumer: consumer, Producer: producer, Handler: first, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	done := make(chan struct{})
	go func() { _ = runner.Run(ctx); close(done) }()
	produce(t, cluster, "once")
	eventually(t, "primeiro processamento", func() bool { return first.count("once") == 1 })
	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done
	consumer.Close()

	second := newRecorder()
	start(t, cluster, "g", second, []string{topic}, 0)
	produce(t, cluster, "after")
	eventually(t, "evento novo processado", func() bool { return second.count("after") == 1 })
	if second.count("once") != 0 {
		t.Fatal("evento já confirmado foi reprocessado depois de reiniciar")
	}
}
