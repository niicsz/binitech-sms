package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/niicsz/binitech-sms/internal/adapter/out/postgres"
	"github.com/niicsz/binitech-sms/internal/adapter/out/webhookhttp"
	"github.com/niicsz/binitech-sms/internal/bootstrap"
	"github.com/niicsz/binitech-sms/internal/contract/events"
	"github.com/niicsz/binitech-sms/internal/platform/kafkax"
)

const fastRetry = 150 * time.Millisecond

var routeEnv = map[string]string{"HTTP1_TOKEN": "segredo", "HTTP1_CALLBACK": "cb-token"}

func (e *env) startFlow(routeIDs ...string) map[string]string {
	e.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	e.t.Cleanup(func() { cancel(); wg.Wait() })
	sink := io.Writer(io.Discard)
	if os.Getenv("E2E_LOG") != "" {
		sink = os.Stderr
	}
	log := slog.New(slog.NewTextHandler(sink, nil))
	producer := e.kafka.Client(e.t, kafkax.ProducerOptions(e.kafka.Brokers(), "flow")...)
	catalog := postgres.NewRouteCatalog(e.pool, 50*time.Millisecond, time.Minute)
	run := func(w bootstrap.Worker) {
		w.Brokers, w.Log, w.Registry, w.ShortDelay, w.LongDelay = e.kafka.Brokers(), log, prometheus.NewRegistry(), fastRetry, 2*fastRetry
		wg.Go(func() { _ = w.Run(ctx) })
	}
	run(bootstrap.Worker{Name: "router", Topics: []string{submitTopic}, Handler: bootstrap.Router(e.pool, producer, catalog)})
	run(bootstrap.Worker{Name: "status-engine", Topics: []string{events.TopicStatusDLR}, Handler: bootstrap.StatusEngine(e.pool, producer)})
	run(bootstrap.Worker{Name: "ledger-consumer", Topics: []string{events.TopicLedgerEvents}, Handler: bootstrap.LedgerConsumer(e.pool)})
	run(bootstrap.Worker{Name: "webhook-dispatcher", Topics: []string{events.TopicWebhookOutbound},
		Handler: bootstrap.WebhookDispatcher(e.pool, producer, testBox(e.t), webhookhttp.NewWithOptions(webhookhttp.Options{
			Timeout: 2 * time.Second, AllowPrivate: true, RootCAs: e.webhookCAs,
		}))})
	callbacks := map[string]string{}
	for _, id := range routeIDs {
		addr := freeAddr(e.t)
		callbacks[id] = "http://" + addr + "/callbacks/" + id
		adapter := bootstrap.RouteAdapter{
			RouteID: id, Pool: e.pool, Producer: producer, Brokers: e.kafka.Brokers(), Log: log,
			Registry: prometheus.NewRegistry(), SandboxDelay: 50 * time.Millisecond, CallbackAddr: addr,
			ShortDelay: fastRetry, LongDelay: 2 * fastRetry, HealthEvery: 50 * time.Millisecond,
			Env: func(k string) string { return routeEnv[k] },
		}
		wg.Go(func() { _ = adapter.Run(ctx) })
	}
	return callbacks
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func (e *env) waitStatus(id, want string) {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	got := ""
	for time.Now().Before(deadline) {
		got = e.scalar(`SELECT status FROM messages WHERE id = $1`, id)
		if got == want {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	e.t.Fatalf("mensagem %s ficou em %s, esperava %s", id, got, want)
}

func (e *env) events(topic string, want int) []map[string]any {
	e.t.Helper()
	records := e.kafka.Consume(e.t, topic, want)
	var result []map[string]any
	for _, r := range records {
		var m map[string]any
		_ = json.Unmarshal(r.Value, &m)
		result = append(result, m)
	}
	return result
}

func send(e *env, key string, body map[string]any) string {
	e.t.Helper()
	res := e.do(http.MethodPost, "/v1/message/send", key, body, nil)
	if res.status != http.StatusAccepted {
		e.t.Fatalf("envio: %d %v", res.status, res.body)
	}
	id, _ := res.data()["message_id"].(string)
	return id
}

func TestSandboxDeliversTestMessages(t *testing.T) {
	e := newEnv(t, options{})
	e.startFlow("sandbox")
	long := map[string]any{"to": "+5511987654321", "from": "BINITECH", "text": strings.Repeat("a", 200)}
	id := send(e, e.testKey, long)
	e.waitStatus(id, "delivered")

	if got := e.scalar(`SELECT count(*)::text FROM message_parts WHERE message_id = $1 AND status = 'delivered'`, id); got != "2" {
		t.Fatalf("as duas partes deveriam estar entregues: %s", got)
	}
	if got := e.scalar(`SELECT count(*)::text FROM dlr_events WHERE message_id = $1`, id); got != "2" {
		t.Fatalf("DLRs gravados: %s", got)
	}
	webhooks := e.events(events.TopicWebhookOutbound, 2)
	if webhooks[0]["status"] != "submitted" || webhooks[1]["status"] != "delivered" {
		t.Fatalf("webhooks: %v", webhooks)
	}
	if got := e.scalar(`SELECT count(*)::text FROM ledger_entries`); got != "0" {
		t.Fatalf("teste não cobra: %s", got)
	}
}

func TestSandboxFailureAndRejection(t *testing.T) {
	e := newEnv(t, options{})
	e.startFlow("sandbox")
	failed := send(e, e.testKey, map[string]any{"to": "+5511987650000", "from": "BINITECH", "text": "oi"})
	rejected := send(e, e.testKey, map[string]any{"to": "+5511987659999", "from": "BINITECH", "text": "oi"})
	e.waitStatus(failed, "undelivered")
	e.waitStatus(rejected, "rejected")
	if got := e.scalar(`SELECT error_code FROM messages WHERE id = $1`, rejected); got != "sandbox_rejected" {
		t.Fatalf("código do erro: %s", got)
	}
}

func TestLiveMessageThroughHTTPRoute(t *testing.T) {
	e := newEnv(t, options{})
	e.credit("1")
	var mu sync.Mutex
	var received []map[string]string
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "segredo" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		received = append(received, body)
		mu.Unlock()
		_, _ = w.Write([]byte(`{"data":{"id":"prov-123"}}`))
	}))
	t.Cleanup(provider.Close)
	config := `{"url":"` + provider.URL + `/send","auth_header":"Authorization","auth_env":"HTTP1_TOKEN",
		"body":{"phone":"{{to_digits}}","sender":"{{from}}","message":"{{text}}","ref":"{{message_id}}"},
		"id_field":"data.id","callback_token_env":"HTTP1_CALLBACK",
		"dlr":{"id_field":"id","status_field":"status","error_field":"err"},"tps":50}`
	if _, err := e.pool.Exec(context.Background(),
		`INSERT INTO routes (id, kind, config, mcc_mnc, cost, priority) VALUES ('http-1', 'http', $1, '{724*}', 0.03, 10)`, config); err != nil {
		t.Fatal(err)
	}
	callbacks := e.startFlow("http-1")
	waitHealthy(e, "http-1")

	id := send(e, e.liveKey, otp)
	e.waitStatus(id, "submitted")
	mu.Lock()
	first := received[0]
	mu.Unlock()
	if first["phone"] != "5511987654321" || first["sender"] != "BINITECH" || first["ref"] != id || first["message"] != otp["text"] {
		t.Fatalf("corpo enviado ao fornecedor: %v", first)
	}
	ledger := e.events(events.TopicLedgerEvents, 1)
	if ledger[0]["kind"] != "debit" || ledger[0]["amount"] != "0.0800" || ledger[0]["message_id"] != id {
		t.Fatalf("evento de ledger: %v", ledger[0])
	}

	if code := postCallback(t, callbacks["http-1"], "errado", `{"id":"prov-123","status":"delivered"}`); code != http.StatusUnauthorized {
		t.Fatalf("callback sem token válido deveria dar 401: %d", code)
	}
	if code := postCallback(t, callbacks["http-1"], "cb-token", `{"id":"prov-123","status":"delivered"}`); code != http.StatusNoContent {
		t.Fatalf("callback: %d", code)
	}
	e.waitStatus(id, "delivered")
}

func TestHTTPRouteRejectionReleasesReservation(t *testing.T) {
	e := newEnv(t, options{})
	e.credit("1")
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(provider.Close)
	config := `{"url":"` + provider.URL + `","body":{"to":"{{to}}"},"id_field":"id"}`
	if _, err := e.pool.Exec(context.Background(),
		`INSERT INTO routes (id, kind, config, mcc_mnc, cost) VALUES ('http-1', 'http', $1, '{724*}', 0.03)`, config); err != nil {
		t.Fatal(err)
	}
	e.startFlow("http-1")
	waitHealthy(e, "http-1")
	id := send(e, e.liveKey, otp)
	e.waitStatus(id, "rejected")
	if got := e.scalar(`SELECT error_code FROM messages WHERE id = $1`, id); got != "http_400" {
		t.Fatalf("código: %s", got)
	}
	ledger := e.events(events.TopicLedgerEvents, 1)
	if ledger[0]["kind"] != "release" {
		t.Fatalf("deveria liberar a reserva: %v", ledger[0])
	}
}

func TestNoRouteRejectsAndReleases(t *testing.T) {
	e := newEnv(t, options{})
	e.credit("1")
	e.startFlow()
	id := send(e, e.liveKey, otp)
	e.waitStatus(id, "rejected")
	if got := e.scalar(`SELECT error_code FROM messages WHERE id = $1`, id); got != "no_route" {
		t.Fatalf("código: %s", got)
	}
	ledger := e.events(events.TopicLedgerEvents, 1)
	if ledger[0]["kind"] != "release" {
		t.Fatalf("deveria liberar a reserva: %v", ledger[0])
	}
}

func postCallback(t *testing.T, url, token, body string) int {
	t.Helper()
	for range 50 {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, url, bytes.NewBufferString(body))
		req.Header.Set("X-Callback-Token", token)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		res.Body.Close()
		return res.StatusCode
	}
	t.Fatalf("callback em %s não respondeu", url)
	return 0
}

func waitHealthy(e *env, routeID string) {
	e.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var healthy bool
		if err := e.pool.QueryRow(context.Background(), `SELECT healthy FROM route_health WHERE route_id = $1`, routeID).Scan(&healthy); err == nil && healthy {
			time.Sleep(100 * time.Millisecond)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	e.t.Fatalf("rota %s não ficou saudável", routeID)
}
