package e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/niicsz/binitech-sms/internal/adapter/out/postgres"
	"github.com/niicsz/binitech-sms/internal/adapter/out/secret"
	"github.com/niicsz/binitech-sms/internal/adapter/out/secretbox"
	"github.com/niicsz/binitech-sms/internal/adapter/out/system"
	"github.com/niicsz/binitech-sms/internal/application/usecase/admin"
	"github.com/niicsz/binitech-sms/internal/bootstrap"
	"github.com/niicsz/binitech-sms/internal/domain/apikey"
	"github.com/niicsz/binitech-sms/internal/domain/money"
	"github.com/niicsz/binitech-sms/internal/platform/kafkax"
	"github.com/niicsz/binitech-sms/internal/testsupport/kafkatest"
	"github.com/niicsz/binitech-sms/internal/testsupport/pgtest"
)

const submitTopic = "sms.submit.e2e"

func allTopics() []string {
	base := []string{submitTopic, "sms.dispatch.sandbox", "sms.dispatch.http-1", "sms.status.dlr", "webhook.outbound", "ledger.events"}
	topics := []string{"dlq.global", "campaign.progress", "link.clicks"}
	for _, t := range base {
		topics = append(topics, t, t+".retry.5m", t+".retry.1h")
	}
	return topics
}

var fastHasher = secret.Scrypt{N: 16, R: 1, P: 1}

type env struct {
	t       *testing.T
	pool    *pgxpool.Pool
	kafka   *kafkatest.Cluster
	server  *httptest.Server
	admin   *admin.Service
	account string
	liveKey string
	testKey string

	webhookCAs *x509.CertPool
}

type options struct {
	burst int
}

func newEnv(t *testing.T, opts options) *env {
	t.Helper()
	if opts.burst == 0 {
		opts.burst = 1000
	}
	pool := pgtest.New(t)
	cluster := kafkatest.New(t, allTopics()...)
	client := cluster.Client(t, kafkax.ProducerOptions(cluster.Brokers(), "e2e")...)
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	handler := bootstrap.CoreAPI{
		Log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		Env:        "e2e",
		Pool:       pool,
		Kafka:      client,
		Redis:      rdb,
		Registry:   prometheus.NewRegistry(),
		Hasher:     fastHasher,
		RatePerSec: 1,
		RateBurst:  opts.burst,
		SecretBox:  testBox(t),
		OTPKey:     []byte("chave-de-teste-do-otp-com-32-bytes!!"),
		LinkBase:   "https://bnt.li",
	}.Handler()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	clock := system.Clock{}
	svc := admin.New(postgres.NewTxManager(pool), postgres.NewAdmin(pool), postgres.NewBalances(pool), fastHasher, system.NewIDs(clock), clock, rand.Reader)
	e := &env{t: t, pool: pool, kafka: cluster, server: server, admin: svc}
	ctx := context.Background()
	var err error
	if e.account, err = svc.CreateAccount(ctx, "Padaria Teste", "default", "BRL"); err != nil {
		t.Fatal(err)
	}
	if e.liveKey, err = svc.IssueKey(ctx, e.account, apikey.Live, "produção"); err != nil {
		t.Fatal(err)
	}
	if e.testKey, err = svc.IssueKey(ctx, e.account, apikey.Test, "testes"); err != nil {
		t.Fatal(err)
	}
	price, _ := money.Parse("0.0800")
	if err := svc.SetTariff(ctx, "default", "724", "", price, time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := svc.ApproveSender(ctx, e.account, "BINITECH", "shortcode", []string{"BR"}); err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *env) credit(amount string) {
	e.t.Helper()
	value, _ := money.Parse(amount)
	if _, err := e.admin.Credit(context.Background(), e.account, value); err != nil {
		e.t.Fatal(err)
	}
}

type response struct {
	status int
	header http.Header
	body   map[string]any
}

func (r response) code() int {
	if v, ok := r.body["code"].(float64); ok {
		return int(v)
	}
	return -1
}

func (r response) data() map[string]any {
	d, _ := r.body["data"].(map[string]any)
	return d
}

func (e *env) do(method, path, key string, body any, headers map[string]string) response {
	e.t.Helper()
	var reader io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(e.t.Context(), method, e.server.URL+path, reader)
	if err != nil {
		e.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("X-Api-Key", key)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer res.Body.Close()
	out := response{status: res.StatusCode, header: res.Header}
	_ = json.NewDecoder(res.Body).Decode(&out.body)
	return out
}

func (e *env) scalar(sql string, args ...any) string {
	e.t.Helper()
	var v string
	if err := e.pool.QueryRow(context.Background(), sql, args...).Scan(&v); err != nil {
		e.t.Fatalf("%s: %v", sql, err)
	}
	return v
}

func testBox(t *testing.T) *secretbox.AESGCM {
	t.Helper()
	box, err := secretbox.FromBase64("MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	if err != nil {
		t.Fatal(err)
	}
	return box
}

func (e *env) producer() *kgo.Client {
	e.t.Helper()
	return e.kafka.Client(e.t, kafkax.ProducerOptions(e.kafka.Brokers(), "campaign")...)
}
