package e2e_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func httpRoute(t *testing.T, e *env, status int) {
	t.Helper()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"id":"prov-1"}`))
	}))
	t.Cleanup(provider.Close)
	config := `{"url":"` + provider.URL + `","body":{"to":"{{to}}"},"id_field":"id"}`
	if _, err := e.pool.Exec(context.Background(),
		`INSERT INTO routes (id, kind, config, mcc_mnc, cost) VALUES ('http-1', 'http', $1, '{724*}', 0.03)`, config); err != nil {
		t.Fatal(err)
	}
}

func (e *env) waitScalar(sql, want string, args ...any) {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	got := ""
	for time.Now().Before(deadline) {
		got = e.scalar(sql, args...)
		if got == want {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	e.t.Fatalf("%s: got %s want %s", sql, got, want)
}

func TestLedgerSettlesDebit(t *testing.T) {
	e := newEnv(t, options{})
	e.credit("1")
	httpRoute(t, e, http.StatusOK)
	e.startFlow("http-1")
	waitHealthy(e, "http-1")

	id := send(e, e.liveKey, otp)
	e.waitStatus(id, "submitted")
	e.waitScalar(`SELECT available::text || '/' || reserved::text FROM balances WHERE account_id = $1`, "0.9200/0.0000", e.account)
	if got := e.scalar(`SELECT count(*)::text FROM ledger_entries WHERE message_id = $1 AND kind = 'debit' AND amount = 0.08`, id); got != "1" {
		t.Fatalf("lançamento de débito: %s", got)
	}
	balance := e.do(http.MethodGet, "/v1/account/balance", e.liveKey, nil, nil)
	if balance.data()["spendable"] != "0.9200" {
		t.Fatalf("saldo depois do débito: %v", balance.body)
	}
}

func TestLedgerReleasesWhenRejected(t *testing.T) {
	e := newEnv(t, options{})
	e.credit("1")
	e.startFlow()
	id := send(e, e.liveKey, otp)
	e.waitStatus(id, "rejected")
	e.waitScalar(`SELECT available::text || '/' || reserved::text FROM balances WHERE account_id = $1`, "1.0000/0.0000", e.account)
	if got := e.scalar(`SELECT count(*)::text FROM ledger_entries WHERE message_id = $1 AND kind = 'release'`, id); got != "1" {
		t.Fatalf("lançamento de liberação: %s", got)
	}
}

func TestWebhookCRUD(t *testing.T) {
	e := newEnv(t, options{})
	body := map[string]any{"url": "https://cliente.example/hooks", "events": []string{"message.delivered", "message.failed"}, "description": "loja"}
	created := e.do(http.MethodPost, "/v1/webhook", e.liveKey, body, nil)
	if created.status != http.StatusCreated {
		t.Fatalf("criar: %d %v", created.status, created.body)
	}
	id, _ := created.data()["id"].(string)
	secret, _ := created.data()["secret"].(string)
	if !strings.HasPrefix(id, "whk_") || !strings.HasPrefix(secret, "whsec_") || created.data()["enabled"] != true {
		t.Fatalf("resposta da criação: %v", created.data())
	}
	if got := e.scalar(`SELECT position($1 in encode(secret, 'escape'))::text FROM webhooks WHERE id = $2`, secret, id); got != "0" {
		t.Fatal("o segredo não pode ficar em texto puro no banco")
	}

	got := e.do(http.MethodGet, "/v1/webhook/"+id, e.liveKey, nil, nil)
	if got.status != http.StatusOK || got.data()["secret"] != nil || got.data()["description"] != "loja" {
		t.Fatalf("consulta não pode devolver o segredo: %d %v", got.status, got.body)
	}
	list := e.do(http.MethodGet, "/v1/webhook", e.liveKey, nil, nil)
	if items, _ := list.body["data"].([]any); len(items) != 1 {
		t.Fatalf("lista: %v", list.body)
	}
	updated := e.do(http.MethodPut, "/v1/webhook/"+id, e.liveKey, map[string]any{"url": "https://cliente.example/v2", "events": []string{"message.submitted"}}, nil)
	if updated.status != http.StatusOK || updated.data()["url"] != "https://cliente.example/v2" {
		t.Fatalf("alterar: %d %v", updated.status, updated.body)
	}

	for name, bad := range map[string]map[string]any{
		"http sem tls":        {"url": "http://cliente.example", "events": []string{"message.delivered"}},
		"evento desconhecido": {"url": "https://cliente.example", "events": []string{"message.lido"}},
		"sem eventos":         {"url": "https://cliente.example", "events": []string{}},
	} {
		if res := e.do(http.MethodPost, "/v1/webhook", e.liveKey, bad, nil); res.status != http.StatusBadRequest {
			t.Errorf("%s: %d %v", name, res.status, res.body)
		}
	}

	other, _ := e.admin.CreateAccount(t.Context(), "Outra", "", "")
	otherKey, _ := e.admin.IssueKey(t.Context(), other, "live", "")
	if res := e.do(http.MethodGet, "/v1/webhook/"+id, otherKey, nil, nil); res.status != http.StatusNotFound {
		t.Fatalf("outra conta não vê o webhook: %d", res.status)
	}
	if res := e.do(http.MethodDelete, "/v1/webhook/"+id, e.liveKey, nil, nil); res.status != http.StatusNoContent {
		t.Fatalf("remover: %d", res.status)
	}
	if res := e.do(http.MethodGet, "/v1/webhook/"+id, e.liveKey, nil, nil); res.status != http.StatusNotFound {
		t.Fatalf("depois de remover: %d", res.status)
	}
}

type receivedHook struct {
	body      []byte
	signature string
	event     string
}

func TestWebhookDeliveredWithValidSignature(t *testing.T) {
	e := newEnv(t, options{})
	var (
		mu       sync.Mutex
		received []receivedHook
		calls    int
	)
	receiver := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		received = append(received, receivedHook{body: body, signature: r.Header.Get("X-BiniTech-Signature"), event: r.Header.Get("X-BiniTech-Event")})
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(receiver.Close)
	e.webhookCAs = x509.NewCertPool()
	e.webhookCAs.AddCert(receiver.Certificate())

	created := e.do(http.MethodPost, "/v1/webhook", e.liveKey,
		map[string]any{"url": receiver.URL + "/hooks", "events": []string{"message.submitted", "message.delivered"}}, nil)
	if created.status != http.StatusCreated {
		t.Fatalf("criar: %d %v", created.status, created.body)
	}
	secret, _ := created.data()["secret"].(string)
	hookID, _ := created.data()["id"].(string)

	e.startFlow("sandbox")
	id := send(e, e.testKey, map[string]any{"to": "+5511987654321", "from": "BINITECH", "text": "oi", "webhook_ref": "pedido-9"})
	e.waitStatus(id, "delivered")

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(received)
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(received) < 2 {
		t.Fatalf("webhooks recebidos: %d (chamadas %d)", len(received), calls)
	}
	seen := map[string]bool{}
	for _, hook := range received {
		if !validSignature(secret, hook.signature, hook.body) {
			t.Fatalf("assinatura inválida: %s", hook.signature)
		}
		var payload struct {
			Event string `json:"event"`
			Data  struct {
				MessageID  string `json:"message_id"`
				WebhookRef string `json:"webhook_ref"`
			} `json:"data"`
		}
		_ = json.Unmarshal(hook.body, &payload)
		if payload.Event != hook.event || payload.Data.MessageID != id || payload.Data.WebhookRef != "pedido-9" {
			t.Fatalf("corpo do webhook: %s", hook.body)
		}
		seen[payload.Event] = true
	}
	if !seen["message.submitted"] || !seen["message.delivered"] {
		t.Fatalf("eventos recebidos: %v", seen)
	}
	e.waitScalar(`SELECT failures::text || '/' || COALESCE(last_status, 0)::text FROM webhooks WHERE id = $1`, "0/200", hookID)
}

func validSignature(secret, header string, body []byte) bool {
	var ts, sig string
	for _, part := range strings.Split(header, ",") {
		k, v, _ := strings.Cut(part, "=")
		switch k {
		case "t":
			ts = v
		case "v1":
			sig = v
		}
	}
	if _, err := strconv.ParseInt(ts, 10, 64); err != nil {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts + "." + string(body)))
	return hmac.Equal([]byte(hex.EncodeToString(mac.Sum(nil))), []byte(sig))
}
