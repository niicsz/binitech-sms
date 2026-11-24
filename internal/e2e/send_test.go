package e2e_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/niicsz/binitech-sms/internal/contract/events"
)

var otp = map[string]any{"to": "+5511987654321", "from": "BINITECH", "text": "Seu codigo e 482913"}

func TestSendChargesPublishesAndQueries(t *testing.T) {
	e := newEnv(t, options{})
	e.credit("10")

	res := e.do(http.MethodPost, "/v1/message/send", e.liveKey, otp, nil)
	if res.status != http.StatusAccepted || res.code() != 0 {
		t.Fatalf("envio: %d %v", res.status, res.body)
	}
	data := res.data()
	id, _ := data["message_id"].(string)
	if !strings.HasPrefix(id, "msg_") || data["cost"] != "0.0800" || data["encoding"] != "GSM7" || data["segments"] != float64(1) {
		t.Fatalf("resposta inesperada: %v", data)
	}

	if got := e.scalar(`SELECT reserved::text FROM balances WHERE account_id = $1`, e.account); got != "0.0800" {
		t.Fatalf("reserva = %s", got)
	}
	if got := e.scalar(`SELECT count(*)::text FROM ledger_entries WHERE account_id = $1 AND kind = 'reserve' AND message_id = $2`, e.account, id); got != "1" {
		t.Fatalf("ledger de reserva = %s", got)
	}
	if got := e.scalar(`SELECT count(*)::text FROM outbox WHERE published_at IS NULL`); got != "0" {
		t.Fatalf("o caminho rápido deveria marcar o outbox como publicado, pendentes=%s", got)
	}

	records := e.kafka.Consume(t, submitTopic, 1)
	var event events.MessageSubmitted
	if err := json.Unmarshal(records[0].Value, &event); err != nil {
		t.Fatal(err)
	}
	if string(records[0].Key) != e.account || event.MessageID != id || event.To != "+5511987654321" || event.MCC != "724" {
		t.Fatalf("evento inesperado: key=%s %+v", records[0].Key, event)
	}

	got := e.do(http.MethodGet, "/v1/message/"+id, e.liveKey, nil, nil)
	if got.status != http.StatusOK || got.data()["status"] != "accepted" || got.data()["from"] != "BINITECH" {
		t.Fatalf("consulta: %d %v", got.status, got.body)
	}
	balance := e.do(http.MethodGet, "/v1/account/balance", e.liveKey, nil, nil)
	if b := balance.data(); b["available"] != "10.0000" || b["reserved"] != "0.0800" || b["spendable"] != "9.9200" || b["currency"] != "BRL" {
		t.Fatalf("saldo: %v", balance.body)
	}
}

func TestIdempotency(t *testing.T) {
	e := newEnv(t, options{})
	e.credit("1")
	key := map[string]string{"Idempotency-Key": "pedido-4521"}

	first := e.do(http.MethodPost, "/v1/message/send", e.liveKey, otp, key)
	second := e.do(http.MethodPost, "/v1/message/send", e.liveKey, otp, key)
	if first.status != http.StatusAccepted || second.status != http.StatusAccepted {
		t.Fatalf("status: %d %d", first.status, second.status)
	}
	if first.data()["message_id"] != second.data()["message_id"] {
		t.Fatalf("a repetição deveria devolver a mesma mensagem: %v x %v", first.data(), second.data())
	}
	if got := e.scalar(`SELECT count(*)::text FROM messages`); got != "1" {
		t.Fatalf("mensagens gravadas = %s", got)
	}
	if got := e.scalar(`SELECT reserved::text FROM balances WHERE account_id = $1`, e.account); got != "0.0800" {
		t.Fatalf("a repetição não pode reservar de novo: %s", got)
	}

	changed := map[string]any{"to": "+5511987654321", "from": "BINITECH", "text": "outro texto"}
	conflict := e.do(http.MethodPost, "/v1/message/send", e.liveKey, changed, key)
	if conflict.status != http.StatusConflict || conflict.code() != 4090 {
		t.Fatalf("mesma chave com outro corpo: %d %v", conflict.status, conflict.body)
	}
}

func TestRejections(t *testing.T) {
	e := newEnv(t, options{})

	noBalance := e.do(http.MethodPost, "/v1/message/send", e.liveKey, otp, nil)
	if noBalance.status != http.StatusPaymentRequired || noBalance.code() != 4020 {
		t.Fatalf("sem saldo: %d %v", noBalance.status, noBalance.body)
	}
	if got := e.scalar(`SELECT count(*)::text FROM messages`); got != "0" {
		t.Fatalf("envio recusado não pode gravar mensagem: %s", got)
	}

	e.credit("1")
	cases := []struct {
		name   string
		body   map[string]any
		status int
		code   int
	}{
		{"remetente não aprovado", map[string]any{"to": "+5511987654321", "from": "OUTRO", "text": "oi"}, http.StatusForbidden, 4030},
		{"telefone fixo", map[string]any{"to": "+551133334444", "from": "BINITECH", "text": "oi"}, http.StatusForbidden, 4030},
		{"telefone inválido", map[string]any{"to": "abc", "from": "BINITECH", "text": "oi"}, http.StatusBadRequest, 4000},
		{"texto vazio", map[string]any{"to": "+5511987654321", "from": "BINITECH", "text": ""}, http.StatusBadRequest, 4000},
		{"texto longo demais", map[string]any{"to": "+5511987654321", "from": "BINITECH", "text": strings.Repeat("a", 1531)}, http.StatusBadRequest, 4000},
	}
	for _, c := range cases {
		res := e.do(http.MethodPost, "/v1/message/send", e.liveKey, c.body, nil)
		if res.status != c.status || res.code() != c.code {
			t.Errorf("%s: %d %v", c.name, res.status, res.body)
		}
	}

	invalid := e.do(http.MethodPost, "/v1/message/send", e.liveKey, map[string]any{"to": "abc", "from": "", "text": ""}, nil)
	errs, _ := invalid.data()["errors"].([]any)
	if len(errs) != 3 {
		t.Fatalf("deveria listar os 3 campos com problema: %v", invalid.body)
	}

	malformed := e.do(http.MethodPost, "/v1/message/send", e.liveKey, "não é objeto", nil)
	if malformed.status != http.StatusBadRequest || malformed.code() != 4000 {
		t.Fatalf("JSON inválido: %d %v", malformed.status, malformed.body)
	}
}

func TestAuthentication(t *testing.T) {
	e := newEnv(t, options{})
	for name, key := range map[string]string{
		"sem chave":        "",
		"chave malformada": "sk_live_xyz",
		"segredo trocado":  e.liveKey[:len(e.liveKey)-4] + "AAAA",
	} {
		res := e.do(http.MethodGet, "/v1/account/balance", key, nil, nil)
		if res.status != http.StatusUnauthorized || res.code() != 4010 {
			t.Errorf("%s: %d %v", name, res.status, res.body)
		}
	}

	bearer := e.do(http.MethodGet, "/v1/account/balance", "", nil, map[string]string{"Authorization": "Bearer " + e.liveKey})
	if bearer.status != http.StatusOK {
		t.Fatalf("Bearer deveria funcionar: %d %v", bearer.status, bearer.body)
	}
	inURL := e.do(http.MethodGet, "/v1/account/balance?api_key=x", e.liveKey, nil, nil)
	if inURL.status != http.StatusUnauthorized {
		t.Fatalf("chave na URL deveria ser recusada: %d", inURL.status)
	}

	other, _ := e.admin.CreateAccount(t.Context(), "Outra loja", "", "")
	otherKey, _ := e.admin.IssueKey(t.Context(), other, "live", "")
	e.credit("1")
	sent := e.do(http.MethodPost, "/v1/message/send", e.liveKey, otp, nil)
	id, _ := sent.data()["message_id"].(string)
	leak := e.do(http.MethodGet, "/v1/message/"+id, otherKey, nil, nil)
	if leak.status != http.StatusNotFound {
		t.Fatalf("outra conta não pode ver a mensagem: %d %v", leak.status, leak.body)
	}
}

func TestTestKeyDoesNotCharge(t *testing.T) {
	e := newEnv(t, options{})
	res := e.do(http.MethodPost, "/v1/message/send", e.testKey, otp, nil)
	if res.status != http.StatusAccepted || res.data()["test"] != true || res.data()["cost"] != "0.0000" {
		t.Fatalf("chave de teste: %d %v", res.status, res.body)
	}
	if got := e.scalar(`SELECT count(*)::text FROM ledger_entries`); got != "0" {
		t.Fatalf("chave de teste não pode mexer no ledger: %s", got)
	}
	records := e.kafka.Consume(t, submitTopic, 1)
	var event events.MessageSubmitted
	_ = json.Unmarshal(records[0].Value, &event)
	if !event.Test {
		t.Fatal("o evento precisa avisar que é teste para não ir para uma rota real")
	}
}

func TestRateLimit(t *testing.T) {
	e := newEnv(t, options{burst: 2})
	for range 2 {
		if res := e.do(http.MethodGet, "/v1/account/balance", e.liveKey, nil, nil); res.status != http.StatusOK {
			t.Fatalf("dentro da rajada: %d", res.status)
		}
	}
	res := e.do(http.MethodGet, "/v1/account/balance", e.liveKey, nil, nil)
	if res.status != http.StatusTooManyRequests || res.code() != 4290 || res.header.Get("Retry-After") == "" {
		t.Fatalf("fora da rajada: %d %v %v", res.status, res.body, res.header)
	}
}
