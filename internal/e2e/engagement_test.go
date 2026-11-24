package e2e_test

import (
	"context"
	"crypto/rand"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	httpadapter "github.com/niicsz/binitech-sms/internal/adapter/in/http"
	"github.com/niicsz/binitech-sms/internal/adapter/out/postgres"
	"github.com/niicsz/binitech-sms/internal/adapter/out/system"
	"github.com/niicsz/binitech-sms/internal/application/usecase/links"
	"github.com/niicsz/binitech-sms/internal/bootstrap"
	"github.com/niicsz/binitech-sms/internal/contract/events"
)

func TestOTPFlow(t *testing.T) {
	e := newEnv(t, options{})
	body := map[string]any{"to": "+5511987654321", "from": "BINITECH"}
	sent := e.do(http.MethodPost, "/v1/otp/send", e.testKey, body, map[string]string{"Idempotency-Key": "login-1"})
	if sent.status != http.StatusAccepted {
		t.Fatalf("enviar: %d %v", sent.status, sent.body)
	}
	otpID, _ := sent.data()["otp_id"].(string)
	code, _ := sent.data()["test_code"].(string)
	if !strings.HasPrefix(otpID, "otp_") || len(code) != 6 {
		t.Fatalf("resposta: %v", sent.data())
	}
	text := e.scalar(`SELECT body FROM messages WHERE id = $1`, sent.data()["message_id"])
	if !strings.Contains(text, code) {
		t.Fatalf("o SMS precisa levar o código: %s", text)
	}
	if got := e.scalar(`SELECT position($1 in encode(code_hmac, 'escape'))::text FROM otp_challenges WHERE id = $2`, code, otpID); got != "0" {
		t.Fatal("o código não pode ficar em texto puro")
	}
	again := e.do(http.MethodPost, "/v1/otp/send", e.testKey, body, map[string]string{"Idempotency-Key": "login-1"})
	if again.data()["otp_id"] != otpID {
		t.Fatalf("mesma chave deveria devolver o mesmo desafio: %v", again.body)
	}
	tooSoon := e.do(http.MethodPost, "/v1/otp/send", e.testKey, body, nil)
	if tooSoon.status != http.StatusTooManyRequests || tooSoon.header.Get("Retry-After") == "" {
		t.Fatalf("reenvio antes de 30 s: %d %v", tooSoon.status, tooSoon.body)
	}

	wrong := e.do(http.MethodPost, "/v1/otp/verify", e.testKey, map[string]any{"otp_id": otpID, "code": "000000"}, nil)
	if code == "000000" {
		wrong = e.do(http.MethodPost, "/v1/otp/verify", e.testKey, map[string]any{"otp_id": otpID, "code": "111111"}, nil)
	}
	if wrong.data()["status"] != "invalid" || wrong.data()["attempts_left"] != float64(4) {
		t.Fatalf("código errado: %v", wrong.body)
	}
	right := e.do(http.MethodPost, "/v1/otp/verify", e.testKey, map[string]any{"otp_id": otpID, "code": code}, nil)
	if right.data()["status"] != "verified" {
		t.Fatalf("código certo: %v", right.body)
	}
	reuse := e.do(http.MethodPost, "/v1/otp/verify", e.testKey, map[string]any{"otp_id": otpID, "code": code}, nil)
	if reuse.data()["status"] != "used" {
		t.Fatalf("código já usado: %v", reuse.body)
	}
	if res := e.do(http.MethodPost, "/v1/otp/verify", e.liveKey, map[string]any{"otp_id": "otp_naoexiste", "code": "1"}, nil); res.status != http.StatusNotFound {
		t.Fatalf("desafio inexistente: %d", res.status)
	}
}

func TestOTPExpiresAndBlocks(t *testing.T) {
	e := newEnv(t, options{})
	expiring := e.do(http.MethodPost, "/v1/otp/send", e.testKey, map[string]any{"to": "+5511987654321", "from": "BINITECH"}, nil)
	id := expiring.data()["otp_id"].(string)
	code := expiring.data()["test_code"].(string)
	if _, err := e.pool.Exec(context.Background(), `UPDATE otp_challenges SET expires_at = now() - interval '1 second' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if res := e.do(http.MethodPost, "/v1/otp/verify", e.testKey, map[string]any{"otp_id": id, "code": code}, nil); res.data()["status"] != "expired" {
		t.Fatalf("expirado: %v", res.body)
	}

	limited := e.do(http.MethodPost, "/v1/otp/send", e.testKey, map[string]any{"to": "+5511912345678", "from": "BINITECH", "max_attempts": 2, "length": 4}, nil)
	lid := limited.data()["otp_id"].(string)
	lcode := limited.data()["test_code"].(string)
	wrongCode := "0000"
	if lcode == wrongCode {
		wrongCode = "9999"
	}
	e.do(http.MethodPost, "/v1/otp/verify", e.testKey, map[string]any{"otp_id": lid, "code": wrongCode}, nil)
	blocked := e.do(http.MethodPost, "/v1/otp/verify", e.testKey, map[string]any{"otp_id": lid, "code": wrongCode}, nil)
	if blocked.data()["status"] != "blocked" {
		t.Fatalf("segunda tentativa errada deveria bloquear: %v", blocked.body)
	}
	if res := e.do(http.MethodPost, "/v1/otp/verify", e.testKey, map[string]any{"otp_id": lid, "code": lcode}, nil); res.data()["status"] != "blocked" {
		t.Fatalf("bloqueado não aceita nem o código certo: %v", res.body)
	}
	bad := e.do(http.MethodPost, "/v1/otp/send", e.testKey, map[string]any{"to": "+5511987654321", "from": "BINITECH", "template": "sem marcador"}, nil)
	if bad.status != http.StatusBadRequest {
		t.Fatalf("modelo sem marcador: %d", bad.status)
	}
}

func TestShortLinkRedirectsAndCounts(t *testing.T) {
	e := newEnv(t, options{})
	res := e.do(http.MethodPost, "/v1/link/shorten", e.liveKey, map[string]any{"url": "https://loja.example/promo?x=1"}, nil)
	if res.status != http.StatusCreated {
		t.Fatalf("encurtar: %d %v", res.status, res.body)
	}
	short, _ := res.data()["short_url"].(string)
	code, _ := res.data()["code"].(string)
	if short != "https://bnt.li/"+code || len(code) != 7 {
		t.Fatalf("link: %v", res.data())
	}

	clock := system.Clock{}
	resolver := links.New(postgres.NewEngagement(e.pool), postgres.NewCampaigns(e.pool), system.NewIDs(clock), clock, rand.Reader, "https://bnt.li")
	redirector := httptest.NewServer(httpadapter.NewRedirector(slog.New(slog.NewTextHandler(io.Discard, nil)), resolver))
	t.Cleanup(redirector.Close)
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, redirector.URL+"/"+code, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusFound || resp.Header.Get("Location") != "https://loja.example/promo?x=1" {
		t.Fatalf("redirecionamento: %d %s", resp.StatusCode, resp.Header.Get("Location"))
	}
	if got := e.scalar(`SELECT clicks::text FROM short_links WHERE code = $1`, code); got != "1" {
		t.Fatalf("cliques: %s", got)
	}
	missingReq, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, redirector.URL+"/naoexiste", nil)
	missing, _ := client.Do(missingReq)
	missing.Body.Close()
	if missing.StatusCode != http.StatusNotFound {
		t.Fatalf("código inexistente: %d", missing.StatusCode)
	}
	for name, body := range map[string]map[string]any{
		"esquema perigoso":     {"url": "javascript:alert(1)"},
		"campanha inexistente": {"url": "https://a.example", "campaign_id": "cmp_x"},
	} {
		if r := e.do(http.MethodPost, "/v1/link/shorten", e.liveKey, body, nil); r.status != http.StatusBadRequest {
			t.Errorf("%s: %d", name, r.status)
		}
	}
}

func TestOptOutByMOBlocksFutureSends(t *testing.T) {
	e := newEnv(t, options{})
	e.credit("1")
	e.startFlow()
	emitter := bootstrap.Emitter(e.pool, e.producer())
	envelope, err := emitter.Envelope(events.TopicStatusDLR, "5511987654321", events.OpMessageMO, "", events.MoReceived{
		RouteID: "op-a", From: "5511987654321", To: "BINITECH", Text: " sair ", ReceivedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := emitter.Publish(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	e.waitScalar(`SELECT count(*)::text FROM opt_outs WHERE account_id = $1 AND msisdn = '+5511987654321'`, "1", e.account)
	res := e.do(http.MethodPost, "/v1/message/send", e.liveKey, otp, nil)
	if res.status != http.StatusForbidden || !strings.Contains(res.body["message"].(string), "não receber") {
		t.Fatalf("envio para quem pediu SAIR: %d %v", res.status, res.body)
	}
	other := e.do(http.MethodPost, "/v1/message/send", e.liveKey, map[string]any{"to": "+5511912345678", "from": "BINITECH", "text": "oi"}, nil)
	if other.status != http.StatusAccepted {
		t.Fatalf("outros destinos seguem liberados: %d %v", other.status, other.body)
	}
}
