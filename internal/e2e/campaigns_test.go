package e2e_test

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/niicsz/binitech-sms/internal/adapter/out/postgres"
	"github.com/niicsz/binitech-sms/internal/adapter/out/system"
	"github.com/niicsz/binitech-sms/internal/application/usecase/campaign"
	"github.com/niicsz/binitech-sms/internal/bootstrap"
)

func TestSendBulk(t *testing.T) {
	e := newEnv(t, options{})
	e.credit("1")
	body := map[string]any{
		"from": "BINITECH", "text": "Promoção de pão",
		"messages": []map[string]any{
			{"to": "+5511987654321"},
			{"to": "telefone-errado"},
			{"to": "+5511912345678", "text": "Texto próprio", "webhook_ref": "c-2"},
		},
	}
	key := map[string]string{"Idempotency-Key": "lote-1"}
	res := e.do(http.MethodPost, "/v1/message/sendBulk", e.liveKey, body, key)
	if res.status != http.StatusAccepted {
		t.Fatalf("lote: %d %v", res.status, res.body)
	}
	accepted, _ := res.data()["accepted"].([]any)
	rejected, _ := res.data()["rejected"].([]any)
	if len(accepted) != 2 || len(rejected) != 1 || res.data()["total_cost"] != "0.1600" {
		t.Fatalf("resultado: %v", res.data())
	}
	if r := rejected[0].(map[string]any); r["index"] != float64(1) || r["field"] != "to" {
		t.Fatalf("recusado: %v", r)
	}
	if got := e.scalar(`SELECT reserved::text FROM balances WHERE account_id = $1`, e.account); got != "0.1600" {
		t.Fatalf("reserva: %s", got)
	}
	if got := e.scalar(`SELECT count(*)::text FROM ledger_entries WHERE kind = 'reserve'`); got != "2" {
		t.Fatalf("lançamentos de reserva: %s", got)
	}
	if got := e.scalar(`SELECT body FROM messages WHERE webhook_ref = 'c-2'`); got != "Texto próprio" {
		t.Fatalf("texto do item: %s", got)
	}
	e.kafka.Consume(t, submitTopic, 2)

	again := e.do(http.MethodPost, "/v1/message/sendBulk", e.liveKey, body, key)
	first := accepted[0].(map[string]any)["message_id"]
	if again.status != http.StatusAccepted || again.data()["accepted"].([]any)[0].(map[string]any)["message_id"] != first {
		t.Fatalf("repetição do lote deveria devolver os mesmos ids: %v", again.body)
	}
	if got := e.scalar(`SELECT count(*)::text FROM messages`); got != "2" {
		t.Fatalf("repetição não pode duplicar: %s", got)
	}
}

func TestBulkWithoutBalanceInsertsNothing(t *testing.T) {
	e := newEnv(t, options{})
	e.credit("0.1")
	body := map[string]any{"from": "BINITECH", "text": "oi", "messages": []map[string]any{{"to": "+5511987654321"}, {"to": "+5511912345678"}}}
	res := e.do(http.MethodPost, "/v1/message/sendBulk", e.liveKey, body, nil)
	if res.status != http.StatusPaymentRequired {
		t.Fatalf("sem saldo para o lote: %d %v", res.status, res.body)
	}
	if got := e.scalar(`SELECT count(*)::text FROM messages`); got != "0" {
		t.Fatalf("nada pode ser gravado: %s", got)
	}
}

func TestMessageListPagination(t *testing.T) {
	e := newEnv(t, options{})
	for range 5 {
		send(e, e.testKey, otp)
	}
	seen := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		path := "/v1/message/list?limit=2"
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		res := e.do(http.MethodGet, path, e.testKey, nil, nil)
		if res.status != http.StatusOK {
			t.Fatalf("lista: %d %v", res.status, res.body)
		}
		items, _ := res.data()["items"].([]any)
		for _, it := range items {
			id := it.(map[string]any)["message_id"].(string)
			if seen[id] {
				t.Fatalf("mensagem repetida entre páginas: %s", id)
			}
			seen[id] = true
		}
		pages++
		next, _ := res.data()["next_cursor"].(string)
		if next == "" {
			break
		}
		cursor = next
	}
	if len(seen) != 5 || pages != 3 {
		t.Fatalf("esperava 5 mensagens em 3 páginas, veio %d em %d", len(seen), pages)
	}
	filtered := e.do(http.MethodGet, "/v1/message/list?status=delivered", e.testKey, nil, nil)
	if items, _ := filtered.data()["items"].([]any); len(items) != 0 {
		t.Fatalf("filtro por status: %v", filtered.body)
	}
	bad := e.do(http.MethodGet, "/v1/message/list?cursor=abc", e.testKey, nil, nil)
	if bad.status != http.StatusBadRequest {
		t.Fatalf("cursor inválido: %d", bad.status)
	}
}

func (e *env) campaignRunner() *campaign.Service {
	clock := system.Clock{}
	return campaign.New(postgres.NewCampaigns(e.pool), postgres.NewAccounts(e.pool),
		bootstrap.SendService(e.pool, e.producer(), "e2e", clock), system.NewIDs(clock), clock)
}

func TestCampaignRunsAndSummarizes(t *testing.T) {
	e := newEnv(t, options{})
	e.startFlow("sandbox")
	body := map[string]any{
		"name": "Black Friday", "from": "BINITECH", "text": "Oi {{nome}}, pão em dobro hoje",
		"scheduled_at": time.Now().Add(-time.Minute).Format(time.RFC3339),
		"recipients": []map[string]any{
			{"to": "+5511987654321", "vars": map[string]string{"nome": "Ana"}},
			{"to": "+5511912345678", "vars": map[string]string{"nome": "Bia"}},
			{"to": "+5511955554444"},
		},
	}
	created := e.do(http.MethodPost, "/v1/campaign/create", e.testKey, body, map[string]string{"Idempotency-Key": "bf-1"})
	if created.status != http.StatusCreated || created.data()["status"] != "scheduled" || created.data()["total"] != float64(3) {
		t.Fatalf("criar: %d %v", created.status, created.body)
	}
	id := created.data()["campaign_id"].(string)
	repeat := e.do(http.MethodPost, "/v1/campaign/create", e.testKey, body, map[string]string{"Idempotency-Key": "bf-1"})
	if repeat.data()["campaign_id"] != id {
		t.Fatalf("mesma chave deveria devolver a mesma campanha: %v", repeat.body)
	}

	runner := e.campaignRunner()
	for range 3 {
		if _, err := runner.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got := e.scalar(`SELECT body FROM messages WHERE to_msisdn = '+5511987654321'`); got != "Oi Ana, pão em dobro hoje" {
		t.Fatalf("variável não aplicada: %s", got)
	}
	e.waitScalar(`SELECT count(*)::text FROM messages WHERE campaign_id = $1 AND status = 'delivered'`, "2", id)
	summary := e.do(http.MethodGet, "/v1/campaign/"+id+"/summary", e.testKey, nil, nil)
	d := summary.data()
	if d["status"] != "done" || d["submitted"] != float64(2) || d["rejected"] != float64(1) || d["delivered"] != float64(2) || d["pending"] != float64(0) {
		t.Fatalf("resumo: %v", d)
	}
	page := e.do(http.MethodGet, "/v1/message/list?campaign_id="+id, e.testKey, nil, nil)
	if items, _ := page.data()["items"].([]any); len(items) != 2 {
		t.Fatalf("lista por campanha: %v", page.body)
	}
}

func TestCampaignRespectsWindowAndValidation(t *testing.T) {
	e := newEnv(t, options{})
	now := time.Now().In(time.FixedZone("BRT", -3*3600))
	start := now.Add(2 * time.Hour).Format("15:04")
	end := now.Add(3 * time.Hour).Format("15:04")
	body := map[string]any{
		"name": "Fora do horário", "from": "BINITECH", "text": "oi", "scheduled_at": time.Now().Add(-time.Minute).Format(time.RFC3339),
		"window_start": start, "window_end": end, "time_zone": "America/Sao_Paulo",
		"recipients": []map[string]any{{"to": "+5511987654321"}},
	}
	created := e.do(http.MethodPost, "/v1/campaign/create", e.testKey, body, nil)
	if created.status != http.StatusCreated {
		t.Fatalf("criar: %d %v", created.status, created.body)
	}
	id := created.data()["campaign_id"].(string)
	if _, err := e.campaignRunner().Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := e.scalar(`SELECT status FROM campaigns WHERE id = $1`, id); got != "scheduled" {
		t.Fatalf("fora da janela não pode começar: %s", got)
	}

	for name, bad := range map[string]map[string]any{
		"janela pela metade": {"name": "x", "from": "B", "text": "t", "scheduled_at": time.Now().Format(time.RFC3339), "window_start": "08:00", "recipients": []map[string]any{{"to": "+5511987654321"}}},
		"longe demais":       {"name": "x", "from": "B", "text": "t", "scheduled_at": time.Now().AddDate(0, 2, 0).Format(time.RFC3339), "recipients": []map[string]any{{"to": "+5511987654321"}}},
		"telefone inválido":  {"name": "x", "from": "B", "text": "t", "scheduled_at": time.Now().Format(time.RFC3339), "recipients": []map[string]any{{"to": "abc"}}},
	} {
		if res := e.do(http.MethodPost, "/v1/campaign/create", e.testKey, bad, nil); res.status != http.StatusBadRequest {
			t.Errorf("%s: %d %v", name, res.status, res.body)
		}
	}
	if res := e.do(http.MethodGet, "/v1/campaign/cmp_naoexiste/summary", e.testKey, nil, nil); res.status != http.StatusNotFound {
		t.Fatalf("campanha inexistente: %d", res.status)
	}
}

func TestAlphanameList(t *testing.T) {
	e := newEnv(t, options{})
	res := e.do(http.MethodGet, "/v1/alphaname/list", e.liveKey, nil, nil)
	items, _ := res.body["data"].([]any)
	if res.status != http.StatusOK || len(items) != 1 {
		t.Fatalf("remetentes: %d %v", res.status, res.body)
	}
	a := items[0].(map[string]any)
	if a["sender"] != "BINITECH" || a["status"] != "approved" || a["kind"] != "shortcode" || a["countries"].([]any)[0] != "BR" {
		t.Fatalf("remetente: %v", a)
	}
}
