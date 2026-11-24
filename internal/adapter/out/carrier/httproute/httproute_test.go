package httproute_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/niicsz/binitech-sms/internal/adapter/out/carrier/httproute"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
)

func carrier(t *testing.T, url string) *httproute.Carrier {
	t.Helper()
	cfg := `{"url":"` + url + `","body":{"n":"{{to}}"},"id_field":"result.id","callback_token_env":"CB","dlr":{"id_field":"id","status_field":"st"}}`
	c, err := httproute.New("r1", []byte(cfg), func(k string) string { return map[string]string{"CB": "tok"}[k] })
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func server(status int, body string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
}

func TestSendOutcomes(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		body      string
		accepted  bool
		retryable bool
		code      string
	}{
		{"aceito", 200, `{"result":{"id":987}}`, true, false, ""},
		{"recusado", 422, `{}`, false, false, "http_422"},
		{"limite", 429, `{}`, false, true, ""},
		{"fora do ar", 503, `{}`, false, true, ""},
		{"sem id", 200, `{"result":{}}`, false, true, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := server(c.status, c.body)
			defer srv.Close()
			res, err := carrier(t, srv.URL).Send(context.Background(), out.OutboundSMS{To: "+5511987654321"})
			if c.retryable {
				if !errors.Is(err, out.ErrCarrierUnavailable) {
					t.Fatalf("esperava erro repetível, veio %v", err)
				}
				return
			}
			if err != nil || res.Accepted != c.accepted || res.ErrorCode != c.code {
				t.Fatalf("got %+v %v", res, err)
			}
			if c.accepted && res.ProviderIDs[0] != "987" {
				t.Fatalf("id numérico deveria virar texto: %v", res.ProviderIDs)
			}
		})
	}
}

func TestRejectsPlainHTTPToRemoteHost(t *testing.T) {
	_, err := httproute.New("r1", []byte(`{"url":"http://fornecedor.example/send","id_field":"id"}`), func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), "https") {
		t.Fatalf("got %v", err)
	}
}

func TestCallbackAuthAndParsing(t *testing.T) {
	c := carrier(t, "https://example.invalid")
	h := c.CallbackHandler()
	post := func(token, body string) int {
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/callbacks/r1", strings.NewReader(body))
		req.Header.Set("X-Callback-Token", token)
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	if post("errado", `{"id":"a","st":"DELIVRD"}`) != http.StatusUnauthorized {
		t.Fatal("token errado")
	}
	if post("tok", `{"id":"a"}`) != http.StatusBadRequest {
		t.Fatal("sem status")
	}
	if post("tok", `{"id":"a","st":"DELIVRD"}`) != http.StatusNoContent {
		t.Fatal("callback válido")
	}
	report := <-c.Reports()
	if report.RouteID != "r1" || report.ProviderID != "a" || report.Status != "DELIVRD" {
		t.Fatalf("relatório: %+v", report)
	}
}
