package http_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	httpadapter "github.com/niicsz/binitech-sms/internal/adapter/in/http"
	"github.com/niicsz/binitech-sms/internal/application/port/in"
)

type stubReadiness []in.CheckResult

func (s stubReadiness) Check(context.Context) []in.CheckResult { return s }

func router(readiness in.Readiness) http.Handler {
	return httpadapter.NewRouter(httpadapter.Deps{
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		Readiness: readiness,
		Registry:  prometheus.NewRegistry(),
	})
}

func get(t *testing.T, h http.Handler, path string) (*httptest.ResponseRecorder, httpadapter.Envelope) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
	var body httpadapter.Envelope
	if strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("corpo inválido: %v", err)
		}
	}
	return rec, body
}

func TestHealthz(t *testing.T) {
	rec, body := get(t, router(stubReadiness{}), "/healthz")
	if rec.Code != http.StatusOK || body.Code != 0 {
		t.Fatalf("status=%d body=%+v", rec.Code, body)
	}
}

func TestReadyzReportsDependencies(t *testing.T) {
	ok := stubReadiness{{Name: "postgres"}, {Name: "kafka"}}
	if rec, _ := get(t, router(ok), "/readyz"); rec.Code != http.StatusOK {
		t.Fatalf("todas ok deveria ser 200, veio %d", rec.Code)
	}
	down := stubReadiness{{Name: "postgres"}, {Name: "kafka", Error: errors.New("sem broker")}}
	rec, body := get(t, router(down), "/readyz")
	if rec.Code != http.StatusServiceUnavailable || body.Code != 5000 {
		t.Fatalf("status=%d body=%+v", rec.Code, body)
	}
	if strings.Contains(rec.Body.String(), "sem broker") {
		t.Fatal("o motivo interno não deve vazar na resposta")
	}
}

func TestUnknownV1RouteUsesEnvelope(t *testing.T) {
	rec, body := get(t, router(stubReadiness{}), "/v1/nao-existe")
	if rec.Code != http.StatusNotFound || body.Code != 4040 {
		t.Fatalf("status=%d body=%+v", rec.Code, body)
	}
}

func TestMetricsExposed(t *testing.T) {
	h := router(stubReadiness{})
	get(t, h, "/healthz")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	if !strings.Contains(rec.Body.String(), `http_requests_total{method="GET",route="/healthz",status="200"} 1`) {
		t.Fatalf("métrica ausente:\n%s", rec.Body.String())
	}
}
