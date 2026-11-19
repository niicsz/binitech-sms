package http

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/niicsz/binitech-sms/internal/application/port/in"
)

const maxBodyBytes = 1 << 20

type Envelope struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data"`
}

type Deps struct {
	Log       *slog.Logger
	Readiness in.Readiness
	Registry  *prometheus.Registry
}

func NewRouter(deps Deps) http.Handler {
	metrics := newMetrics(deps.Registry)
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(recoverer(deps.Log))
	r.Use(accessLog(deps.Log))
	r.Use(metrics.middleware)
	r.Use(middleware.RequestSize(maxBodyBytes))
	r.Use(middleware.Timeout(10 * time.Second))

	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, Envelope{Data: map[string]string{"status": "ok"}})
	})
	r.Get("/readyz", readyHandler(deps.Readiness))
	r.Handle("/metrics", promhttp.HandlerFor(deps.Registry, promhttp.HandlerOpts{Registry: deps.Registry}))

	r.Route("/v1", func(v1 chi.Router) {
		v1.NotFound(func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusNotFound, Envelope{Code: 4040, Message: "Rota não encontrada."})
		})
	})
	return r
}

func readyHandler(readiness in.Readiness) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		results := readiness.Check(r.Context())
		status := http.StatusOK
		checks := make(map[string]string, len(results))
		for _, result := range results {
			if result.Error != nil {
				status = http.StatusServiceUnavailable
				checks[result.Name] = "indisponível"
				continue
			}
			checks[result.Name] = "ok"
		}
		code := 0
		if status != http.StatusOK {
			code = 5000
		}
		writeJSON(w, status, Envelope{Code: code, Data: checks})
	}
}

func writeJSON(w http.ResponseWriter, status int, body Envelope) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
