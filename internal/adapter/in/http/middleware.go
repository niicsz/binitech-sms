package http

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
}

func newMetrics(registry *prometheus.Registry) *metrics {
	m := &metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Requisições HTTP por rota, método e status.",
		}, []string{"route", "method", "status"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Latência das requisições HTTP por rota e método.",
			Buckets: []float64{.005, .01, .025, .05, .1, .15, .25, .5, 1, 2.5},
		}, []string{"route", "method"}),
	}
	registry.MustRegister(m.requests, m.latency)
	return m
}

func (m *metrics) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = "desconhecida"
		}
		m.requests.WithLabelValues(route, r.Method, strconv.Itoa(ww.Status())).Inc()
		m.latency.WithLabelValues(route, r.Method).Observe(time.Since(start).Seconds())
	})
}

func accessLog(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			log.LogAttrs(r.Context(), slog.LevelInfo, "http",
				slog.String("request_id", middleware.GetReqID(r.Context())),
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", ww.Status()),
				slog.Duration("duration", time.Since(start)),
			)
		})
	}
}

func recoverer(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					if err, ok := rec.(error); ok && errors.Is(err, http.ErrAbortHandler) {
						panic(rec)
					}
					log.Error("http: panic", "request_id", middleware.GetReqID(r.Context()), "panic", rec)
					writeJSON(w, http.StatusInternalServerError, Envelope{Code: 5000, Message: "Erro interno."})
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
