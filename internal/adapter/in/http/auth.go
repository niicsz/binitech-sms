package http

import (
	"context"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/niicsz/binitech-sms/internal/application/apperr"
	"github.com/niicsz/binitech-sms/internal/application/port/in"
)

type principalKey struct{}

func principalFrom(ctx context.Context) (in.Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(in.Principal)
	return p, ok
}

func authenticate(log *slog.Logger, authn in.Authenticator, limiter in.RateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw := r.Header.Get("X-Api-Key")
			if raw == "" {
				if scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " "); ok && strings.EqualFold(scheme, "Bearer") {
					raw = strings.TrimSpace(token)
				}
			}
			if r.URL.Query().Has("api_key") || r.URL.Query().Has("apikey") {
				writeError(log, w, r, apperr.New(apperr.Unauthorized, "Envie a chave no header X-Api-Key ou Authorization: Bearer, nunca na URL."))
				return
			}
			if raw == "" {
				writeError(log, w, r, apperr.New(apperr.Unauthorized, "Chave de API ausente, inválida ou revogada."))
				return
			}
			principal, err := authn.Authenticate(r.Context(), raw)
			if err != nil {
				writeError(log, w, r, err)
				return
			}
			if limiter != nil {
				limit, err := limiter.Allow(r.Context(), principal)
				if limit.Limit > 0 {
					w.Header().Set("RateLimit-Limit", strconv.Itoa(limit.Limit))
					w.Header().Set("RateLimit-Remaining", strconv.Itoa(limit.Remaining))
					w.Header().Set("RateLimit-Reset", strconv.Itoa(int(math.Ceil(limit.ResetAfter.Seconds()))))
				}
				if err != nil {
					writeError(log, w, r, err)
					return
				}
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, principal)))
		})
	}
}
