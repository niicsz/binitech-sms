package http

import (
	"errors"
	"log/slog"
	"math"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5/middleware"

	"github.com/niicsz/binitech-sms/internal/application/apperr"
)

type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

type errorData struct {
	Errors []FieldError `json:"errors,omitempty"`
}

var kinds = map[apperr.Kind]struct {
	status int
	code   int
}{
	apperr.Invalid:             {http.StatusBadRequest, 4000},
	apperr.Unauthorized:        {http.StatusUnauthorized, 4010},
	apperr.InsufficientBalance: {http.StatusPaymentRequired, 4020},
	apperr.Forbidden:           {http.StatusForbidden, 4030},
	apperr.NotFound:            {http.StatusNotFound, 4040},
	apperr.Conflict:            {http.StatusConflict, 4090},
	apperr.RateLimited:         {http.StatusTooManyRequests, 4290},
}

func writeError(log *slog.Logger, w http.ResponseWriter, r *http.Request, err error) {
	var appErr *apperr.Error
	if !errors.As(err, &appErr) {
		log.Error("http: erro interno", "request_id", middleware.GetReqID(r.Context()), "path", r.URL.Path, "error", err)
		writeJSON(w, http.StatusInternalServerError, Envelope{Code: 5000, Message: "Erro interno. Tente de novo em instantes."})
		return
	}
	mapped, ok := kinds[appErr.Kind]
	if !ok {
		log.Error("http: erro interno", "request_id", middleware.GetReqID(r.Context()), "path", r.URL.Path, "error", err)
		writeJSON(w, http.StatusInternalServerError, Envelope{Code: 5000, Message: "Erro interno. Tente de novo em instantes."})
		return
	}
	if appErr.RetryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(appErr.RetryAfter.Seconds()))))
	}
	body := Envelope{Code: mapped.code, Message: appErr.Message}
	if len(appErr.Fields) > 0 {
		data := errorData{}
		for _, f := range appErr.Fields {
			data.Errors = append(data.Errors, FieldError(f))
		}
		body.Data = data
	}
	writeJSON(w, mapped.status, body)
}

func badRequest(w http.ResponseWriter, message string) {
	writeJSON(w, http.StatusBadRequest, Envelope{Code: 4000, Message: message})
}
