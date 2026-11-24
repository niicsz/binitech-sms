package http

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"github.com/niicsz/binitech-sms/internal/adapter/in/http/api"
	"github.com/niicsz/binitech-sms/internal/application/apperr"
	"github.com/niicsz/binitech-sms/internal/application/port/in"
)

func (h *handlers) SendOtp(ctx context.Context, req api.SendOtpRequestObject) (api.SendOtpResponseObject, error) {
	principal, ok := principalFrom(ctx)
	if !ok {
		return nil, errNoPrincipal
	}
	cmd := in.OTPSendCommand{Principal: principal}
	if req.Params.IdempotencyKey != nil {
		cmd.IdempotencyKey = *req.Params.IdempotencyKey
	}
	if b := req.Body; b != nil {
		cmd.To, cmd.From, cmd.Template = b.To, b.From, deref(b.Template)
		if b.Length != nil {
			cmd.Length = *b.Length
		}
		if b.TtlSeconds != nil {
			cmd.TTLSeconds = *b.TtlSeconds
		}
		if b.MaxAttempts != nil {
			cmd.MaxAttempts = *b.MaxAttempts
		}
	}
	sent, err := h.otp.Send(ctx, cmd)
	if err != nil {
		return nil, err
	}
	data := api.OtpSent{OtpId: sent.OTPID, MessageId: sent.MessageID, ExpiresAt: sent.ExpiresAt}
	if sent.TestCode != "" {
		data.TestCode = &sent.TestCode
	}
	return api.SendOtp202JSONResponse{Data: data}, nil
}

func (h *handlers) VerifyOtp(ctx context.Context, req api.VerifyOtpRequestObject) (api.VerifyOtpResponseObject, error) {
	principal, ok := principalFrom(ctx)
	if !ok {
		return nil, errNoPrincipal
	}
	if req.Body == nil {
		return nil, apperr.Validation([]apperr.FieldError{{Field: "otp_id", Message: "Informe otp_id e code."}})
	}
	result, err := h.otp.Verify(ctx, principal, req.Body.OtpId, req.Body.Code)
	if err != nil {
		return nil, err
	}
	return api.VerifyOtp200JSONResponse{Data: api.OtpVerification{
		Status: api.OtpVerificationStatus(result.Status), AttemptsLeft: result.AttemptsLeft,
	}}, nil
}

func (h *handlers) ShortenLink(ctx context.Context, req api.ShortenLinkRequestObject) (api.ShortenLinkResponseObject, error) {
	principal, ok := principalFrom(ctx)
	if !ok {
		return nil, errNoPrincipal
	}
	if req.Body == nil {
		return nil, apperr.Validation([]apperr.FieldError{{Field: "url", Message: "Informe a URL."}})
	}
	link, err := h.links.Shorten(ctx, principal, req.Body.Url, deref(req.Body.CampaignId), deref(req.Body.MessageId))
	if err != nil {
		return nil, err
	}
	return api.ShortenLink201JSONResponse{Data: api.ShortLink{LinkId: link.ID, Code: link.Code, ShortUrl: link.ShortURL, Url: link.URL}}, nil
}

func NewRedirector(log *slog.Logger, resolver in.LinkResolver) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		code := strings.Trim(r.URL.Path, "/")
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if code == "healthz" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if code == "" || len(code) > 16 || strings.ContainsAny(code, "/.") {
			http.NotFound(w, r)
			return
		}
		ip := r.RemoteAddr
		if i := strings.LastIndex(ip, ":"); i > 0 {
			ip = ip[:i]
		}
		target, err := resolver.Resolve(r.Context(), in.Click{Code: code, IP: ip, UserAgent: r.UserAgent()})
		if apperr.KindOf(err) == apperr.NotFound {
			http.NotFound(w, r)
			return
		}
		if err != nil {
			log.Error("redirector: erro", "error", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Referrer-Policy", "no-referrer")
		http.Redirect(w, r, target, http.StatusFound)
	})
}
