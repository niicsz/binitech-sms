package otp

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/niicsz/binitech-sms/internal/application/apperr"
	"github.com/niicsz/binitech-sms/internal/application/port/in"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/domain/apikey"
	"github.com/niicsz/binitech-sms/internal/domain/phone"
)

const (
	DefaultTemplate = "Seu código de verificação é {{code}}. Não compartilhe."
	placeholder     = "{{code}}"
	perHour         = 5
	resendGap       = 30 * time.Second

	StatusVerified = "verified"
	StatusInvalid  = "invalid"
	StatusExpired  = "expired"
	StatusBlocked  = "blocked"
	StatusUsed     = "used"
)

type Service struct {
	tx     out.TxManager
	store  out.OTPStore
	sender in.MessageSender
	ids    out.IDGenerator
	clock  out.Clock
	random io.Reader
	key    []byte
}

var _ in.OTPService = (*Service)(nil)

func New(tx out.TxManager, store out.OTPStore, sender in.MessageSender, ids out.IDGenerator, clock out.Clock, random io.Reader, key []byte) *Service {
	return &Service{tx: tx, store: store, sender: sender, ids: ids, clock: clock, random: random, key: key}
}

func (s *Service) Send(ctx context.Context, cmd in.OTPSendCommand) (in.OTPSent, error) {
	length, ttl, attempts := defaults(cmd.Length, 6), time.Duration(defaults(cmd.TTLSeconds, 600))*time.Second, defaults(cmd.MaxAttempts, 5)
	template := cmd.Template
	if template == "" {
		template = DefaultTemplate
	}
	var fields []apperr.FieldError
	to, err := phone.Parse(cmd.To)
	if err != nil {
		fields = append(fields, apperr.FieldError{Field: "to", Message: "Use o formato E.164."})
	}
	if length < 4 || length > 8 {
		fields = append(fields, apperr.FieldError{Field: "length", Message: "Use de 4 a 8 dígitos."})
	}
	if ttl < time.Minute || ttl > 30*time.Minute {
		fields = append(fields, apperr.FieldError{Field: "ttl_seconds", Message: "Use de 60 a 1800 segundos."})
	}
	if attempts < 1 || attempts > 10 {
		fields = append(fields, apperr.FieldError{Field: "max_attempts", Message: "Use de 1 a 10 tentativas."})
	}
	if strings.Count(template, placeholder) != 1 {
		fields = append(fields, apperr.FieldError{Field: "template", Message: "O texto precisa ter {{code}} uma vez."})
	}
	if len(fields) > 0 {
		return in.OTPSent{}, apperr.Validation(fields)
	}
	if cmd.IdempotencyKey != "" {
		existing, err := s.store.ByIdempotency(ctx, cmd.Principal.AccountID, cmd.IdempotencyKey)
		if err == nil {
			return in.OTPSent{OTPID: existing.ID, MessageID: existing.MessageID, ExpiresAt: existing.ExpiresAt}, nil
		}
		if !errors.Is(err, out.ErrNotFound) {
			return in.OTPSent{}, err
		}
	}
	now := s.clock.Now()
	count, last, err := s.store.RecentFor(ctx, cmd.Principal.AccountID, to.String(), now.Add(-time.Hour))
	if err != nil {
		return in.OTPSent{}, err
	}
	if count >= perHour {
		e := apperr.New(apperr.RateLimited, "Limite de 5 códigos por hora para este destino.")
		e.RetryAfter = time.Hour
		return in.OTPSent{}, e
	}
	if wait := last.Add(resendGap).Sub(now); !last.IsZero() && wait > 0 {
		e := apperr.New(apperr.RateLimited, "Aguarde 30 segundos para pedir outro código.")
		e.RetryAfter = wait
		return in.OTPSent{}, e
	}
	code, err := s.code(length)
	if err != nil {
		return in.OTPSent{}, err
	}
	challenge := out.OTPChallenge{
		ID: "otp_" + strings.ToLower(strings.TrimPrefix(s.ids.MessageID(), "msg_"))[:16], AccountID: cmd.Principal.AccountID,
		To: to.String(), Status: "pending", MaxAttempts: attempts, ExpiresAt: now.Add(ttl), CreatedAt: now, IdemKey: cmd.IdempotencyKey,
	}
	challenge.CodeHMAC = s.mac(challenge.ID, code)
	if err := s.store.Create(ctx, challenge); err != nil {
		return in.OTPSent{}, err
	}
	sent, err := s.sender.Send(ctx, in.SendCommand{
		Principal: cmd.Principal, To: to.String(), From: cmd.From, Text: strings.Replace(template, placeholder, code, 1),
		ValidityMinutes: int(ttl.Minutes()), WebhookRef: challenge.ID,
	})
	if err != nil {
		return in.OTPSent{}, err
	}
	if err := s.store.AttachMessage(ctx, challenge.ID, sent.MessageID); err != nil {
		return in.OTPSent{}, err
	}
	result := in.OTPSent{OTPID: challenge.ID, MessageID: sent.MessageID, ExpiresAt: challenge.ExpiresAt}
	if cmd.Principal.Env == apikey.Test {
		result.TestCode = code
	}
	return result, nil
}

func (s *Service) Verify(ctx context.Context, p in.Principal, otpID, code string) (in.OTPVerification, error) {
	var result in.OTPVerification
	err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
		c, err := s.store.ForUpdate(ctx, p.AccountID, otpID)
		if errors.Is(err, out.ErrNotFound) {
			return apperr.New(apperr.NotFound, "Código não encontrado.")
		}
		if err != nil {
			return err
		}
		now := s.clock.Now()
		left := max(c.MaxAttempts-c.Attempts, 0)
		switch {
		case c.Status == StatusVerified:
			result = in.OTPVerification{Status: StatusUsed}
			return nil
		case c.Status == StatusBlocked:
			result = in.OTPVerification{Status: StatusBlocked}
			return nil
		case now.After(c.ExpiresAt):
			result = in.OTPVerification{Status: StatusExpired, AttemptsLeft: left}
			return nil
		}
		attempts := c.Attempts + 1
		if hmac.Equal(c.CodeHMAC, s.mac(c.ID, strings.TrimSpace(code))) {
			result = in.OTPVerification{Status: StatusVerified, AttemptsLeft: c.MaxAttempts - attempts}
			return s.store.SaveAttempt(ctx, c.ID, attempts, StatusVerified, now)
		}
		status := "pending"
		result = in.OTPVerification{Status: StatusInvalid, AttemptsLeft: c.MaxAttempts - attempts}
		if attempts >= c.MaxAttempts {
			status = StatusBlocked
			result = in.OTPVerification{Status: StatusBlocked}
		}
		return s.store.SaveAttempt(ctx, c.ID, attempts, status, now)
	})
	return result, err
}

func (s *Service) mac(id, code string) []byte {
	m := hmac.New(sha256.New, s.key)
	m.Write([]byte(id + ":" + code))
	return m.Sum(nil)
}

func (s *Service) code(length int) (string, error) {
	digits := make([]byte, 0, length)
	buf := make([]byte, 1)
	for len(digits) < length {
		if _, err := io.ReadFull(s.random, buf); err != nil {
			return "", fmt.Errorf("otp: aleatoriedade: %w", err)
		}
		if buf[0] >= 250 {
			continue
		}
		digits = append(digits, '0'+buf[0]%10)
	}
	return string(digits), nil
}

func defaults(v, d int) int {
	if v == 0 {
		return d
	}
	return v
}
