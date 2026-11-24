package send

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/niicsz/binitech-sms/internal/application/apperr"
	"github.com/niicsz/binitech-sms/internal/application/port/in"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/application/usecase/eventing"
	"github.com/niicsz/binitech-sms/internal/contract/events"
	"github.com/niicsz/binitech-sms/internal/domain/apikey"
	"github.com/niicsz/binitech-sms/internal/domain/message"
	"github.com/niicsz/binitech-sms/internal/domain/money"
	"github.com/niicsz/binitech-sms/internal/domain/phone"
	"github.com/niicsz/binitech-sms/internal/domain/segment"
)

const (
	defaultValidity = 24 * time.Hour
	maxValidity     = 72 * time.Hour
	maxSender       = 11
	maxWebhookRef   = 64
	maxIdemKey      = 64
)

type Deps struct {
	Tx           out.TxManager
	Accounts     out.AccountRepository
	Balances     out.BalanceRepository
	Ledger       out.LedgerRepository
	Destinations out.DestinationResolver
	Tariffs      out.TariffRepository
	Senders      out.SenderRepository
	Messages     out.MessageRepository
	Idempotency  out.IdempotencyStore
	Events       *eventing.Emitter
	Clock        out.Clock
	IDs          out.IDGenerator
	Env          string
}

type Service struct {
	d Deps
}

var _ in.MessageSender = (*Service)(nil)

func New(d Deps) *Service {
	return &Service{d: d}
}

type prepared struct {
	to       phone.Number
	text     string
	sender   string
	analysis segment.Result
	validity time.Duration
}

func (s *Service) Send(ctx context.Context, cmd in.SendCommand) (in.SendResult, error) {
	p, err := validate(cmd)
	if err != nil {
		return in.SendResult{}, err
	}
	account, err := s.d.Accounts.Get(ctx, cmd.Principal.AccountID)
	if err != nil {
		return in.SendResult{}, fmt.Errorf("conta: %w", err)
	}
	now := s.d.Clock.Now()
	test := cmd.Principal.Env == apikey.Test
	msg, err := s.build(ctx, newLookups(), account, p, cmd.WebhookRef, test, now)
	if err != nil {
		return in.SendResult{}, err
	}
	msg.IdemKey = cmd.IdempotencyKey
	result := in.SendResult{
		MessageID: msg.ID,
		Status:    string(message.Accepted),
		Segments:  msg.Segments,
		Encoding:  string(msg.Encoding),
		Cost:      msg.Cost,
		Currency:  account.Currency,
		Test:      test,
	}
	envelope, err := s.submittedEvent(msg, now)
	if err != nil {
		return in.SendResult{}, err
	}

	var (
		replay *in.SendResult
		staged eventing.Staged
	)
	err = s.d.Tx.WithinTx(ctx, func(ctx context.Context) error {
		if cmd.IdempotencyKey != "" {
			previous, err := s.claimIdempotency(ctx, cmd, account.ID)
			if err != nil || previous != nil {
				replay = previous
				return err
			}
		}
		if !test && msg.Cost > 0 {
			balance, err := s.d.Balances.Reserve(ctx, account.ID, msg.Cost)
			if errors.Is(err, out.ErrInsufficientBalance) {
				return apperr.New(apperr.InsufficientBalance, "Saldo insuficiente para este envio.")
			}
			if err != nil {
				return fmt.Errorf("reserva: %w", err)
			}
			if err := s.d.Ledger.Append(ctx, out.LedgerEntry{
				AccountID: account.ID, Kind: "reserve", Amount: msg.Cost, MessageID: msg.ID,
				OpID: s.d.IDs.OpID(), BalanceAfter: balance.Available,
			}); err != nil {
				return fmt.Errorf("ledger: %w", err)
			}
		}
		if err := s.d.Messages.Insert(ctx, msg); err != nil {
			return fmt.Errorf("mensagem: %w", err)
		}
		if err := s.d.Events.Stage(ctx, &staged, envelope); err != nil {
			return err
		}
		if cmd.IdempotencyKey != "" {
			stored, err := json.Marshal(result)
			if err != nil {
				return err
			}
			return s.d.Idempotency.Complete(ctx, account.ID, cmd.IdempotencyKey, stored)
		}
		return nil
	})
	if err != nil {
		return in.SendResult{}, err
	}
	if replay != nil {
		return *replay, nil
	}
	s.d.Events.Flush(ctx, &staged)
	return result, nil
}

type lookups struct {
	senders map[string]bool
	tariffs map[string]money.Amount
}

func newLookups() *lookups {
	return &lookups{senders: map[string]bool{}, tariffs: map[string]money.Amount{}}
}

func (s *Service) build(ctx context.Context, cache *lookups, account out.Account, p prepared, webhookRef string, test bool, now time.Time) (out.StoredMessage, error) {
	dest, err := s.d.Destinations.Resolve(ctx, p.to)
	if errors.Is(err, out.ErrNotFound) {
		return out.StoredMessage{}, apperr.New(apperr.Forbidden, "Destino não atendido pela plataforma.")
	}
	if err != nil {
		return out.StoredMessage{}, fmt.Errorf("destino: %w", err)
	}
	senderKey := p.sender + "|" + dest.Country
	approved, ok := cache.senders[senderKey]
	if !ok {
		if approved, err = s.d.Senders.IsApproved(ctx, account.ID, p.sender, dest.Country); err != nil {
			return out.StoredMessage{}, fmt.Errorf("remetente: %w", err)
		}
		cache.senders[senderKey] = approved
	}
	if !approved {
		return out.StoredMessage{}, apperr.New(apperr.Forbidden, "Remetente "+p.sender+" não está aprovado para este destino.")
	}
	var unit money.Amount
	if !test {
		tariffKey := dest.MCC + dest.MNC
		cached, ok := cache.tariffs[tariffKey]
		if !ok {
			cached, err = s.d.Tariffs.UnitPrice(ctx, account.Plan, dest.MCC, dest.MNC, now)
			if errors.Is(err, out.ErrNotFound) {
				return out.StoredMessage{}, apperr.New(apperr.Forbidden, "Não há tarifa para este destino no seu plano.")
			}
			if err != nil {
				return out.StoredMessage{}, fmt.Errorf("tarifa: %w", err)
			}
			cache.tariffs[tariffKey] = cached
		}
		unit = cached
	}
	msg := out.StoredMessage{
		Message: message.Message{
			ID:         s.d.IDs.MessageID(),
			AccountID:  account.ID,
			To:         p.to,
			Sender:     p.sender,
			Body:       p.text,
			Encoding:   p.analysis.Encoding,
			Segments:   p.analysis.Segments,
			Status:     message.Accepted,
			WebhookRef: webhookRef,
			ExpiresAt:  now.Add(p.validity),
			CreatedAt:  now,
		},
		MCC:  dest.MCC,
		MNC:  dest.MNC,
		Test: test,
	}
	msg.Price(unit)
	return msg, nil
}

func (s *Service) claimIdempotency(ctx context.Context, cmd in.SendCommand, accountID string) (*in.SendResult, error) {
	hash := requestHash(cmd)
	existing, err := s.d.Idempotency.Begin(ctx, accountID, cmd.IdempotencyKey, hash)
	if err != nil {
		return nil, fmt.Errorf("idempotência: %w", err)
	}
	if existing == nil {
		return nil, nil
	}
	if !bytes.Equal(existing.RequestHash, hash) {
		return nil, apperr.New(apperr.Conflict, "Esta Idempotency-Key já foi usada com outro conteúdo.")
	}
	if !existing.Completed {
		return nil, apperr.New(apperr.Conflict, "Uma requisição com esta Idempotency-Key ainda está em andamento.")
	}
	var previous in.SendResult
	if err := json.Unmarshal(existing.Response, &previous); err != nil {
		return nil, fmt.Errorf("idempotência: resposta guardada: %w", err)
	}
	return &previous, nil
}

func (s *Service) submittedEvent(msg out.StoredMessage, now time.Time) (events.Envelope, error) {
	return s.d.Events.Envelope(events.TopicSubmit(s.d.Env), msg.AccountID, events.OpMessageSubmit, msg.AccountID, events.MessageSubmitted{
		MessageID:   msg.ID,
		AccountID:   msg.AccountID,
		To:          msg.To.String(),
		From:        msg.Sender,
		Text:        msg.Body,
		Encoding:    string(msg.Encoding),
		Segments:    msg.Segments,
		MCC:         msg.MCC,
		MNC:         msg.MNC,
		UnitPrice:   msg.UnitPrice,
		ExpiresAt:   msg.ExpiresAt,
		Test:        msg.Test,
		CampaignID:  campaign(msg.CampaignID),
		SubmittedAt: now,
	})
}

func validate(cmd in.SendCommand) (prepared, error) {
	var fields []apperr.FieldError
	add := func(field, msg string) { fields = append(fields, apperr.FieldError{Field: field, Message: msg}) }

	to, err := phone.Parse(cmd.To)
	if err != nil {
		add("to", "Use o formato E.164, como +5511987654321.")
	}
	sender := strings.TrimSpace(cmd.From)
	if sender == "" || len([]rune(sender)) > maxSender {
		add("from", "Informe um remetente aprovado de até 11 caracteres.")
	}
	text := cmd.Text
	if cmd.Normalize {
		text = segment.Normalize(text)
	}
	analysis, err := segment.Analyze(text)
	switch {
	case errors.Is(err, segment.ErrEmpty):
		add("text", "Informe o texto da mensagem.")
	case errors.Is(err, segment.ErrTooLong):
		add("text", fmt.Sprintf("O texto passa de %d segmentos.", segment.MaxSegments))
	case err != nil:
		add("text", "O texto tem caracteres inválidos.")
	}
	validity := defaultValidity
	if cmd.ValidityMinutes != 0 {
		validity = time.Duration(cmd.ValidityMinutes) * time.Minute
		if validity <= 0 || validity > maxValidity {
			add("validity_minutes", "Use de 1 a 4320 minutos.")
		}
	}
	if len(cmd.WebhookRef) > maxWebhookRef {
		add("webhook_ref", "Use até 64 caracteres.")
	}
	if len(cmd.IdempotencyKey) > maxIdemKey {
		add("Idempotency-Key", "Use até 64 caracteres.")
	}
	if len(fields) > 0 {
		return prepared{}, apperr.Validation(fields)
	}
	return prepared{to: to, text: text, sender: sender, analysis: analysis, validity: validity}, nil
}

func requestHash(cmd in.SendCommand) []byte {
	canonical, _ := json.Marshal(struct {
		To, From, Text, WebhookRef string
		Normalize                  bool
		ValidityMinutes            int
	}{cmd.To, cmd.From, cmd.Text, cmd.WebhookRef, cmd.Normalize, cmd.ValidityMinutes})
	sum := sha256.Sum256(canonical)
	return sum[:]
}

func campaign(id string) *string {
	if id == "" {
		return nil
	}
	return &id
}
