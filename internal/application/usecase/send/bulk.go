package send

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/niicsz/binitech-sms/internal/application/apperr"
	"github.com/niicsz/binitech-sms/internal/application/port/in"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/application/usecase/eventing"
	"github.com/niicsz/binitech-sms/internal/contract/events"
	"github.com/niicsz/binitech-sms/internal/domain/apikey"
)

const MaxBulk = 10000

var _ in.BulkSender = (*Service)(nil)

func (s *Service) SendBulk(ctx context.Context, cmd in.BulkCommand) (in.BulkResult, error) {
	if len(cmd.Items) == 0 || len(cmd.Items) > MaxBulk {
		return in.BulkResult{}, apperr.Validation([]apperr.FieldError{{Field: "messages", Message: fmt.Sprintf("Envie de 1 a %d mensagens por lote.", MaxBulk)}})
	}
	if len(cmd.IdempotencyKey) > maxIdemKey {
		return in.BulkResult{}, apperr.Validation([]apperr.FieldError{{Field: "Idempotency-Key", Message: "Use até 64 caracteres."}})
	}
	account, err := s.d.Accounts.Get(ctx, cmd.Principal.AccountID)
	if err != nil {
		return in.BulkResult{}, fmt.Errorf("conta: %w", err)
	}
	now := s.d.Clock.Now()
	test := cmd.Principal.Env == apikey.Test
	cache := newLookups()
	result := in.BulkResult{Currency: account.Currency, Test: test, Accepted: []in.BulkAccepted{}, Rejected: []in.BulkRejected{}}
	var (
		msgs      []out.StoredMessage
		envelopes []events.Envelope
		ledger    []out.LedgerEntry
	)
	for i, item := range cmd.Items {
		text := item.Text
		if strings.TrimSpace(text) == "" {
			text = cmd.Text
		}
		p, err := validate(in.SendCommand{To: item.To, From: cmd.From, Text: text, Normalize: cmd.Normalize, ValidityMinutes: cmd.ValidityMinutes, WebhookRef: item.WebhookRef})
		if err == nil {
			var msg out.StoredMessage
			msg, err = s.build(ctx, cache, account, p, item.WebhookRef, test, now)
			if err == nil {
				msg.CampaignID = cmd.CampaignID
				envelope, eerr := s.submittedEvent(msg, now)
				if eerr != nil {
					return in.BulkResult{}, eerr
				}
				msgs = append(msgs, msg)
				envelopes = append(envelopes, envelope)
				result.TotalCost += msg.Cost
				result.Accepted = append(result.Accepted, in.BulkAccepted{Index: i, MessageID: msg.ID, Segments: msg.Segments, Cost: msg.Cost})
				continue
			}
		}
		var appErr *apperr.Error
		if !errors.As(err, &appErr) {
			return in.BulkResult{}, err
		}
		field, message := "", appErr.Message
		if len(appErr.Fields) > 0 {
			field, message = appErr.Fields[0].Field, appErr.Fields[0].Message
		}
		result.Rejected = append(result.Rejected, in.BulkRejected{Index: i, Field: field, Message: message})
	}

	var (
		replay *in.BulkResult
		staged eventing.Staged
	)
	err = s.d.Tx.WithinTx(ctx, func(ctx context.Context) error {
		if cmd.IdempotencyKey != "" {
			previous, err := s.claimBulkIdempotency(ctx, cmd, account.ID)
			if err != nil || previous != nil {
				replay = previous
				return err
			}
		}
		if len(msgs) > 0 {
			if !test && result.TotalCost > 0 {
				balance, err := s.d.Balances.Reserve(ctx, account.ID, result.TotalCost)
				if errors.Is(err, out.ErrInsufficientBalance) {
					return apperr.New(apperr.InsufficientBalance, fmt.Sprintf("Saldo insuficiente para o lote de %s.", result.TotalCost))
				}
				if err != nil {
					return fmt.Errorf("reserva: %w", err)
				}
				for _, m := range msgs {
					if m.Cost > 0 {
						ledger = append(ledger, out.LedgerEntry{AccountID: account.ID, Kind: "reserve", Amount: m.Cost, MessageID: m.ID, OpID: s.d.IDs.OpID(), BalanceAfter: balance.Available})
					}
				}
				if err := s.d.Ledger.AppendMany(ctx, ledger); err != nil {
					return err
				}
			}
			if err := s.d.Messages.InsertMany(ctx, msgs); err != nil {
				return err
			}
			if err := s.d.Events.StageMany(ctx, &staged, envelopes); err != nil {
				return err
			}
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
		return in.BulkResult{}, err
	}
	if replay != nil {
		return *replay, nil
	}
	s.d.Events.Flush(ctx, &staged)
	return result, nil
}

func (s *Service) claimBulkIdempotency(ctx context.Context, cmd in.BulkCommand, accountID string) (*in.BulkResult, error) {
	canonical, _ := json.Marshal(struct {
		From, Text      string
		Normalize       bool
		ValidityMinutes int
		Items           []in.BulkItem
		CampaignID      string
	}{cmd.From, cmd.Text, cmd.Normalize, cmd.ValidityMinutes, cmd.Items, cmd.CampaignID})
	sum := sha256.Sum256(canonical)
	existing, err := s.d.Idempotency.Begin(ctx, accountID, cmd.IdempotencyKey, sum[:])
	if err != nil || existing == nil {
		return nil, err
	}
	if !bytes.Equal(existing.RequestHash, sum[:]) {
		return nil, apperr.New(apperr.Conflict, "Esta Idempotency-Key já foi usada com outro conteúdo.")
	}
	var previous in.BulkResult
	if err := json.Unmarshal(existing.Response, &previous); err != nil {
		return nil, fmt.Errorf("idempotência: resposta guardada: %w", err)
	}
	return &previous, nil
}
