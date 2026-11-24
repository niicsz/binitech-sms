package status

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/niicsz/binitech-sms/internal/application/port/in"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/application/usecase/eventing"
	"github.com/niicsz/binitech-sms/internal/contract/events"
	"github.com/niicsz/binitech-sms/internal/domain/message"
	"github.com/niicsz/binitech-sms/internal/domain/route"
)

const consumer = "status-engine"

var ErrUnknownPart = errors.New("status: DLR de parte ainda desconhecida")

type Engine struct {
	tx      out.TxManager
	ops     out.ProcessedOps
	store   out.StatusStore
	events  *eventing.Emitter
	optOuts out.OptOutStore
}

var _ in.EventHandler = (*Engine)(nil)

func New(tx out.TxManager, ops out.ProcessedOps, store out.StatusStore, emitter *eventing.Emitter, optOuts out.OptOutStore) *Engine {
	return &Engine{tx: tx, ops: ops, store: store, events: emitter, optOuts: optOuts}
}

func (s *Engine) Handle(ctx context.Context, e in.Event) error {
	opID := e.Headers[events.HeaderOpID]
	if opID == "" {
		return in.Permanent(errors.New("status: evento sem op_id"))
	}
	var apply func(ctx context.Context, staged *eventing.Staged) error
	switch e.Headers[events.HeaderOperation] {
	case events.OpMessageStatus:
		var m events.MessageStatus
		if err := json.Unmarshal(e.Payload, &m); err != nil {
			return in.Permanent(err)
		}
		apply = func(ctx context.Context, staged *eventing.Staged) error {
			return s.closeBeforeSubmit(ctx, staged, m.MessageID, message.Status(m.Status), m.ErrorCode)
		}
	case events.OpCarrierRejected:
		var m events.CarrierRejected
		if err := json.Unmarshal(e.Payload, &m); err != nil {
			return in.Permanent(err)
		}
		apply = func(ctx context.Context, staged *eventing.Staged) error {
			return s.closeBeforeSubmit(ctx, staged, m.MessageID, message.Rejected, m.ErrorCode)
		}
	case events.OpCarrierAccepted:
		var m events.CarrierAccepted
		if err := json.Unmarshal(e.Payload, &m); err != nil {
			return in.Permanent(err)
		}
		apply = func(ctx context.Context, staged *eventing.Staged) error { return s.submitted(ctx, staged, m) }
	case events.OpMessageDLR:
		var m events.DlrReceived
		if err := json.Unmarshal(e.Payload, &m); err != nil {
			return in.Permanent(err)
		}
		apply = func(ctx context.Context, staged *eventing.Staged) error { return s.dlr(ctx, staged, m, opID) }
	case events.OpMessageMO:
		var m events.MoReceived
		if err := json.Unmarshal(e.Payload, &m); err != nil {
			return in.Permanent(err)
		}
		keyword, ok := OptOutKeyword(m.Text)
		if !ok || s.optOuts == nil {
			return nil
		}
		apply = func(ctx context.Context, _ *eventing.Staged) error {
			_, err := s.optOuts.OptOut(ctx, m.To, normalizeMSISDN(m.From), keyword)
			return err
		}
	default:
		return nil
	}

	var staged eventing.Staged
	err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
		first, err := s.ops.MarkOnce(ctx, consumer, opID)
		if err != nil || !first {
			return err
		}
		return apply(ctx, &staged)
	})
	if err != nil {
		return err
	}
	s.events.Flush(ctx, &staged)
	return nil
}

func (s *Engine) closeBeforeSubmit(ctx context.Context, staged *eventing.Staged, id string, to message.Status, code string) error {
	m, changed, err := s.store.Transition(ctx, out.Transition{
		MessageID: id, From: []message.Status{message.Accepted, message.Routed}, To: to, ErrorCode: code, At: s.events.Now(),
	})
	if errors.Is(err, out.ErrNotFound) {
		return in.Permanent(err)
	}
	if err != nil || !changed {
		return err
	}
	if err := s.ledger(ctx, staged, m, "release", string(to)); err != nil {
		return err
	}
	return s.notify(ctx, staged, m)
}

func (s *Engine) submitted(ctx context.Context, staged *eventing.Staged, a events.CarrierAccepted) error {
	m, changed, err := s.store.Transition(ctx, out.Transition{
		MessageID: a.MessageID, From: []message.Status{message.Accepted, message.Routed}, To: message.Submitted, At: a.AcceptedAt,
	})
	if errors.Is(err, out.ErrNotFound) {
		return in.Permanent(err)
	}
	if err != nil || !changed {
		return err
	}
	if len(a.ProviderMsgIDs) > 0 {
		if err := s.store.SaveParts(ctx, a.MessageID, a.RouteID, a.ProviderMsgIDs); err != nil {
			return err
		}
	}
	if err := s.ledger(ctx, staged, m, "debit", "submitted"); err != nil {
		return err
	}
	return s.notify(ctx, staged, m)
}

func (s *Engine) dlr(ctx context.Context, staged *eventing.Staged, d events.DlrReceived, opID string) error {
	messageID, part, err := s.store.FindPart(ctx, d.RouteID, d.ProviderMsgID)
	if errors.Is(err, out.ErrNotFound) {
		return fmt.Errorf("%w: rota %s id %s", ErrUnknownPart, d.RouteID, d.ProviderMsgID)
	}
	if err != nil {
		return err
	}
	mapped := route.NormalizeDLRStatus(d.ProviderStatus)
	if err := s.store.RecordDLR(ctx, messageID, d.RouteID, d.ProviderStatus, mapped, d.ErrorCode, opID, nil); err != nil {
		return err
	}
	if mapped == "" {
		return nil
	}
	parts, err := s.store.UpdatePart(ctx, messageID, part, mapped, d.ErrorCode)
	if err != nil {
		return err
	}
	final, ok := finalStatus(parts)
	if !ok {
		return nil
	}
	m, changed, err := s.store.Transition(ctx, out.Transition{
		MessageID: messageID, From: []message.Status{message.Submitted}, To: final, ErrorCode: d.ErrorCode, At: d.DoneAt,
	})
	if err != nil || !changed {
		return err
	}
	return s.notify(ctx, staged, m)
}

func finalStatus(parts []out.PartStatus) (message.Status, bool) {
	expired := false
	for _, p := range parts {
		switch p.Status {
		case "submitted":
			return "", false
		case "undelivered":
			return message.Undelivered, true
		case "expired":
			expired = true
		}
	}
	if expired {
		return message.Expired, true
	}
	return message.Delivered, true
}

func (s *Engine) ledger(ctx context.Context, staged *eventing.Staged, m out.StoredMessage, kind, reason string) error {
	if m.Test || m.Cost <= 0 {
		return nil
	}
	envelope, err := s.events.Envelope(events.TopicLedgerEvents, m.AccountID, "ledger."+kind, m.AccountID, events.LedgerRequest{
		OpID: s.events.OpID(), AccountID: m.AccountID, MessageID: m.ID, Kind: kind, Amount: m.Cost, Reason: reason, At: s.events.Now(),
	})
	if err != nil {
		return err
	}
	return s.events.Stage(ctx, staged, envelope)
}

func (s *Engine) notify(ctx context.Context, staged *eventing.Staged, m out.StoredMessage) error {
	envelope, err := s.events.Envelope(events.TopicWebhookOutbound, m.AccountID, events.OpStatusChanged, m.AccountID, events.MessageStatusChanged{
		MessageID: m.ID, AccountID: m.AccountID, Status: string(m.Status), ErrorCode: m.ErrorCode, WebhookRef: m.WebhookRef,
		To: m.To.String(), Segments: m.Segments, Cost: m.Cost.String(), Test: m.Test, ChangedAt: s.events.Now(),
	})
	if err != nil {
		return err
	}
	return s.events.Stage(ctx, staged, envelope)
}

var optOutWords = map[string]bool{"SAIR": true, "PARAR": true, "STOP": true, "CANCELAR": true, "DESCADASTRAR": true, "REMOVER": true}

func OptOutKeyword(text string) (string, bool) {
	word := strings.ToUpper(strings.TrimSpace(text))
	word = strings.Trim(word, ".!,;: ")
	if fields := strings.Fields(word); len(fields) > 0 {
		word = fields[0]
	}
	return word, optOutWords[word]
}

func normalizeMSISDN(raw string) string {
	digits := strings.TrimPrefix(strings.TrimSpace(raw), "+")
	return "+" + digits
}
