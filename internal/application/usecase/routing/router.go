package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/niicsz/binitech-sms/internal/application/port/in"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/application/usecase/eventing"
	"github.com/niicsz/binitech-sms/internal/contract/events"
	"github.com/niicsz/binitech-sms/internal/domain/message"
	"github.com/niicsz/binitech-sms/internal/domain/route"
)

const consumer = "router"

type Router struct {
	tx      out.TxManager
	ops     out.ProcessedOps
	store   out.RoutingStore
	catalog out.RouteCatalog
	events  *eventing.Emitter
	pick    func(n int) int
}

var _ in.EventHandler = (*Router)(nil)

func New(tx out.TxManager, ops out.ProcessedOps, store out.RoutingStore, catalog out.RouteCatalog, emitter *eventing.Emitter, pick func(n int) int) *Router {
	return &Router{tx: tx, ops: ops, store: store, catalog: catalog, events: emitter, pick: pick}
}

func (r *Router) Handle(ctx context.Context, e in.Event) error {
	if e.Headers[events.HeaderOperation] != events.OpMessageSubmit {
		return nil
	}
	opID := e.Headers[events.HeaderOpID]
	if opID == "" {
		return in.Permanent(errors.New("routing: evento sem op_id"))
	}
	var m events.MessageSubmitted
	if err := json.Unmarshal(e.Payload, &m); err != nil {
		return in.Permanent(fmt.Errorf("routing: payload: %w", err))
	}
	now := r.events.Now()

	var envelope events.Envelope
	var routeID string
	var err error
	switch {
	case now.After(m.ExpiresAt):
		envelope, err = r.statusEvent(m, message.Expired, "expired_before_routing")
	case m.Test:
		routeID = route.SandboxID
		envelope, err = r.dispatchEvent(m, route.Sandbox(), "test_key")
	default:
		routes, cerr := r.catalog.Routes(ctx)
		if cerr != nil {
			return fmt.Errorf("routing: rotas: %w", cerr)
		}
		choice, cerr := route.Choose(routes, m.MCC, m.MNC, r.pick)
		if errors.Is(cerr, route.ErrNoRoute) {
			envelope, err = r.statusEvent(m, message.Rejected, "no_route")
			break
		}
		routeID = choice.Route.ID
		envelope, err = r.dispatchEvent(m, choice.Route, choice.Reason)
	}
	if err != nil {
		return err
	}

	var staged eventing.Staged
	err = r.tx.WithinTx(ctx, func(ctx context.Context) error {
		first, err := r.ops.MarkOnce(ctx, consumer, opID)
		if err != nil || !first {
			return err
		}
		if routeID != "" {
			moved, err := r.store.MarkRouted(ctx, m.MessageID, routeID)
			if err != nil || !moved {
				return err
			}
		}
		return r.events.Stage(ctx, &staged, envelope)
	})
	if err != nil {
		return err
	}
	r.events.Flush(ctx, &staged)
	return nil
}

func (r *Router) dispatchEvent(m events.MessageSubmitted, chosen route.Route, reason string) (events.Envelope, error) {
	return r.events.Envelope(events.TopicDispatch(chosen.ID), m.AccountID, events.OpMessageDispatch, m.AccountID, events.MessageDispatched{
		MessageID:    m.MessageID,
		AccountID:    m.AccountID,
		RouteID:      chosen.ID,
		RouteReason:  reason,
		To:           m.To,
		From:         m.From,
		Text:         m.Text,
		Encoding:     m.Encoding,
		Segments:     m.Segments,
		ExpiresAt:    m.ExpiresAt,
		Test:         m.Test,
		Attempt:      1,
		DispatchedAt: r.events.Now(),
	})
}

func (r *Router) statusEvent(m events.MessageSubmitted, status message.Status, code string) (events.Envelope, error) {
	return r.events.Envelope(events.TopicStatusDLR, m.MessageID, events.OpMessageStatus, m.AccountID, events.MessageStatus{
		MessageID: m.MessageID,
		AccountID: m.AccountID,
		Status:    string(status),
		ErrorCode: code,
		At:        r.events.Now(),
	})
}
