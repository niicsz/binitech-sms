package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/niicsz/binitech-sms/internal/application/port/in"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/application/usecase/eventing"
	"github.com/niicsz/binitech-sms/internal/contract/events"
	"github.com/niicsz/binitech-sms/internal/domain/message"
)

type Throttle interface {
	Wait(ctx context.Context) error
}

type Deliverer struct {
	carrier  out.Carrier
	events   *eventing.Emitter
	throttle Throttle
}

var _ in.EventHandler = (*Deliverer)(nil)

func New(carrier out.Carrier, emitter *eventing.Emitter, throttle Throttle) *Deliverer {
	return &Deliverer{carrier: carrier, events: emitter, throttle: throttle}
}

func (d *Deliverer) Handle(ctx context.Context, e in.Event) error {
	if e.Headers[events.HeaderOperation] != events.OpMessageDispatch {
		return nil
	}
	var m events.MessageDispatched
	if err := json.Unmarshal(e.Payload, &m); err != nil {
		return in.Permanent(fmt.Errorf("delivery: payload: %w", err))
	}
	if m.RouteID != d.carrier.ID() {
		return in.Permanent(fmt.Errorf("delivery: evento da rota %s chegou na rota %s", m.RouteID, d.carrier.ID()))
	}
	now := d.events.Now()
	if now.After(m.ExpiresAt) {
		return d.publish(ctx, events.TopicStatusDLR, m.MessageID, events.OpMessageStatus, m.AccountID, events.MessageStatus{
			MessageID: m.MessageID, AccountID: m.AccountID, Status: string(message.Expired), ErrorCode: "expired_before_submit", At: now,
		})
	}
	if d.throttle != nil {
		if err := d.throttle.Wait(ctx); err != nil {
			return err
		}
	}
	result, err := d.carrier.Send(ctx, out.OutboundSMS{
		MessageID: m.MessageID, AccountID: m.AccountID, To: m.To, From: m.From, Text: m.Text,
		Encoding: m.Encoding, Segments: m.Segments, ExpiresAt: m.ExpiresAt,
	})
	if err != nil {
		return fmt.Errorf("delivery: %s: %w", d.carrier.ID(), err)
	}
	at := d.events.Now()
	if result.Accepted {
		return d.publish(ctx, events.TopicStatusDLR, m.MessageID, events.OpCarrierAccepted, m.AccountID, events.CarrierAccepted{
			MessageID: m.MessageID, AccountID: m.AccountID, RouteID: m.RouteID, ProviderMsgIDs: result.ProviderIDs, AcceptedAt: at,
		})
	}
	return d.publish(ctx, events.TopicStatusDLR, m.MessageID, events.OpCarrierRejected, m.AccountID, events.CarrierRejected{
		MessageID: m.MessageID, AccountID: m.AccountID, RouteID: m.RouteID, ErrorCode: result.ErrorCode, RejectedAt: at,
	})
}

func (d *Deliverer) publish(ctx context.Context, topic, key, op, accountID string, payload any) error {
	envelope, err := d.events.Envelope(topic, key, op, accountID, payload)
	if err != nil {
		return in.Permanent(err)
	}
	return d.events.Publish(ctx, envelope)
}

func PumpReports(ctx context.Context, carrier out.Carrier, emitter *eventing.Emitter, onError func(error)) {
	reports := carrier.Reports()
	for {
		select {
		case <-ctx.Done():
			return
		case report, ok := <-reports:
			if !ok {
				return
			}
			envelope, err := emitter.Envelope(events.TopicStatusDLR, report.RouteID+":"+report.ProviderID, events.OpMessageDLR, "", events.DlrReceived{
				MessageID:      report.MessageID,
				RouteID:        report.RouteID,
				ProviderMsgID:  report.ProviderID,
				ProviderStatus: report.Status,
				ErrorCode:      report.ErrorCode,
				DoneAt:         report.DoneAt,
				ReceivedAt:     emitter.Now(),
			})
			if err != nil {
				onError(err)
				continue
			}
			publishWithRetry(ctx, emitter, envelope, onError)
		}
	}
}

func publishWithRetry(ctx context.Context, emitter *eventing.Emitter, envelope events.Envelope, onError func(error)) {
	wait := 200 * time.Millisecond
	for {
		err := emitter.Publish(ctx, envelope)
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		onError(err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = min(wait*2, 10*time.Second)
	}
}

func WatchHealth(ctx context.Context, carrier out.Carrier, store out.RouteHealthStore, every time.Duration, onError func(error)) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		err := carrier.Healthy(ctx)
		detail := "ok"
		if err != nil {
			detail = err.Error()
		}
		if rerr := store.Report(ctx, carrier.ID(), err == nil, detail); rerr != nil && ctx.Err() == nil {
			onError(rerr)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
