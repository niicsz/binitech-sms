package eventing

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/contract/events"
)

const flushWait = 300 * time.Millisecond

type Emitter struct {
	outbox    out.Outbox
	publisher out.EventPublisher
	clock     out.Clock
	ids       out.IDGenerator
}

type Staged struct {
	ids       []int64
	envelopes []events.Envelope
}

func NewEmitter(outbox out.Outbox, publisher out.EventPublisher, clock out.Clock, ids out.IDGenerator) *Emitter {
	return &Emitter{outbox: outbox, publisher: publisher, clock: clock, ids: ids}
}

func (e *Emitter) Envelope(topic, key, operation, accountID string, payload any) (events.Envelope, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return events.Envelope{}, fmt.Errorf("eventing: %s: %w", operation, err)
	}
	headers := map[string]string{
		events.HeaderOperation:  operation,
		events.HeaderOpID:       e.ids.OpID(),
		events.HeaderSchema:     events.SchemaV1,
		events.HeaderProducedAt: e.clock.Now().Format(time.RFC3339Nano),
	}
	if accountID != "" {
		headers[events.HeaderAccountID] = accountID
	}
	return events.Envelope{Topic: topic, Key: key, Headers: headers, Payload: body}, nil
}

func (e *Emitter) Stage(ctx context.Context, staged *Staged, envelopes ...events.Envelope) error {
	for _, envelope := range envelopes {
		id, err := e.outbox.Enqueue(ctx, envelope)
		if err != nil {
			return fmt.Errorf("eventing: outbox: %w", err)
		}
		staged.ids = append(staged.ids, id)
		staged.envelopes = append(staged.envelopes, envelope)
	}
	return nil
}

func (e *Emitter) Flush(ctx context.Context, staged *Staged) {
	if staged == nil || len(staged.envelopes) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), flushWait)
	defer cancel()
	if err := e.publisher.Publish(ctx, staged.envelopes...); err != nil {
		return
	}
	_ = e.outbox.MarkPublished(ctx, staged.ids, e.clock.Now())
}

func (e *Emitter) Publish(ctx context.Context, envelopes ...events.Envelope) error {
	if err := e.publisher.Publish(ctx, envelopes...); err != nil {
		return fmt.Errorf("eventing: publicar: %w", err)
	}
	return nil
}

func (e *Emitter) Now() time.Time {
	return e.clock.Now()
}

func (e *Emitter) OpID() string {
	return e.ids.OpID()
}
