package outboxrelay

import (
	"context"
	"fmt"
	"time"

	"github.com/niicsz/binitech-sms/internal/application/port/in"
	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/contract/events"
)

type Options struct {
	BatchSize int
	Grace     time.Duration
}

type Relay struct {
	tx        out.TxManager
	outbox    out.Outbox
	publisher out.EventPublisher
	clock     out.Clock
	options   Options
}

var _ in.OutboxRelay = (*Relay)(nil)

func New(tx out.TxManager, outbox out.Outbox, publisher out.EventPublisher, clock out.Clock, options Options) *Relay {
	if options.BatchSize <= 0 {
		options.BatchSize = 500
	}
	return &Relay{tx: tx, outbox: outbox, publisher: publisher, clock: clock, options: options}
}

func (r *Relay) RelayOnce(ctx context.Context) (in.RelayResult, error) {
	var result in.RelayResult
	err := r.tx.WithinTx(ctx, func(ctx context.Context) error {
		records, err := r.outbox.ClaimPending(ctx, r.options.Grace, r.options.BatchSize)
		if err != nil {
			return fmt.Errorf("claim outbox: %w", err)
		}
		if len(records) == 0 {
			return nil
		}
		envelopes := make([]events.Envelope, len(records))
		ids := make([]int64, len(records))
		for i, record := range records {
			envelopes[i] = record.Envelope
			ids[i] = record.ID
		}
		if err := r.publisher.Publish(ctx, envelopes...); err != nil {
			return fmt.Errorf("publish outbox: %w", err)
		}
		if err := r.outbox.MarkPublished(ctx, ids, r.clock.Now()); err != nil {
			return fmt.Errorf("mark outbox: %w", err)
		}
		result.Published = len(records)
		return nil
	})
	return result, err
}
