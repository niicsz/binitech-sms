package outboxrelay_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/application/usecase/outboxrelay"
	"github.com/niicsz/binitech-sms/internal/contract/events"
)

type fakeTx struct{ rolledBack bool }

func (f *fakeTx) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	if err := fn(ctx); err != nil {
		f.rolledBack = true
		return err
	}
	return nil
}

type fakeOutbox struct {
	pending   []out.OutboxRecord
	published []int64
}

func (f *fakeOutbox) Enqueue(context.Context, events.Envelope) (int64, error) { return 0, nil }

func (f *fakeOutbox) ClaimPending(_ context.Context, _ time.Duration, limit int) ([]out.OutboxRecord, error) {
	if len(f.pending) > limit {
		return f.pending[:limit], nil
	}
	return f.pending, nil
}

func (f *fakeOutbox) MarkPublished(_ context.Context, ids []int64, _ time.Time) error {
	f.published = append(f.published, ids...)
	return nil
}

type fakePublisher struct {
	sent []events.Envelope
	err  error
}

func (f *fakePublisher) Publish(_ context.Context, envelopes ...events.Envelope) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, envelopes...)
	return nil
}

type fixedClock struct{}

func (fixedClock) Now() time.Time { return time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC) }

func records(n int) []out.OutboxRecord {
	list := make([]out.OutboxRecord, n)
	for i := range list {
		list[i] = out.OutboxRecord{ID: int64(i + 1), Envelope: events.Envelope{Topic: "sms.submit.test", Key: "acc_1"}}
	}
	return list
}

func TestPublishesAndMarksPending(t *testing.T) {
	tx, outbox, publisher := &fakeTx{}, &fakeOutbox{pending: records(3)}, &fakePublisher{}
	relay := outboxrelay.New(tx, outbox, publisher, fixedClock{}, outboxrelay.Options{BatchSize: 10})

	result, err := relay.RelayOnce(context.Background())

	if err != nil || result.Published != 3 {
		t.Fatalf("got %+v, %v", result, err)
	}
	if len(publisher.sent) != 3 || len(outbox.published) != 3 {
		t.Fatalf("enviados=%d marcados=%d", len(publisher.sent), len(outbox.published))
	}
}

func TestRespectsBatchSize(t *testing.T) {
	outbox, publisher := &fakeOutbox{pending: records(5)}, &fakePublisher{}
	relay := outboxrelay.New(&fakeTx{}, outbox, publisher, fixedClock{}, outboxrelay.Options{BatchSize: 2})

	result, _ := relay.RelayOnce(context.Background())

	if result.Published != 2 || len(outbox.published) != 2 {
		t.Fatalf("got %+v, marcados=%d", result, len(outbox.published))
	}
}

func TestKeepsPendingWhenKafkaFails(t *testing.T) {
	tx, outbox := &fakeTx{}, &fakeOutbox{pending: records(2)}
	relay := outboxrelay.New(tx, outbox, &fakePublisher{err: errors.New("broker fora")}, fixedClock{}, outboxrelay.Options{})

	_, err := relay.RelayOnce(context.Background())

	if err == nil || !tx.rolledBack || len(outbox.published) != 0 {
		t.Fatalf("err=%v rollback=%v marcados=%d", err, tx.rolledBack, len(outbox.published))
	}
}

func TestNothingPending(t *testing.T) {
	publisher := &fakePublisher{}
	relay := outboxrelay.New(&fakeTx{}, &fakeOutbox{}, publisher, fixedClock{}, outboxrelay.Options{})

	result, err := relay.RelayOnce(context.Background())

	if err != nil || result.Published != 0 || len(publisher.sent) != 0 {
		t.Fatalf("got %+v, %v", result, err)
	}
}
