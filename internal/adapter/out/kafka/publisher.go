package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/niicsz/binitech-sms/internal/application/port/out"
	"github.com/niicsz/binitech-sms/internal/contract/events"
)

type Publisher struct {
	client *kgo.Client
}

var _ out.EventPublisher = (*Publisher)(nil)

func NewPublisher(client *kgo.Client) *Publisher {
	return &Publisher{client: client}
}

func (p *Publisher) Publish(ctx context.Context, envelopes ...events.Envelope) error {
	if len(envelopes) == 0 {
		return nil
	}
	records := make([]*kgo.Record, len(envelopes))
	for i, envelope := range envelopes {
		records[i] = toRecord(envelope)
	}
	if err := p.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("kafka: produce: %w", err)
	}
	return nil
}

func toRecord(envelope events.Envelope) *kgo.Record {
	headers := make([]kgo.RecordHeader, 0, len(envelope.Headers))
	for key, value := range envelope.Headers {
		headers = append(headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
	}
	return &kgo.Record{
		Topic:   envelope.Topic,
		Key:     []byte(envelope.Key),
		Value:   envelope.Payload,
		Headers: headers,
	}
}
