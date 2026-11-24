package kafka_test

import (
	"context"
	"testing"
	"time"

	kafkaadapter "github.com/niicsz/binitech-sms/internal/adapter/out/kafka"
	"github.com/niicsz/binitech-sms/internal/contract/events"
	"github.com/niicsz/binitech-sms/internal/platform/kafkax"
	"github.com/niicsz/binitech-sms/internal/testsupport/kafkatest"
)

func TestPublishKeepsKeyHeadersAndOrderPerKey(t *testing.T) {
	cluster := kafkatest.New(t, "sms.submit.test")
	client := cluster.Client(t, kafkax.ProducerOptions(cluster.Brokers(), "test")...)
	publisher := kafkaadapter.NewPublisher(client)

	var envelopes []events.Envelope
	for _, payload := range []string{"1", "2", "3"} {
		envelopes = append(envelopes, events.Envelope{
			Topic:   "sms.submit.test",
			Key:     "acc_7H2K",
			Headers: map[string]string{events.HeaderOperation: events.OpMessageSubmit, events.HeaderSchema: events.SchemaV1},
			Payload: []byte(payload),
		})
	}
	if err := publisher.Publish(context.Background(), envelopes...); err != nil {
		t.Fatal(err)
	}

	records := cluster.Consume(t, "sms.submit.test", 3)
	partition := records[0].Partition
	for i, r := range records {
		if string(r.Key) != "acc_7H2K" || r.Partition != partition {
			t.Fatalf("mesma chave deveria cair na mesma partição: %+v", r)
		}
		if string(r.Value) != string(envelopes[i].Payload) {
			t.Fatalf("ordem trocada: posição %d veio %s", i, r.Value)
		}
		headers := map[string]string{}
		for _, h := range r.Headers {
			headers[h.Key] = string(h.Value)
		}
		if headers[events.HeaderOperation] != events.OpMessageSubmit || headers[events.HeaderSchema] != events.SchemaV1 {
			t.Fatalf("headers perdidos: %v", headers)
		}
	}
}

func TestPublishFailsOnUnknownTopic(t *testing.T) {
	cluster := kafkatest.New(t, "existe")
	client := cluster.Client(t, kafkax.ProducerOptions(cluster.Brokers(), "test")...)
	publisher := kafkaadapter.NewPublisher(client)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := publisher.Publish(ctx, events.Envelope{Topic: "nao.existe", Key: "k", Headers: map[string]string{}, Payload: []byte("x")})
	if err == nil {
		t.Fatal("publicar em tópico inexistente deveria falhar")
	}
}
