package kafkatest

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Cluster struct {
	fake *kfake.Cluster
}

func New(t *testing.T, topics ...string) *Cluster {
	t.Helper()
	opts := []kfake.Opt{kfake.NumBrokers(1), kfake.DefaultNumPartitions(3)}
	if len(topics) > 0 {
		opts = append(opts, kfake.SeedTopics(3, topics...))
	}
	fake, err := kfake.NewCluster(opts...)
	if err != nil {
		t.Fatalf("kafkatest: %v", err)
	}
	t.Cleanup(fake.Close)
	return &Cluster{fake: fake}
}

func (c *Cluster) Brokers() []string {
	return c.fake.ListenAddrs()
}

func (c *Cluster) Client(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	client, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(c.Brokers()...)}, opts...)...)
	if err != nil {
		t.Fatalf("kafkatest: client: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

func (c *Cluster) Consume(t *testing.T, topic string, want int) []*kgo.Record {
	t.Helper()
	client := c.Client(t, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var records []*kgo.Record
	for len(records) < want {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("kafkatest: esperava %d registros em %s, chegaram %d", want, topic, len(records))
		}
		fetches.EachRecord(func(r *kgo.Record) { records = append(records, r) })
	}
	return records
}
