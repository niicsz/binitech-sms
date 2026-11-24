package kafkax

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func ProducerOptions(brokers []string, clientID string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.Lz4Compression(), kgo.NoCompression()),
		kgo.ProducerLinger(5 * time.Millisecond),
		kgo.ProduceRequestTimeout(10 * time.Second),
		kgo.RecordDeliveryTimeout(30 * time.Second),
	}
}

func ConsumerOptions(brokers []string, clientID, group string, topics ...string) []kgo.Opt {
	return append(DelayedConsumerOptions(brokers, clientID, group, topics...), kgo.BlockRebalanceOnPoll())
}

func DelayedConsumerOptions(brokers []string, clientID, group string, topics ...string) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.FetchMaxWait(500 * time.Millisecond),
	}
}

func NewClient(opts ...kgo.Opt) (*kgo.Client, error) {
	return kgo.NewClient(opts...)
}
