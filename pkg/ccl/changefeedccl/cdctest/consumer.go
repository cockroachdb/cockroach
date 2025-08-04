package cdctest

import (
	"context"
	"strconv"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"golang.org/x/sync/errgroup"
)

type ConsumerMessage struct {
	Topic     string
	Partition string
	Resolved  hlc.Timestamp
	Updated   hlc.Timestamp
	Key       string
	Value     string
}

type Consumer interface {
	Start(ctx context.Context) error
	Output() <-chan *ConsumerMessage
}

type kafkaConsumer struct {
	uri                string
	topic              string
	partitions         []string
	consumer           sarama.Consumer
	partitionConsumers map[int32]sarama.PartitionConsumer
	output             chan *ConsumerMessage
}

func NewKafkaConsumer(ctx context.Context, uri, topic string) (*kafkaConsumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Fetch.Default = 1000012
	consumer, err := sarama.NewConsumer([]string{uri}, config)
	if err != nil {
		return nil, err
	}

	parts, err := consumer.Partitions(topic)
	if err != nil {
		return nil, err
	}
	partitions := make([]string, len(parts))
	for i, partition := range parts {
		partitions[i] = strconv.Itoa(int(partition))
	}

	partitionConsumers := make(map[int32]sarama.PartitionConsumer, len(parts))
	for _, partition := range parts {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			return nil, err
		}
		partitionConsumers[partition] = pc
	}

	return &kafkaConsumer{
		uri:                uri,
		topic:              topic,
		partitions:         partitions,
		consumer:           consumer,
		partitionConsumers: partitionConsumers,
		output:             make(chan *ConsumerMessage),
	}, nil
}

func (c *kafkaConsumer) Start(ctx context.Context) error {
	defer close(c.output)

	partConsumerOutputs := make([]<-chan *sarama.ConsumerMessage, len(c.partitionConsumers))
	for i, partition := range c.partitionConsumers {
		partConsumerOutputs[i] = partition.Messages()
	}
	return fanIn(ctx, partConsumerOutputs, c.output, func(msg *sarama.ConsumerMessage) *ConsumerMessage {
		return &ConsumerMessage{
			Topic:     c.topic,
			Partition: strconv.Itoa(int(msg.Partition)),
			Key:       string(msg.Key),
			Value:     string(msg.Value),
		}
	})
}

func (c *kafkaConsumer) Output() <-chan *ConsumerMessage {
	return c.output
}


// TODO: other consumers
// - [ ] cloud storage (sinkURI = `experimental-gs://cockroach-tmp/roachtest/` + ts + "?AUTH=implicit)
// - [ ] webhook (need a webhook server that buffers data to disk and can replay it back)
// - any others are a bonus

func fanIn[I, O any](ctx context.Context, inputs []<-chan I, output chan<- O, f func(I) O) error {
	wg, ctx := errgroup.WithContext(ctx)
	for _, input := range inputs {
		input := input
		wg.Go(func() error {
			for msg := range input {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case output <- f(msg):
				}
			}
			return nil
		})
	}
	return wg.Wait()
}

func ConsumeAndValidate(ctx context.Context, consumer Consumer, validator Validator) error {
	msgs := consumer.Output()
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return consumer.Start(ctx)
	})
	eg.Go(func() error {
		for msg := range msgs {
			if msg.Resolved.IsSet() {
				if err := validator.NoteResolved(msg.Partition, msg.Resolved); err != nil {
					return err
				}
			} else {
				if err := validator.NoteRow(msg.Partition, msg.Key, msg.Value, msg.Updated, msg.Topic); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return eg.Wait()
}
