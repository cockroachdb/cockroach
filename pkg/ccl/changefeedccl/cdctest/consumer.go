package cdctest

import (
	"bufio"
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
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
	defer func() { _ = c.consumer.Close() }()
	defer func() {
		for _, pc := range c.partitionConsumers {
			_ = pc.Close()
		}
	}()

	partConsumerOutputs := make([]<-chan *sarama.ConsumerMessage, len(c.partitionConsumers))
	for i, partition := range c.partitionConsumers {
		partConsumerOutputs[i] = partition.Messages()
	}
	return fanIn(ctx, partConsumerOutputs, c.output, func(msg *sarama.ConsumerMessage) *ConsumerMessage {
		// TODO: support other formats (?)
		updated, resolved, _, err := tryGetUpdatedResolvedKeyFromJSONRow(msg.Value)
		if err != nil {
			return nil
		}
		return &ConsumerMessage{
			Topic:     c.topic,
			Resolved:  resolved,
			Updated:   updated,
			Partition: strconv.Itoa(int(msg.Partition)),
			Key:       string(msg.Key),
			Value:     string(msg.Value),
		}
	})
}

func (c *kafkaConsumer) Output() <-chan *ConsumerMessage {
	return c.output
}

type cloudStorageConsumer struct {
	gcs    *storage.Client
	bucket *storage.BucketHandle
	prefix string
	output chan *ConsumerMessage
}

func NewCloudStorageConsumer(ctx context.Context, uri string) (*cloudStorageConsumer, error) {
	gcs, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	parsedURI, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	bucket := gcs.Bucket(parsedURI.Host)
	return &cloudStorageConsumer{gcs: gcs, bucket: bucket, prefix: parsedURI.Path}, nil
}

func (c *cloudStorageConsumer) Start(ctx context.Context) error {
	defer close(c.output)
	defer func() { _ = c.gcs.Close() }()

	// TODO: better data structure for this
	seenFiles := make(map[string]struct{})
	pendingFiles := make(map[string]struct{})
	getNextFile := func() (first string) {
		for file := range pendingFiles {
			if first == "" || file < first {
				first = file
			}
		}
		if first != "" {
			delete(pendingFiles, first)
		}
		return first
	}

	for ctx.Err() == nil {
		// Refresh files.
		objs := c.bucket.Objects(ctx, &storage.Query{Prefix: c.prefix})
		var err error
		var obj *storage.ObjectAttrs
		for obj, err = objs.Next(); err == nil; obj, err = objs.Next() {
			if _, ok := seenFiles[obj.Name]; ok {
				continue
			}
			pendingFiles[obj.Name] = struct{}{}
		}
		if err != nil && !errors.Is(err, iterator.Done) {
			return err
		}

		nextFile := getNextFile()
		if nextFile == "" {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(1 * time.Second):
			}
			continue
		}

		// Get the topic from the file name. (`<timestamp>-<uniquer>-<topic_id>-<schema_id>.<ext>`)
		parts := strings.Split(nextFile, "-")
		if len(parts) < 4 {
			return errors.Newf("invalid file name: %s", nextFile)
		}
		topic := parts[2]

		reader, err := c.bucket.Object(nextFile).NewReader(ctx)
		if err != nil {
			return err
		}
		// TODO: handle parquet files
		// TODO: handle parquet files
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			value := scanner.Bytes()
			updated, resolved, key, err := tryGetUpdatedResolvedKeyFromJSONRow(value)
			if err != nil {
				return nil
			}

			c.output <- &ConsumerMessage{
				Topic:     topic,
				Key:       key,
				Value:     string(value),
				Partition: "",
				Resolved:  resolved,
				Updated:   updated,
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}

		// Mark the file as seen.
		seenFiles[nextFile] = struct{}{}
	}
	return nil
}

func (c *cloudStorageConsumer) Output() <-chan *ConsumerMessage {
	return c.output
}

// TODO: other consumers
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

func tryGetUpdatedResolvedKeyFromJSONRow(value []byte) (updated, resolved hlc.Timestamp, key string, err error) {
	var val jsonMessageVal
	if err := json.Unmarshal(value, &val); err != nil {
		return hlc.Timestamp{}, hlc.Timestamp{}, "", err
	}
	if val.Updated != "" {
		updated, err = hlc.ParseTimestamp(val.Updated)
		if err != nil {
			return hlc.Timestamp{}, hlc.Timestamp{}, "", err
		}
	}
	if val.Resolved != "" {
		resolved, err = hlc.ParseTimestamp(val.Resolved)
		if err != nil {
			return hlc.Timestamp{}, hlc.Timestamp{}, "", err
		}
	}
	return updated, resolved, val.Key, nil
}

type jsonMessageVal struct {
	Updated  string `json:"updated"`
	Resolved string `json:"resolved"`
	Key      string `json:"key"`
}
