// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"strings"

	"github.com/dustin/go-humanize"

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// SinkRow is a row to be emitting to a changefeed sink.
type SinkRow struct {
	Topic      string
	Key, Value []byte
}

// Sink is an abstration for anything that a changefeed may emit into.
type Sink interface {
	EmitRows(ctx context.Context, rows []SinkRow) error
	EmitResolvedTimestamp(ctx context.Context, payload []byte) error
	Close() error
}

// testKafkaProducersHook is used as a Kafka mock instead of an external
// connection. The map key is the bootstrap servers part of the sink URI, so use
// it in a test with something like `INTO 'kafka://<map key>'`. If this map is
// non-nil, it's guaranteed that no external Kafka connections will be
// attempted.
var testKafkaProducersHook map[string]sarama.SyncProducer

type kafkaSink struct {
	// TODO(dan): This uses the shopify kafka producer library because the
	// official confluent one depends on librdkafka and it didn't seem worth it
	// to add a new c dep for the prototype. Revisit before 2.1 and check
	// stability, performance, etc.
	sarama.SyncProducer
	client sarama.Client

	kafkaTopicPrefix string
	topicsSeen       map[string]struct{}

	rowsEmitted  uint64
	bytesEmitted uint64
}

func getKafkaSink(kafkaTopicPrefix string, bootstrapServers string) (Sink, error) {
	sink := &kafkaSink{
		kafkaTopicPrefix: kafkaTopicPrefix,
		topicsSeen:       make(map[string]struct{}),
	}

	if testKafkaProducersHook != nil {
		if sink.SyncProducer = testKafkaProducersHook[bootstrapServers]; sink.SyncProducer == nil {
			return nil, errors.Errorf(`no test producer: %s`, bootstrapServers)
		}
		return sink, nil
	}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = newChangefeedPartitioner

	var err error
	sink.client, err = sarama.NewClient(strings.Split(bootstrapServers, `,`), config)
	if err != nil {
		return nil, errors.Wrapf(err, `connecting to kafka: %s`, bootstrapServers)
	}
	sink.SyncProducer, err = sarama.NewSyncProducerFromClient(sink.client)
	if err != nil {
		return nil, errors.Wrapf(err, `connecting to kafka: %s`, bootstrapServers)
	}
	return sink, nil
}

func (s *kafkaSink) Close() error {
	err := s.SyncProducer.Close()
	if s.client != nil {
		if e := s.client.Close(); err == nil {
			err = e
		}
	}
	return err
}

func (s *kafkaSink) EmitRows(ctx context.Context, rows []SinkRow) error {
	// TODO(dan): Figure out if it's safe to reuse these after SendMessages
	// returns and save them across calls to EmitRows.
	m := make([]sarama.ProducerMessage, len(rows))
	messages := make([]*sarama.ProducerMessage, len(rows))

	var bytes uint64
	for i, row := range rows {
		topic := s.kafkaTopicPrefix + row.Topic
		if _, ok := s.topicsSeen[topic]; !ok {
			s.topicsSeen[topic] = struct{}{}
		}
		m[i] = sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.ByteEncoder(row.Key),
			Value: sarama.ByteEncoder(row.Value),
		}
		messages[i] = &m[i]
		bytes += uint64(len(row.Key) + len(row.Value))
	}
	if err := s.SendMessages(messages); err != nil {
		return errors.Wrapf(err, `sending %d messages to kafka`, len(rows))
	}

	s.rowsEmitted += uint64(len(rows))
	s.bytesEmitted += bytes
	if log.V(1) {
		log.Infof(ctx, "emitted %d records (%s) to kafka. total %d records (%s)",
			len(rows), humanize.IBytes(bytes), s.rowsEmitted, humanize.IBytes(s.bytesEmitted))
	}

	return nil
}

func (s *kafkaSink) EmitResolvedTimestamp(ctx context.Context, payload []byte) error {
	// Staleness here does not impact correctness. Some new partitions will miss
	// this resolved timestamp, but they'll eventually be picked up and get
	// later ones.
	messages := make([]*sarama.ProducerMessage, 0, len(s.topicsSeen))
	for topic := range s.topicsSeen {
		// TODO(dan): Figure out how expensive this is to call. Maybe we need to
		// cache it and rate limit?
		partitions, err := s.client.Partitions(topic)
		if err != nil {
			return err
		}
		for _, partition := range partitions {
			messages = append(messages, &sarama.ProducerMessage{
				Topic:     topic,
				Partition: partition,
				Key:       nil,
				Value:     sarama.ByteEncoder(payload),
			})
		}
	}
	return s.SendMessages(messages)
}

type changefeedPartitioner struct {
	hash sarama.Partitioner
}

var _ sarama.Partitioner = &changefeedPartitioner{}
var _ sarama.PartitionerConstructor = newChangefeedPartitioner

func newChangefeedPartitioner(topic string) sarama.Partitioner {
	return &changefeedPartitioner{
		hash: sarama.NewHashPartitioner(topic),
	}
}

func (p *changefeedPartitioner) RequiresConsistency() bool { return true }
func (p *changefeedPartitioner) Partition(
	message *sarama.ProducerMessage, numPartitions int32,
) (int32, error) {
	if message.Key == nil {
		return message.Partition, nil
	}
	return p.hash.Partition(message, numPartitions)
}

type channelSink struct {
	resultsCh chan<- tree.Datums
	alloc     sqlbase.DatumAlloc
}

func (s *channelSink) EmitRows(ctx context.Context, rows []SinkRow) error {
	for _, row := range rows {
		if err := s.emitDatums(ctx, tree.Datums{
			s.alloc.NewDString(tree.DString(row.Topic)),
			s.alloc.NewDBytes(tree.DBytes(row.Key)),
			s.alloc.NewDBytes(tree.DBytes(row.Value)),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *channelSink) EmitResolvedTimestamp(ctx context.Context, payload []byte) error {
	return s.emitDatums(ctx, tree.Datums{
		tree.DNull,
		tree.DNull,
		s.alloc.NewDBytes(tree.DBytes(payload)),
	})
}

func (s *channelSink) emitDatums(ctx context.Context, row tree.Datums) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.resultsCh <- row:
		return nil
	}
}

func (s *channelSink) Close() error {
	// nil the channel so any later calls to EmitRow (there shouldn't be any)
	// don't work.
	s.resultsCh = nil
	return nil
}
