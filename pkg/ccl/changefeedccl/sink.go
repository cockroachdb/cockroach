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

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

// Sink is an abstration for anything that a changefeed may emit into.
type Sink interface {
	EmitRow(ctx context.Context, topic string, key, value []byte) error
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

	kafkaTopicPrefix string
}

func getKafkaSink(kafkaTopicPrefix string, bootstrapServers string) (Sink, error) {
	sink := &kafkaSink{kafkaTopicPrefix: kafkaTopicPrefix}

	if testKafkaProducersHook != nil {
		if sink.SyncProducer = testKafkaProducersHook[bootstrapServers]; sink.SyncProducer == nil {
			return nil, errors.Errorf(`no test producer: %s`, bootstrapServers)
		}
		return sink, nil
	}

	var err error
	sink.SyncProducer, err = sarama.NewSyncProducer(strings.Split(bootstrapServers, `,`), nil)
	if err != nil {
		return nil, errors.Wrapf(err, `connecting to kafka: %s`, bootstrapServers)
	}
	return sink, nil
}

func (s *kafkaSink) EmitRow(_ context.Context, topic string, key, value []byte) error {
	topic = s.kafkaTopicPrefix + topic
	_, _, err := s.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	})
	return errors.Wrapf(err, `sending message to kafka topic %s`, topic)
}

type channelSink struct {
	resultsCh chan<- tree.Datums
	alloc     sqlbase.DatumAlloc
}

func (s *channelSink) EmitRow(ctx context.Context, topic string, key, value []byte) error {
	row := tree.Datums{
		s.alloc.NewDString(tree.DString(topic)),
		s.alloc.NewDBytes(tree.DBytes(key)),
		s.alloc.NewDBytes(tree.DBytes(value)),
	}
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
