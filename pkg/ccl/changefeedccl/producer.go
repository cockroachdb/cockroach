// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

// testProducersHook is used as a kafka mock instead of an external connection.
// Values of the kafkaBootstrapServers setting is the map key. If this map is
// non-nil, it's guaranteed that no external connections will be attempted.
var testProducersHook map[string]KafkaProducer

// KafkaProducer contains the relevant methods of the real Kafka implementation.
// It's used for testing.
type KafkaProducer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Flush(timeoutMs int) int
}

func getKafkaProducer(bootstrapServers string) (KafkaProducer, error) {
	if testProducersHook != nil {
		producer, ok := testProducersHook[bootstrapServers]
		if !ok {
			return nil, errors.Errorf(`no test producer: %s`, bootstrapServers)
		}
		return producer, nil
	}

	config := &kafka.ConfigMap{`bootstrap.servers`: bootstrapServers}
	producer, err := kafka.NewProducer(config)
	if err != nil {
		return nil, errors.Wrapf(err, `connecting to kafka: %s`, bootstrapServers)
	}
	return producer, nil
}
