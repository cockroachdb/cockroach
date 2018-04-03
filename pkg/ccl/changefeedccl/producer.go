// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"strings"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

// TODO(dan): This uses the shopify kafka producer library because the official
// confluent one depends on librdkafka and it didn't seem worth it to add a new
// c dep for the prototype. Revisit before 2.1 and check stability, performance,
// etc.

// testProducersHook is used as a kafka mock instead of an external connection.
// Values of the kafkaBootstrapServers setting is the map key. If this map is
// non-nil, it's guaranteed that no external connections will be attempted.
var testProducersHook map[string]sarama.SyncProducer

func getKafkaProducer(bootstrapServers string) (sarama.SyncProducer, error) {
	if testProducersHook != nil {
		producer, ok := testProducersHook[bootstrapServers]
		if !ok {
			return nil, errors.Errorf(`no test producer: %s`, bootstrapServers)
		}
		return producer, nil
	}

	producer, err := sarama.NewSyncProducer(strings.Split(bootstrapServers, `,`), nil)
	if err != nil {
		return nil, errors.Wrapf(err, `connecting to kafka: %s`, bootstrapServers)
	}
	return producer, nil
}
