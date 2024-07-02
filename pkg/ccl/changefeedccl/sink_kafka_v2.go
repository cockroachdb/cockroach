// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"bytes"
	"context"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

type kafkaSinkClient struct {
	ctx            context.Context
	format         changefeedbase.FormatType
	batchCfg       sinkBatchConfig
	bootstrapAddrs string
	kafkaCfg       *sarama.Config
	client         kafkaClient
	producer       sarama.AsyncProducer
	topics         *TopicNamer

	knobs kafkaSinkKnobs

	lastMetadataRefresh time.Time
}

var _ SinkClient = (*kafkaSinkClient)(nil)
var _ SinkPayload = (*sarama.ProducerMessage)(nil)

func makeKafkaSinkClient(
	ctx context.Context,
	batchCfg sinkBatchConfig,
	kafkaCfg *sarama.Config,
	bootstrapAddrs string,
	topics *TopicNamer,
	knobs kafkaSinkKnobs,
) (SinkClient, error) {
	var err error

	sinkClient := &kafkaSinkClient{
		ctx:      ctx,
		batchCfg: batchCfg,
		kafkaCfg: kafkaCfg,
		topics:   topics,
	}
	sinkClient.client, err = makeKafkaClient(kafkaCfg, bootstrapAddrs, knobs)
	if err != nil {
		return nil, err
	}
	sinkClient.producer, err = newAsyncProducer(sinkClient.client, bootstrapAddrs, knobs)
	if err != nil {
		return nil, err
	}
	return sinkClient, nil
}

func makeKafkaClient(
	config *sarama.Config,
	bootstrapAddrs string,
	knobs kafkaSinkKnobs,
) (kafkaClient, error) {
	// Initialize client and producer
	if knobs.OverrideClientInit != nil {
		client, err := knobs.OverrideClientInit(config)
		return client, err
	}

	client, err := sarama.NewClient(strings.Split(bootstrapAddrs, `,`), config)
	if err != nil {
		return nil, pgerror.Wrapf(err, pgcode.CannotConnectNow,
			`connecting to kafka: %s`, bootstrapAddrs)
	}
	return client, err
}

func (kc *kafkaSinkClient) MakeBatchBuffer(topic string) BatchBuffer {
	kb := &kafkaBuffer{kc: kc, topic: topic}
	return kb
}

func (kc *kafkaSinkClient) FlushResolvedPayload(ctx context.Context, body []byte, forEachTopic func(func(topic string) error) error, retryOpts retry.Options) error {
	return forEachTopic(func(topic string) error {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   nil,
			Value: sarama.ByteEncoder(body),
		}
		// TODO: Is this correct behavior for kafka retries?
		return retry.WithMaxAttempts(ctx, retryOpts, retryOpts.MaxRetries+1, func() error {
			return kc.Flush(ctx, msg)
		})
	})
}

func (kc *kafkaSinkClient) Flush(ctx context.Context, payload SinkPayload) error {
	// Wait for producer to finish sending messages, then return.
	// msg := payload.(*sarama.Message)
	kc.producer.Input() <- payload.(*sarama.ProducerMessage)
	kc.waitForResponse(ctx)
	return unimplemented.New("kafka batching sink", "FlushResolvedPayload")
}

func (kc *kafkaSinkClient) waitForResponse(ctx context.Context) {
	//var ackMsg *sarama.ProducerMessage
	var ackError error

	select {
	case <-ctx.Done():
		return
	case m := <-kc.producer.Successes():
		// ackMsg = m
		_ = m
	case err := <-kc.producer.Errors():
		_, ackError = err.Msg, err.Err
		//ackMsg, ackError = err.Msg, err.Err
		if ackError != nil {
			// Msg should never be nil but we're being defensive around a vendor library.
			// Msg.Key is nil for sentinel errors (e.g. producer shutting down)
			// and errors sending dummy messages used to prefetch metadata.
			if err.Msg != nil && err.Msg.Key != nil && err.Msg.Value != nil {
				// ackError = errors.Wrapf(ackError,
				_ = errors.Wrapf(ackError,
					"while sending message with key=%s, size=%d",
					err.Msg.Key, err.Msg.Key.Length()+err.Msg.Value.Length())
			}
		}
	}
	// TODO: Add kafka retries here.
}

func (kc *kafkaSinkClient) Close() error {
	return kc.producer.Close()
}

// Should the kafka buffer look like a sarama.ProducerMessage? A MessageSet?
type kafkaBuffer struct {
	kc        *kafkaSinkClient
	byteCount int
	msgCount  int
	messages  [][]byte
	topic     string
}

var _ BatchBuffer = (*kafkaBuffer)(nil)

func (kb *kafkaBuffer) Append(key []byte, value []byte, attributes attributes) {
	kb.messages = append(kb.messages, value)
	kb.byteCount += len(value)
	kb.msgCount++
}

func (kb *kafkaBuffer) ShouldFlush() bool {
	return shouldFlushBatch(kb.byteCount, kb.msgCount, kb.kc.batchCfg)
}

func (kb *kafkaBuffer) Close() (SinkPayload, error) {
	var buffer bytes.Buffer
	buffer.Grow(kb.byteCount /* msgs */ + len(kb.messages) /* commas */)

	for i, msg := range kb.messages {
		if i != 0 {
			buffer.WriteByte(',')
		}
		buffer.Write(msg)
	}
	return sarama.ProducerMessage{
		Topic: kb.topic,
		// key is a partitioning key
		// TODO: might need to pass through a data key?
		Key:   nil,
		Value: sarama.ByteEncoder(buffer.Bytes()),
	}, nil
}

func newAsyncProducer(client kafkaClient, bootstrapAddrs string, knobs kafkaSinkKnobs) (sarama.AsyncProducer, error) {
	var producer sarama.AsyncProducer
	var err error
	if knobs.OverrideAsyncProducerFromClient != nil {
		producer, err = knobs.OverrideAsyncProducerFromClient(client)
	} else {
		producer, err = sarama.NewAsyncProducerFromClient(client.(sarama.Client))
	}
	if err != nil {
		return nil, pgerror.Wrapf(err, pgcode.CannotConnectNow,
			`connecting to kafka: %s`, bootstrapAddrs)
	}
	return producer, nil
}

func makeKafkaSink(
	ctx context.Context,
	u sinkURL,
	targets changefeedbase.Targets,
	jsonConfig changefeedbase.SinkSpecificJSONConfig,
	parallelism int,
	pacerFactory func() *admission.Pacer,
	source timeutil.TimeSource,
	settings *cluster.Settings,
	mb metricsRecorderBuilder,
) (Sink, error) {
	batchCfg, retryOpts, err := getSinkConfigFromJson(jsonConfig, sinkJSONConfig{
		// TODO: Change to kafka defaults
		Flush: sinkBatchConfig{
			Frequency: jsonDuration(10 * time.Millisecond),
			Messages:  100,
			Bytes:     1e6,
		},
	})
	if err != nil {
		return nil, err
	}

	kafkaTopicPrefix := u.consumeParam(changefeedbase.SinkParamTopicPrefix)
	kafkaTopicName := u.consumeParam(changefeedbase.SinkParamTopicName)
	if schemaTopic := u.consumeParam(changefeedbase.SinkParamSchemaTopic); schemaTopic != `` {
		return nil, errors.Errorf(`%s is not yet supported`, changefeedbase.SinkParamSchemaTopic)
	}

	m := mb(requiresResourceAccounting)

	kafkaCfg, err := buildKafkaConfig(ctx, u, jsonConfig, m.getKafkaThrottlingMetrics(settings))
	if err != nil {
		return nil, err
	}

	topics, err := MakeTopicNamer(
		targets,
		WithPrefix(kafkaTopicPrefix), WithSingleName(kafkaTopicName), WithSanitizeFn(SQLNameToKafkaName))

	if err != nil {
		return nil, err
	}

	// TODO: how to make batching sink framework work with this kafka-specific retry with smaller batches setting.
	// internalRetryEnabled := settings != nil && changefeedbase.BatchReductionRetryEnabled.Get(&settings.SV)

	// Knobs are only set in tests and are very much kafka specific. How to handle them?
	sinkClient, err := makeKafkaSinkClient(ctx, batchCfg, kafkaCfg, u.Host, topics, kafkaSinkKnobs{})
	if err != nil {
		return nil, err
	}

	if unknownParams := u.remainingQueryParams(); len(unknownParams) > 0 {
		return nil, errors.Errorf(
			`unknown kafka sink query parameters: %s`, strings.Join(unknownParams, ", "))
	}

	return makeBatchingSink(
		ctx,
		sinkTypeKafka,
		sinkClient,
		time.Duration(batchCfg.Frequency),
		retryOpts,
		parallelism,
		topics,
		pacerFactory,
		source,
		mb(requiresResourceAccounting),
		settings,
	), nil
}
