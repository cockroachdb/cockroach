package changefeedccl

import (
	"context"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func newKafkaSinkClient(
	ctx context.Context, // TODO: do we need this ctx
	batchCfg sinkBatchConfig,
	kafkaCfg *sarama.Config,
	bootstrapAddrs string,
	topics *TopicNamer,
	knobs kafkaSinkKnobs,
) (*kafkaSinkClient, error) {
	var err error

	sinkClient := &kafkaSinkClient{
		ctx:      ctx,
		batchCfg: batchCfg,
		kafkaCfg: kafkaCfg,
		topics:   topics,
	}
	sinkClient.client, err = newKafkaClient(kafkaCfg, bootstrapAddrs, knobs)
	if err != nil {
		return nil, err
	}
	sinkClient.producer, err = newAsyncProducer(sinkClient.client, bootstrapAddrs, knobs)
	if err != nil {
		return nil, err
	}
	return sinkClient, nil
}

func newKafkaClient(
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

type kafkaSinkClient struct {
	ctx            context.Context
	format         changefeedbase.FormatType
	batchCfg       sinkBatchConfig
	topics         *TopicNamer
	bootstrapAddrs string
	kafkaCfg       *sarama.Config
	client         kafkaClient
	producer       sarama.AsyncProducer

	knobs kafkaSinkKnobs

	lastMetadataRefresh time.Time
}

// Close implements SinkClient.
func (k *kafkaSinkClient) Close() error {
	panic("unimplemented")
}

// Flush implements SinkClient.
func (k *kafkaSinkClient) Flush(context.Context, SinkPayload) error {
	panic("unimplemented")
}

// FlushResolvedPayload implements SinkClient.
func (k *kafkaSinkClient) FlushResolvedPayload(context.Context, []byte, func(func(topic string) error) error, retry.Options) error {
	panic("unimplemented")
}

// MakeBatchBuffer implements SinkClient.
func (k *kafkaSinkClient) MakeBatchBuffer(topic string) BatchBuffer {
	panic("unimplemented")
}

var _ SinkClient = (*kafkaSinkClient)(nil)
var _ SinkPayload = (*sarama.ProducerMessage)(nil)

func makeKafkaSinkV2(ctx context.Context,
	u sinkURL,
	targets changefeedbase.Targets,
	jsonConfig changefeedbase.SinkSpecificJSONConfig,
	parallelism int,
	pacerFactory func() *admission.Pacer,
	timeSource timeutil.TimeSource,
	settings *cluster.Settings,
	mb metricsRecorderBuilder,
) (Sink, error) {
	batchCfg, retryOpts, err := getSinkConfigFromJson(jsonConfig, sinkJSONConfig{
		// TODO[rachael]: Change to kafka defaults
		// ..but the defaults for these are all zero -- flush immediately.
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

	topicNamer, err := MakeTopicNamer(
		targets,
		WithPrefix(kafkaTopicPrefix), WithSingleName(kafkaTopicName), WithSanitizeFn(SQLNameToKafkaName))

	if err != nil {
		return nil, err
	}

	// TODO: how to make batching sink framework work with this kafka-specific retry with smaller batches setting.
	// internalRetryEnabled := settings != nil && changefeedbase.BatchReductionRetryEnabled.Get(&settings.SV)

	// TODO: how to handle knobs
	sinkClient, err := newKafkaSinkClient(ctx, batchCfg, kafkaCfg, u.Host, topicNamer, kafkaSinkKnobs{})
	if err != nil {
		return nil, err
	}

	if unknownParams := u.remainingQueryParams(); len(unknownParams) > 0 {
		return nil, errors.Errorf(
			`unknown kafka sink query parameters: %s`, strings.Join(unknownParams, ", "))
	}

	return makeBatchingSink(ctx, sinkTypeKafka, sinkClient, time.Second, retryOpts,
		parallelism, topicNamer, pacerFactory, timeSource, mb(true), settings), nil
}
