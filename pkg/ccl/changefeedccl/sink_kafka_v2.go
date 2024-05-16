package changefeedccl

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

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

func makeKafkaSinkClient(
	ctx context.Context,
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
) (*batchingSink, error) {

	return makeBatchingSink(ctx, sinkTypeKafka, sinkClient, time.Second, retry.Options{},
		parallelism, topicNamer, pacerFactory, timeSource, mb(true), settings)
}
