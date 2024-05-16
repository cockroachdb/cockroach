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
func (k *kafkaSinkClient) Flush(ctx context.Context, payload SinkPayload) error {
	msgs := payload.([]*sarama.ProducerMessage)
	for _, msg := range msgs {
		k.producer.Input() <- msg
	}
	// TODO: next: we need to basically account for each of the messages we sent. but this will be called in parallel from the batching sink / parallel io guy
	// so we probably need a parent goroutine to do accounting. ideally i think i'd just have a kafkaSinkClient per parallelIO worker. is that possible with the current api?

	// TODO: this STILL doesnt guarantee per-key ordering though (see below). is there a way we can do that?
	// if we enable the idempotent producer, then that should work right? if we have (for one key) A B C D. C fails and we retry it, the idempotent magic will put it in the right place?
	// TODO: look into this. in the meantime proceed under the assumption that this works
	// enable.idempotence=true; acks=all
	a := <-k.producer.Successes()

	panic("unimplemented")
}

// FlushResolvedPayload implements SinkClient.
func (k *kafkaSinkClient) FlushResolvedPayload(context.Context, []byte, func(func(topic string) error) error, retry.Options) error {
	panic("unimplemented")
}

// MakeBatchBuffer implements SinkClient.
func (k *kafkaSinkClient) MakeBatchBuffer(topic string) BatchBuffer {
	kb := &kafkaBuffer{kc: k, topic: topic}
	return kb
}

var _ SinkClient = (*kafkaSinkClient)(nil)
var _ SinkPayload = ([]*sarama.ProducerMessage)(nil) // this doesnt actually assert anything lol

type keyPlusPayload struct {
	key     []byte
	payload []byte
}

// TODO[rachael]: Should the kafka buffer look like a sarama.ProducerMessage? A MessageSet?
type kafkaBuffer struct {
	kc        *kafkaSinkClient
	topic     string
	messages  []keyPlusPayload
	byteCount int
	msgCount  int
}

// Append implements BatchBuffer.
func (k *kafkaBuffer) Append(key []byte, value []byte, _ attributes) {
	k.messages = append(k.messages, keyPlusPayload{key: key, payload: value})
	k.byteCount += len(value)
	k.msgCount++
}

// Close implements BatchBuffer. Convert the buffer into a SinkPayload for sending to kafka.
func (k *kafkaBuffer) Close() (SinkPayload, error) {
	// return &sarama.ProducerMessage{
	// 	Topic: k.topic,
	// 	// TODO: the key has to be the row key, but batches aren't per row. they have Keys() in fact (but we dont expose that here anyway)
	// 	// so we probably can't use this api. maybe one sync producer per buffer? or smth like that
	// 	// or we could store the keys in the buffer and emit one message per key...?
	// 	// ...
	// 	// even if we do that, or use a sync producer with SendMessages(), is it even possible to guarantee no out-of-order-by-key delivery?
	// 	// what if we send 1,2,3,4 - 1,2,4 are delivered but 3 fails. then we need to retry 3 and it will be out of order. afaik
	// 	Key: nil,
	// }, nil

	msgs := make([]*sarama.ProducerMessage, 0, len(k.messages))
	for _, m := range k.messages {
		msgs = append(msgs, &sarama.ProducerMessage{
			Topic: k.topic,
			Key:   sarama.ByteEncoder(m.key),
			Value: sarama.ByteEncoder(m.payload),
		})
	}
	return msgs, nil
}

// ShouldFlush implements BatchBuffer.
func (k *kafkaBuffer) ShouldFlush() bool {
	return true // TODO
}

var _ BatchBuffer = (*kafkaBuffer)(nil)

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
	kafkaCfg.Producer.Idempotent = true                // ?
	kafkaCfg.Producer.RequiredAcks = sarama.WaitForAll // TODO: this is user configurable so idk lol

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
