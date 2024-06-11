package changefeedccl

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

func newKafkaSinkClient(
	ctx context.Context,
	kafkaCfg []kgo.Opt,
	batchCfg sinkBatchConfig,
	bootstrapAddrs string,
	topics *TopicNamer,
	settings *cluster.Settings,
	knobs kafkaSinkKnobs,
) (*kafkaSinkClient, error) {
	kafkaCfg = append([]kgo.Opt{
		// TODO: hooks for metrics / metric support at all
		kgo.SeedBrokers(bootstrapAddrs),
		kgo.AllowAutoTopicCreation(), // is this configurable?
		kgo.ClientID(`cockroach`),
		// kgo.DefaultProduceTopic(topic), // TODO: maybe, depending on if we end up sharing producers
		kgo.WithLogger(kgoLogAdapter{ctx: ctx}),
		// TODO: or use the recommended kgo.StickyKeyPartitioner(kgo.SaramaCompatHasher(fnv.New32a())) (but still need to override for resolved ts so this is probably fine)
		kgo.RecordPartitioner(kgo.BasicConsistentPartitioner(func(topic string) func(r *kgo.Record, n int) int {
			// this must (and should) have the same behaviour as our old partitioner which wraps sarama.NewCustomHashPartitioner(fnv.New32a)
			hasher := fnv.New32a()
			// rand := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
			return func(r *kgo.Record, n int) int {
				if r.Key == nil {
					// TODO: how to handle resolved msgs? the docs suggest that we can't/shouldnt use r.Partition, but
					// then there exists the ManualPartitioner which uses it sooooo idk
					// return rand.Intn(n)
					return int(r.Partition)
				}
				hasher.Reset()
				_, _ = hasher.Write(r.Key)
				part := int32(hasher.Sum32()) % int32(n)
				if part < 0 {
					part = -part
				}
				return int(part)
			}
		})),
		kgo.ProducerBatchMaxBytes(2 << 27), // nearly parity - this is the max the library allows
		kgo.BrokerMaxWriteBytes(2 << 27),   // have to bump this as well
		// idempotent production is strictly a win, but does require the IDEMPOTENT_WRITE permission on CLUSTER (pre Kafka 3.0), and not all clients can have that permission.
		// i think sarama also transparently enables this and we dont disable it there so we shouldnt need to here.. right?
		// kgo.DisableIdempotentWrite(),
	}, kafkaCfg...)

	// TODO: test hook
	client, err := kgo.NewClient(kafkaCfg...)
	if err != nil {
		return nil, err
	}

	return &kafkaSinkClient{
		client:         client,
		knobs:          knobs,
		topics:         topics,
		batchCfg:       batchCfg,
		canTryResizing: changefeedbase.BatchReductionRetryEnabled.Get(&settings.SV),
	}, nil
}

// TODO: rename, with v2 in there somewhere
// single threaded ONLY
type kafkaSinkClient struct {
	format   changefeedbase.FormatType
	topics   *TopicNamer
	batchCfg sinkBatchConfig
	client   *kgo.Client

	knobs          kafkaSinkKnobs
	canTryResizing bool

	lastMetadataRefresh time.Time

	debuggingId int64
}

// Close implements SinkClient.
func (k *kafkaSinkClient) Close() error {
	k.client.Close()
	return nil
}

// Flush implements SinkClient. Does not retry -- retries will be handled by ParallelIO.
func (k *kafkaSinkClient) Flush(ctx context.Context, payload SinkPayload) (retErr error) {
	msgs := payload.([]*kgo.Record)
	defer log.Infof(ctx, `flushed %d messages to kafka (id=%d, err=%v)`, len(msgs), k.debuggingId, retErr)

	log.Infof(ctx, `sending %d messages to kafka`, len(msgs))
	// debugDir := path.Join(debugRoot, strconv.Itoa(int(k.debuggingId)))
	// dfn := path.Join(debugDir, time.Now().Format(`2006-01-02T15:04:05.000000000`))
	// fh, err := os.OpenFile(dfn, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	// if err != nil {
	// 	return err
	// }
	// defer fh.Close()
	// out := json.NewEncoder(fh)
	// for _, m := range msgs {
	// 	mm := map[string]any{
	// 		`topic`:     m.Topic,
	// 		`partition`: m.Partition,
	// 		`key`:       string(m.Key.(sarama.ByteEncoder)),
	// 		`value`:     string(m.Value.(sarama.ByteEncoder)),
	// 		`offset`:    m.Offset,
	// 	}
	// 	if err := out.Encode(mm); err != nil {
	// 		return err
	// 	}
	// }
	// log.Infof(ctx, `KAFKADEBUG: %d wrote %d messages to %s`, k.debuggingId, len(msgs), fh.Name())

	// defer func() {
	// 	fh2, err := os.OpenFile(dfn+".after", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	out := json.NewEncoder(fh2)
	// 	for _, m := range msgs {
	// 		mm := map[string]any{
	// 			`topic`:     m.Topic,
	// 			`partition`: m.Partition,
	// 			`key`:       string(m.Key.(sarama.ByteEncoder)),
	// 			`value`:     string(m.Value.(sarama.ByteEncoder)),
	// 			`offset`:    m.Offset,
	// 		}
	// 		if err := out.Encode(mm); err != nil {
	// 			panic(err)
	// 		}
	// 	}
	// 	log.Infof(ctx, `KAFKADEBUG.after: %d wrote %d messages to %s`, k.debuggingId, len(msgs), fh2.Name())
	// 	_ = fh2.Close()
	// }()

	// TODO: make this better. possibly moving the resizing up into the batch worker would help a bit
	var flushMsgs func(msgs []*kgo.Record) error
	flushMsgs = func(msgs []*kgo.Record) error {
		handleErr := func(err error) error {
			log.Infof(ctx, `kafka error in %d: %s`, k.debuggingId, err.Error())
			if k.shouldTryResizing(err, msgs) {
				a, b := msgs[0:len(msgs)/2], msgs[len(msgs)/2:]
				// recurse
				return errors.Join(flushMsgs(a), flushMsgs(b))
			}

			return err
		}
		if err := k.client.ProduceSync(ctx, msgs...).FirstErr(); err != nil {
			return handleErr(err)
		}
		return nil
	}
	return flushMsgs(msgs)
}

// FlushResolvedPayload implements SinkClient.
func (k *kafkaSinkClient) FlushResolvedPayload(
	ctx context.Context,
	body []byte,
	forEachTopic func(func(topic string) error) error,
	retryOpts retry.Options,
) error {
	return nil
	// TODO
	// msgs := []*kgo.Record{{
	// 	Topic:     topic,
	// 	Partition: int(partition), // this wont be honored. have to use the partitioner api?
	// 	Key:       nil,
	// 	Value:     sarama.ByteEncoder(body),
	// }}
}

// MakeBatchBuffer implements SinkClient.
func (k *kafkaSinkClient) MakeBatchBuffer(topic string) BatchBuffer {
	return &kafkaBuffer{topic: topic, batchCfg: k.batchCfg}
}

func (k *kafkaSinkClient) shouldTryResizing(err error, msgs []*kgo.Record) bool {
	if !k.canTryResizing || err == nil || len(msgs) < 2 {
		return false
	}
	var kError sarama.KError
	return errors.As(err, &kError) && kError == sarama.ErrMessageSizeTooLarge
}

var _ SinkClient = (*kafkaSinkClient)(nil)
var _ SinkPayload = ([]*kgo.Record)(nil) // this doesnt actually assert anything fyi

type keyPlusPayload struct {
	key     []byte
	payload []byte
}

type kafkaBuffer struct {
	topic     string
	messages  []keyPlusPayload
	byteCount int

	batchCfg sinkBatchConfig
}

// Append implements BatchBuffer.
func (b *kafkaBuffer) Append(key []byte, value []byte, _ attributes) {
	b.messages = append(b.messages, keyPlusPayload{key: key, payload: value})
	b.byteCount += len(value)
}

// Close implements BatchBuffer. Convert the buffer into a SinkPayload for sending to kafka.
func (b *kafkaBuffer) Close() (SinkPayload, error) {
	msgs := make([]*kgo.Record, 0, len(b.messages))
	for _, m := range b.messages {
		msgs = append(msgs, &kgo.Record{
			Topic: b.topic,
			Key:   m.key,
			Value: m.payload,
			// can use Context field for tracking/metrics if desired
		})
	}
	return msgs, nil
}

// ShouldFlush implements BatchBuffer.
func (b *kafkaBuffer) ShouldFlush() bool {
	return shouldFlushBatch(b.byteCount, len(b.messages), b.batchCfg)
}

var _ BatchBuffer = (*kafkaBuffer)(nil)

var lastSinkId atomic.Int64

const debugRoot = `/mnt/data2/kafka_sink_debug/`

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
	oldKafkaCfg, err := buildKafkaConfig(ctx, u, jsonConfig, m.getKafkaThrottlingMetrics(settings))
	if err != nil {
		return nil, err
	}
	_ = oldKafkaCfg

	topicNamer, err := MakeTopicNamer(
		targets,
		WithPrefix(kafkaTopicPrefix), WithSingleName(kafkaTopicName), WithSanitizeFn(SQLNameToKafkaName))

	if err != nil {
		return nil, err
	}

	if unknownParams := u.remainingQueryParams(); len(unknownParams) > 0 {
		return nil, errors.Errorf(
			`unknown kafka sink query parameters: %s`, strings.Join(unknownParams, ", "))
	}

	// TODO: might be ok to just have one client shared between workers. try it out
	clientFactory := func(ctx context.Context) (any, error) {
		log.Infof(ctx, `creating kafka sink client`)
		topicNamer2, err := MakeTopicNamer(
			targets,
			WithPrefix(kafkaTopicPrefix), WithSingleName(kafkaTopicName), WithSanitizeFn(SQLNameToKafkaName))

		if err != nil {
			return nil, err
		}

		// TODO: how to handle knobs
		// TODO: parse into this
		kafkaCfg := []kgo.Opt{
			// set the below from the config
			// TODO: tls stuff, etc. retry stuff
			// kgo.MaxVersions() TODO: required if interacting with kafka <0.10
			// kgo.MaxBufferedBytes(), // default unlimited
			// kgo.MaxBufferedRecords(), // default 10k
			// kgo.ProducerLinger(kafkaCfg.Producer.Flush.Frequency), // ?
			// kgo.MaxProduceRequestsInflightPerBroker(1) // default is 1, or 5 if idempotent is enabled (it is by default)
			kgo.ProducerBatchCompression(kgo.NoCompression()), // NOTE: unlike sarama this is not the default. maintain parity etc.
			// kgo.RecordRetries() // do we want to set this? The default is to always retry records forever, but this can be dropped with the RecordRetries and RecordDeliveryTimeout opt
			kgo.RequiredAcks(kgo.AllISRAcks()), // TODO: use kafkaCfg.Producer.RequiredAcks
			// kgo.ManualFlushing() ?
			// kgo.RecordRetries(10), // this seems like a can of worms with idempotency on. default unlimited but we don't want that, do we?
		}
		client, err := newKafkaSinkClient(ctx, kafkaCfg, batchCfg, u.Host, topicNamer2, settings, kafkaSinkKnobs{})
		if err != nil {
			return nil, err
		}
		client.debuggingId = lastSinkId.Add(1)
		debugDir := path.Join(debugRoot, strconv.Itoa(int(client.debuggingId)))
		if err := os.MkdirAll(debugDir, 0755); err != nil {
			return nil, err
		}

		return client, nil
	}

	return makeBatchingSink(ctx, sinkTypeKafka, nil, clientFactory, time.Second, retryOpts,
		parallelism, topicNamer, pacerFactory, timeSource, mb(true), settings), nil
}

type kgoLogAdapter struct {
	ctx context.Context
}

func (k kgoLogAdapter) Level() kgo.LogLevel {
	return kgo.LogLevelInfo
}

func (k kgoLogAdapter) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	kvbs, err := json.Marshal(keyvals)
	if err != nil {
		kvbs = []byte(`["error marshalling keyvals"]`)
	}
	log.Infof(k.ctx, `kafka: %s: %s: %s`, level, msg, string(kvbs))
}

var _ kgo.Logger = kgoLogAdapter{}
