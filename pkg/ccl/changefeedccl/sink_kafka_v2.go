package changefeedccl

import (
	"context"
	"encoding/json"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func newKafkaSinkClient(
	kafkaCfg *sarama.Config,
	batchCfg sinkBatchConfig,
	bootstrapAddrs string,
	topics *TopicNamer,
	settings *cluster.Settings,
	knobs kafkaSinkKnobs,
) (*kafkaSinkClient, error) {
	client, err := newKafkaClient(kafkaCfg, bootstrapAddrs, knobs)
	if err != nil {
		return nil, err
	}

	var producer sarama.SyncProducer
	producer, err = sarama.NewSyncProducerFromClient(client)
	// if knobs.OverrideAsyncProducerFromClient != nil {
	// 	producer, err = knobs.OverrideAsyncProducerFromClient(client)
	// } else {
	// 	producer, err = sarama.NewAsyncProducerFromClient(client)
	// }
	if err != nil {
		return nil, err
	}

	return &kafkaSinkClient{
		client:         client,
		producer:       producer,
		knobs:          knobs,
		topics:         topics,
		batchCfg:       batchCfg,
		canTryResizing: changefeedbase.BatchReductionRetryEnabled.Get(&settings.SV),
	}, nil
}

func newKafkaClient(
	config *sarama.Config,
	bootstrapAddrs string,
	knobs kafkaSinkKnobs,
) (sarama.Client, error) {
	// Initialize client and producer
	if knobs.OverrideClientInit != nil {
		client, err := knobs.OverrideClientInit(config)
		return client.(sarama.Client), err // TODO: unhack
	}

	client, err := sarama.NewClient(strings.Split(bootstrapAddrs, `,`), config)
	if err != nil {
		return nil, pgerror.Wrapf(err, pgcode.CannotConnectNow,
			`connecting to kafka: %s`, bootstrapAddrs)
	}

	return client, err
}

// TODO: rename, with v2 in there somewhere
// single threaded ONLY
type kafkaSinkClient struct {
	format   changefeedbase.FormatType
	topics   *TopicNamer
	batchCfg sinkBatchConfig
	client   sarama.Client
	producer sarama.SyncProducer

	knobs          kafkaSinkKnobs
	canTryResizing bool

	lastMetadataRefresh time.Time

	debuggingId   int64
	didFirstFlush bool
}

// Close implements SinkClient.
func (k *kafkaSinkClient) Close() error {
	if err := k.producer.Close(); err != nil {
		return err
	}
	return k.client.Close()
}

func (k *kafkaSinkClient) isSortedRight(ctx context.Context, msgs []*sarama.ProducerMessage) bool {
	// split by topic & partition
	topicParts := make(map[string][]*sarama.ProducerMessage)
	for _, m := range msgs {
		if k.didFirstFlush && m.Offset == 0 { // first offset can actually be zero
			log.Infof(ctx, `kafka message has offset 0: %v (id=%d)`, m, k.debuggingId)
			return false
		}
		topicParts[m.Topic+strconv.Itoa(int(m.Partition))] = append(topicParts[m.Topic+strconv.Itoa(int(m.Partition))], m)
	}
	for _, msgs := range topicParts {
		if !slices.IsSortedFunc(msgs, func(a, b *sarama.ProducerMessage) int { return int(a.Offset - b.Offset) }) {
			log.Infof(ctx, `kafka messages are not sorted right. id=%d %#+v`, k.debuggingId, msgs)
			return false
		}
	}
	return true
}

// Flush implements SinkClient. Does not retry -- retries will be handled by ParallelIO.
func (k *kafkaSinkClient) Flush(ctx context.Context, payload SinkPayload) (retErr error) {
	defer func() {
		k.didFirstFlush = true
	}()
	msgs := payload.([]*sarama.ProducerMessage)
	defer log.Infof(ctx, `flushed %d messages to kafka (id=%d, err=%v)`, len(msgs), k.debuggingId, retErr)

	log.Infof(ctx, `sending %d messages to kafka`, len(msgs))
	debugDir := path.Join(debugRoot, strconv.Itoa(int(k.debuggingId)))
	dfn := path.Join(debugDir, time.Now().Format(`2006-01-02T15:04:05.000000000`))
	fh, err := os.OpenFile(dfn, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer fh.Close()
	out := json.NewEncoder(fh)
	for _, m := range msgs {
		mm := map[string]any{
			`topic`:     m.Topic,
			`partition`: m.Partition,
			`key`:       string(m.Key.(sarama.ByteEncoder)),
			`value`:     string(m.Value.(sarama.ByteEncoder)),
			`offset`:    m.Offset,
		}
		if err := out.Encode(mm); err != nil {
			return err
		}
	}
	log.Infof(ctx, `KAFKADEBUG: %d wrote %d messages to %s`, k.debuggingId, len(msgs), fh.Name())

	defer func() {
		fh2, err := os.OpenFile(dfn+".after", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
		out := json.NewEncoder(fh2)
		for _, m := range msgs {
			mm := map[string]any{
				`topic`:     m.Topic,
				`partition`: m.Partition,
				`key`:       string(m.Key.(sarama.ByteEncoder)),
				`value`:     string(m.Value.(sarama.ByteEncoder)),
				`offset`:    m.Offset,
			}
			if err := out.Encode(mm); err != nil {
				panic(err)
			}
		}
		log.Infof(ctx, `KAFKADEBUG.after: %d wrote %d messages to %s`, k.debuggingId, len(msgs), fh2.Name())
		_ = fh2.Close()
	}()

	// TODO: make this better. possibly moving the resizing up into the batch worker would help a bit
	var flushMsgs func(msgs []*sarama.ProducerMessage) error
	flushMsgs = func(msgs []*sarama.ProducerMessage) error {
		handleErr := func(err error) error {
			log.Infof(ctx, `kafka error in %d: %s`, k.debuggingId, err.Error())
			if k.shouldTryResizing(err, msgs) {
				a, b := msgs[0:len(msgs)/2], msgs[len(msgs)/2:]
				// recurse
				return errors.Join(flushMsgs(a), flushMsgs(b))
			}
			// offsets should be ordered right. maybe we can catch intra batch reorderings here
			if !k.isSortedRight(ctx, msgs) {
				log.Errorf(ctx, `kafka messages are not sorted right. id=%d, debugfile=%s`, k.debuggingId, fh.Name())
			}

			return err
		}

		if err := k.producer.SendMessages(msgs); err != nil {
			return handleErr(err)
		}

		// offsets should be ordered right. maybe we can catch intra batch reorderings here
		if !k.isSortedRight(ctx, msgs) {
			log.Errorf(ctx, `kafka messages are not sorted right. id=%d, debugfile=%s`, k.debuggingId, fh.Name())
		}

		// trk := tracker{pendingIDs: make(map[int]struct{})}
		// for _, m := range msgs {
		// 	m.Metadata = map[string]any{`id`: trk.next()}
		// }

		// // send input, while watching for errors & close
		// for sent := 0; sent < len(msgs); {
		// 	m := msgs[sent]
		// 	select {
		// 	case <-ctx.Done():
		// 		return ctx.Err()
		// 	case k.producer.Input() <- m:
		// 		sent++
		// 	case ms := <-k.producer.Successes():
		// 		// TODO: i saw a panic here: panic: id 1 not found in pendingIDs. not sure how it happened tho
		// 		trk.remove(ms.Metadata.(map[string]any)[`id`].(int))
		// 		// TODO: re add metrics support
		// 	case err := <-k.producer.Errors():
		// 		return handleErr(err)
		// 	}
		// }

		// // make sure all messages are confirmed or errored
		// for !trk.empty() {
		// 	select {
		// 	case <-ctx.Done():
		// 		return ctx.Err()
		// 	case err := <-k.producer.Errors():
		// 		return handleErr(err)
		// 	case ms := <-k.producer.Successes():
		// 		// TODO: re add metrics support
		// 		trk.remove(ms.Metadata.(map[string]any)[`id`].(int))
		// 	}
		// }

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
	// disabled to reduce variables
	return nil

	// const metadataRefreshMinDuration = time.Minute
	// if timeutil.Since(k.lastMetadataRefresh) > metadataRefreshMinDuration {
	// 	if err := k.client.RefreshMetadata(k.topics.DisplayNamesSlice()...); err != nil {
	// 		return err
	// 	}
	// 	k.lastMetadataRefresh = timeutil.Now()
	// }

	// return forEachTopic(func(topic string) error {
	// 	partitions, err := k.client.Partitions(topic)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	for _, partition := range partitions {
	// 		msgs := []*sarama.ProducerMessage{{
	// 			Topic:     topic,
	// 			Partition: partition,
	// 			Key:       nil,
	// 			Value:     sarama.ByteEncoder(body),
	// 		}}
	// 		if err := k.Flush(ctx, msgs); err != nil {
	// 			return err
	// 		}
	// 	}
	// 	return nil
	// })
}

// MakeBatchBuffer implements SinkClient.
func (k *kafkaSinkClient) MakeBatchBuffer(topic string) BatchBuffer {
	return &kafkaBuffer{topic: topic, batchCfg: k.batchCfg}
}

func (k *kafkaSinkClient) shouldTryResizing(err error, msgs []*sarama.ProducerMessage) bool {
	if !k.canTryResizing || err == nil || len(msgs) < 2 {
		return false
	}
	var kError sarama.KError
	return errors.As(err, &kError) && kError == sarama.ErrMessageSizeTooLarge
}

var _ SinkClient = (*kafkaSinkClient)(nil)
var _ SinkPayload = ([]*sarama.ProducerMessage)(nil) // this doesnt actually assert anything fyi

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
	msgs := make([]*sarama.ProducerMessage, 0, len(b.messages))
	for _, m := range b.messages {
		msgs = append(msgs, &sarama.ProducerMessage{
			Topic: b.topic,
			Key:   sarama.ByteEncoder(m.key),
			Value: sarama.ByteEncoder(m.payload),
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
	kafkaCfg, err := buildKafkaConfig(ctx, u, jsonConfig, m.getKafkaThrottlingMetrics(settings))
	if err != nil {
		return nil, err
	}
	kafkaCfg.Producer.Retry.Max = 0 // retry is handled by the batching sink / parallelIO

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

	clientFactory := func(ctx context.Context) (any, error) {
		log.Infof(ctx, `creating kafka sink client`)
		topicNamer2, err := MakeTopicNamer(
			targets,
			WithPrefix(kafkaTopicPrefix), WithSingleName(kafkaTopicName), WithSanitizeFn(SQLNameToKafkaName))

		if err != nil {
			return nil, err
		}

		// TODO: how to handle knobs
		client, err := newKafkaSinkClient(kafkaCfg, batchCfg, u.Host, topicNamer2, settings, kafkaSinkKnobs{})
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

// type tracker struct {
// 	nextID     int
// 	pendingIDs map[int]struct{}
// }

// func (t *tracker) next() int {
// 	t.nextID++
// 	t.pendingIDs[t.nextID] = struct{}{}
// 	return t.nextID
// }

// func (t *tracker) remove(id int) {
// 	if _, ok := t.pendingIDs[id]; !ok {
// 		panic(errors.Errorf(`id %d not found in pendingIDs`, id))
// 	}
// 	delete(t.pendingIDs, id)
// }

// func (t *tracker) empty() bool {
// 	return len(t.pendingIDs) == 0
// }
