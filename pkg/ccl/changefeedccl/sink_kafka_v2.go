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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func newKafkaSinkClient(
	kafkaCfg *sarama.Config,
	batchCfg sinkBatchConfig,
	bootstrapAddrs string,
	topics *TopicNamer,
	knobs kafkaSinkKnobs,
) (*kafkaSinkClient, error) {
	client, err := newKafkaClient(kafkaCfg, bootstrapAddrs, knobs)
	if err != nil {
		return nil, err
	}

	return &kafkaSinkClient{client: client, knobs: knobs, topics: topics, batchCfg: batchCfg}, nil
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

type kafkaSinkClient struct {
	format    changefeedbase.FormatType
	topics    *TopicNamer
	batchCfg  sinkBatchConfig
	client    sarama.Client
	producers struct {
		mu        syncutil.Mutex
		producers []sarama.AsyncProducer
	}

	knobs kafkaSinkKnobs

	lastMetadataRefresh time.Time
}

// Close implements SinkClient.
func (k *kafkaSinkClient) Close() error {
	k.client.Close()
	return nil
}

// Flush implements SinkClient. Does not retry -- retries will be handled by ParallelIO.
func (k *kafkaSinkClient) Flush(ctx context.Context, payload SinkPayload) error {
	producer, err := k.getProducer()
	if err != nil {
		return err
	}
	defer k.returnProducer(producer)

	msgs := payload.([]*sarama.ProducerMessage)

	sent, confirmed := 0, 0
	for {
		if sent == len(msgs) {
			break
		}
		m := msgs[sent]
		select {
		case <-ctx.Done():
			return ctx.Err()
		case producer.Input() <- m:
			sent++
		case <-producer.Successes():
			// TODO: will this emit only one mesage per? or do we need to do more advanced tracking?
			// TODO: re add metrics support
			confirmed++
		case err := <-producer.Errors():
			// TODO: resize option
			return err
		}
	}

	return nil
}

// FlushResolvedPayload implements SinkClient.
func (k *kafkaSinkClient) FlushResolvedPayload(
	ctx context.Context,
	body []byte,
	forEachTopic func(func(topic string) error) error,
	retryOpts retry.Options,
) error {
	panic("unimplemented")
}

// MakeBatchBuffer implements SinkClient.
func (k *kafkaSinkClient) MakeBatchBuffer(topic string) BatchBuffer {
	return &kafkaBuffer{topic: topic, batchCfg: k.batchCfg}
}

// getProducer returns a producer from the pool, or creates a new one if the
// pool is empty. k.returnProducer must be called when done to prevent leaks.
func (k *kafkaSinkClient) getProducer() (sarama.AsyncProducer, error) {
	var ap sarama.AsyncProducer
	var err error
	k.producers.mu.Lock()
	defer k.producers.mu.Unlock()

	if len(k.producers.producers) == 0 {
		if k.knobs.OverrideAsyncProducerFromClient != nil {
			ap, err = k.knobs.OverrideAsyncProducerFromClient(k.client)
		} else {
			ap, err = sarama.NewAsyncProducerFromClient(k.client)
		}
		if err != nil {
			return nil, err
		}
		k.producers.producers = append(k.producers.producers, ap)
	}

	last := len(k.producers.producers) - 1
	ap = k.producers.producers[last]
	k.producers.producers[last] = nil
	k.producers.producers = k.producers.producers[:last]

	return ap, nil
}

func (k *kafkaSinkClient) returnProducer(ap sarama.AsyncProducer) {
	k.producers.mu.Lock()
	defer k.producers.mu.Unlock()
	k.producers.producers = append(k.producers.producers, ap)
}

var _ SinkClient = (*kafkaSinkClient)(nil)
var _ SinkPayload = (*[]sarama.ProducerMessage)(nil) // this doesnt actually assert anything fyi

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

	// TODO: how to handle knobs
	sinkClient, err := newKafkaSinkClient(kafkaCfg, batchCfg, u.Host, topicNamer, kafkaSinkKnobs{})
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

// // adapted from sarama.produceSet.buildRequest
// func buildRequest(config *sarama.Config) *sarama.ProduceRequest {
// 	req := &sarama.ProduceRequest{
// 		RequiredAcks: config.Producer.RequiredAcks,
// 		Timeout:      int32(config.Producer.Timeout / time.Millisecond),
// 	}
// 	if config.Version.IsAtLeast(sarama.V0_10_0_0) {
// 		req.Version = 2
// 	}
// 	if config.Version.IsAtLeast(sarama.V0_11_0_0) {
// 		req.Version = 3
// 		if ps.parent.IsTransactional() {
// 			req.TransactionalID = &config.Producer.Transaction.ID
// 		}
// 	}
// 	if config.Version.IsAtLeast(sarama.V1_0_0_0) {
// 		req.Version = 5
// 	}
// 	if config.Version.IsAtLeast(sarama.V2_0_0_0) {
// 		req.Version = 6
// 	}
// 	if config.Version.IsAtLeast(sarama.V2_1_0_0) {
// 		req.Version = 7
// 	}

// 	for topic, partitionSets := range ps.msgs {
// 		for partition, set := range partitionSets {
// 			if req.Version >= 3 {
// 				// If the API version we're hitting is 3 or greater, we need to calculate
// 				// offsets for each record in the batch relative to FirstOffset.
// 				// Additionally, we must set LastOffsetDelta to the value of the last offset
// 				// in the batch. Since the OffsetDelta of the first record is 0, we know that the
// 				// final record of any batch will have an offset of (# of records in batch) - 1.
// 				// (See https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
// 				//  under the RecordBatch section for details.)
// 				rb := set.recordsToSend.RecordBatch
// 				if len(rb.Records) > 0 {
// 					rb.LastOffsetDelta = int32(len(rb.Records) - 1)
// 					for i, record := range rb.Records {
// 						record.OffsetDelta = int64(i)
// 					}
// 				}

// 				// Set the batch as transactional when a transactionalID is set
// 				rb.IsTransactional = ps.parent.IsTransactional()

// 				req.AddBatch(topic, partition, rb)
// 				continue
// 			}
// 			if config.Producer.Compression == CompressionNone {
// 				req.AddSet(topic, partition, set.recordsToSend.MsgSet)
// 			} else {
// 				// When compression is enabled, the entire set for each partition is compressed
// 				// and sent as the payload of a single fake "message" with the appropriate codec
// 				// set and no key. When the server sees a message with a compression codec, it
// 				// decompresses the payload and treats the result as its message set.

// 				if config.Version.IsAtLeast(sarama.V0_10_0_0) {
// 					// If our version is 0.10 or later, assign relative offsets
// 					// to the inner messages. This lets the broker avoid
// 					// recompressing the message set.
// 					// (See https://cwiki.apache.org/confluence/display/KAFKA/KIP-31+-+Move+to+relative+offsets+in+compressed+message+sets
// 					// for details on relative offsets.)
// 					for i, msg := range set.recordsToSend.MsgSet.Messages {
// 						msg.Offset = int64(i)
// 					}
// 				}
// 				payload, err := encode(set.recordsToSend.MsgSet, ps.parent.metricsRegistry)
// 				if err != nil {
// 					Logger.Println(err) // if this happens, it's basically our fault.
// 					panic(err)
// 				}
// 				compMsg := &Message{
// 					Codec:            config.Producer.Compression,
// 					CompressionLevel: config.Producer.CompressionLevel,
// 					Key:              nil,
// 					Value:            payload,
// 					Set:              set.recordsToSend.MsgSet, // Provide the underlying message set for accurate metrics
// 				}
// 				if config.Version.IsAtLeast(sarama.V0_10_0_0) {
// 					compMsg.Version = 1
// 					compMsg.Timestamp = set.recordsToSend.MsgSet.Messages[0].Msg.Timestamp
// 				}
// 				req.AddMessage(topic, partition, compMsg)
// 			}
// 		}
// 	}

// 	return req
// }

// // adapted from sarama.produceSet.add
// func add(config *sarama.Config, pr *sarama.ProduceRequest, msgs []*sarama.ProducerMessage) error {
// 	var err error
// 	type kv struct {
// 		key, val []byte
// 		ts       time.Time
// 	}
// 	kvs := make([]kv, 0, len(msgs))
// 	for _, msg := range msgs {
// 		kv := kv{}
// 		if msg.Key != nil {
// 			if kv.key, err = msg.Key.Encode(); err != nil {
// 				return err
// 			}
// 		}

// 		if msg.Value != nil {
// 			if kv.val, err = msg.Value.Encode(); err != nil {
// 				return err
// 			}
// 		}
// 		kv.ts = msg.Timestamp
// 		if kv.ts.IsZero() {
// 			kv.ts = time.Now()
// 		}
// 		kv.ts = kv.ts.Truncate(time.Millisecond)
// 		kvs = append(kvs, kv)
// 	}

// 	var size int

// 	if config.Version.IsAtLeast(sarama.V0_11_0_0) {
// 		batch := &sarama.RecordBatch{
// 			FirstTimestamp:   kvs[0].ts,
// 			Version:          2,
// 			Codec:            config.Producer.Compression,
// 			CompressionLevel: config.Producer.CompressionLevel,
// 			ProducerID:       42, // ???
// 			ProducerEpoch:    42, // ???
// 		}
// 		if config.Producer.Idempotent {
// 			batch.FirstSequence = 42 // ???
// 		}
// 		set = &partitionSet{recordsToSend: newDefaultRecords(batch)}
// 		size = recordBatchOverhead
// 	} else {
// 		set = &partitionSet{recordsToSend: newLegacyRecords(new(sarama.MessageSet))}
// 	}

// 	if config.Version.IsAtLeast(sarama.V0_11_0_0) {
// 		if config.Producer.Idempotent && msg.sequenceNumber < set.recordsToSend.RecordBatch.FirstSequence {
// 			return errors.New("assertion failed: message out of sequence added to a batch")
// 		}
// 	}

// 	// Past this point we can't return an error, because we've already added the message to the set.
// 	set.msgs = append(set.msgs, msg)

// 	if config.Version.IsAtLeast(sarama.V0_11_0_0) {
// 		// We are being conservative here to avoid having to prep encode the record
// 		size += maximumRecordOverhead
// 		rec := &sarama.Record{
// 			Key:            key,
// 			Value:          val,
// 			TimestampDelta: timestamp.Sub(set.recordsToSend.RecordBatch.FirstTimestamp),
// 		}
// 		size += len(key) + len(val)
// 		if len(msg.Headers) > 0 {
// 			rec.Headers = make([]*RecordHeader, len(msg.Headers))
// 			for i := range msg.Headers {
// 				rec.Headers[i] = &msg.Headers[i]
// 				size += len(rec.Headers[i].Key) + len(rec.Headers[i].Value) + 2*binary.MaxVarintLen32
// 			}
// 		}
// 		set.recordsToSend.RecordBatch.addRecord(rec)
// 	} else {
// 		msgToSend := &Message{Codec: CompressionNone, Key: key, Value: val}
// 		if config.Version.IsAtLeast(sarama.V0_10_0_0) {
// 			msgToSend.Timestamp = timestamp
// 			msgToSend.Version = 1
// 		}
// 		set.recordsToSend.MsgSet.addMessage(msgToSend)
// 		size = producerMessageOverhead + len(key) + len(val)
// 	}

// 	set.bufferBytes += size
// 	ps.bufferBytes += size
// 	ps.bufferCount++

// 	return nil
// }
