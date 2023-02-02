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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// maybeLocker is a wrapper around a Locker that allows for successive Unlocks
type maybeLocker struct {
	wrapped sync.Locker
	locked  bool
}

func (l *maybeLocker) Lock() {
	l.wrapped.Lock()
	l.locked = true
}
func (l *maybeLocker) Unlock() {
	if l.locked {
		l.wrapped.Unlock()
		l.locked = false
	}
}

type kafkaLogAdapter struct {
	ctx context.Context
}

type kafkaSinkKnobs struct {
	OverrideClientInit              func(config *sarama.Config) (kafkaClient, error)
	OverrideAsyncProducerFromClient func(kafkaClient) (sarama.AsyncProducer, error)
	OverrideSyncProducerFromClient  func(kafkaClient) (sarama.SyncProducer, error)
}

var _ sarama.StdLogger = (*kafkaLogAdapter)(nil)

func (l *kafkaLogAdapter) Print(v ...interface{}) {
	log.InfofDepth(l.ctx, 1, "", v...)
}
func (l *kafkaLogAdapter) Printf(format string, v ...interface{}) {
	log.InfofDepth(l.ctx, 1, format, v...)
}
func (l *kafkaLogAdapter) Println(v ...interface{}) {
	log.InfofDepth(l.ctx, 1, "", v...)
}

func init() {
	// We'd much prefer to make one of these per sink, so we can use the real
	// context, but quite unfortunately, sarama only has a global logger hook.
	ctx := context.Background()
	ctx = logtags.AddTag(ctx, "kafka-producer", nil)
	sarama.Logger = &kafkaLogAdapter{ctx: ctx}

	// Sarama should not be rejecting messages based on some arbitrary limits.
	// This sink already manages its resource usage.  Sarama should attempt to deliver
	// messages, no matter their size.  Of course, the downstream kafka may reject
	// those messages, but this rejection should not be done locally.
	sarama.MaxRequestSize = math.MaxInt32
}

// kafkaClient is a small interface restricting the functionality in sarama.Client
type kafkaClient interface {
	// Partitions returns the sorted list of all partition IDs for the given topic.
	Partitions(topic string) ([]int32, error)
	// RefreshMetadata takes a list of topics and queries the cluster to refresh the
	// available metadata for those topics. If no topics are provided, it will refresh
	// metadata for all topics.
	RefreshMetadata(topics ...string) error
	// Config returns the sarama config used on the client
	Config() *sarama.Config
	// Close closes kafka connection.
	Close() error
}

// kafkaSink emits to Kafka asynchronously. It is not concurrency-safe; all
// calls to Emit and Flush should be from the same goroutine.
type kafkaSink struct {
	ctx            context.Context
	bootstrapAddrs string
	kafkaCfg       *sarama.Config
	client         kafkaClient
	producer       sarama.AsyncProducer
	topics         *TopicNamer

	lastMetadataRefresh time.Time

	stopWorkerCh chan struct{}
	worker       sync.WaitGroup
	scratch      bufalloc.ByteAllocator
	metrics      metricsRecorder

	knobs kafkaSinkKnobs

	stats kafkaStats

	// Only synchronized between the client goroutine and the worker goroutine.
	mu struct {
		syncutil.Mutex
		inflight int64
		flushErr error
		flushCh  chan struct{}
	}

	disableInternalRetry bool
}

func (s *kafkaSink) getConcreteType() sinkType {
	return sinkTypeKafka
}

var saramaCompressionCodecOptions = map[string]sarama.CompressionCodec{
	"NONE":   sarama.CompressionNone,
	"GZIP":   sarama.CompressionGZIP,
	"SNAPPY": sarama.CompressionSnappy,
	"LZ4":    sarama.CompressionLZ4,
	"ZSTD":   sarama.CompressionZSTD,
}

func validateCompressionCodec(s string) (sarama.CompressionCodec, error) {
	codec, ok := saramaCompressionCodecOptions[s]
	if !ok {
		return -1, errors.Newf("could not validate compression codec '%s'", s)
	}
	return codec, nil
}

type compressionCodec sarama.CompressionCodec

func (j *compressionCodec) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	codec, err := validateCompressionCodec(s)
	if err != nil {
		return err
	}
	*j = compressionCodec(codec)
	return nil
}

type saramaConfig struct {
	// These settings mirror ones in sarama config.
	// We just tag them w/ JSON annotations.
	// Flush describes settings specific to producer flushing.
	// See sarama.Config.Producer.Flush
	Flush struct {
		Bytes       int          `json:",omitempty"`
		Messages    int          `json:",omitempty"`
		Frequency   jsonDuration `json:",omitempty"`
		MaxMessages int          `json:",omitempty"`
	}

	Compression compressionCodec `json:",omitempty"`

	RequiredAcks string `json:",omitempty"`

	Version string `json:",omitempty"`
}

func (c saramaConfig) Validate() error {
	// If Flush.Bytes > 0 or Flush.Messages > 1 without
	// Flush.Frequency, sarama may wait forever to flush the
	// messages to Kafka.  We want to guard against such
	// configurations to ensure that we don't get into a situation
	// where our call to Flush() would block forever.
	if (c.Flush.Bytes > 0 || c.Flush.Messages > 1) && c.Flush.Frequency == 0 {
		return errors.New("Flush.Frequency must be > 0 when Flush.Bytes > 0 or Flush.Messages > 1")
	}
	return nil
}

func defaultSaramaConfig() *saramaConfig {
	config := &saramaConfig{}

	// When we emit messages to sarama, they're placed in a queue
	// (as does any reasonable kafka producer client). When our
	// sink's Flush is called, we have to wait for all buffered
	// and inflight requests to be sent and then
	// acknowledged. Quite unfortunately, we have no way to hint
	// to the producer that it should immediately send out
	// whatever is buffered. This configuration can have a
	// dramatic impact on how quickly this happens naturally (and
	// some configurations will block forever!).
	//
	// The default configuration of all 0 values will send
	// messages as quickly as possible.
	config.Flush.Messages = 0
	config.Flush.Frequency = jsonDuration(0)
	config.Flush.Bytes = 0

	// The default compression protocol is sarama.CompressionNone,
	// which is 0.
	config.Compression = 0

	// This works around what seems to be a bug in sarama where it isn't
	// computing the right value to compare against `Producer.MaxMessageBytes`
	// and the server sends it back with a "Message was too large, server
	// rejected it to avoid allocation" error. The other flush tunings are
	// hints, but this one is a hard limit, so it's useful here as a workaround.
	//
	// This workaround should probably be something like setting
	// `Producer.MaxMessageBytes` to 90% of it's value for some headroom, but
	// this workaround is the one that's been running in roachtests and I'd want
	// to test this one more before changing it.
	config.Flush.MaxMessages = 1000

	return config
}

// Dial implements the Sink interface.
func (s *kafkaSink) Dial() error {
	client, err := s.newClient(s.kafkaCfg)
	if err != nil {
		return err
	}

	producer, err := s.newAsyncProducer(client)
	if err != nil {
		return err
	}

	s.client = client
	s.producer = producer

	// Start the worker
	s.stopWorkerCh = make(chan struct{})
	s.worker.Add(1)
	go s.workerLoop()
	return nil
}

func (s *kafkaSink) newClient(config *sarama.Config) (kafkaClient, error) {
	// Initialize client and producer
	if s.knobs.OverrideClientInit != nil {
		client, err := s.knobs.OverrideClientInit(config)
		return client, err
	}

	client, err := sarama.NewClient(strings.Split(s.bootstrapAddrs, `,`), config)
	if err != nil {
		return nil, pgerror.Wrapf(err, pgcode.CannotConnectNow,
			`connecting to kafka: %s`, s.bootstrapAddrs)
	}
	return client, err
}

func (s *kafkaSink) newAsyncProducer(client kafkaClient) (sarama.AsyncProducer, error) {
	var producer sarama.AsyncProducer
	var err error
	if s.knobs.OverrideAsyncProducerFromClient != nil {
		producer, err = s.knobs.OverrideAsyncProducerFromClient(client)
	} else {
		producer, err = sarama.NewAsyncProducerFromClient(client.(sarama.Client))
	}
	if err != nil {
		return nil, pgerror.Wrapf(err, pgcode.CannotConnectNow,
			`connecting to kafka: %s`, s.bootstrapAddrs)
	}
	return producer, nil
}

func (s *kafkaSink) newSyncProducer(client kafkaClient) (sarama.SyncProducer, error) {
	var producer sarama.SyncProducer
	var err error
	if s.knobs.OverrideSyncProducerFromClient != nil {
		producer, err = s.knobs.OverrideSyncProducerFromClient(client)
	} else {
		producer, err = sarama.NewSyncProducerFromClient(client.(sarama.Client))
	}
	if err != nil {
		return nil, pgerror.Wrapf(err, pgcode.CannotConnectNow,
			`connecting to kafka: %s`, s.bootstrapAddrs)
	}
	return producer, nil
}

// Close implements the Sink interface.
func (s *kafkaSink) Close() error {
	if s.stopWorkerCh != nil {
		close(s.stopWorkerCh)
		s.worker.Wait()
	}

	if s.producer != nil {
		// Ignore errors related to outstanding messages since we're either shutting
		// down or beginning to retry regardless
		_ = s.producer.Close()
	}
	// s.client is only nil in tests.
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

type messageMetadata struct {
	alloc         kvevent.Alloc
	updateMetrics recordOneMessageCallback
	mvcc          hlc.Timestamp
}

// EmitRow implements the Sink interface.
func (s *kafkaSink) EmitRow(
	ctx context.Context,
	topicDescr TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	topic, err := s.topics.Name(topicDescr)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic:    topic,
		Key:      sarama.ByteEncoder(key),
		Value:    sarama.ByteEncoder(value),
		Metadata: messageMetadata{alloc: alloc, mvcc: mvcc, updateMetrics: s.metrics.recordOneMessage()},
	}
	s.stats.startMessage(int64(msg.Key.Length() + msg.Value.Length()))
	return s.emitMessage(ctx, msg)
}

// EmitResolvedTimestamp implements the Sink interface.
func (s *kafkaSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	defer s.metrics.recordResolvedCallback()()

	// Periodically ping sarama to refresh its metadata. This means talking to
	// zookeeper, so it shouldn't be done too often, but beyond that this
	// constant was picked pretty arbitrarily.
	//
	// TODO(dan): Add a test for this. We can't right now (2018-11-13) because
	// we'd need to bump sarama, but that's a bad idea while we're still
	// actively working on stability. At the same time, revisit this tuning.
	const metadataRefreshMinDuration = time.Minute
	if timeutil.Since(s.lastMetadataRefresh) > metadataRefreshMinDuration {
		if err := s.client.RefreshMetadata(s.topics.DisplayNamesSlice()...); err != nil {
			return err
		}
		s.lastMetadataRefresh = timeutil.Now()
	}

	return s.topics.Each(func(topic string) error {
		payload, err := encoder.EncodeResolvedTimestamp(ctx, topic, resolved)
		if err != nil {
			return err
		}
		s.scratch, payload = s.scratch.Copy(payload, 0 /* extraCap */)

		// sarama caches this, which is why we have to periodically refresh the
		// metadata above. Staleness here does not impact correctness. Some new
		// partitions will miss this resolved timestamp, but they'll eventually
		// be picked up and get later ones.
		partitions, err := s.client.Partitions(topic)
		if err != nil {
			return err
		}
		for _, partition := range partitions {
			msg := &sarama.ProducerMessage{
				Topic:     topic,
				Partition: partition,
				Key:       nil,
				Value:     sarama.ByteEncoder(payload),
			}
			if err := s.emitMessage(ctx, msg); err != nil {
				return err
			}
		}
		return nil
	})
}

// Flush implements the Sink interface.
func (s *kafkaSink) Flush(ctx context.Context) error {
	defer s.metrics.recordFlushRequestCallback()()

	flushCh := make(chan struct{}, 1)

	s.mu.Lock()
	inflight := s.mu.inflight
	flushErr := s.mu.flushErr
	s.mu.flushErr = nil
	immediateFlush := inflight == 0 || flushErr != nil
	if !immediateFlush {
		s.mu.flushCh = flushCh
	}
	s.mu.Unlock()

	if immediateFlush {
		return flushErr
	}

	if log.V(1) {
		log.Infof(ctx, "flush waiting for %d inflight messages", inflight)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-flushCh:
		s.mu.Lock()
		flushErr := s.mu.flushErr
		s.mu.flushErr = nil
		s.mu.Unlock()
		return flushErr
	}
}

func (s *kafkaSink) startInflightMessage(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.flushErr != nil {
		return s.mu.flushErr
	}

	s.mu.inflight++
	if log.V(2) {
		log.Infof(ctx, "emitting %d inflight records to kafka", s.mu.inflight)
	}
	return nil
}

func (s *kafkaSink) emitMessage(ctx context.Context, msg *sarama.ProducerMessage) error {
	if err := s.startInflightMessage(ctx); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.producer.Input() <- msg:
	}

	return nil
}

// isInternallyRetryable returns true if the sink should attempt to re-emit the
// messages with a non-batching config first rather than surfacing the error to
// the overarching feed.
func (s *kafkaSink) isInternalRetryable(err error) bool {
	if s.disableInternalRetry || err == nil { // Avoid allocating a KError if we don't need to
		return false
	}
	var kError sarama.KError
	return errors.As(err, &kError) && kError == sarama.ErrMessageSizeTooLarge
}

func (s *kafkaSink) workerLoop() {
	defer s.worker.Done()

	// Locking/Unlocking of s.mu during the retry process must be done
	// through a maybeLocker with a deferred Unlock to allow for mu to
	// always be unlocked upon worker completion even if it is mid-internal-retry
	muLocker := &maybeLocker{&s.mu, false}
	defer muLocker.Unlock()

	// For an error like ErrMessageSizeTooLarge, we freeze incoming messages and
	// retry internally with a more finely grained client until one is found that
	// can successfully send the errored messages.
	var retryBuf []*sarama.ProducerMessage
	var retryErr error

	startInternalRetry := func(err error) {
		s.mu.AssertHeld()
		log.Infof(
			s.ctx,
			"kafka sink with flush config (%+v) beginning internal retry with %d inflight messages due to error: %s",
			s.kafkaCfg.Producer.Flush,
			s.mu.inflight,
			err.Error(),
		)
		retryErr = err
		// Note that even if we reserve mu.inflight space, it may not be filled as
		// some inflight messages won't error (ex: they're for a different topic
		// than the one with the original message that errored).
		retryBuf = make([]*sarama.ProducerMessage, 0, s.mu.inflight)
	}
	endInternalRetry := func() {
		retryErr = nil
		retryBuf = nil
	}
	isRetrying := func() bool {
		return retryErr != nil
	}

	for {
		var ackMsg *sarama.ProducerMessage
		var ackError error

		select {
		case <-s.ctx.Done():
			return
		case <-s.stopWorkerCh:
			return
		case m := <-s.producer.Successes():
			ackMsg = m
		case err := <-s.producer.Errors():
			ackMsg, ackError = err.Msg, err.Err
			if ackError != nil {
				// Msg should never be nil but we're being defensive around a vendor library.
				// Msg.Key is nil for sentinel errors (e.g. producer shutting down)
				// and errors sending dummy messages used to prefetch metadata.
				if err.Msg != nil && err.Msg.Key != nil && err.Msg.Value != nil {
					ackError = errors.Wrapf(ackError,
						"while sending message with key=%s, size=%d, stats=%s",
						err.Msg.Key, err.Msg.Key.Length()+err.Msg.Value.Length(), s.stats.String())
				}
			}
		}

		// If we're in a retry we already had the lock.
		if !isRetrying() {
			muLocker.Lock()
		}
		s.mu.inflight--

		if !isRetrying() && s.mu.flushErr == nil && s.isInternalRetryable(ackError) {
			startInternalRetry(ackError)
		}

		// If we're retrying and its a valid but errored message, buffer it to be retried.
		isValidMessage := ackMsg != nil && ackMsg.Key != nil && ackMsg.Value != nil
		if isRetrying() && isValidMessage {
			retryBuf = append(retryBuf, ackMsg)
		} else {
			s.finishProducerMessage(ackMsg, ackError)
		}

		// Once inflight messages to retry are done buffering, find a new client
		// that successfully resends and continue on with it.
		if isRetrying() && s.mu.inflight == 0 {
			if err := s.handleBufferedRetries(retryBuf, retryErr); err != nil {
				s.mu.flushErr = err
			}
			endInternalRetry()
		}

		// If we're in a retry inflight can be 0 but messages in retryBuf are yet to
		// be resent.
		if !isRetrying() && s.mu.inflight == 0 && s.mu.flushCh != nil {
			s.mu.flushCh <- struct{}{}
			s.mu.flushCh = nil
		}

		// If we're in a retry we keep hold of the lock to stop all other operations
		// until the retry has completed.
		if !isRetrying() {
			muLocker.Unlock()
		}
	}
}

func (s *kafkaSink) finishProducerMessage(ackMsg *sarama.ProducerMessage, ackError error) {
	s.mu.AssertHeld()
	if m, ok := ackMsg.Metadata.(messageMetadata); ok {
		if ackError == nil {
			sz := ackMsg.Key.Length() + ackMsg.Value.Length()
			s.stats.finishMessage(int64(sz))
			m.updateMetrics(m.mvcc, sz, sinkDoesNotCompress)
		}
		m.alloc.Release(s.ctx)
	}
	if s.mu.flushErr == nil && ackError != nil {
		s.mu.flushErr = ackError
	}
}

func (s *kafkaSink) handleBufferedRetries(msgs []*sarama.ProducerMessage, retryErr error) error {
	lastSendErr := retryErr
	activeConfig := s.kafkaCfg
	log.Infof(s.ctx, "kafka sink handling %d buffered messages for internal retry", len(msgs))

	// Ensure memory for messages are always cleaned up
	defer func() {
		for _, msg := range msgs {
			s.finishProducerMessage(msg, lastSendErr)
		}
	}()

	for {
		select {
		case <-s.stopWorkerCh:
			log.Infof(s.ctx, "kafka sink ending retries due to worker close")
			return lastSendErr
		default:
		}

		newConfig, wasReduced := reduceBatchingConfig(activeConfig)

		// Surface the error if its not retryable or we weren't able to reduce the
		// batching config any further
		if !s.isInternalRetryable(lastSendErr) {
			log.Infof(s.ctx, "kafka sink abandoning internal retry due to error: %s", lastSendErr.Error())
			return lastSendErr
		} else if !wasReduced {
			log.Infof(s.ctx, "kafka sink abandoning internal retry due to being unable to reduce batching size")
			return lastSendErr
		}

		log.Infof(s.ctx, "kafka sink retrying %d messages with reduced flush config: (%+v)", len(msgs), newConfig.Producer.Flush)
		activeConfig = newConfig

		newClient, err := s.newClient(newConfig)
		if err != nil {
			return err
		}
		newProducer, err := s.newSyncProducer(newClient)
		if err != nil {
			return err
		}

		s.metrics.recordInternalRetry(int64(len(msgs)), true /* reducedBatchSize */)

		// SendMessages will attempt to send all messages into an AsyncProducer with
		// the client's config and then block until the results come in.
		lastSendErr = newProducer.SendMessages(msgs)
		if lastSendErr != nil {
			// nolint:errcmp
			if sendErrs, ok := lastSendErr.(sarama.ProducerErrors); ok && len(sendErrs) > 0 {
				// Just check the first error since all these messages being retried
				// were likely from a single partition and therefore would've been
				// marked with the same error.
				lastSendErr = sendErrs[0].Err
			}
		}

		if err := newProducer.Close(); err != nil {
			log.Errorf(s.ctx, "closing of previous sarama producer for retry failed with: %s", err.Error())
		}
		if err := newClient.Close(); err != nil {
			log.Errorf(s.ctx, "closing of previous sarama client for retry failed with: %s", err.Error())
		}

		if lastSendErr == nil {
			log.Infof(s.ctx, "kafka sink internal retry succeeded")
			return nil
		}
	}
}

func reduceBatchingConfig(c *sarama.Config) (*sarama.Config, bool) {
	flooredHalve := func(num int) int {
		if num < 2 {
			return num
		}
		return num / 2
	}

	newConfig := *c

	newConfig.Producer.Flush.Messages = flooredHalve(c.Producer.Flush.Messages)
	// MaxMessages of 0 means unlimited, so treat "halving" it as reducing it to
	// 250 (an arbitrary number)
	if c.Producer.Flush.MaxMessages == 0 {
		newConfig.Producer.Flush.MaxMessages = 250
	} else {
		newConfig.Producer.Flush.MaxMessages = flooredHalve(c.Producer.Flush.MaxMessages)
	}

	wasReduced := newConfig.Producer.Flush != c.Producer.Flush
	return &newConfig, wasReduced
}

// Topics gives the names of all topics that have been initialized
// and will receive resolved timestamps.
func (s *kafkaSink) Topics() []string {
	return s.topics.DisplayNamesSlice()
}

type changefeedPartitioner struct {
	hash sarama.Partitioner
}

var _ sarama.Partitioner = &changefeedPartitioner{}
var _ sarama.PartitionerConstructor = newChangefeedPartitioner

func newChangefeedPartitioner(topic string) sarama.Partitioner {
	return &changefeedPartitioner{
		hash: sarama.NewHashPartitioner(topic),
	}
}

func (p *changefeedPartitioner) RequiresConsistency() bool { return true }
func (p *changefeedPartitioner) Partition(
	message *sarama.ProducerMessage, numPartitions int32,
) (int32, error) {
	if message.Key == nil {
		return message.Partition, nil
	}
	return p.hash.Partition(message, numPartitions)
}

type jsonDuration time.Duration

func (j *jsonDuration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*j = jsonDuration(dur)
	return nil
}

// Apply configures provided kafka configuration struct based on this config.
func (c *saramaConfig) Apply(kafka *sarama.Config) error {
	// Sarama limits the size of each message to be MaxMessageSize (1MB) bytes.
	// This is silly;  This sink already manages its memory, and therefore, if we
	// had enough resources to ingest and process this message, then sarama shouldn't
	// get in a way.  Set this limit to be just a bit under maximum request size.
	kafka.Producer.MaxMessageBytes = int(sarama.MaxRequestSize - 1)

	kafka.Producer.Flush.Bytes = c.Flush.Bytes
	kafka.Producer.Flush.Messages = c.Flush.Messages
	kafka.Producer.Flush.Frequency = time.Duration(c.Flush.Frequency)
	kafka.Producer.Flush.MaxMessages = c.Flush.MaxMessages
	if c.Version != "" {
		parsedVersion, err := sarama.ParseKafkaVersion(c.Version)
		if err != nil {
			return err
		}
		kafka.Version = parsedVersion
	}
	if c.RequiredAcks != "" {
		parsedAcks, err := parseRequiredAcks(c.RequiredAcks)
		if err != nil {
			return err
		}
		kafka.Producer.RequiredAcks = parsedAcks
	}
	kafka.Producer.Compression = sarama.CompressionCodec(c.Compression)
	return nil
}

func parseRequiredAcks(a string) (sarama.RequiredAcks, error) {
	switch a {
	case "0", "NONE":
		return sarama.NoResponse, nil
	case "1", "ONE":
		return sarama.WaitForLocal, nil
	case "-1", "ALL":
		return sarama.WaitForAll, nil
	default:
		return sarama.WaitForLocal,
			fmt.Errorf(`invalid acks value "%s", must be "NONE"/"0", "ONE"/"1", or "ALL"/"-1"`, a)
	}
}

func getSaramaConfig(
	jsonStr changefeedbase.SinkSpecificJSONConfig,
) (config *saramaConfig, err error) {
	config = defaultSaramaConfig()
	if jsonStr != `` {
		err = json.Unmarshal([]byte(jsonStr), config)
	}
	return
}

func buildKafkaConfig(
	u sinkURL, jsonStr changefeedbase.SinkSpecificJSONConfig,
) (*sarama.Config, error) {
	dialConfig := struct {
		tlsEnabled    bool
		tlsSkipVerify bool
		caCert        []byte
		clientCert    []byte
		clientKey     []byte
		saslEnabled   bool
		saslHandshake bool
		saslUser      string
		saslPassword  string
		saslMechanism string
	}{}

	if _, err := u.consumeBool(changefeedbase.SinkParamTLSEnabled, &dialConfig.tlsEnabled); err != nil {
		return nil, err
	}
	if _, err := u.consumeBool(changefeedbase.SinkParamSkipTLSVerify, &dialConfig.tlsSkipVerify); err != nil {
		return nil, err
	}
	if err := u.decodeBase64(changefeedbase.SinkParamCACert, &dialConfig.caCert); err != nil {
		return nil, err
	}
	if err := u.decodeBase64(changefeedbase.SinkParamClientCert, &dialConfig.clientCert); err != nil {
		return nil, err
	}
	if err := u.decodeBase64(changefeedbase.SinkParamClientKey, &dialConfig.clientKey); err != nil {
		return nil, err
	}

	if _, err := u.consumeBool(changefeedbase.SinkParamSASLEnabled, &dialConfig.saslEnabled); err != nil {
		return nil, err
	}

	if wasSet, err := u.consumeBool(changefeedbase.SinkParamSASLHandshake, &dialConfig.saslHandshake); !wasSet && err == nil {
		dialConfig.saslHandshake = true
	} else {
		if err != nil {
			return nil, err
		}
		if !dialConfig.saslEnabled {
			return nil, errors.Errorf(`%s must be enabled to configure SASL handshake behavior`, changefeedbase.SinkParamSASLEnabled)
		}
	}

	dialConfig.saslMechanism = u.consumeParam(changefeedbase.SinkParamSASLMechanism)
	if dialConfig.saslMechanism != `` && !dialConfig.saslEnabled {
		return nil, errors.Errorf(`%s must be enabled to configure SASL mechanism`, changefeedbase.SinkParamSASLEnabled)
	}
	if dialConfig.saslMechanism == `` {
		dialConfig.saslMechanism = sarama.SASLTypePlaintext
	}
	switch dialConfig.saslMechanism {
	case sarama.SASLTypeSCRAMSHA256, sarama.SASLTypeSCRAMSHA512, sarama.SASLTypePlaintext:
	default:
		return nil, errors.Errorf(`param %s must be one of %s, %s, or %s`,
			changefeedbase.SinkParamSASLMechanism,
			sarama.SASLTypeSCRAMSHA256, sarama.SASLTypeSCRAMSHA512, sarama.SASLTypePlaintext)
	}

	dialConfig.saslUser = u.consumeParam(changefeedbase.SinkParamSASLUser)
	dialConfig.saslPassword = u.consumeParam(changefeedbase.SinkParamSASLPassword)
	if dialConfig.saslEnabled {
		if dialConfig.saslUser == `` {
			return nil, errors.Errorf(`%s must be provided when SASL is enabled`, changefeedbase.SinkParamSASLUser)
		}
		if dialConfig.saslPassword == `` {
			return nil, errors.Errorf(`%s must be provided when SASL is enabled`, changefeedbase.SinkParamSASLPassword)
		}
	} else {
		if dialConfig.saslUser != `` {
			return nil, errors.Errorf(`%s must be enabled if a SASL user is provided`, changefeedbase.SinkParamSASLEnabled)
		}
		if dialConfig.saslPassword != `` {
			return nil, errors.Errorf(`%s must be enabled if a SASL password is provided`, changefeedbase.SinkParamSASLEnabled)
		}
	}

	config := sarama.NewConfig()
	config.ClientID = `CockroachDB`
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = newChangefeedPartitioner

	if dialConfig.tlsEnabled {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: dialConfig.tlsSkipVerify,
		}

		if dialConfig.caCert != nil {
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(dialConfig.caCert)
			config.Net.TLS.Config.RootCAs = caCertPool
		}

		if dialConfig.clientCert != nil && dialConfig.clientKey == nil {
			return nil, errors.Errorf(`%s requires %s to be set`, changefeedbase.SinkParamClientCert, changefeedbase.SinkParamClientKey)
		} else if dialConfig.clientKey != nil && dialConfig.clientCert == nil {
			return nil, errors.Errorf(`%s requires %s to be set`, changefeedbase.SinkParamClientKey, changefeedbase.SinkParamClientCert)
		}

		if dialConfig.clientCert != nil && dialConfig.clientKey != nil {
			cert, err := tls.X509KeyPair(dialConfig.clientCert, dialConfig.clientKey)
			if err != nil {
				return nil, errors.Wrap(err, `invalid client certificate data provided`)
			}
			config.Net.TLS.Config.Certificates = []tls.Certificate{cert}
		}
	} else {
		if dialConfig.caCert != nil {
			return nil, errors.Errorf(`%s requires %s=true`, changefeedbase.SinkParamCACert, changefeedbase.SinkParamTLSEnabled)
		}
		if dialConfig.clientCert != nil {
			return nil, errors.Errorf(`%s requires %s=true`, changefeedbase.SinkParamClientCert, changefeedbase.SinkParamTLSEnabled)
		}
	}

	if dialConfig.saslEnabled {
		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = dialConfig.saslHandshake
		config.Net.SASL.User = dialConfig.saslUser
		config.Net.SASL.Password = dialConfig.saslPassword
		config.Net.SASL.Mechanism = sarama.SASLMechanism(dialConfig.saslMechanism)
		switch config.Net.SASL.Mechanism {
		case sarama.SASLTypeSCRAMSHA512:
			config.Net.SASL.SCRAMClientGeneratorFunc = sha512ClientGenerator
		case sarama.SASLTypeSCRAMSHA256:
			config.Net.SASL.SCRAMClientGeneratorFunc = sha256ClientGenerator
		}
	}

	// Apply statement level overrides.
	saramaCfg, err := getSaramaConfig(jsonStr)
	if err != nil {
		return nil, errors.Wrapf(err,
			"failed to parse sarama config; check %s option", changefeedbase.OptKafkaSinkConfig)
	}

	if err := saramaCfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid sarama configuration")
	}

	if err := saramaCfg.Apply(config); err != nil {
		return nil, errors.Wrap(err, "failed to apply kafka client configuration")
	}
	return config, nil
}

func makeKafkaSink(
	ctx context.Context,
	u sinkURL,
	targets changefeedbase.Targets,
	jsonStr changefeedbase.SinkSpecificJSONConfig,
	settings *cluster.Settings,
	mb metricsRecorderBuilder,
) (Sink, error) {
	kafkaTopicPrefix := u.consumeParam(changefeedbase.SinkParamTopicPrefix)
	kafkaTopicName := u.consumeParam(changefeedbase.SinkParamTopicName)
	if schemaTopic := u.consumeParam(changefeedbase.SinkParamSchemaTopic); schemaTopic != `` {
		return nil, errors.Errorf(`%s is not yet supported`, changefeedbase.SinkParamSchemaTopic)
	}

	config, err := buildKafkaConfig(u, jsonStr)
	if err != nil {
		return nil, err
	}

	topics, err := MakeTopicNamer(
		targets,
		WithPrefix(kafkaTopicPrefix), WithSingleName(kafkaTopicName), WithSanitizeFn(SQLNameToKafkaName))

	if err != nil {
		return nil, err
	}

	internalRetryEnabled := settings != nil && changefeedbase.BatchReductionRetryEnabled.Get(&settings.SV)

	sink := &kafkaSink{
		ctx:                  ctx,
		kafkaCfg:             config,
		bootstrapAddrs:       u.Host,
		metrics:              mb(requiresResourceAccounting),
		topics:               topics,
		disableInternalRetry: !internalRetryEnabled,
	}

	if unknownParams := u.remainingQueryParams(); len(unknownParams) > 0 {
		return nil, errors.Errorf(
			`unknown kafka sink query parameters: %s`, strings.Join(unknownParams, ", "))
	}

	return sink, nil
}

type kafkaStats struct {
	outstandingBytes    int64
	outstandingMessages int64
	largestMessageSize  int64
}

func (s *kafkaStats) startMessage(sz int64) {
	atomic.AddInt64(&s.outstandingBytes, sz)
	atomic.AddInt64(&s.outstandingMessages, 1)
	if atomic.LoadInt64(&s.largestMessageSize) < sz {
		atomic.AddInt64(&s.largestMessageSize, sz)
	}
}

func (s *kafkaStats) finishMessage(sz int64) {
	atomic.AddInt64(&s.outstandingBytes, -sz)
	atomic.AddInt64(&s.outstandingMessages, -1)
}

func (s *kafkaStats) String() string {
	return fmt.Sprintf("m=%d/b=%d/largest=%d",
		atomic.LoadInt64(&s.outstandingMessages),
		atomic.LoadInt64(&s.outstandingBytes),
		atomic.LoadInt64(&s.largestMessageSize),
	)
}
