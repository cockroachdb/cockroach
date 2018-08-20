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
	"math"
	"net/url"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

// Sink is an abstraction for anything that a changefeed may emit into.
type Sink interface {
	// EmitRow enqueues a row message for asynchronous delivery on the sink. An
	// error may be returned if a previously enqueued message has failed.
	EmitRow(ctx context.Context, topic string, key, value []byte) error
	// EmitResolvedTimestamp enqueues a resolved timestamp message for
	// asynchronous delivery on every partition of every topic that has been
	// seen by EmitRow. The list of partitions used may be stale. An error may
	// be returned if a previously enqueued message has failed.
	EmitResolvedTimestamp(ctx context.Context, payload []byte) error
	// Flush blocks until every message enqueued by EmitRow and
	// EmitResolvedTimestamp has been acknowledged by the sink. If an error is
	// returned, no guarantees are given about which messages have been
	// delivered or not delivered.
	Flush(ctx context.Context) error
	// Close does not guarantee delivery of outstanding messages.
	Close() error
}

func getSink(sinkURI string, targets map[sqlbase.ID]string) (Sink, error) {
	u, err := url.Parse(sinkURI)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case sinkSchemeBuffer:
		return &bufferSink{}, nil
	case sinkSchemeKafka:
		kafkaTopicPrefix := u.Query().Get(sinkParamTopicPrefix)
		return getKafkaSink(kafkaTopicPrefix, u.Host, targets)
	default:
		return nil, errors.Errorf(`unsupported sink: %s`, u.Scheme)
	}
}

// kafkaSink emits to Kafka asynchronously. It is not concurrency-safe; all
// calls to Emit and Flush should be from the same goroutine.
type kafkaSink struct {
	// TODO(dan): This uses the shopify kafka producer library because the
	// official confluent one depends on librdkafka and it didn't seem worth it
	// to add a new c dep for the prototype. Revisit before 2.1 and check
	// stability, performance, etc.
	kafkaTopicPrefix string
	client           sarama.Client
	producer         sarama.AsyncProducer
	topics           map[string]struct{}

	stopWorkerCh chan struct{}
	worker       sync.WaitGroup

	// Only synchronized between the client goroutine and the worker goroutine.
	mu struct {
		syncutil.Mutex
		inflight int64
		flushErr error
		flushCh  chan struct{}
	}
}

func getKafkaSink(
	kafkaTopicPrefix string, bootstrapServers string, targets map[sqlbase.ID]string,
) (Sink, error) {
	sink := &kafkaSink{
		kafkaTopicPrefix: kafkaTopicPrefix,
	}
	sink.topics = make(map[string]struct{})
	for _, tableName := range targets {
		sink.topics[kafkaTopicPrefix+tableName] = struct{}{}
	}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = newChangefeedPartitioner

	// When we emit messages to sarama, they're placed in a queue (as does any
	// reasonable kafka producer client). When our sink's Flush is called, we
	// have to wait for all buffered and inflight requests to be sent and then
	// acknowledged. Quite unfortunately, we have no way to hint to the producer
	// that it should immediately send out whatever is buffered. This
	// configuration can have a dramatic impact on how quickly this happens
	// naturally (and some configurations will block forever!).
	//
	// We can configure the producer to send out its batches based on number of
	// messages and/or total buffered message size and/or time. If none of them
	// are set, it uses some defaults, but if any of the three are set, it does
	// no defaulting. Which means that if `Flush.Messages` is set to 10 and
	// nothing else is set, then 9/10 times `Flush` will block forever. We can
	// work around this by also setting `Flush.Frequency` but a cleaner way is
	// to set `Flush.Messages` to 1. In the steady state, this sends a request
	// with some messages, buffers any messages that come in while it is in
	// flight, then sends those out.
	config.Producer.Flush.Messages = 1

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
	config.Producer.Flush.MaxMessages = 1000

	var err error
	sink.client, err = sarama.NewClient(strings.Split(bootstrapServers, `,`), config)
	if err != nil {
		return nil, errors.Wrapf(err, `connecting to kafka: %s`, bootstrapServers)
	}
	sink.producer, err = sarama.NewAsyncProducerFromClient(sink.client)
	if err != nil {
		return nil, errors.Wrapf(err, `connecting to kafka: %s`, bootstrapServers)
	}

	sink.start()
	return sink, nil
}

func (s *kafkaSink) start() {
	s.stopWorkerCh = make(chan struct{})
	s.worker.Add(1)
	go s.workerLoop()
}

// Close implements the Sink interface.
func (s *kafkaSink) Close() error {
	close(s.stopWorkerCh)
	s.worker.Wait()

	// If we're shutting down, we don't care what happens to the outstanding
	// messages, so ignore this error.
	_ = s.producer.Close()
	// s.client is only nil in tests.
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

// EmitRow implements the Sink interface.
func (s *kafkaSink) EmitRow(ctx context.Context, tableName string, key, value []byte) error {
	topic := s.kafkaTopicPrefix + tableName
	if _, ok := s.topics[topic]; !ok {
		return errors.Errorf(`cannot emit to undeclared topic: %s`, topic)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
	return s.emitMessage(ctx, msg)
}

// EmitResolvedTimestamp implements the Sink interface.
func (s *kafkaSink) EmitResolvedTimestamp(ctx context.Context, payload []byte) error {
	// Staleness here does not impact correctness. Some new partitions will miss
	// this resolved timestamp, but they'll eventually be picked up and get
	// later ones.
	for topic := range s.topics {
		// TODO(dan): Figure out how expensive this is to call. Maybe we need to
		// cache it and rate limit?
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
	}
	return nil
}

// Flush implements the Sink interface.
func (s *kafkaSink) Flush(ctx context.Context) error {
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

func (s *kafkaSink) emitMessage(ctx context.Context, msg *sarama.ProducerMessage) error {
	s.mu.Lock()
	s.mu.inflight++
	inflight := s.mu.inflight
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.producer.Input() <- msg:
	}

	if log.V(2) {
		log.Infof(ctx, "emitted %d inflight records to kafka", inflight)
	}
	return nil
}

func (s *kafkaSink) workerLoop() {
	defer s.worker.Done()

	for {
		select {
		case <-s.stopWorkerCh:
			return
		case <-s.producer.Successes():
		case err := <-s.producer.Errors():
			s.mu.Lock()
			if s.mu.flushErr == nil {
				s.mu.flushErr = err
			}
			s.mu.Unlock()
		}

		s.mu.Lock()
		s.mu.inflight--
		if s.mu.inflight == 0 && s.mu.flushCh != nil {
			s.mu.flushCh <- struct{}{}
			s.mu.flushCh = nil
		}
		s.mu.Unlock()
	}
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

// encDatumRowBuffer is a FIFO of `EncDatumRow`s.
//
// TODO(dan): There's some potential allocation savings here by reusing the same
// backing array.
type encDatumRowBuffer []sqlbase.EncDatumRow

func (b *encDatumRowBuffer) IsEmpty() bool {
	return b == nil || len(*b) == 0
}
func (b *encDatumRowBuffer) Push(r sqlbase.EncDatumRow) {
	*b = append(*b, r)
}
func (b *encDatumRowBuffer) Pop() sqlbase.EncDatumRow {
	ret := (*b)[0]
	*b = (*b)[1:]
	return ret
}

type bufferSink struct {
	buf    encDatumRowBuffer
	alloc  sqlbase.DatumAlloc
	closed bool
}

// EmitRow implements the Sink interface.
func (s *bufferSink) EmitRow(_ context.Context, topic string, key, value []byte) error {
	if s.closed {
		return errors.New(`cannot EmitRow on a closed sink`)
	}
	s.buf.Push(sqlbase.EncDatumRow{
		{Datum: tree.DNull},                              // resolved span
		{Datum: s.alloc.NewDString(tree.DString(topic))}, // topic
		{Datum: s.alloc.NewDBytes(tree.DBytes(key))},     // key
		{Datum: s.alloc.NewDBytes(tree.DBytes(value))},   //value
	})
	return nil
}

// EmitResolvedTimestamp implements the Sink interface.
func (s *bufferSink) EmitResolvedTimestamp(_ context.Context, payload []byte) error {
	if s.closed {
		return errors.New(`cannot EmitRow on a closed sink`)
	}
	s.buf.Push(sqlbase.EncDatumRow{
		{Datum: tree.DNull},                              // resolved span
		{Datum: tree.DNull},                              // topic
		{Datum: tree.DNull},                              // key
		{Datum: s.alloc.NewDBytes(tree.DBytes(payload))}, // value
	})
	return nil
}

// Flush implements the Sink interface.
func (s *bufferSink) Flush(_ context.Context) error {
	return nil
}

// Close implements the Sink interface.
func (s *bufferSink) Close() error {
	s.closed = true
	return nil
}

type metricsSink struct {
	metrics *Metrics
	wrapped Sink
}

func makeMetricsSink(metrics *Metrics, s Sink) *metricsSink {
	m := &metricsSink{
		metrics: metrics,
		wrapped: s,
	}
	return m
}

func (s *metricsSink) EmitRow(ctx context.Context, topic string, key, value []byte) error {
	start := timeutil.Now()
	err := s.wrapped.EmitRow(ctx, topic, key, value)
	if err == nil {
		s.metrics.EmittedMessages.Inc(1)
		s.metrics.EmittedBytes.Inc(int64(len(key) + len(value)))
		s.metrics.EmitNanos.Inc(timeutil.Since(start).Nanoseconds())
	}
	return err
}

func (s *metricsSink) EmitResolvedTimestamp(ctx context.Context, payload []byte) error {
	start := timeutil.Now()
	err := s.wrapped.EmitResolvedTimestamp(ctx, payload)
	if err == nil {
		s.metrics.EmittedMessages.Inc(1)
		s.metrics.EmittedBytes.Inc(int64(len(payload)))
		s.metrics.EmitNanos.Inc(timeutil.Since(start).Nanoseconds())
	}
	return err
}

func (s *metricsSink) Flush(ctx context.Context) error {
	start := timeutil.Now()
	err := s.wrapped.Flush(ctx)
	if err == nil {
		s.metrics.Flushes.Inc(1)
		s.metrics.FlushNanos.Inc(timeutil.Since(start).Nanoseconds())
	}
	return err
}

func (s *metricsSink) Close() error {
	return s.wrapped.Close()
}

var (
	metaChangefeedEmittedMessages = metric.Metadata{
		Name:        "changefeed.emitted_messages",
		Help:        "Messages emitted by all feeds",
		Measurement: "Messages",
		Unit:        metric.Unit_COUNT,
	}
	metaChangefeedEmittedBytes = metric.Metadata{
		Name:        "changefeed.emitted_bytes",
		Help:        "Bytes emitted by all feeds",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaChangefeedEmitNanos = metric.Metadata{
		Name:        "changefeed.emit_nanos",
		Help:        "Total time spent emitting all feeds",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}
	// This is more naturally a histogram but that creates a lot of timeseries
	// and it's not clear that the additional fidelity is worth it. Revisit if
	// evidence suggests otherwise.
	metaChangefeedFlushes = metric.Metadata{
		Name:        "changefeed.flushes",
		Help:        "Total flushes across all feeds",
		Measurement: "Flushes",
		Unit:        metric.Unit_COUNT,
	}
	metaChangefeedFlushNanos = metric.Metadata{
		Name:        "changefeed.flush_nanos",
		Help:        "Total time spent flushing all feeds",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_NANOSECONDS,
	}

	// TODO(dan): This was intended to be a measure of the minimum distance of
	// any changefeed ahead of its gc ttl threshold, but keeping that correct in
	// the face of changing zone configs is much harder, so this will have to do
	// for now.
	metaChangefeedMinHighWater = metric.Metadata{
		Name:        "changefeed.min_high_water",
		Help:        "Latest high_water timestamp of most behind feed",
		Measurement: "Nanoseconds",
		Unit:        metric.Unit_TIMESTAMP_NS,
	}
)

const noMinHighWaterSentinel = int64(math.MaxInt64)

// Metrics are for production monitoring of changefeeds.
type Metrics struct {
	EmittedMessages *metric.Counter
	EmittedBytes    *metric.Counter
	EmitNanos       *metric.Counter
	Flushes         *metric.Counter
	FlushNanos      *metric.Counter

	mu struct {
		syncutil.Mutex
		id       int
		resolved map[int]hlc.Timestamp
	}
	MinHighWater *metric.Gauge
}

// MetricStruct implements the metric.Struct interface.
func (*Metrics) MetricStruct() {}

// MakeMetrics makes the metrics for changefeed monitoring.
func MakeMetrics() metric.Struct {
	m := &Metrics{
		EmittedMessages: metric.NewCounter(metaChangefeedEmittedMessages),
		EmittedBytes:    metric.NewCounter(metaChangefeedEmittedBytes),
		EmitNanos:       metric.NewCounter(metaChangefeedEmitNanos),
		Flushes:         metric.NewCounter(metaChangefeedFlushes),
		FlushNanos:      metric.NewCounter(metaChangefeedFlushNanos),
	}
	m.mu.resolved = make(map[int]hlc.Timestamp)
	m.MinHighWater = metric.NewFunctionalGauge(metaChangefeedMinHighWater, func() int64 {
		minHighWater := noMinHighWaterSentinel
		m.mu.Lock()
		for _, resolved := range m.mu.resolved {
			if minHighWater == noMinHighWaterSentinel || resolved.WallTime < minHighWater {
				minHighWater = resolved.WallTime
			}
		}
		m.mu.Unlock()
		return minHighWater
	})
	return m
}

func init() {
	jobs.MakeChangefeedMetricsHook = MakeMetrics
}
