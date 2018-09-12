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
	gosql "database/sql"
	"fmt"
	"hash"
	"hash/fnv"
	"net/url"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

func getSink(sinkURI string, targets jobspb.ChangefeedTargets) (Sink, error) {
	u, err := url.Parse(sinkURI)
	if err != nil {
		return nil, err
	}
	q := u.Query()

	var s Sink
	switch u.Scheme {
	case sinkSchemeBuffer:
		s = &bufferSink{}
	case sinkSchemeKafka:
		kafkaTopicPrefix := q.Get(sinkParamTopicPrefix)
		q.Del(sinkParamTopicPrefix)
		schemaTopic := q.Get(sinkParamSchemaTopic)
		q.Del(sinkParamSchemaTopic)
		if schemaTopic != `` {
			return nil, errors.Errorf(`%s is not yet supported`, sinkParamSchemaTopic)
		}
		confluentSchemaRegistry := q.Get(sinkParamConfluentSchemaRegistry)
		q.Del(sinkParamConfluentSchemaRegistry)
		if confluentSchemaRegistry != `` {
			return nil, errors.Errorf(`%s is not yet supported`, sinkParamConfluentSchemaRegistry)
		}
		s, err = getKafkaSink(kafkaTopicPrefix, u.Host, targets)
		if err != nil {
			return nil, err
		}
	case sinkSchemeExperimentalSQL:
		// Swap the changefeed prefix for the sql connection one that sqlSink
		// expects.
		u.Scheme = `postgres`
		// TODO(dan): Make tableName configurable or based on the job ID or
		// something.
		tableName := `sqlsink`
		s, err = makeSQLSink(u.String(), tableName, targets)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errors.Errorf(`unsupported sink: %s`, u.Scheme)
	}

	for k := range q {
		return nil, errors.Errorf(`unknown sink query parameter: %s`, k)
	}

	return s, nil
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
	kafkaTopicPrefix string, bootstrapServers string, targets jobspb.ChangefeedTargets,
) (Sink, error) {
	sink := &kafkaSink{
		kafkaTopicPrefix: kafkaTopicPrefix,
	}
	sink.topics = make(map[string]struct{})
	for _, t := range targets {
		sink.topics[kafkaTopicPrefix+t.StatementTimeName] = struct{}{}
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
		if _, ok := flushErr.(*sarama.ProducerError); ok {
			flushErr = &retryableSinkError{cause: flushErr}
		}
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

const (
	sqlSinkCreateTableStmt = `CREATE TABLE IF NOT EXISTS "%s" (
		topic STRING,
		partition INT,
		message_id INT,
		key BYTES, value BYTES,
		resolved BYTES,
		PRIMARY KEY (topic, partition, message_id)
	)`
	sqlSinkEmitStmt = `INSERT INTO "%s" (topic, partition, message_id, key, value, resolved)`
	sqlSinkEmitCols = 6
	// Some amount of batching to mirror a bit how kafkaSink works.
	sqlSinkRowBatchSize = 3
	// While sqlSink is only used for testing, hardcode the number of
	// partitions to something small but greater than 1.
	sqlSinkNumPartitions = 3
)

// sqlSink mirrors the semantics offered by kafkaSink as closely as possible,
// but writes to a SQL table (presumably in CockroachDB). Currently only for
// testing.
//
// Each emitted row or resolved timestamp is stored as a row in the table. Each
// table gets 3 partitions. Similar to kafkaSink, the order between two emits is
// only preserved if they are emitted to by the same node and to the same
// partition.
type sqlSink struct {
	db *gosql.DB

	tableName string
	topics    map[string]struct{}
	hasher    hash.Hash32

	rowBuf []interface{}
}

func makeSQLSink(uri, tableName string, targets jobspb.ChangefeedTargets) (*sqlSink, error) {
	if u, err := url.Parse(uri); err != nil {
		return nil, err
	} else if u.Path == `` {
		return nil, errors.Errorf(`must specify database`)
	}
	db, err := gosql.Open(`postgres`, uri)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(fmt.Sprintf(sqlSinkCreateTableStmt, tableName)); err != nil {
		db.Close()
		return nil, err
	}

	s := &sqlSink{
		db:        db,
		tableName: tableName,
		topics:    make(map[string]struct{}),
		hasher:    fnv.New32a(),
	}
	for _, t := range targets {
		s.topics[t.StatementTimeName] = struct{}{}
	}
	return s, nil
}

// EmitRow implements the Sink interface.
func (s *sqlSink) EmitRow(ctx context.Context, topic string, key, value []byte) error {
	if _, ok := s.topics[topic]; !ok {
		return errors.Errorf(`cannot emit to undeclared topic: %s`, topic)
	}

	// Hashing logic copied from sarama.HashPartitioner.
	s.hasher.Reset()
	if _, err := s.hasher.Write(key); err != nil {
		return err
	}
	partition := int32(s.hasher.Sum32()) % sqlSinkNumPartitions
	if partition < 0 {
		partition = -partition
	}

	var noResolved []byte
	return s.emit(ctx, topic, partition, key, value, noResolved)
}

// EmitResolvedTimestamp implements the Sink interface.
func (s *sqlSink) EmitResolvedTimestamp(ctx context.Context, payload []byte) error {
	var noKey, noValue []byte
	for topic := range s.topics {
		for partition := int32(0); partition < sqlSinkNumPartitions; partition++ {
			if err := s.emit(ctx, topic, partition, noKey, noValue, payload); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *sqlSink) emit(
	ctx context.Context, topic string, partition int32, key, value, resolved []byte,
) error {
	// Generate the message id on the client to match the guaranttees of kafka
	// (two messages are only guaranteed to keep their order if emitted from the
	// same producer to the same partition).
	messageID := builtins.GenerateUniqueInt(roachpb.NodeID(partition))
	s.rowBuf = append(s.rowBuf, topic, partition, messageID, key, value, resolved)
	if len(s.rowBuf)/sqlSinkEmitCols >= sqlSinkRowBatchSize {
		return s.Flush(ctx)
	}
	return nil
}

// Flush implements the Sink interface.
func (s *sqlSink) Flush(ctx context.Context) error {
	if len(s.rowBuf) == 0 {
		return nil
	}

	var stmt strings.Builder
	fmt.Fprintf(&stmt, sqlSinkEmitStmt, s.tableName)
	for i := 0; i < len(s.rowBuf); i++ {
		if i == 0 {
			stmt.WriteString(` VALUES (`)
		} else if i%sqlSinkEmitCols == 0 {
			stmt.WriteString(`),(`)
		} else {
			stmt.WriteString(`,`)
		}
		fmt.Fprintf(&stmt, `$%d`, i+1)
	}
	stmt.WriteString(`)`)
	_, err := s.db.Exec(stmt.String(), s.rowBuf...)
	if err != nil {
		return err
	}
	s.rowBuf = s.rowBuf[:0]
	return nil
}

// Close implements the Sink interface.
func (s *sqlSink) Close() error {
	return s.db.Close()
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

// causer matches the (unexported) interface used by Go to allow errors to wrap
// their parent cause.
type causer interface {
	Cause() error
}

// retryableSinkError should be used by sinks to wrap any error which may
// be retried.
type retryableSinkError struct {
	cause error
}

func (e retryableSinkError) Error() string { return e.cause.Error() }
func (e retryableSinkError) Cause() error  { return e.cause }

// isRetryableSinkError returns true if the supplied error, or any of its parent
// causes, is a retryableSinkError.
func isRetryableSinkError(err error) bool {
	for {
		if _, ok := err.(retryableSinkError); ok {
			return true
		}
		if e, ok := err.(causer); ok {
			err = e.Cause()
			continue
		}
		return false
	}
}
