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
	"fmt"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
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

type kafkaSink struct {
	// TODO(dan): This uses the shopify kafka producer library because the
	// official confluent one depends on librdkafka and it didn't seem worth it
	// to add a new c dep for the prototype. Revisit before 2.1 and check
	// stability, performance, etc.
	inputCh     chan<- *sarama.ProducerMessage
	successesCh <-chan *sarama.ProducerMessage
	errorsCh    <-chan *sarama.ProducerError
	producer    sarama.AsyncProducer
	client      sarama.Client

	kafkaTopicPrefix string
	topicsSeen       map[string]struct{}

	mu struct {
		syncutil.Mutex

		// ids is used to give a total ordering to messages and flushes. Each is
		// is given a unique, increasing id.
		ids int64

		// emitted is the total number of messages enqueued on behalf of calls
		// to EmitRow and EmitResolvedTimestamp.
		emitted int64
		// inflight is how many emitted messages have not been acknowledged by
		// kafka as a success or error.
		inflight int64

		// flushes is a map from a flush id to a wait group initialized with
		// however many messages were inflight when the flush was created. The
		// flush id is guaranteed to be greater than all the inflight message
		// ids.
		flushes map[int64]*sync.WaitGroup
	}
}

func getKafkaSink(kafkaTopicPrefix string, bootstrapServers string) (Sink, error) {
	sink := &kafkaSink{
		kafkaTopicPrefix: kafkaTopicPrefix,
		topicsSeen:       make(map[string]struct{}),
	}
	sink.mu.flushes = make(map[int64]*sync.WaitGroup)

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = newChangefeedPartitioner

	var err error
	sink.client, err = sarama.NewClient(strings.Split(bootstrapServers, `,`), config)
	if err != nil {
		return nil, errors.Wrapf(err, `connecting to kafka: %s`, bootstrapServers)
	}
	sink.producer, err = sarama.NewAsyncProducerFromClient(sink.client)
	if err != nil {
		return nil, errors.Wrapf(err, `connecting to kafka: %s`, bootstrapServers)
	}
	sink.inputCh = sink.producer.Input()
	sink.successesCh = sink.producer.Successes()
	sink.errorsCh = sink.producer.Errors()
	return sink, nil
}

// Close implements the Sink interface.
func (s *kafkaSink) Close() error {
	if s.producer != nil {
		s.producer.AsyncClose()
	}
	// Empty out the success and errors channels as required by AsyncClose.
	for range s.errorsCh {
	}
	for range s.successesCh {
	}
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

// EmitRow implements the Sink interface.
func (s *kafkaSink) EmitRow(ctx context.Context, topic string, key, value []byte) error {
	topic = s.kafkaTopicPrefix + topic
	if _, ok := s.topicsSeen[topic]; !ok {
		s.topicsSeen[topic] = struct{}{}
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
	for topic := range s.topicsSeen {
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
	// When a flush is created, it is given a flush id that is guaranteed to be
	// greater than the unique id attached to all inflight messages. Under the
	// same lock, the number of inflight messages is used to initialize a
	// WaitGroup which is kept in a registry along with the flush id. Whenever a
	// message is acknowledged by kafka, the WaitGroups of any flushes with a
	// higher id than the message id are decremented.

	s.mu.Lock()
	flushID := s.mu.ids
	s.mu.ids++
	inflight := s.mu.inflight
	var wg sync.WaitGroup
	wg.Add(int(inflight))
	s.mu.flushes[flushID] = &wg
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.mu.flushes, flushID)
		s.mu.Unlock()
	}()

	if log.V(1) {
		log.Infof(ctx, "flush ID %d waiting for %d inflight messages", flushID, inflight)
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		doneCh <- struct{}{}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-doneCh:
			return nil
		case msg := <-s.successesCh:
			s.acknowledgedMsg(ctx, msg)
		case err := <-s.errorsCh:
			s.acknowledgedMsg(ctx, err.Msg)
			// TODO(dan): I had to add this to make leaktest.AfterTest happy,
			// but it seems like a bad idea.
			<-doneCh
			return err
		}
	}
}

func (s *kafkaSink) emitMessage(ctx context.Context, msg *sarama.ProducerMessage) error {
	s.mu.Lock()
	msg.Metadata = s.mu.ids
	s.mu.ids++
	s.mu.emitted++
	s.mu.inflight++
	emitted, inflight := s.mu.emitted, s.mu.inflight
	s.mu.Unlock()

	// TODO(dan): The async producer documentation recommends that we use a
	// select over the input, success, and error channels when sending a
	// message, but this has led to quite a bit of complexity and fragility.
	// Having a goroutine handle all of the reads from the success and error
	// channels is almost certainly more straightforward.
	if err := func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case s.inputCh <- msg:
				return nil
			case err := <-s.errorsCh:
				s.acknowledgedMsg(ctx, err.Msg)
				return err
			case msg := <-s.successesCh:
				s.acknowledgedMsg(ctx, msg)
			}
		}
	}(); err != nil {
		return err
	}

	if log.V(2) {
		log.Infof(ctx, "emitted %d finished and %d inflight records to kafka",
			emitted-inflight, inflight)
	}
	return nil
}

func (s *kafkaSink) acknowledgedMsg(ctx context.Context, msg *sarama.ProducerMessage) {
	msgID, ok := msg.Metadata.(int64)
	if !ok {
		panic(fmt.Sprintf(`received message with no id: %v`, msg))
	}
	s.mu.Lock()
	s.mu.inflight--
	for flushID, wg := range s.mu.flushes {
		if msgID <= flushID {
			wg.Done()
		}
	}
	s.mu.Unlock()
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

type channelSink struct {
	resultsCh chan<- tree.Datums
	alloc     sqlbase.DatumAlloc
}

// EmitRow implements the Sink interface.
func (s *channelSink) EmitRow(ctx context.Context, topic string, key, value []byte) error {
	return s.emitDatums(ctx, tree.Datums{
		s.alloc.NewDString(tree.DString(topic)),
		s.alloc.NewDBytes(tree.DBytes(key)),
		s.alloc.NewDBytes(tree.DBytes(value)),
	})
}

// EmitResolvedTimestamp implements the Sink interface.
func (s *channelSink) EmitResolvedTimestamp(ctx context.Context, payload []byte) error {
	return s.emitDatums(ctx, tree.Datums{
		tree.DNull,
		tree.DNull,
		s.alloc.NewDBytes(tree.DBytes(payload)),
	})
}

func (s *channelSink) emitDatums(ctx context.Context, row tree.Datums) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.resultsCh <- row:
		return nil
	}
}

// Flush implements the Sink interface.
func (s *channelSink) Flush(_ context.Context) error {
	return nil
}

// Close implements the Sink interface.
func (s *channelSink) Close() error {
	// nil the channel so any later calls to EmitRow (there shouldn't be any)
	// don't work.
	s.resultsCh = nil
	return nil
}
