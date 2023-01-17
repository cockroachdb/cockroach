// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// parallelBatchingSink takes a SinkClient implementation and constructs a Sink
// implmentation with the following behaviors:
// 1. Events are fanned out by key to multiple parallel worker routines
// 2. Events in a given topic are batched together according to given batchCfg
// 3. Emitting a batch to a sink can be  retried according to given retryOpts
type parallelBatchingSink struct {
	emitter FlushingEmitter

	client       SinkClient
	topicNamer   *TopicNamer
	concreteType sinkType
}

var _ Sink = (*parallelBatchingSink)(nil)

func makeParallelBatchingSink(
	ctx context.Context,
	concreteType sinkType,
	client SinkClient,
	batchCfg sinkBatchConfig,
	retryOpts retry.Options,
	numWorkers int64,
	topicNamer *TopicNamer,
	timeSource timeutil.TimeSource,
	metrics metricsRecorder,
	pacer SinkPacer,
) Sink {
	batchingEmitterFactory := func(topic string, successCh chan int, errorCh chan error) SinkEmitter {
		return makeBatchingSinkEmitter(
			ctx,
			client,
			batchCfg,
			retryOpts,
			topic,
			successCh,
			errorCh,
			timeSource,
			metrics,
			pacer,
		)
	}

	parallelEmitter := makeParallelSinkEmitter(
		ctx,
		batchingEmitterFactory,
		numWorkers,
		metrics,
	)

	flushingEmitter := makeFlushingEmitter(
		ctx,
		parallelEmitter,
	)

	return &parallelBatchingSink{
		emitter:      flushingEmitter,
		client:       client,
		topicNamer:   topicNamer,
		concreteType: concreteType,
	}
}

type sinkEvent struct {
	// Set if its a Flush event
	shouldFlush bool

	// Set if its a Resolved event
	resolved *resolvedMessagePayload

	// Set if its a KV Event
	msg   messagePayload
	alloc kvevent.Alloc
	mvcc  hlc.Timestamp
}

// EmitRow implements the Sink interface.
func (ps *parallelBatchingSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	var topicName string
	var err error
	if ps.topicNamer != nil {
		topicName, err = ps.topicNamer.Name(topic)
		if err != nil {
			return err
		}
	}

	payload := newSinkEvent()
	payload.msg = messagePayload{
		key:   key,
		val:   value,
		topic: topicName,
	}
	payload.mvcc = mvcc
	payload.alloc = alloc
	ps.emitter.Emit(payload)

	return nil
}

// EmitResolvedTimestamp implements the Sink interface.
func (ps *parallelBatchingSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	data, err := encoder.EncodeResolvedTimestamp(ctx, "", resolved)
	if err != nil {
		return err
	}

	var topics []string
	if ps.topicNamer == nil {
		topics = []string{""}
	} else {
		topics = ps.topicNamer.DisplayNamesSlice()
	}
	for _, topic := range topics {
		event := newSinkEvent()
		event.resolved = &resolvedMessagePayload{
			body:  data,
			topic: topic,
		}
		ps.emitter.Emit(event)
	}
	return ps.Flush(ctx)
}

// Flush implements the Sink interface.
func (ps *parallelBatchingSink) Flush(ctx context.Context) error {
	return ps.emitter.Flush()
}

// Close implements the Sink interface.
func (ps *parallelBatchingSink) Close() error {
	ps.emitter.Close()
	return ps.client.Close()
}

// Dial implements the Sink interface.
func (ps *parallelBatchingSink) Dial() error {
	return nil
}

// getConcreteType implements the Sink interface.
func (ps *parallelBatchingSink) getConcreteType() sinkType {
	return ps.concreteType
}

var sinkEventPool = sync.Pool{
	New: func() interface{} {
		return new(sinkEvent)
	},
}

func newSinkEvent() *sinkEvent {
	return sinkEventPool.Get().(*sinkEvent)
}

func freeSinkEvent(e *sinkEvent) {
	*e = sinkEvent{}
	sinkEventPool.Put(e)
}

func newSinkFlushEvent() *sinkEvent {
	e := newSinkEvent()
	e.shouldFlush = true
	return e
}
