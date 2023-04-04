// Copyright 2023 The Cockroach Authors.
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
	"hash"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// SinkClient is an interface to an external sink, where messages are written
// into batches as they arrive and once ready are flushed out.
type SinkClient interface {
	MakeResolvedPayload(body []byte, topic string) (SinkPayload, error)
	// Batches can only hold messages for one unique topic
	MakeBatchBuffer(topic string) BatchBuffer
	Flush(context.Context, SinkPayload) error
	Close() error
}

// BatchBuffer is an interface to aggregate KVs into a payload that can be sent
// to the sink.
type BatchBuffer interface {
	Append(key []byte, value []byte)
	ShouldFlush() bool

	// Once all data has been Append'ed, Close can be called to return a finalized
	// Payload that is valid for a pure IO operation via Flush. All final encoding
	// work (ex: wrapping an array in an object with metadata) should be done in
	// Close, as non-io-related work done in Flush would be unnecessarily repeated
	// upon retries.
	Close() (SinkPayload, error)
}

// SinkPayload is an interface representing a sink-specific representation of a
// batch of messages that is ready to be emitted by its Flush method.
type SinkPayload interface{}

// batchingSink wraps a SinkClient to provide a Sink implementation that calls
// the SinkClient methods to form batches and flushes those batches across
// multiple parallel IO workers.
type batchingSink struct {
	client       SinkClient
	topicNamer   *TopicNamer
	concreteType sinkType

	ioWorkers         int
	minFlushFrequency time.Duration
	retryOpts         retry.Options

	ts      timeutil.TimeSource
	metrics metricsRecorder
	knobs   batchingSinkKnobs

	// eventCh is the channel used to send requests from the Sink caller routines
	// to the batching routine.  Messages can either be a flushReq or a rowEvent.
	eventCh chan interface{}

	pacer        *admission.Pacer
	pacerFactory func() *admission.Pacer

	termErr error
	wg      ctxgroup.Group
	hasher  hash.Hash32
	doneCh  chan struct{}
}

type batchingSinkKnobs struct {
	OnAppend func(*rowEvent)
}

type flushReq struct {
	waiter chan struct{}
}

type rowEvent struct {
	key             []byte
	val             []byte
	topicDescriptor TopicDescriptor

	alloc kvevent.Alloc
	mvcc  hlc.Timestamp
}

// Flush implements the Sink interface, returning the first error that has
// occured in the past EmitRow calls.
func (s *batchingSink) Flush(ctx context.Context) error {
	defer s.metrics.recordFlushRequestCallback()()
	flushWaiter := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.doneCh:
	case s.eventCh <- flushReq{waiter: flushWaiter}:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.doneCh:
		return nil
	case <-flushWaiter:
		if s.termErr != nil {
			return s.termErr
		}
	}

	// Refresh the pacer in case any settings have changed. s.pacer can safely be
	// assigned since once the Flush has completed waiting, no new messages exist
	// to be processed so pacer.Pace won't be called by the batching worker.
	s.pacer = s.pacerFactory()

	return nil
}

var _ Sink = (*batchingSink)(nil)

// Event structs and batch structs which are transferred across routines (and
// therefore escape to the heap) can both be incredibly frequent (every event
// may be its own batch) and temporary, so to avoid GC thrashing they are both
// claimed and freed from object pools.
var eventPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return new(rowEvent)
	},
}

func newRowEvent() *rowEvent {
	return eventPool.Get().(*rowEvent)
}
func freeRowEvent(e *rowEvent) {
	*e = rowEvent{}
	eventPool.Put(e)
}

var batchPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return new(sinkBatch)
	},
}

func newSinkBatch() *sinkBatch {
	return batchPool.Get().(*sinkBatch)
}
func freeSinkBatchEvent(b *sinkBatch) {
	*b = sinkBatch{}
	batchPool.Put(b)
}

// EmitRow implements the Sink interface.
func (s *batchingSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	s.metrics.recordMessageSize(int64(len(key) + len(value)))

	payload := newRowEvent()
	payload.key = key
	payload.val = value
	payload.topicDescriptor = topic
	payload.mvcc = mvcc
	payload.alloc = alloc

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.eventCh <- payload:
	case <-s.doneCh:
	}

	return nil
}

// EmitResolvedTimestamp implements the Sink interface.
func (s *batchingSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	data, err := encoder.EncodeResolvedTimestamp(ctx, "", resolved)
	if err != nil {
		return err
	}
	payload, err := s.client.MakeResolvedPayload(data, "")
	if err != nil {
		return err
	}

	if err = s.Flush(ctx); err != nil {
		return err
	}
	return retry.WithMaxAttempts(ctx, s.retryOpts, s.retryOpts.MaxRetries+1, func() error {
		return s.client.Flush(ctx, payload)
	})
}

// Close implements the Sink interface.
func (s *batchingSink) Close() error {
	close(s.doneCh)
	_ = s.wg.Wait()
	s.pacer.Close()
	return s.client.Close()
}

// Dial implements the Sink interface.
func (s *batchingSink) Dial() error {
	return nil
}

// getConcreteType implements the Sink interface.
func (s *batchingSink) getConcreteType() sinkType {
	return s.concreteType
}

// sinkBatch stores an in-progress/complete batch of messages, along with
// metadata related to the batch.
type sinkBatch struct {
	buffer  BatchBuffer
	payload SinkPayload // payload is nil until FinalizePayload has been called

	numMessages int
	numKVBytes  int          // the total amount of uncompressed kv data in the batch
	keys        intsets.Fast // the set of keys within the batch to provide to parallelIO
	bufferTime  time.Time    // the earliest time a message was inserted into the batch
	mvcc        hlc.Timestamp

	alloc  kvevent.Alloc
	hasher hash.Hash32
}

// FinalizePayload closes the writer to produce a payload that is ready to be
// Flushed by the SinkClient.
func (sb *sinkBatch) FinalizePayload() error {
	payload, err := sb.buffer.Close()
	if err != nil {
		return err
	}
	sb.payload = payload
	return nil
}

// Keys implements the IORequest interface.
func (sb *sinkBatch) Keys() intsets.Fast {
	return sb.keys
}

func (sb *sinkBatch) isEmpty() bool {
	return sb.numMessages == 0
}

func hashToInt(h hash.Hash32, buf []byte) int {
	h.Reset()
	h.Write(buf)
	return int(h.Sum32())
}

// Append adds the contents of a kvEvent to the batch, merging its alloc pool.
func (sb *sinkBatch) Append(e *rowEvent) {
	if sb.isEmpty() {
		sb.bufferTime = timeutil.Now()
	}

	sb.buffer.Append(e.key, e.val)

	sb.keys.Add(hashToInt(sb.hasher, e.key))
	sb.numMessages += 1
	sb.numKVBytes += len(e.key) + len(e.val)

	if sb.mvcc.IsEmpty() || e.mvcc.Less(sb.mvcc) {
		sb.mvcc = e.mvcc
	}

	sb.alloc.Merge(&e.alloc)
}

func (s *batchingSink) handleError(err error) {
	if s.termErr == nil {
		s.termErr = err
	}
}

func (s *batchingSink) newBatchBuffer(topic string) *sinkBatch {
	batch := newSinkBatch()
	batch.buffer = s.client.MakeBatchBuffer(topic)
	batch.hasher = s.hasher
	return batch
}

// runBatchingWorker combines 1 or more row events into batches, sending the IO
// requests out either once the batch is full or a flush request arrives.
func (s *batchingSink) runBatchingWorker(ctx context.Context) {
	// topicBatches stores per-topic sinkBatches which are flushed individually
	// when one reaches its size limit, but are all flushed together if the
	// frequency timer triggers.  Messages for different topics cannot be allowed
	// to be batched together as the data may need to end up at a specific
	// endpoint for that topic.
	topicBatches := make(map[string]*sinkBatch)

	// Once finalized, batches are sent to a parallelIO struct which handles
	// performing multiple Flushes in parallel while maintaining Keys() ordering.
	ioHandler := func(ctx context.Context, req IORequest) error {
		batch, _ := req.(*sinkBatch)
		defer s.metrics.recordSinkIOInflightChange(int64(-batch.numMessages))
		s.metrics.recordSinkIOInflightChange(int64(batch.numMessages))
		return s.client.Flush(ctx, batch.payload)
	}
	ioEmitter := newParallelIO(ctx, s.retryOpts, s.ioWorkers, ioHandler, s.metrics)
	defer ioEmitter.Close()

	// Flushing requires tracking the number of inflight messages and confirming
	// completion to the requester once the counter reaches 0.
	inflight := 0
	var sinkFlushWaiter chan struct{}

	handleResult := func(result *ioResult) {
		batch, _ := result.request.(*sinkBatch)

		if result.err != nil {
			s.handleError(result.err)
		} else {
			s.metrics.recordEmittedBatch(
				batch.bufferTime, batch.numMessages, batch.mvcc, batch.numKVBytes, sinkDoesNotCompress,
			)
		}

		inflight -= batch.numMessages

		if (result.err != nil || inflight == 0) && sinkFlushWaiter != nil {
			close(sinkFlushWaiter)
			sinkFlushWaiter = nil
		}

		freeIOResult(result)
		batch.alloc.Release(ctx)
		freeSinkBatchEvent(batch)
	}

	tryFlushBatch := func(topic string) error {
		batchBuffer, ok := topicBatches[topic]
		if !ok || batchBuffer.isEmpty() {
			return nil
		}
		topicBatches[topic] = s.newBatchBuffer(topic)

		if err := batchBuffer.FinalizePayload(); err != nil {
			return err
		}

		// Emitting needs to also handle any incoming results to avoid a deadlock
		// with trying to emit while the emitter is blocked on returning a result.
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ioEmitter.requestCh <- batchBuffer:
			case result := <-ioEmitter.resultCh:
				handleResult(result)
				continue
			case <-s.doneCh:
			}
			break
		}

		return nil
	}

	flushAll := func() error {
		for topic := range topicBatches {
			if err := tryFlushBatch(topic); err != nil {
				return err
			}
		}
		return nil
	}

	// flushTimer is used to ensure messages do not remain batched longer than a
	// given timeout. Every minFlushFrequency seconds after the first event for
	// any topic has arrived, batches for all topics are flushed out immediately
	// and the timer once again waits for the first message to arrive.
	flushTimer := s.ts.NewTimer()
	defer flushTimer.Stop()
	isTimerPending := false

	for {
		select {
		case req := <-s.eventCh:
			if err := s.pacer.Pace(ctx); err != nil {
				if pacerLogEvery.ShouldLog() {
					log.Errorf(ctx, "automatic sink batcher pacing: %v", err)
				}
			}

			switch r := req.(type) {
			case *rowEvent:
				if s.termErr != nil {
					continue
				}

				inflight += 1

				var topic string
				var err error
				if s.topicNamer != nil {
					topic, err = s.topicNamer.Name(r.topicDescriptor)
					if err != nil {
						s.handleError(err)
						continue
					}
				}

				// If the timer isn't pending then this message is the first message to
				// arrive either ever or since the timer last triggered a flush,
				// therefore we're going from 0 messages batched to 1, and should
				// restart the timer.
				if !isTimerPending && s.minFlushFrequency > 0 {
					flushTimer.Reset(s.minFlushFrequency)
					isTimerPending = true
				}

				batchBuffer, ok := topicBatches[topic]
				if !ok {
					batchBuffer = s.newBatchBuffer(topic)
					topicBatches[topic] = batchBuffer
				}

				batchBuffer.Append(r)
				if s.knobs.OnAppend != nil {
					s.knobs.OnAppend(r)
				}

				// The event struct can be freed as the contents are expected to be
				// managed by the batch instead.
				freeRowEvent(r)

				if batchBuffer.buffer.ShouldFlush() {
					s.metrics.recordSizeBasedFlush()
					if err := tryFlushBatch(topic); err != nil {
						s.handleError(err)
					}
				}
			case flushReq:
				if inflight == 0 || s.termErr != nil {
					close(r.waiter)
				} else {
					sinkFlushWaiter = r.waiter
					if err := flushAll(); err != nil {
						s.handleError(err)
					}
				}
			default:
				s.handleError(fmt.Errorf("received unknown request of unknown type: %v", r))
			}
		case result := <-ioEmitter.resultCh:
			handleResult(result)
		case <-flushTimer.Ch():
			flushTimer.MarkRead()
			isTimerPending = false
			if err := flushAll(); err != nil {
				s.handleError(err)
			}
		case <-ctx.Done():
			return
		case <-s.doneCh:
			return
		}
	}
}

func makeBatchingSink(
	ctx context.Context,
	concreteType sinkType,
	client SinkClient,
	minFlushFrequency time.Duration,
	retryOpts retry.Options,
	numWorkers int,
	topicNamer *TopicNamer,
	pacerFactory func() *admission.Pacer,
	timeSource timeutil.TimeSource,
	metrics metricsRecorder,
) Sink {
	sink := &batchingSink{
		client:            client,
		topicNamer:        topicNamer,
		concreteType:      concreteType,
		minFlushFrequency: minFlushFrequency,
		ioWorkers:         numWorkers,
		retryOpts:         retryOpts,
		ts:                timeSource,
		metrics:           metrics,
		eventCh:           make(chan interface{}, flushQueueDepth),
		wg:                ctxgroup.WithContext(ctx),
		hasher:            makeHasher(),
		pacerFactory:      pacerFactory,
		pacer:             pacerFactory(),
		doneCh:            make(chan struct{}),
	}

	sink.wg.GoCtx(func(ctx context.Context) error {
		sink.runBatchingWorker(ctx)
		return nil
	})
	return sink
}
