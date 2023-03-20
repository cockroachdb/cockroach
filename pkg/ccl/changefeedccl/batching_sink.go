// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     httbs://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// SinkClient is an interface to an external sink, where messages are written
// into batches as they arrive and once ready are flushed out.
type SinkClient interface {
	MakeResolvedPayload(body []byte, topic string) (SinkPayload, error)
	MakeBatchWriter() BatchWriter
	Flush(context.Context, SinkPayload) error
	Close() error
}

// BatchWriter is an interface to aggregate KVs into a payload that can be sent
// to the sink.
type BatchWriter interface {
	AppendKV(key []byte, value []byte, topic string)
	ShouldFlush() bool
	Close() (SinkPayload, error)
}

// SinkPayload is an interface representing a sink-specific representation of a
// batch of messages that is ready to be emitted by its EmitRow method.
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

	// Event structs and batch structs which are transferred across routines (and
	// therefore escape to the heap) can both be incredibly frequent (every event
	// may be its own batch) and temporary, so to avoid GC thrashing they are both
	// claimed and freed from object pools.
	eventPool sync.Pool
	batchPool sync.Pool

	// eventCh is the channel used to send requests from the Sink caller routines
	// to the batching routine.  Messages can either be a flushReq or a kvEvent.
	eventCh chan interface{}

	mu struct {
		syncutil.Mutex
		termErr error
	}
	wg     ctxgroup.Group
	hasher hash.Hash32
	pacer  *admission.Pacer
	doneCh chan struct{}
}

type batchingSinkKnobs struct {
	OnAppend func(*kvEvent)
}

type flushReq struct {
	waiter chan struct{}
}

type kvEvent struct {
	key   []byte
	val   []byte
	topic string

	alloc kvevent.Alloc
	mvcc  hlc.Timestamp
}

// Flush implements the Sink interface, returning the first error that has
// occured in the past EmitRow calls.
func (bs *batchingSink) Flush(ctx context.Context) error {
	flushWaiter := make(chan struct{})
	select {
	case <-ctx.Done():
	case <-bs.doneCh:
	case bs.eventCh <- flushReq{waiter: flushWaiter}:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-bs.doneCh:
		return nil
	case <-flushWaiter:
		bs.mu.Lock()
		defer bs.mu.Unlock()
		return bs.mu.termErr
	}
}

var _ Sink = (*batchingSink)(nil)

// EmitRow implements the Sink interface.
func (bs *batchingSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	bs.metrics.recordMessageSize(int64(len(key) + len(value)))

	payload := bs.eventPool.Get().(*kvEvent)
	payload.key = key
	payload.val = value
	payload.topic = "" // unimplemented for now
	payload.mvcc = mvcc
	payload.alloc = alloc

	select {
	case <-ctx.Done():
		return ctx.Err()
	case bs.eventCh <- payload:
	case <-bs.doneCh:
	}

	return nil
}

// EmitResolvedTimestamp implements the Sink interface.
func (bs *batchingSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	data, err := encoder.EncodeResolvedTimestamp(ctx, "", resolved)
	if err != nil {
		return err
	}
	payload, err := bs.client.MakeResolvedPayload(data, "")
	if err != nil {
		return err
	}

	if err = bs.Flush(ctx); err != nil {
		return err
	}
	return retry.WithMaxAttempts(ctx, bs.retryOpts, bs.retryOpts.MaxRetries+1, func() error {
		defer bs.metrics.recordFlushRequestCallback()()
		return bs.client.Flush(ctx, payload)
	})
}

// Close implements the Sink interface.
func (bs *batchingSink) Close() error {
	close(bs.doneCh)
	_ = bs.wg.Wait()
	if bs.pacer != nil {
		bs.pacer.Close()
	}
	return bs.client.Close()
}

// Dial implements the Sink interface.
func (bs *batchingSink) Dial() error {
	return nil
}

// getConcreteType implements the Sink interface.
func (bs *batchingSink) getConcreteType() sinkType {
	return bs.concreteType
}

// sinkBatchBuffer stores an in-progress/complete batch of messages, along with
// metadata related to the batch.
type sinkBatchBuffer struct {
	writer  BatchWriter
	payload SinkPayload // payload is nil until FinalizePayload has been called

	numMessages int
	numKVBytes  int // the total amount of uncompressed kv data in the batch
	keys        intsets.Fast
	bufferTime  time.Time // The earliest time a message was inserted into the batch
	mvcc        hlc.Timestamp

	alloc  kvevent.Alloc
	hasher hash.Hash32
}

// FinalizePayload closes the writer to produce a payload that is ready to be
// Flushed by the SinkClient.
func (sb *sinkBatchBuffer) FinalizePayload() error {
	payload, err := sb.writer.Close()
	if err != nil {
		return err
	}
	sb.payload = payload
	return nil
}

// Keys implements the IORequest interface.
func (sb *sinkBatchBuffer) Keys() intsets.Fast {
	return sb.keys
}

func (sb *sinkBatchBuffer) isEmpty() bool {
	return sb.numMessages == 0
}

func hashToInt(h hash.Hash32, buf []byte) int {
	h.Reset()
	h.Write(buf)
	return int(h.Sum32())
}

// Append adds the contents of a kvEvent to the batch, merging its alloc pool
func (sb *sinkBatchBuffer) Append(e *kvEvent) {
	if sb.isEmpty() {
		sb.bufferTime = timeutil.Now()
	}

	sb.writer.AppendKV(e.key, e.val, e.topic)

	sb.keys.Add(hashToInt(sb.hasher, e.key))
	sb.numMessages += 1
	sb.numKVBytes += len(e.key) + len(e.val)

	if sb.mvcc.IsEmpty() || e.mvcc.Less(sb.mvcc) {
		sb.mvcc = e.mvcc
	}

	sb.alloc.Merge(&e.alloc)
}

func (bs *batchingSink) handleError(err error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if bs.mu.termErr == nil {
		bs.mu.termErr = err
	}
}

func (bs *batchingSink) newBatchBuffer() *sinkBatchBuffer {
	batch := bs.batchPool.Get().(*sinkBatchBuffer)
	batch.writer = bs.client.MakeBatchWriter()
	batch.hasher = bs.hasher
	return batch
}

// runBatchingWorker combines 1 or more KV events into batches, sending the IO
// requests out either once the batch is full or a flush request arrives.
func (bs *batchingSink) runBatchingWorker(ctx context.Context) {
	batchBuffer := bs.newBatchBuffer()

	// Once finalized, batches are sent to a parallelIO struct which handles
	// performing multiple Flushes in parallel while maintaining Keys() ordering
	ioHandler := func(ctx context.Context, req IORequest) error {
		defer bs.metrics.recordFlushRequestCallback()()
		batch, _ := req.(*sinkBatchBuffer)
		return bs.client.Flush(ctx, batch.payload)
	}
	ioEmitter := newParallelIO(ctx, bs.retryOpts, bs.ioWorkers, ioHandler, bs.metrics)
	defer ioEmitter.Close()

	var handleResult func(result ioResult)

	tryFlushBatch := func() {
		if batchBuffer.isEmpty() {
			return
		}
		defer func() {
			batchBuffer = bs.newBatchBuffer()
		}()

		if err := batchBuffer.FinalizePayload(); err != nil {
			bs.handleError(err)
			return
		}

		// Emitting needs to also handle any incoming results to avoid a deadlock
		// with trying to emit while the emitter is blocked on returning a result.
		for {
			select {
			case <-ctx.Done():
			case ioEmitter.requestCh <- batchBuffer:
			case result := <-ioEmitter.resultCh:
				handleResult(result)
				continue
			case <-bs.doneCh:
			}
			break
		}

	}

	// Flushing requires tracking the number of inflight messages and confirming
	// completion to the requester once the counter reaches 0.
	inflight := 0
	var sinkFlushWaiter chan struct{}

	handleResult = func(result ioResult) {
		batch, _ := result.request.(*sinkBatchBuffer)
		defer func() {
			batch.alloc.Release(ctx)
			*batch = sinkBatchBuffer{}
			bs.batchPool.Put(batch)
		}()

		if result.err != nil {
			bs.handleError(result.err)
		} else {
			bs.metrics.recordEmittedBatch(
				batch.bufferTime, batch.numMessages, batch.mvcc, batch.numKVBytes, sinkDoesNotCompress,
			)
		}

		inflight -= batch.numMessages

		if (result.err != nil || inflight == 0) && sinkFlushWaiter != nil {
			close(sinkFlushWaiter)
			sinkFlushWaiter = nil
		}
	}

	flushTimer := bs.ts.NewTimer()
	defer flushTimer.Stop()

	for {
		if bs.pacer != nil {
			if err := bs.pacer.Pace(ctx); err != nil {
				if pacerLogEvery.ShouldLog() {
					log.Errorf(ctx, "automatic sink batcher pacing: %v", err)
				}
			}
		}

		select {
		case req := <-bs.eventCh:
			if flush, isFlush := req.(flushReq); isFlush {
				if inflight == 0 {
					close(flush.waiter)
				} else {
					sinkFlushWaiter = flush.waiter
					tryFlushBatch()
				}
			} else if event, isKV := req.(*kvEvent); isKV {
				inflight += 1

				// If we're about to append to an empty batch, start the timer to
				// guarantee the messages do not stay buffered longer than the
				// configured frequency.
				if batchBuffer.isEmpty() && bs.minFlushFrequency > 0 {
					flushTimer.Reset(bs.minFlushFrequency)
				}

				batchBuffer.Append(event)
				if bs.knobs.OnAppend != nil {
					bs.knobs.OnAppend(event)
				}

				// The event struct can be freed as the contents are expected to be
				// managed by the batch instead.
				*event = kvEvent{}
				bs.eventPool.Put(event)

				if batchBuffer.writer.ShouldFlush() {
					bs.metrics.recordSizeBasedFlush()
					tryFlushBatch()
				}
			}
		case result := <-ioEmitter.resultCh:
			handleResult(result)
		case <-flushTimer.Ch():
			flushTimer.MarkRead()
			tryFlushBatch()
		case <-ctx.Done():
			return
		case <-bs.doneCh:
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
	pacer *admission.Pacer,
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
		eventPool: sync.Pool{
			New: func() interface{} {
				return new(kvEvent)
			},
		},
		batchPool: sync.Pool{
			New: func() interface{} {
				return new(sinkBatchBuffer)
			},
		},
		eventCh: make(chan interface{}, flushQueueDepth),
		wg:      ctxgroup.WithContext(ctx),
		hasher:  makeHasher(),
		pacer:   pacer,
		doneCh:  make(chan struct{}),
	}

	sink.wg.GoCtx(func(ctx context.Context) error {
		sink.runBatchingWorker(ctx)
		return nil
	})
	return sink
}
