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
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// SinkEmitter is an interface for a struct that emits sinkEvents
type SinkEmitter interface {
	// Emit handles sending a sinkEvent.  sinkEvent is expected to be a pointer to
	// an object created by sinkEventPool and once it has been emitted the emitter
	// will handle cleaning it up with freeSinkEvent
	Emit(*sinkEvent)

	// Close closes all resources and goroutines associated with the SinkEmitter
	Close()
}

// batchingSinkEmitter is a SinkEmitter which combines individual sinkEvents
// into batches then encode batches and emits those batches via a SinkClient.
// Confirmation of delivery / notification of errors are done through the
// provided successCh and errorCh properties.
type batchingSinkEmitter struct {
	ctx    context.Context
	client SinkClient
	topic  string

	batchCfg  sinkBatchConfig
	retryOpts retry.Options

	successCh chan int
	errorCh   chan error

	rowCh chan *sinkEvent

	mu struct {
		syncutil.RWMutex
		termErr error
	}
	doneCh     chan struct{}
	wg         ctxgroup.Group
	timeSource timeutil.TimeSource
	metrics    metricsRecorder
	pacer      SinkPacer

	knobs batchingSinkEmitterKnobs
}

var _ SinkEmitter = (*batchingSinkEmitter)(nil)

func makeBatchingSinkEmitter(
	ctx context.Context,
	sink SinkClient,
	config sinkBatchConfig,
	retryOpts retry.Options,
	topic string,
	successCh chan int,
	errorCh chan error,
	timeSource timeutil.TimeSource,
	metrics metricsRecorder,
	pacer SinkPacer,
) *batchingSinkEmitter {
	bse := batchingSinkEmitter{
		ctx:       ctx,
		client:    sink,
		topic:     topic,
		batchCfg:  config,
		retryOpts: retryOpts,

		successCh: successCh,
		errorCh:   errorCh,

		rowCh:      make(chan *sinkEvent, 64),
		doneCh:     make(chan struct{}),
		wg:         ctxgroup.WithContext(ctx),
		timeSource: timeSource,
		metrics:    metrics,
		pacer:      pacer,
	}

	bse.wg.GoCtx(func(ctx context.Context) error {
		return bse.startBatchWorker()
	})

	return &bse
}

// Emit implements the SinkEmitter interface.
func (bse *batchingSinkEmitter) Emit(payload *sinkEvent) {
	bse.metrics.recordBatchingEmitterAdmit()
	select {
	case <-bse.ctx.Done():
		return
	case <-bse.doneCh:
		return
	case bse.rowCh <- payload:
		return
	}
}

// Close implements the SinkEmitter interface.
func (bse *batchingSinkEmitter) Close() {
	close(bse.doneCh)
	_ = bse.wg.Wait()
}

func (bse *batchingSinkEmitter) handleError(err error) {
	bse.mu.Lock()
	defer bse.mu.Unlock()
	if bse.mu.termErr == nil {
		bse.mu.termErr = err
	} else {
		return
	}

	select {
	case <-bse.ctx.Done():
		return
	case <-bse.doneCh:
		return
	case bse.errorCh <- bse.mu.termErr:
		return
	}
}

type batchWorkerMessage struct {
	sinkPayload SinkPayload
	numMessages int
	mvcc        hlc.Timestamp
	alloc       kvevent.Alloc
	kvBytes     int
	bufferTime  time.Time
}

func (bse *batchingSinkEmitter) startBatchWorker() error {
	currentBatch := newMessageBatch()

	// Emitting a batch is a blocking I/O operation so performing it in its own
	// goroutine allows for the next batch to be constructed and queued up in
	// parallel.
	batchCh := make(chan *batchWorkerMessage, 64)
	bse.wg.GoCtx(func(ctx context.Context) error {
		return bse.startEmitWorker(batchCh)
	})

	flushBatch := func() {
		if currentBatch.isEmpty() {
			return
		}

		// Reuse the same batch to avoid need for garbage collection
		defer currentBatch.reset()

		// Process messages into a payload ready to be emitted to the sink
		sinkPayload, err := bse.client.EncodeBatch(bse.topic, currentBatch.buffer)
		if err != nil {
			bse.handleError(err)
			return
		}

		// Send the encoded batch to a separate worker so that flushes do not block
		// further message aggregation
		select {
		case <-bse.ctx.Done():
			return
		case <-bse.doneCh:
			return
		case batchCh <- newBatchWorkerMessage(sinkPayload, &currentBatch):
			return
		}
	}

	flushTimer := bse.timeSource.NewTimer()

	handleEvent := func(event *sinkEvent) {
		defer freeSinkEvent(event)

		// If an error has occured resulting in termination, no events will be
		// emitted due to ordering constraints.
		if bse.isTerminated() {
			return
		}
		if event.shouldFlush {
			flushBatch()
			return
		}
		// Resolved messages aren't batched with normal messages
		if event.resolved != nil {
			flushBatch()
			bse.emitResolvedEvent(event.resolved.body)
			return
		}

		bse.metrics.recordMessageSize(int64(len(event.msg.key) + len(event.msg.val)))

		// If the batch is about to no longer be empty, start the flush timer
		if currentBatch.isEmpty() && time.Duration(bse.batchCfg.Frequency) > 0 {
			flushTimer.Reset(time.Duration(bse.batchCfg.Frequency))
		}
		if bse.knobs.OnAppend != nil {
			bse.knobs.OnAppend(event)
		}
		currentBatch.Append(event)

		if bse.shouldFlushBatch(currentBatch) {
			bse.metrics.recordSizeBasedFlush()
			flushBatch()
		}
	}

	for {
		bse.pacer.Pace(bse.ctx)

		select {
		case <-bse.ctx.Done():
			return bse.ctx.Err()
		case <-bse.doneCh:
			return nil
		case event := <-bse.rowCh:
			handleEvent(event)
		case <-flushTimer.Ch():
			flushTimer.MarkRead()
			flushBatch()
		}
	}
}

func (bse *batchingSinkEmitter) emitResolvedEvent(resolvedBody []byte) {
	payload, err := bse.client.EncodeResolvedMessage(resolvedMessagePayload{
		body:  resolvedBody,
		topic: bse.topic,
	})
	if err != nil {
		bse.handleError(err)
		return
	}

	err = bse.emitWithRetries(payload, 1)
	if err != nil {
		bse.handleError(err)
		return
	}
	bse.successCh <- 1
}

func (bse *batchingSinkEmitter) isTerminated() bool {
	bse.mu.RLock()
	defer bse.mu.RUnlock()
	return bse.mu.termErr != nil
}

func (bse *batchingSinkEmitter) startEmitWorker(batchCh chan *batchWorkerMessage) error {
	handleBatch := func(batch *batchWorkerMessage) {
		defer freeBatchWorkerMessage(batch)

		// Never emit messages if an error has occured as ordering guarantees may be compromised
		if bse.isTerminated() {
			return
		}

		flushCallback := bse.metrics.recordFlushRequestCallback()
		err := bse.emitWithRetries(batch.sinkPayload, batch.numMessages)
		if err != nil {
			bse.handleError(err)
			return
		}

		bse.metrics.recordBatchingEmitterEmit(batch.numMessages)
		bse.metrics.recordEmittedBatch(
			batch.bufferTime, batch.numMessages, batch.mvcc, batch.kvBytes, sinkDoesNotCompress)
		batch.alloc.Release(bse.ctx)

		bse.successCh <- batch.numMessages
		flushCallback()
	}

	for {
		select {
		case <-bse.ctx.Done():
			return bse.ctx.Err()
		case <-bse.doneCh:
			return nil
		case batch := <-batchCh:
			handleBatch(batch)
		}
	}
}

func (bse *batchingSinkEmitter) emitWithRetries(payload SinkPayload, numMessages int) error {
	initialSend := true
	return retry.WithMaxAttempts(bse.ctx, bse.retryOpts, bse.retryOpts.MaxRetries+1, func() error {
		if !initialSend {
			bse.metrics.recordInternalRetry(int64(numMessages), false)
		}
		err := bse.client.EmitPayload(payload)
		initialSend = false
		return err
	})
}

func (bse *batchingSinkEmitter) shouldFlushBatch(batch messageBatch) bool {
	switch {
	// all zero values is interpreted as flush every time
	case bse.batchCfg.Messages == 0 && bse.batchCfg.Bytes == 0 && bse.batchCfg.Frequency == 0:
		return true
	// messages threshold has been reached
	case bse.batchCfg.Messages > 0 && len(batch.buffer) >= bse.batchCfg.Messages:
		return true
	// bytes threshold has been reached
	case bse.batchCfg.Bytes > 0 && batch.bufferBytes >= bse.batchCfg.Bytes:
		return true
	default:
		return false
	}
}

type messageBatch struct {
	buffer      []messagePayload
	bufferBytes int
	alloc       kvevent.Alloc
	mvcc        hlc.Timestamp
	bufferTime  time.Time // The earliest time a message was inserted into the batch
}

func newMessageBatch() messageBatch {
	return messageBatch{
		buffer:      make([]messagePayload, 0),
		bufferBytes: 0,
	}
}

func (mb *messageBatch) isEmpty() bool {
	return len(mb.buffer) == 0
}

func (mb *messageBatch) reset() {
	mb.buffer = mb.buffer[:0]
	mb.bufferBytes = 0
	mb.alloc = kvevent.Alloc{}
}

// Append adds the contents of a sinkEvent to the batch, merging its alloc pool
func (mb *messageBatch) Append(e *sinkEvent) {
	if mb.isEmpty() {
		mb.bufferTime = timeutil.Now()
	}

	mb.buffer = append(mb.buffer, e.msg)
	mb.bufferBytes += len(e.msg.val)
	mb.bufferBytes += len(e.msg.key)

	if mb.mvcc.IsEmpty() || e.mvcc.Less(mb.mvcc) {
		mb.mvcc = e.mvcc
	}

	mb.alloc.Merge(&e.alloc)
}

var batchWorkerMessagePool = sync.Pool{
	New: func() interface{} {
		return new(batchWorkerMessage)
	},
}

func newBatchWorkerMessage(payload SinkPayload, batch *messageBatch) *batchWorkerMessage {
	message := batchWorkerMessagePool.Get().(*batchWorkerMessage)
	message.sinkPayload = payload
	message.alloc = batch.alloc
	message.numMessages = len(batch.buffer)
	message.mvcc = batch.mvcc
	message.kvBytes = batch.bufferBytes
	message.bufferTime = batch.bufferTime
	return message
}

func freeBatchWorkerMessage(r *batchWorkerMessage) {
	*r = batchWorkerMessage{}
	batchWorkerMessagePool.Put(r)
}

type batchingSinkEmitterKnobs struct {
	OnAppend func(*sinkEvent)
}
