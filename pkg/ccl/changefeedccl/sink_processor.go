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
	"hash/crc32"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/system"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// ThinSink is the interface to an external sink used by a sink processor.
// Messages go through two steps, an Encoding step which creates a payload of
// one or more messages, then an Emit stage which sends the payload, with
// EmitPayload potentially being retried multiple times with the same payload if
// errors are observed.
type ThinSink interface {
	EncodeBatch([]MessagePayload) (SinkPayload, error)
	EncodeResolvedMessage(ResolvedMessagePayload) (SinkPayload, error)
	EmitPayload(SinkPayload) error
	Close() error
}

// SinkPayload is an interface representing a sink-specific representation of a
// batch of messages that is ready to be emitted by its EmitRow method.
type SinkPayload interface{}

// MessagePayload represents a KV event to be emitted.
type MessagePayload struct {
	key []byte
	val []byte
}

// ResolvedMessagePayload represents a Resolved event to be emitted.
type ResolvedMessagePayload struct {
	body       []byte
	resolvedTs hlc.Timestamp
}

// sinkProcessor takes a ThinSink and handles the tasks necessary to emit kv and
// resolved messages to it in a parallel, efficient, and resilient manner.
//
// KV messages arrive through single-threaded calls to EmitRow and are then fanned
// out to a group of worker threads.  Each worker aggregates messages into
// batches and emits them to the underlying sink either when there are enough
// messages batched or enough time has passed.  Batch emits are retried
// according to the given retry.Options.  To maintain per-key ordering, each
// worker only ever has one batch being emitted at a time.
//
// Resolved messages are simply sent to the sink directly, therefore it is
// assumed that EmitResolvedTimestamp is only called _after_ all prior KV events
// have been flushed.
//
// Closing the sinkProcessor will wait for all buffered messages to drain out of
// their worker as no-ops rather than terminating workers immediately, as this
// makes the race-condition-free code easier to reason about.
type sinkProcessor struct {
	ctx context.Context

	sink ThinSink

	flushCfg   batchConfig
	retryOpts  retry.Options
	timeSource timeutil.TimeSource
	keyInValue bool

	numWorkers  int
	eventChans  []chan rowWorkerPayload
	workerGroup ctxgroup.Group

	// sinkProcessor uses a WaitGroup to be able to block until all messages have
	// been flushed to the sink.  It is *crucial* that *every* message that enters
	// EmitRow resulting in an Add() has a matching Done() or Add(-n) such that
	// the inFlight counter is always eventually returned to 0 and calls to Wait()
	// will not block forever.
	inFlight sync.WaitGroup

	// Upon an error or a Close(), closing is atomically set to 1 to signal
	// workers to drain messages out and return inFlight to 0.
	closing int32
	mu      struct {
		syncutil.Mutex
		closeErr error
	}

	metrics metricsRecorder
}

var _ Sink = (*sinkProcessor)(nil) // sinkProcessor should implement Sink

// Messages are sharded across goroutines by hashing the key to ensure that
// per-key ordering is maintained.
func (sp *sinkProcessor) workerIndex(key []byte) uint32 {
	return crc32.ChecksumIEEE(key) % uint32(sp.numWorkers)
}

func defaultSinkProcessorNumWorkers() int {
	// With a webhook sink on a 3 node 16cpu cluster on tpcc.order_line this was
	// observed to produce 222k messages per second without any batching, which is
	// more than enough.  Larger multiples result in more CPU usage.
	//
	// TODO(samiskin): Verify throughput when emits are slower to complete.
	return 5 * system.NumCPU()
}

type sinkProcessorConfig struct {
	numWorkers int
	timeSource timeutil.TimeSource
	flushCfg   batchConfig
	retryOpts  retry.Options
	keyInValue bool
}

func makeSinkProcessor(
	ctx context.Context, sink ThinSink, cfg sinkProcessorConfig, mb metricsRecorderBuilder,
) (Sink, error) {
	if err := validateBatchConfig(cfg.flushCfg); err != nil {
		return nil, err
	}
	var timeSource timeutil.TimeSource = timeutil.DefaultTimeSource{}
	if cfg.timeSource != nil {
		timeSource = cfg.timeSource
	}

	numWorkers := cfg.numWorkers
	if numWorkers == 0 {
		numWorkers = defaultSinkProcessorNumWorkers()
	}

	sp := sinkProcessor{
		ctx:         ctx,
		numWorkers:  numWorkers,
		sink:        sink,
		eventChans:  make([]chan rowWorkerPayload, numWorkers),
		workerGroup: ctxgroup.WithContext(ctx),
		closing:     0,
		timeSource:  timeSource,
		flushCfg:    cfg.flushCfg,
		retryOpts:   cfg.retryOpts,
		keyInValue:  cfg.keyInValue,
		metrics:     mb(requiresResourceAccounting),
	}

	for i := 0; i < sp.numWorkers; i++ {
		i := i
		sp.eventChans[i] = make(chan rowWorkerPayload)
		sp.workerGroup.GoCtx(func(ctx context.Context) error {
			return sp.startRowWorker(i)
		})
	}

	return &sp, nil
}

func (sp *sinkProcessor) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	if atomic.LoadInt32(&sp.closing) != 0 {
		sp.mu.Lock()
		defer sp.mu.Unlock()
		return sp.mu.closeErr
	}

	sp.inFlight.Add(1)

	worker := sp.workerIndex(key)
	sp.eventChans[worker] <- rowWorkerPayload{
		msg: MessagePayload{
			key: key,
			val: value,
		},
		mvcc:  mvcc,
		alloc: alloc,
	}

	sp.metrics.recordMessageSize(int64(len(key) + len(value)))
	return nil
}

func (sp *sinkProcessor) Dial() error {
	return nil
}

type rowWorkerPayload struct {
	msg   MessagePayload
	alloc kvevent.Alloc
	mvcc  hlc.Timestamp
}

func (sp *sinkProcessor) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	defer sp.metrics.recordResolvedCallback()()
	if atomic.LoadInt32(&sp.closing) != 0 {
		sp.mu.Lock()
		defer sp.mu.Unlock()
		return sp.mu.closeErr
	}

	data, err := encoder.EncodeResolvedTimestamp(ctx, "", resolved)
	if err != nil {
		return err
	}
	encoded, err := sp.sink.EncodeResolvedMessage(ResolvedMessagePayload{
		resolvedTs: resolved,
		body:       data,
	})
	if err != nil {
		return err
	}

	sp.inFlight.Add(1)
	defer sp.inFlight.Done()
	defer sp.metrics.recordResolvedCallback()()
	return sp.sendWithRetries(encoded, 1)
}

// startRowWorker creates a rowWorker which handles aggregating individual
// messages into batches which can be forwarded to a separate batchWorker to be
// emitted to the sink.
func (sp *sinkProcessor) startRowWorker(workerIndex int) error {
	// currentBatch tracks the current aggregation of messages prior to flush
	currentBatch := newMessageBatch()

	// TODO(samiskin): Investigate different numbers for this buffer size, this
	// number is just re-using the value from sink_cloudstorage
	batchCh := make(chan batchWorkerMessage, 256)
	defer close(batchCh)

	flushBatch := func() {
		if currentBatch.isEmpty() {
			return
		}

		// Reuse the same batch to avoid need for garbage collection
		defer currentBatch.reset()

		// Process messages into a payload ready to be emitted to the sink
		sinkPayload, err := sp.sink.EncodeBatch(currentBatch.buffer)
		if err != nil {
			defer sp.inFlight.Add(-len(currentBatch.buffer))
			sp.handleError(err)
			return
		}

		// Send the encoded batch to a separate worker so that flushes do not block
		// further message aggregation
		batchCh <- batchWorkerMessage{
			sinkPayload: sinkPayload,
			alloc:       currentBatch.alloc,
			numMessages: len(currentBatch.buffer),
			mvcc:        currentBatch.mvcc,
			kvBytes:     currentBatch.bufferBytes,
		}
	}

	// Start a worker to handle sending / retrying sink payloads
	sp.workerGroup.GoCtx(func(ctx context.Context) error {
		sp.startBatchWorker(batchCh)
		return nil
	})

	flushTimer := sp.timeSource.NewTimer()

	for {
		select {
		case msg, ok := <-sp.eventChans[workerIndex]:
			if !ok {
				flushBatch() // Ensure all batched messages are handled prior to exit.
				return nil
			}

			// If we're closing, allow all messages in the channel to drain out to
			// ensure that inFlight is correctly reset.
			if atomic.LoadInt32(&sp.closing) != 0 {
				sp.inFlight.Done()
				continue
			}

			// If the batch is about to no longer be empty, start the flush timer
			if currentBatch.isEmpty() && time.Duration(sp.flushCfg.Frequency) > 0 {
				flushTimer.Reset(time.Duration(sp.flushCfg.Frequency))
			}
			currentBatch.moveIntoBuffer(msg, sp.keyInValue)

			if sp.shouldFlushBatch(currentBatch) {
				sp.metrics.recordSizeBasedFlush()
				flushBatch()
			}
		case <-flushTimer.Ch():
			flushTimer.MarkRead()
			flushBatch()
		}
	}
}

func (sp *sinkProcessor) shouldFlushBatch(batch messageBatch) bool {
	switch {
	// all zero values is interpreted as flush every time
	case sp.flushCfg.Messages == 0 && sp.flushCfg.Bytes == 0 && sp.flushCfg.Frequency == 0:
		return true
	// messages threshold has been reached
	case sp.flushCfg.Messages > 0 && len(batch.buffer) >= sp.flushCfg.Messages:
		return true
	// bytes threshold has been reached
	case sp.flushCfg.Bytes > 0 && batch.bufferBytes >= sp.flushCfg.Bytes:
		return true
	default:
		return false
	}
}

type batchWorkerMessage struct {
	sinkPayload SinkPayload
	numMessages int
	mvcc        hlc.Timestamp
	alloc       kvevent.Alloc
	kvBytes     int
}

// startBatchWorker creates a batchWorker which handles sending batches of
// messages to the external sink, attempting to retry if needed.
func (sp *sinkProcessor) startBatchWorker(batchCh chan batchWorkerMessage) {
	// Batch handling is in its own function to ensure that sp.inFlight.Done is
	// always called even on error/drain cases.
	handleBatch := func(batch batchWorkerMessage) {
		defer sp.inFlight.Add(-batch.numMessages)

		if atomic.LoadInt32(&sp.closing) != 0 {
			return
		}

		defer sp.metrics.recordFlushRequestCallback()()
		err := sp.sendWithRetries(batch.sinkPayload, batch.numMessages)
		if err != nil {
			sp.handleError(err)
			return
		}

		emitTime := timeutil.Now()
		sp.metrics.recordEmittedBatch(
			emitTime, batch.numMessages, batch.mvcc, batch.kvBytes, sinkDoesNotCompress)

		batch.alloc.Release(sp.ctx)
	}

	for batch := range batchCh {
		handleBatch(batch)
	}
}

func (sp *sinkProcessor) sendWithRetries(payload SinkPayload, numMessages int) error {
	initialSend := true
	return retry.WithMaxAttempts(sp.ctx, sp.retryOpts, sp.retryOpts.MaxRetries+1, func() error {
		if !initialSend {
			sp.metrics.recordInternalRetry(int64(numMessages), false)
		}
		err := sp.sink.EmitPayload(payload)
		initialSend = false
		return err
	})
}

func (sp *sinkProcessor) Flush(ctx context.Context) error {
	sp.inFlight.Wait()
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sp.mu.closeErr != nil {
		return sp.mu.closeErr
	}
	return nil
}

func (sp *sinkProcessor) handleError(err error) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.mu.closeErr = err
	atomic.StoreInt32(&sp.closing, 1)
}

func (sp *sinkProcessor) Close() error {
	atomic.StoreInt32(&sp.closing, 1)
	for _, eventCh := range sp.eventChans {
		close(eventCh)
	}
	_ = sp.workerGroup.Wait()
	return sp.sink.Close()
}

type messageBatch struct {
	buffer      []MessagePayload
	bufferBytes int
	alloc       kvevent.Alloc
	mvcc        hlc.Timestamp
}

func newMessageBatch() messageBatch {
	return messageBatch{
		buffer:      make([]MessagePayload, 0),
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

func (mb *messageBatch) moveIntoBuffer(p rowWorkerPayload, keyInValue bool) {
	mb.buffer = append(mb.buffer, p.msg)
	mb.bufferBytes += len(p.msg.val)

	// Don't double-count the key bytes if the key is included in the value
	if !keyInValue {
		mb.bufferBytes += len(p.msg.key)
	}

	if mb.mvcc.IsEmpty() || p.mvcc.Less(mb.mvcc) {
		mb.mvcc = p.mvcc
	}

	mb.alloc.Merge(&p.alloc)
}

func validateBatchConfig(cfg batchConfig) error {
	if cfg.Messages < 0 || cfg.Bytes < 0 || cfg.Frequency < 0 {
		return errors.Errorf("invalid flush configuration: all values must be non-negative")
	}
	if (cfg.Messages > 0 || cfg.Bytes > 0) && cfg.Frequency == 0 {
		return errors.Errorf("invalid flush configuration: frequency is not set, messages may never be sent")
	}
	return nil
}
