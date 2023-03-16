// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// SinkClient is the interface to an external sink, where batches of messages
// can be encoded into a formatted payload that can be emitted to the sink, and
// these payloads can be emitted.  Emitting is a separate step to Encoding to
// allow for the same encoded payload to retry the Emitting.
type SinkClient interface {
	// TODO this might end up being a list of KVEvent/RowEvents
	EncodeBatch(topic string, batch []messagePayload) (SinkPayload, error)
	EncodeResolvedMessage(resolvedMessagePayload) (SinkPayload, error)
	EmitPayload(SinkPayload) error

	MakeBatchWriter() BatchWriter
	Flush(context.Context, SinkPayload) error

	Close() error
}

type BatchWriter interface {
	AppendKV(messagePayload)
	ShouldFlush() bool
	Close() SinkPayload
}

// SinkPayload is an interface representing a sink-specific representation of a
// batch of messages that is ready to be emitted by its EmitRow method.
type SinkPayload interface{}

// messagePayload represents a KV event to be emitted.
type messagePayload struct {
	key   []byte
	val   []byte
	topic string
}

// resolvedMessagePayload represents a Resolved event to be emitted.
type resolvedMessagePayload struct {
	body  []byte
	topic string
}

type flushReq struct{}

type kvEvent struct {
	msg   messagePayload
	alloc kvevent.Alloc
	mvcc  hlc.Timestamp
}

var kvEventPool = sync.Pool{
	New: func() interface{} {
		return new(kvEvent)
	},
}

func newKVEvent() *kvEvent {
	return kvEventPool.Get().(*kvEvent)
}
func freeKVEvent(e *kvEvent) {
	*e = kvEvent{}
	kvEventPool.Put(e)
}

type batchingSink struct {
	client       SinkClient
	topicNamer   *TopicNamer
	concreteType sinkType
	ioWorkers    int
	retryOpts    retry.Options
	timeSource   timeutil.TimeSource
	metrics      metricsRecorder

	eventCh chan interface{}
	doneCh  chan struct{}
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
	var topicName string
	var err error
	if bs.topicNamer != nil {
		topicName, err = bs.topicNamer.Name(topic)
		if err != nil {
			return err
		}
	}

	payload := newKVEvent()
	payload.msg = messagePayload{
		key:   key,
		val:   value,
		topic: topicName,
	}
	payload.mvcc = mvcc
	payload.alloc = alloc
	// inc in flihgt

	// var topicName string
	// var err error
	// if bs.topicNamer != nil {
	// 	topicName, err = bs.topicNamer.Name(topic)
	// 	if err != nil {
	// 		return err
	// 	}
	// }

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

	var topics []string
	if bs.topicNamer == nil {
		topics = []string{""}
	} else {
		topics = bs.topicNamer.DisplayNamesSlice()
	}

	bs.Flush(ctx)

	// TODO Do this in parallel by sending it through same pipeline with the
	// workers and then flush
	for _, topic := range topics {
		resolvedMessage := resolvedMessagePayload{
			body:  data,
			topic: topic,
		}
		payload, err := bs.client.EncodeResolvedMessage(resolvedMessage)
		if err != nil {
			return nil
		}
		bs.client.EmitPayload(payload)
	}
	return nil
}

// Flush implements the Sink interface.
func (bs *batchingSink) Flush(ctx context.Context) error {
	// return bs.emitter.Flush()
	bs.eventCh <- flushReq{}
	return nil
}

// Close implements the Sink interface.
func (bs *batchingSink) Close() error {
	// TODO
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

type sinkBatch struct {
	writer  BatchWriter
	payload SinkPayload

	numMessages int
	numBytes    int
	keys        intsets.Fast
	alloc       kvevent.Alloc
	mvcc        hlc.Timestamp
	bufferTime  time.Time // The earliest time a message was inserted into the batch
}

var hasher = makeHasher()

func (mb *sinkBatch) Keys() intsets.Fast {
	return mb.keys
}

func newMessageBatch(writer BatchWriter) sinkBatch {
	return sinkBatch{
		writer:   writer,
		numBytes: 0,
	}
}

func (mb *sinkBatch) isEmpty() bool {
	return mb.numMessages == 0
}

func hash32(h hash.Hash32, buf []byte) uint32 {
	h.Reset()
	h.Write(buf)
	return h.Sum32()
}

// Append adds the contents of a sinkEvent to the batch, merging its alloc pool
func (mb *sinkBatch) Append(e *kvEvent) {
	if mb.isEmpty() {
		mb.bufferTime = timeutil.Now()
	}

	mb.keys.Add(int(hash32(hasher, e.msg.key)))

	mb.writer.AppendKV(e.msg)
	mb.numMessages += 1
	mb.numBytes += len(e.msg.val)
	mb.numBytes += len(e.msg.key)

	if mb.mvcc.IsEmpty() || e.mvcc.Less(mb.mvcc) {
		mb.mvcc = e.mvcc
	}

	mb.alloc.Merge(&e.alloc)
	freeKVEvent(e)
}

// func (bs *batchingSink) shouldFlushBatch(batch *sinkBatch) bool {
// 	switch {
// 	// all zero values is interpreted as flush every time
// 	case bs.batchCfg.Messages == 0 && bs.batchCfg.Bytes == 0 && bs.batchCfg.Frequency == 0:
// 		return true
// 	// messages threshold has been reached
// 	case bs.batchCfg.Messages > 0 && batch.numMessages >= bs.batchCfg.Messages:
// 		return true
// 	// bytes threshold has been reached
// 	case bs.batchCfg.Bytes > 0 && batch.numBytes >= bs.batchCfg.Bytes:
// 		return true
// 	default:
// 		return false
// 	}
// }

func (bs *batchingSink) runBatchingWorker(ctx context.Context) {

	pendingBatch := &sinkBatch{}

	submitIOCh := make(chan IORequest)
	ioHandler := func(ctx context.Context, payload interface{}) error {
		batch, ok := payload.(*sinkBatch)
		if !ok {
			return errors.New("could not cast batch")
		}
		return bs.client.Flush(ctx, batch.payload)
	}
	ioEmitter := makeParallelIOEmitter(
		ctx, bs.retryOpts, bs.ioWorkers, ioHandler, submitIOCh,
	)
	defer close(submitIOCh)
	defer ioEmitter.Close()

	flushBatch := func() {
		pendingBatch.payload = pendingBatch.writer.Close()
		select {
		case <-ctx.Done():
		case submitIOCh <- pendingBatch:
		}
		pendingBatch = &sinkBatch{}
	}

	for {
		select {
		case req := <-bs.eventCh:
			if _, isFlush := req.(flushReq); isFlush {
				flushBatch()
				continue
			} else if event, isKV := req.(*kvEvent); isKV {
				pendingBatch.Append(event)
				if pendingBatch.writer.ShouldFlush() {
					flushBatch()
				}
			}
		case result := <-ioEmitter.resultCh:
			if result.err != nil {
				// set err
				return
			}

			batch, ok := result.payload.(*sinkBatch)
			if !ok {
				// set err
				return
			}
			batch.alloc.Release(ctx)
		case <-ctx.Done():
			break
		case <-bs.doneCh:
			break
		}
	}
}

type IORequest interface {
	Keys() intsets.Fast
}
type ioResult struct {
	payload IORequest
	err     error
}
type IOHandler func(context.Context, interface{}) error

type parallelIO struct {
	retryOpts retry.Options
	wg        ctxgroup.Group
	ioHandler IOHandler
	resultCh  chan ioResult
}

func (pe *parallelIO) Close() error {
	if err := pe.wg.Wait(); err != nil {
		return err
	}
	close(pe.resultCh)
	return nil
}

func makeParallelIOEmitter(
	ctx context.Context,
	retryOpts retry.Options,
	numWorkers int,
	handler IOHandler,
	requestCh <-chan IORequest,
) *parallelIO {
	wg := ctxgroup.WithContext(ctx)
	emitter := &parallelIO{
		retryOpts: retryOpts,
		wg:        wg,
		ioHandler: handler,
		resultCh:  make(chan ioResult, numWorkers),
	}

	wg.GoCtx(func(ctx context.Context) error {
		return emitter.runWorkers(ctx, requestCh, numWorkers)
	})

	return emitter
}

func (pe *parallelIO) runWorkers(ctx context.Context, requestCh <-chan IORequest, numEmitWorkers int) error {
	emitWithRetries := func(ctx context.Context, payload IORequest) error {
		return retry.WithMaxAttempts(ctx, pe.retryOpts, pe.retryOpts.MaxRetries+1, func() error {
			return pe.ioHandler(ctx, payload)
		})
	}

	//

	emitCh := make(chan IORequest, numEmitWorkers)
	submitPayload := func(ctx context.Context, req IORequest) {
		select {
		case <-ctx.Done():
		case emitCh <- req:
		}
	}
	defer close(emitCh)

	emitResultCh := make(chan ioResult, numEmitWorkers)
	submitResult := func(ctx context.Context, req IORequest) {
		select {
		case <-ctx.Done():
		case emitResultCh <- ioResult{
			err:     emitWithRetries(ctx, req),
			payload: req,
		}:
		}
	}

	for i := 0; i < numEmitWorkers; i++ {
		pe.wg.GoCtx(func(ctx context.Context) error {
			for req := range emitCh {
				submitResult(ctx, req)
			}
			return nil
		})
	}

	//

	var inflight intsets.Fast
	var pending []IORequest

	handleResult := func(res ioResult) {
		if res.err == nil {
			// Clear out the completed keys
			inflight.DifferenceWith(res.payload.Keys())

			// Submit any now-compliant pending work
			var stillPending []IORequest
			for _, pendingReq := range pending {
				if inflight.Intersects(pendingReq.Keys()) {
					stillPending = append(stillPending, pendingReq)
				} else {
					inflight.UnionWith(pendingReq.Keys())
					submitPayload(ctx, pendingReq)
				}
			}
			pending = stillPending
		}

		select {
		case <-ctx.Done():
		case pe.resultCh <- res:
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case req := <-requestCh:
			if inflight.Intersects(req.Keys()) {
				pending = append(pending, req)
			} else {
				inflight.UnionWith(req.Keys())
				submitPayload(ctx, req)

				// In order to send on emitCh we have to also check for receive on
				// emitResultCh to avoid all emit workers being blocked on
				// emitResultCh<- and therefore being unable to read from emitCh
				select {
				case <-ctx.Done():
				case emitCh <- req:
				case res := <-emitResultCh:
					handleResult(res)
				}
			}
		case res := <-emitResultCh:
			handleResult(res)
		}
	}
	return nil
}

func makeBatchingSink(
	ctx context.Context,
	concreteType sinkType,
	client SinkClient,
	retryOpts retry.Options,
	numWorkers int64,
	topicNamer *TopicNamer,
	timeSource timeutil.TimeSource,
	metrics metricsRecorder,
) Sink {
	sink := &batchingSink{
		client:       client,
		topicNamer:   topicNamer,
		concreteType: concreteType,
		ioWorkers:    int(numWorkers),
		retryOpts:    retryOpts,
		timeSource:   timeSource,
		metrics:      metrics,
	}
	sink.runBatchingWorker(ctx)
	return sink
}
