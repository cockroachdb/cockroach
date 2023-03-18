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
	"fmt"
	"hash"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// SinkClient is the interface to an external sink, where batches of messages
// can be encoded into a formatted payload that can be emitted to the sink, and
// these payloads can be emitted.  Emitting is a separate step to Encoding to
// allow for the same encoded payload to retry the Emitting.
type SinkClient interface {
	EncodeResolvedMessage(resolvedMessagePayload) (SinkPayload, error)
	MakeBatchWriter() BatchWriter
	Flush(context.Context, SinkPayload) error

	Close() error
}

type BatchWriter interface {
	AppendKV(messagePayload)
	ShouldFlush() bool
	Close() (SinkPayload, error)
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
	frequency    time.Duration
	ioWorkers    int
	retryOpts    retry.Options
	ts           timeutil.TimeSource
	metrics      metricsRecorder

	flushCh chan struct{}
	mu      struct {
		syncutil.RWMutex
		shouldNotify bool
		termErr      error
		inflight     int64
	}

	wg      ctxgroup.Group
	eventCh chan interface{}
	termCh  chan struct{}
	doneCh  chan struct{}
}

func (bs *batchingSink) incInFlight() {
	cur := atomic.AddInt64(&bs.mu.inflight, 1)
	fmt.Printf("\x1b[31mbatchingSink incInFlight now %d \x1b[0m\n", cur)
}

func (bs *batchingSink) decInFlight(flushed int) {
	bs.mu.RLock()
	remaining := atomic.AddInt64(&bs.mu.inflight, -int64(flushed))
	fmt.Printf("\x1b[31mbatchingSink decInFlight %d remaining \x1b[0m\n", remaining)
	notifyFlush := remaining == 0 && bs.mu.shouldNotify
	bs.mu.RUnlock()
	// If shouldNotify is true, it is assumed that no new Emits could happen,
	// therefore it is not possible for this to occur multiple times for a single
	// Flush call
	if notifyFlush {
		bs.flushCh <- struct{}{}
	}
}

// Flush implements the Sink interface, returning the first error that has
// occured in the past EmitRow calls.
func (bs *batchingSink) Flush(ctx context.Context) error {
	fmt.Printf("\x1b[31mbatchingSink Flush\x1b[0m\n")
	bs.mu.Lock()
	// Since Flush is never called concurrently with EmitRow which is the only
	// place that mutates inflight without taking the lock, inflight can be read
	// and trusted safely
	if bs.mu.inflight == 0 {
		defer bs.mu.Unlock()
		return bs.mu.termErr
	}
	// Signify that flushCh should be emitted to upon inFlight reaching 0
	bs.mu.shouldNotify = true
	bs.mu.Unlock()

	// Send a flush event through the emitter to force any buffered events to
	// flush out immediately.
	bs.eventCh <- flushReq{}

	fmt.Printf("\x1b[31mbatchingSink WAIT Flush\x1b[0m\n")
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-bs.doneCh:
		return nil
	case <-bs.termCh:
		return bs.mu.termErr
	case <-bs.flushCh:
		bs.mu.Lock()
		defer bs.mu.Unlock()
		bs.mu.shouldNotify = false
		return nil
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
	fmt.Printf("\x1b[31mbatchingSink EmitRow %s\x1b[0m\n", string(key))

	var topicName string
	var err error
	if bs.topicNamer != nil {
		topicName, err = bs.topicNamer.Name(topic)
		if err != nil {
			fmt.Printf("\x1b[31mbatchingSink topicErr\x1b[0m\n")
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

	bs.incInFlight()

	fmt.Printf("\x1b[31mbatchingSink SELECT WAIT EmitRow \x1b[0m\n")
	select {
	case <-ctx.Done():
		return ctx.Err()
	case bs.eventCh <- payload:
		fmt.Printf("\x1b[31mbatchingSink sent to eventCh<-\x1b[0m\n")
	case <-bs.doneCh:
		return nil
	case <-bs.termCh:
		return nil
	}

	return nil
}

// EmitResolvedTimestamp implements the Sink interface.
func (bs *batchingSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	fmt.Printf("\x1b[31mbatchingSink EmitResolvedTimestamp\x1b[0m\n")
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

	if err = bs.Flush(ctx); err != nil {
		return err
	}

	// TODO Do this in parallel by sending it through same pipeline with the
	// workers and then flush
	for _, topic := range topics {
		resolvedMessage := resolvedMessagePayload{
			body:  data,
			topic: topic,
		}
		payload, err := bs.client.EncodeResolvedMessage(resolvedMessage)
		if err != nil {
			return err
		}

		err = retry.WithMaxAttempts(ctx, bs.retryOpts, bs.retryOpts.MaxRetries+1, func() error {
			return bs.client.Flush(ctx, payload)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Close implements the Sink interface.
func (bs *batchingSink) Close() error {
	fmt.Printf("\x1b[31mbatchingSink Close\x1b[0m\n")
	close(bs.doneCh)
	fmt.Printf("\x1b[31mbatchingSink wg wait\x1b[0m\n")
	_ = bs.wg.Wait()
	fmt.Printf("\x1b[31mbatchingSink wg wait done\x1b[0m\n")
	return bs.client.Close()
}

// Dial implements the Sink interface.
func (bs *batchingSink) Dial() error {
	fmt.Printf("\x1b[31mbatchingSink Dial\x1b[0m\n")
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

func (bs *batchingSink) handleError(err error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if bs.mu.termErr == nil {
		bs.mu.termErr = err
		close(bs.termCh)
	}
}

func (bs *batchingSink) runBatchingWorker(ctx context.Context) {
	fmt.Printf("\x1b[31mbatchingSink runBatchingWorker\x1b[0m\n")
	pendingBatch := &sinkBatch{
		writer: bs.client.MakeBatchWriter(),
	}

	ioHandler := func(ctx context.Context, req IORequest) error {
		batch, _ := req.(*sinkBatch)
		return bs.client.Flush(ctx, batch.payload)
	}
	ioEmitter := newParallelIO(
		ctx, bs.retryOpts, bs.ioWorkers, ioHandler,
	)
	defer ioEmitter.Close()

	handleResult := func(result ioResult) {
		fmt.Printf("\x1b[31mbatchingSink HANDLE RESULT runBatchingWorker\x1b[0m\n")
		batch, _ := result.payload.(*sinkBatch)
		defer bs.decInFlight(batch.numMessages)
		defer batch.alloc.Release(ctx)
		if result.err != nil {
			fmt.Printf("\x1b[31mbatchingSink EMIT ERR runBatchingWorker\x1b[0m\n")
			bs.handleError(result.err)
		}
	}

	tryFlushBatch := func() {
		if pendingBatch.isEmpty() {
			return
		}
		fmt.Printf("\x1b[35mbatchingSink flushBatch\x1b[0m\n")
		payload, err := pendingBatch.writer.Close()
		if err != nil {
			bs.handleError(err)
			return
		}
		pendingBatch.payload = payload
		fmt.Printf("\x1b[31mbatchingSink FLUSH WAIT runBatchingWorker\x1b[0m\n")
	L:
		for {
			select {
			case <-ctx.Done():
				break L
			case ioEmitter.requestCh <- pendingBatch:
				fmt.Printf("\x1b[31mbatchingSink sent to requestCh\x1b[0m\n")
				break L
			case result := <-ioEmitter.resultCh:
				handleResult(result)
			case <-bs.doneCh:
				break L
			case <-bs.termCh:
				break L
			}
		}
		pendingBatch = &sinkBatch{
			writer: bs.client.MakeBatchWriter(),
		}
	}

	flushTimer := bs.ts.NewTimer()
	defer flushTimer.Stop()

	defer fmt.Printf("\x1b[31mbatchingSink DONE runBatchingWorker\x1b[0m\n")
	for {
		fmt.Printf("\x1b[31mbatchingSink FOR WAIT runBatchingWorker\x1b[0m\n")
		select {
		case req := <-bs.eventCh:
			if _, isFlush := req.(flushReq); isFlush {
				fmt.Printf("\x1b[31mbatchingSink receive FlushReq <-eventCh\x1b[0m\n")
				tryFlushBatch()
				continue
			} else if event, isKV := req.(*kvEvent); isKV {
				fmt.Printf("\x1b[31mbatchingSink receive KV <-eventCh\x1b[0m\n")
				if pendingBatch.isEmpty() && bs.frequency > 0 {
					flushTimer.Reset(bs.frequency)
				}
				pendingBatch.Append(event)

				if pendingBatch.writer.ShouldFlush() {
					tryFlushBatch()
				}
			}
		case result := <-ioEmitter.resultCh:
			handleResult(result)
		case <-flushTimer.Ch():
			fmt.Printf("\x1b[35mbatchingSink FLUSHTIMER FIRE runBatchingWorker\x1b[0m\n")
			flushTimer.MarkRead()
			tryFlushBatch()
		case <-ctx.Done():
			fmt.Printf("\x1b[31mbatchingSink ctx done runBatchingWorker\x1b[0m\n")
			return
		case <-bs.doneCh:
			fmt.Printf("\x1b[31mbatchingSink doneCh runBatchingWorker\x1b[0m\n")
			return
		case <-bs.termCh:
			fmt.Printf("\x1b[31mbatchingSink termCh runBatchingWorker\x1b[0m\n")
			return
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
type IOHandler func(context.Context, IORequest) error

type parallelIO struct {
	retryOpts retry.Options
	wg        ctxgroup.Group
	ioHandler IOHandler
	resultCh  chan ioResult
	requestCh chan IORequest
	doneCh    chan struct{}
}

func (pe *parallelIO) Close() {
	// if err := pe.wg.Wait(); err != nil {
	// 	return err
	// }
	fmt.Printf("\x1b[32mparallelIO wg wait\x1b[0m\n")
	_ = pe.wg.Wait()
	fmt.Printf("\x1b[32mparallelIO wg wait done\x1b[0m\n")
	close(pe.resultCh)
}

func newParallelIO(
	ctx context.Context,
	retryOpts retry.Options,
	numWorkers int,
	handler IOHandler,
) *parallelIO {
	wg := ctxgroup.WithContext(ctx)
	io := &parallelIO{
		retryOpts: retryOpts,
		wg:        wg,
		ioHandler: handler,
		requestCh: make(chan IORequest, numWorkers),
		resultCh:  make(chan ioResult, numWorkers),
		doneCh:    make(chan struct{}),
	}

	wg.GoCtx(func(ctx context.Context) error {
		return io.runWorkers(ctx, numWorkers)
	})

	return io
}

func (pe *parallelIO) runWorkers(ctx context.Context, numEmitWorkers int) error {
	emitWithRetries := func(ctx context.Context, payload IORequest) error {
		fmt.Printf("\x1b[32mparallelIO emitWithRetries\x1b[0m\n")
		defer fmt.Printf("\x1b[34mparallelIO SENT\x1b[0m\n")
		return retry.WithMaxAttempts(ctx, pe.retryOpts, pe.retryOpts.MaxRetries+1, func() error {
			return pe.ioHandler(ctx, payload)
		})
	}

	//

	emitCh := make(chan IORequest, numEmitWorkers)
	defer close(emitCh)

	emitResultCh := make(chan ioResult, numEmitWorkers)
	var handleResult func(ioResult)

	submitPayload := func(ctx context.Context, req IORequest) {
		for {
			select {
			case <-ctx.Done():
				return
			case <-pe.doneCh:
				return

				// In order to send on emitCh we have to also check for receive on
				// emitResultCh to avoid all emit workers being blocked on
				// emitResultCh<- and therefore being unable to read from emitCh
			case emitCh <- req:
				return
			case res := <-emitResultCh:
				handleResult(res)
			}
		}
	}

	emitRequest := func(ctx context.Context, req IORequest) {
		fmt.Printf("\x1b[32mparallelIO EMIT REQUEST\x1b[0m\n")
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
			fmt.Printf("\x1b[32mparallelIO WORKER WAIT\x1b[0m\n")
			for req := range emitCh {
				emitRequest(ctx, req)
				fmt.Printf("\x1b[32mparallelIO WORKER WAIT\x1b[0m\n")
			}
			return nil
		})
	}

	//

	var inflight intsets.Fast
	var pending []IORequest

	handleResult = func(res ioResult) {
		fmt.Printf("\x1b[32mparallelIO HANDLE RESULT\x1b[0m\n")
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
					fmt.Printf("\x1b[32mparallelIO resub\x1b[0m\n")
					submitPayload(ctx, pendingReq)
				}
			}
			pending = stillPending
		}

		select {
		case <-ctx.Done():
		case <-pe.doneCh:
		case pe.resultCh <- res:
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pe.doneCh:
			return nil
		case req := <-pe.requestCh:
			fmt.Printf("\x1b[32mparallelIO requestCh\x1b[0m\n")
			if inflight.Intersects(req.Keys()) {
				fmt.Printf("\x1b[32mparallelIO pending\x1b[0m\n")
				pending = append(pending, req)
			} else {
				inflight.UnionWith(req.Keys())
				fmt.Printf("\x1b[32mparallelIO sub\x1b[0m\n")
				submitPayload(ctx, req)
			}
		case res := <-emitResultCh:
			handleResult(res)
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
	timeSource timeutil.TimeSource,
	metrics metricsRecorder,
) Sink {
	sink := &batchingSink{
		client:       client,
		topicNamer:   topicNamer,
		concreteType: concreteType,
		frequency:    minFlushFrequency,
		ioWorkers:    1,
		retryOpts:    retryOpts,
		ts:           timeSource,
		metrics:      metrics,
		flushCh:      make(chan struct{}, 1),
		wg:           ctxgroup.WithContext(ctx),
		eventCh:      make(chan interface{}),
		termCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
	}

	sink.wg.GoCtx(func(ctx context.Context) error {
		sink.runBatchingWorker(ctx)
		return nil
	})
	return sink
}
