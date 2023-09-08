// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// request is any action on processor which is not a data path. e.g. request
// active filter, length, add registration etc.
// This request type is only serving as execution mechanism (think RunAsyncTask).
// Processor state is exclusively updated by the running request, the only
// concurrent activity that could happen is enqueueing events that is handled by
// data and request queues independently.
// To execute a request that returns a value, use runRequest that accepts
// function that returns value.
type request func(context.Context)

// ScheduledProcessor is an implementation of processor that uses external
// scheduler to use processing.
type ScheduledProcessor struct {
	Config
	scheduler ClientScheduler

	reg nonBufferedRegistry
	rts resolvedTimestamp

	requestQueue chan request
	eventC       chan *event
	// If true, processor is not processing data anymore and waiting for registrations
	// to be complete.
	stopping bool
	stoppedC chan struct{}

	// Processor startup runs background tasks to scan intents. If processor is
	// stopped early, this task needs to be terminated to avoid resource waste.
	startupCancel func()
	// stopper passed by start that is used for firing up async work from scheduler.
	stopper       *stop.Stopper
	txnPushActive bool
}

// NewScheduledProcessor creates a new scheduler based rangefeed Processor.
// Processor needs to be explicitly started after creation.
func NewScheduledProcessor(cfg Config) *ScheduledProcessor {
	cfg.SetDefaults()
	cfg.AmbientContext.AddLogTag("rangefeed", nil)
	p := &ScheduledProcessor{
		Config:    cfg,
		scheduler: NewClientScheduler(cfg.Scheduler),
		reg:       makeNonBufferedRegistry(cfg.Metrics),
		rts:       makeResolvedTimestamp(),

		requestQueue: make(chan request, 20),
		eventC:       make(chan *event, cfg.EventChanCap),
		// Closed when scheduler removed callback.
		stoppedC: make(chan struct{}),
	}
	return p
}

// Start performs processor one-time initialization e.g registers with
// scheduler and fires up background tasks to populate processor state.
// The provided iterator is used to initialize the rangefeed's resolved
// timestamp. It must obey the contract of an iterator used for an
// initResolvedTSScan. The Processor promises to clean up the iterator by
// calling its Close method when it is finished. If the iterator is nil then
// no initialization scan will be performed and the resolved timestamp will
// immediately be considered initialized.
func (p *ScheduledProcessor) Start(stopper *stop.Stopper, rtsIter IntentScanner) error {
	ctx := p.Config.AmbientContext.AnnotateCtx(context.Background())
	ctx, p.startupCancel = context.WithCancel(ctx)
	p.stopper = stopper

	// Note that callback registration must be performed before starting resolved
	// timestamp init because resolution posts resolvedTS event when it is done.
	if err := p.scheduler.Register(p.process); err != nil {
		if rtsIter != nil {
			rtsIter.Close()
		}
		p.cleanup()
		return err
	}

	// Launch an async task to scan over the resolved timestamp iterator and
	// initialize the unresolvedIntentQueue.
	if rtsIter != nil {
		initScan := newInitResolvedTSScan(p.Span, p, rtsIter)
		// TODO(oleg): we need to cap number of tasks that we can fire up across
		// all feeds as they could potentially generate O(n) tasks during start.
		if err := stopper.RunAsyncTask(ctx, "rangefeed: init resolved ts", initScan.Run); err != nil {
			initScan.Cancel()
			p.scheduler.StopProcessor()
			return err
		}
	} else {
		p.initResolvedTS(ctx)
	}
	return nil
}

// process is a scheduler callback that is processing scheduled events and
// requests.
func (p *ScheduledProcessor) process(e processorEventType) processorEventType {
	ctx := p.Config.AmbientContext.AnnotateCtx(context.Background())
	if e&RequestQueued != 0 {
		p.processRequests(ctx)
	}
	if e&EventQueued != 0 {
		p.processEvents(ctx)
	}
	if e&PushTxnQueued != 0 {
		p.processPushTxn(ctx)
	}
	if e&Stopped != 0 {
		p.processStop()
	}
	return 0
}

// process pending requests.
func (p *ScheduledProcessor) processRequests(ctx context.Context) {
	// No need to limit number of processed requests as we don't expect more than
	// a handful requests within the whole lifecycle.
	for {
		select {
		case e := <-p.requestQueue:
			e(ctx)
		default:
			return
		}
	}
}

// Transform and route pending events.
func (p *ScheduledProcessor) processEvents(ctx context.Context) {
	// TODO(oleg): maybe limit max count and allow returning some data for
	// further processing on next iteration.
	// Only process as much data as was present at the start of the processing
	// run to avoid starving other processors.
	for max := len(p.eventC); max > 0; max-- {
		select {
		case e := <-p.eventC:
			if !p.stopping {
				// If we are stopping, there's no need to forward any remaining
				// data since registrations already have errors set.
				p.consumeEvent(ctx, e)
			}
			e.alloc.Release(ctx)
			putPooledEvent(e)
		default:
			return
		}
	}
}

func (p *ScheduledProcessor) processPushTxn(ctx context.Context) {
	if !p.txnPushActive && p.rts.IsInit() {
		now := p.Clock.Now()
		before := now.Add(-p.PushTxnsAge.Nanoseconds(), 0)
		oldTxns := p.rts.intentQ.Before(before)

		if len(oldTxns) > 0 {
			toPush := make([]enginepb.TxnMeta, len(oldTxns))
			for i, txn := range oldTxns {
				toPush[i] = txn.asTxnMeta()
			}

			// Launch an async transaction push attempt that pushes the
			// timestamp of all transactions beneath the push offset.
			// Ignore error if quiescing.
			pushTxns := newTxnPushAttempt(p.Span, p.TxnPusher, p, toPush, now, func() {
				p.enqueueRequest(func(ctx context.Context) {
					p.txnPushActive = false
				})
			})
			p.txnPushActive = true
			// TODO(oleg): we need to cap number of tasks that we can fire up across
			// all feeds as they could potentially generate O(n) tasks for push.
			err := p.stopper.RunAsyncTask(ctx, "rangefeed: pushing old txns", pushTxns.Run)
			if err != nil {
				pushTxns.Cancel()
			}
		}
	}
}

func (p *ScheduledProcessor) processStop() {
	p.cleanup()
}

func (p *ScheduledProcessor) cleanup() {
	// Disconnect all registrations unconditionally first. This is only
	// permissible when shutting down because we don't wait for registrations to
	// free up all the resources.
	pErr := kvpb.NewError(&kvpb.NodeUnavailableError{})
	p.reg.DisconnectWithErr(all, pErr)

	// Unregister callback from scheduler
	p.scheduler.Unregister()

	p.startupCancel()
	close(p.stoppedC)
	p.MemBudget.Close(context.Background())
}

// Stop shuts down the processor and closes all registrations. Safe to call on
// nil Processor. It is not valid to restart a processor after it has been
// stopped.
func (p *ScheduledProcessor) Stop() {
	p.StopWithErr(nil)
}

// StopWithErr shuts down the processor and closes all registrations with the
// specified error. Safe to call on nil Processor. It is not valid to restart a
// processor after it has been stopped.
func (p *ScheduledProcessor) StopWithErr(pErr *kvpb.Error) {
	// Flush any remaining events before stopping.
	p.syncEventC()
	// Send the processor a stop signal.
	p.sendStop(pErr)
}

// DisconnectSpanWithErr disconnects all rangefeed registrations that overlap
// the given span with the given error.
func (p *ScheduledProcessor) DisconnectSpanWithErr(span roachpb.Span, pErr *kvpb.Error) {
	if p == nil {
		return
	}
	p.enqueueRequest(func(ctx context.Context) {
		p.reg.DisconnectWithErr(span, pErr)
	})
}

func (p *ScheduledProcessor) sendStop(pErr *kvpb.Error) {
	p.enqueueRequest(func(ctx context.Context) {
		p.reg.DisconnectWithErr(all, pErr)
		// First set stopping flag to ensure that once all registrations are removed
		// processor should stop.
		p.stopping = true
		p.scheduler.StopProcessor()
	})
}

// Register registers the stream over the specified span of keys.
//
// The registration will not observe any events that were consumed before this
// method was called. It is undefined whether the registration will observe
// events that are consumed concurrently with this call. The channel will be
// provided an error when the registration closes.
//
// The optionally provided "catch-up" iterator is used to read changes from the
// engine which occurred after the provided start timestamp (exclusive).
//
// If the method returns false, the processor will have been stopped, so calling
// Stop is not necessary. If the method returns true, it will also return an
// updated operation filter that includes the operations required by the new
// registration.
//
// NB: startTS is exclusive; the first possible event will be at startTS.Next().
func (p *ScheduledProcessor) Register(
	span roachpb.RSpan,
	startTS hlc.Timestamp,
	catchUpIterConstructor CatchUpIteratorConstructor,
	withDiff bool,
	_ NewStream,
	newBufferedStream NewBufferedStream,
	disconnectFn func(),
) (bool, *Filter) {
	// Synchronize the event channel so that this registration doesn't see any
	// events that were consumed before this registration was called. Instead,
	// it should see these events during its catch up scan.
	p.syncEventC()

	filter := runRequest(p, func(ctx context.Context, p *ScheduledProcessor) *Filter {
		if p.stopping {
			return nil
		}

		regSpan := span.AsRawSpanWithNoLocals()
		if !p.Span.AsRawSpanWithNoLocals().Contains(regSpan) {
			log.Fatalf(ctx, "registration span %v not in Processor's key range %v", regSpan, p.Span)
		}

		r := newNonBufferedRegistration(regSpan, startTS, withDiff, p.Metrics)

		// This callback is notified by external worker that is responsible for
		// handling buffered stream. We can't rely on this being called on processor
		// scheduler and we can't make assumptions if it would be called only after
		// this registration request is complete.
		regDrained := func() {
			// It's ok to stop unconnected registration.
			r.cancelCatchUp()
			if p.unregisterClient(r) {
				// disconnectFn callback is provided by replica to tear down processors
				// that have zero registrations left and to update event filters.
				if disconnectFn != nil {
					disconnectFn()
				}
			}
		}

		// First construct a stream, we might need to send an error if we fail
		// to construct registration for any reason.
		stream := newBufferedStream(regDrained)

		var catchUpIter *CatchUpIterator
		if catchUpIterConstructor != nil {
			var err error
			if catchUpIter, err = catchUpIterConstructor(regSpan, startTS); err != nil {
				// We don't have a registration ready yet, must fall back to sending error
				// directly. It is fine to receive drained notification at this point as
				// we have appropriate checks.
				stream.SendError(kvpb.NewError(err))
				return nil
			}
		}

		// Connect registration and check if we were not disconnected by stream
		// in between.
		if !r.connect(stream, p.Config.EventChanCap, catchUpIter) {
			// Stream was cancelled, we can just abandon the request here as we didn't
			// acquire any resources.
			return nil
		}

		// Add the new registration to the registry.
		p.reg.Register(r)

		// Prep response with filter that includes the new registration.
		f := p.reg.NewFilter()

		// Immediately publish a checkpoint event to the registry. This will be the first event
		// published to this registration after its initial catch-up scan completes. The resolved
		// timestamp might be empty but the checkpoint event is still useful to indicate that the
		// catch-up scan has completed. This allows clients to rely on stronger ordering semantics
		// once they observe the first checkpoint event.
		r.publish(ctx, p.newCheckpointEvent(), nil)

		// Run an output loop for the registry.
		runCatchupScan := func(ctx context.Context) {
			r.runCatchupScan(ctx, p.RangeID)
		}
		if err := p.Stopper.RunAsyncTask(ctx, "rangefeed: output loop", runCatchupScan); err != nil {
			// If we can't schedule internally, processor is already stopped which
			// could only happen on shutdown.
			r.abortAndDisconnectNonStarted(kvpb.NewError(err))
			p.reg.Unregister(r)
			return nil
		}
		return f
	})

	if filter != nil {
		return true, filter
	}
	return false, nil
}

func (p *ScheduledProcessor) unregisterClient(r *nonBufferedRegistration) bool {
	return runRequest(p, func(ctx context.Context, p *ScheduledProcessor) bool {
		p.reg.Unregister(r)
		return true
	})
}

// ConsumeLogicalOps informs the rangefeed processor of the set of logical
// operations. It returns false if consuming the operations hit a timeout, as
// specified by the EventChanTimeout configuration. If the method returns false,
// the processor will have been stopped, so calling Stop is not necessary. Safe
// to call on nil Processor.
func (p *ScheduledProcessor) ConsumeLogicalOps(
	ctx context.Context, ops ...enginepb.MVCCLogicalOp,
) bool {
	if p == nil {
		return true
	}
	if len(ops) == 0 {
		return true
	}
	return p.sendEvent(ctx, event{ops: ops}, p.EventChanTimeout)
}

// ConsumeSSTable informs the rangefeed processor of an SSTable that was added
// via AddSSTable. It returns false if consuming the SSTable hit a timeout, as
// specified by the EventChanTimeout configuration. If the method returns false,
// the processor will have been stopped, so calling Stop is not necessary. Safe
// to call on nil Processor.
func (p *ScheduledProcessor) ConsumeSSTable(
	ctx context.Context, sst []byte, sstSpan roachpb.Span, writeTS hlc.Timestamp,
) bool {
	if p == nil {
		return true
	}
	return p.sendEvent(ctx, event{sst: &sstEvent{sst, sstSpan, writeTS}}, p.EventChanTimeout)
}

// ForwardClosedTS indicates that the closed timestamp that serves as the basis
// for the rangefeed processor's resolved timestamp has advanced. It returns
// false if forwarding the closed timestamp hit a timeout, as specified by the
// EventChanTimeout configuration. If the method returns false, the processor
// will have been stopped, so calling Stop is not necessary.  Safe to call on
// nil Processor.
func (p *ScheduledProcessor) ForwardClosedTS(ctx context.Context, closedTS hlc.Timestamp) bool {
	if p == nil {
		return true
	}
	if closedTS.IsEmpty() {
		return true
	}
	return p.sendEvent(ctx, event{ct: ctEvent{closedTS}}, p.EventChanTimeout)
}

// sendEvent informs the Processor of a new event. If a timeout is specified,
// the method will wait for no longer than that duration before giving up,
// shutting down the Processor, and returning false. 0 for no timeout.
func (p *ScheduledProcessor) sendEvent(ctx context.Context, e event, timeout time.Duration) bool {
	if p.enqueueEventInternal(ctx, e, timeout) {
		// We can ignore the event because we don't guarantee that we will drain
		// all the events after processor was stopped. Memory budget will also be
		// closed, releasing info about pending events that would be discarded with
		// processor.
		p.scheduler.Enqueue(EventQueued)
		return true
	}
	return false
}

func (p *ScheduledProcessor) enqueueEventInternal(
	ctx context.Context, e event, timeout time.Duration,
) bool {
	// The code is a bit unwieldy because we try to avoid any allocations on fast
	// path where we have enough budget and outgoing channel is free. If not, we
	// try to set up timeout for acquiring budget and then reuse this timeout when
	// inserting value into channel.
	var alloc *SharedBudgetAllocation
	if p.MemBudget != nil {
		size := calculateDateEventSize(e)
		if size > 0 {
			var err error
			// First we will try non-blocking fast path to allocate memory budget.
			alloc, err = p.MemBudget.TryGet(ctx, size)
			// If budget is already closed, then just let it through because processor
			// is terminating.
			if err != nil && !errors.Is(err, budgetClosedError) {
				// Since we don't have enough budget, we should try to wait for
				// allocation returns before failing.
				if timeout > 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, timeout) // nolint:context
					defer cancel()
					// We reset timeout here so that subsequent channel write op doesn't
					// try to wait beyond what is already set up.
					timeout = 0
				}
				p.Metrics.RangeFeedBudgetBlocked.Inc(1)
				alloc, err = p.MemBudget.WaitAndGet(ctx, size)
			}
			if err != nil && !errors.Is(err, budgetClosedError) {
				p.Metrics.RangeFeedBudgetExhausted.Inc(1)
				p.sendStop(newErrBufferCapacityExceeded())
				return false
			}
			// Always release allocation pointer after sending as it is nil safe.
			// In normal case its value is moved into event, in case of allocation
			// errors it is nil, in case of send errors it is non-nil and this call
			// ensures that unused allocation is released.
			defer func() {
				alloc.Release(ctx)
			}()
		}
	}
	ev := getPooledEvent(e)
	ev.alloc = alloc
	if timeout == 0 {
		// Timeout is zero if no timeout was requested or timeout is already set on
		// the context by budget allocation. Just try to write using context as a
		// timeout.
		select {
		case p.eventC <- ev:
			// Reset allocation after successful posting to prevent deferred cleanup
			// from freeing it (see comment on defer for explanation).
			alloc = nil
		case <-p.stoppedC:
			// Already stopped. Do nothing.
		case <-ctx.Done():
			p.sendStop(newErrBufferCapacityExceeded())
			return false
		}
	} else {
		// First try fast path operation without blocking and without creating any
		// contexts in case channel has capacity.
		select {
		case p.eventC <- ev:
			// Reset allocation after successful posting to prevent deferred cleanup
			// from freeing it (see comment on defer for explanation).
			alloc = nil
		case <-p.stoppedC:
			// Already stopped. Do nothing.
		default:
			// Fast path failed since we don't have capacity in channel. Wait for
			// slots to clear up using context timeout.
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout) // nolint:context
			defer cancel()
			select {
			case p.eventC <- ev:
				// Reset allocation after successful posting to prevent deferred cleanup
				// from freeing it  (see comment on defer for explanation).
				alloc = nil
			case <-p.stoppedC:
				// Already stopped. Do nothing.
			case <-ctx.Done():
				// Sending on the eventC channel would have blocked.
				// Instead, tear down the processor and return immediately.
				p.sendStop(newErrBufferCapacityExceeded())
				return false
			}
		}
	}
	return true
}

// setResolvedTSInitialized informs the Processor that its resolved timestamp has
// all the information it needs to be considered initialized.
func (p *ScheduledProcessor) setResolvedTSInitialized(ctx context.Context) {
	p.sendEvent(ctx, event{initRTS: true}, 0)
}

// syncEventC synchronizes access to the Processor goroutine, allowing the
// caller to establish causality with actions taken by the Processor goroutine.
// It does so by flushing the event pipeline.
func (p *ScheduledProcessor) syncEventC() {
	p.syncSendAndWait(&syncEvent{c: make(chan struct{})})
}

// syncSendAndWait allows sync event to be sent and waited on its channel.
// Exposed to allow special test syneEvents that contain span to be sent.
func (p *ScheduledProcessor) syncSendAndWait(se *syncEvent) {
	ev := getPooledEvent(event{sync: se})
	select {
	case p.eventC <- ev:
		// This shouldn't happen as there should be no sync events after disconnect,
		// but if there's a bug don't wait it can hang waiting for sync chan.
		p.scheduler.Enqueue(EventQueued)
		select {
		case <-se.c:
		// Synchronized.
		case <-p.stoppedC:
			// Already stopped. Do nothing.
		}
	case <-p.stoppedC:
		// Already stopped. Do nothing.
		putPooledEvent(ev)
	}
}

// Len returns the number of registrations attached to the processor.
func (p *ScheduledProcessor) Len() int {
	return runRequest(p, func(_ context.Context, p *ScheduledProcessor) int {
		return p.reg.Len()
	})
}

// Filter returns a new operation filter based on the registrations attached to
// the processor. Returns nil if the processor has been stopped already.
func (p *ScheduledProcessor) Filter() *Filter {
	return runRequest(p, func(_ context.Context, p *ScheduledProcessor) *Filter {
		return newFilterFromFilterTree(p.reg.tree)
	})
}

// runRequest will enqueue request to processor and wait for it to be complete.
// Function f will be executed on processor callback by scheduler worker. It
// is guaranteed that only single request is modifying processor at any given
// time. It is advisable to use provided processor reference for operations
// rather than using one within closure itself.
// If request can't be queued or processor stoppedC is closed then default
// value is returned.
func runRequest[T interface{}](
	p *ScheduledProcessor, f func(ctx context.Context, p *ScheduledProcessor) T,
) (r T) {
	result := make(chan T, 1)
	p.enqueueRequest(func(ctx context.Context) {
		result <- f(ctx, p)
	})
	select {
	case r = <-result:
		return r
	case <-p.stoppedC:
		return r
	}
}

func (p *ScheduledProcessor) enqueueRequest(req request) {
	select {
	case p.requestQueue <- req:
		p.scheduler.Enqueue(RequestQueued)
	case <-p.stoppedC:
	}
}

func (p *ScheduledProcessor) consumeEvent(ctx context.Context, e *event) {
	switch {
	case e.ops != nil:
		p.consumeLogicalOps(ctx, e.ops, e.alloc)
	case !e.ct.IsEmpty():
		p.forwardClosedTS(ctx, e.ct.Timestamp)
	case bool(e.initRTS):
		p.initResolvedTS(ctx)
	case e.sst != nil:
		p.consumeSSTable(ctx, e.sst.data, e.sst.span, e.sst.ts, e.alloc)
	case e.sync != nil:
		// Note that this behaviour is different form LegacyProcessor as it can
		// ensure that registrations that it controls directly flushed all their
		// buffers. Unbuffered registrations used by ScheduledProcessor doesn't have
		// such luxury.
		close(e.sync.c)
	default:
		panic(fmt.Sprintf("missing event variant: %+v", e))
	}
}

func (p *ScheduledProcessor) consumeLogicalOps(
	ctx context.Context, ops []enginepb.MVCCLogicalOp, alloc *SharedBudgetAllocation,
) {
	for _, op := range ops {
		// Publish RangeFeedValue updates, if necessary.
		switch t := op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			// Publish the new value directly.
			p.publishValue(ctx, t.Key, t.Timestamp, t.Value, t.PrevValue, alloc)

		case *enginepb.MVCCDeleteRangeOp:
			// Publish the range deletion directly.
			p.publishDeleteRange(ctx, t.StartKey, t.EndKey, t.Timestamp, alloc)

		case *enginepb.MVCCWriteIntentOp:
			// No updates to publish.

		case *enginepb.MVCCUpdateIntentOp:
			// No updates to publish.

		case *enginepb.MVCCCommitIntentOp:
			// Publish the newly committed value.
			p.publishValue(ctx, t.Key, t.Timestamp, t.Value, t.PrevValue, alloc)

		case *enginepb.MVCCAbortIntentOp:
			// No updates to publish.

		case *enginepb.MVCCAbortTxnOp:
			// No updates to publish.

		default:
			panic(errors.AssertionFailedf("unknown logical op %T", t))
		}

		// Determine whether the operation caused the resolved timestamp to
		// move forward. If so, publish a RangeFeedCheckpoint notification.
		if p.rts.ConsumeLogicalOp(op) {
			p.publishCheckpoint(ctx)
		}
	}
}

func (p *ScheduledProcessor) consumeSSTable(
	ctx context.Context,
	sst []byte,
	sstSpan roachpb.Span,
	sstWTS hlc.Timestamp,
	alloc *SharedBudgetAllocation,
) {
	p.publishSSTable(ctx, sst, sstSpan, sstWTS, alloc)
}

func (p *ScheduledProcessor) forwardClosedTS(ctx context.Context, newClosedTS hlc.Timestamp) {
	if p.rts.ForwardClosedTS(newClosedTS) {
		p.publishCheckpoint(ctx)
	}
}

func (p *ScheduledProcessor) initResolvedTS(ctx context.Context) {
	if p.rts.Init() {
		p.publishCheckpoint(ctx)
	}
}

func (p *ScheduledProcessor) publishValue(
	ctx context.Context,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value, prevValue []byte,
	alloc *SharedBudgetAllocation,
) {
	if !p.Span.ContainsKey(roachpb.RKey(key)) {
		log.Fatalf(ctx, "key %v not in Processor's key range %v", key, p.Span)
	}

	var prevVal roachpb.Value
	if prevValue != nil {
		prevVal.RawBytes = prevValue
	}
	var event kvpb.RangeFeedEvent
	event.MustSetValue(&kvpb.RangeFeedValue{
		Key: key,
		Value: roachpb.Value{
			RawBytes:  value,
			Timestamp: timestamp,
		},
		PrevValue: prevVal,
	})
	p.reg.PublishToOverlapping(ctx, roachpb.Span{Key: key}, &event, alloc)
}

func (p *ScheduledProcessor) publishDeleteRange(
	ctx context.Context,
	startKey, endKey roachpb.Key,
	timestamp hlc.Timestamp,
	alloc *SharedBudgetAllocation,
) {
	span := roachpb.Span{Key: startKey, EndKey: endKey}
	if !p.Span.ContainsKeyRange(roachpb.RKey(startKey), roachpb.RKey(endKey)) {
		log.Fatalf(ctx, "span %s not in Processor's key range %v", span, p.Span)
	}

	var event kvpb.RangeFeedEvent
	event.MustSetValue(&kvpb.RangeFeedDeleteRange{
		Span:      span,
		Timestamp: timestamp,
	})
	p.reg.PublishToOverlapping(ctx, span, &event, alloc)
}

func (p *ScheduledProcessor) publishSSTable(
	ctx context.Context,
	sst []byte,
	sstSpan roachpb.Span,
	sstWTS hlc.Timestamp,
	alloc *SharedBudgetAllocation,
) {
	if sstSpan.Equal(roachpb.Span{}) {
		panic(errors.AssertionFailedf("received SSTable without span"))
	}
	if sstWTS.IsEmpty() {
		panic(errors.AssertionFailedf("received SSTable without write timestamp"))
	}
	p.reg.PublishToOverlapping(ctx, sstSpan, &kvpb.RangeFeedEvent{
		SST: &kvpb.RangeFeedSSTable{
			Data:    sst,
			Span:    sstSpan,
			WriteTS: sstWTS,
		},
	}, alloc)
}

func (p *ScheduledProcessor) publishCheckpoint(ctx context.Context) {
	// TODO(nvanbenschoten): persist resolvedTimestamp. Give Processor a client.DB.
	// TODO(nvanbenschoten): rate limit these? send them periodically?

	event := p.newCheckpointEvent()
	p.reg.PublishToOverlapping(ctx, all, event, nil)
}

func (p *ScheduledProcessor) newCheckpointEvent() *kvpb.RangeFeedEvent {
	// Create a RangeFeedCheckpoint over the Processor's entire span. Each
	// individual registration will trim this down to just the key span that
	// it is listening on in registration.maybeStripEvent before publishing.
	var event kvpb.RangeFeedEvent
	event.MustSetValue(&kvpb.RangeFeedCheckpoint{
		Span:       p.Span.AsRawSpanWithNoLocals(),
		ResolvedTS: p.rts.Get(),
	})
	return &event
}

// ID implements Processor interface.
func (p *ScheduledProcessor) ID() int64 {
	return p.scheduler.ID()
}
