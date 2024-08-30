// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
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

	reg registry
	rts resolvedTimestamp

	// processCtx is the annotated background context used for process(). It is
	// stored here to avoid reconstructing it on every call.
	processCtx context.Context
	// taskCtx is the context used to spawn async tasks (e.g. the txn pusher),
	// along with its cancel function which is called when the processor stops or
	// the stopper quiesces. It is independent of processCtx, and constructed
	// during Start().
	//
	// TODO(erikgrinaker): the context handling here should be cleaned up.
	// processCtx should be passed in from the scheduler and propagate stopper
	// quiescence, and the async tasks should probably be run on scheduler
	// threads or at least a separate bounded worker pool. But this will do for
	// now.
	taskCtx    context.Context
	taskCancel func()

	requestQueue chan request
	eventC       chan *event
	// If true, processor is not processing data anymore and waiting for registrations
	// to be complete.
	stopping bool
	stoppedC chan struct{}

	// stopper passed by start that is used for firing up async work from scheduler.
	stopper       *stop.Stopper
	txnPushActive bool

	// pendingUnregistrations indicates that the registry may have registrations
	// that can be unregistered. This is handled outside of the requestQueue to
	// avoid blocking clients who only need to signal unregistration.
	pendingUnregistrations atomic.Bool
}

// NewScheduledProcessor creates a new scheduler based rangefeed Processor.
// Processor needs to be explicitly started after creation.
func NewScheduledProcessor(cfg Config) *ScheduledProcessor {
	cfg.SetDefaults()
	cfg.AmbientContext.AddLogTag("rangefeed", nil)
	p := &ScheduledProcessor{
		Config:     cfg,
		scheduler:  cfg.Scheduler.NewClientScheduler(),
		reg:        makeRegistry(cfg.Metrics),
		rts:        makeResolvedTimestamp(cfg.Settings),
		processCtx: cfg.AmbientContext.AnnotateCtx(context.Background()),

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
func (p *ScheduledProcessor) Start(
	stopper *stop.Stopper, rtsIterFunc IntentScannerConstructor,
) error {
	p.stopper = stopper
	p.taskCtx, p.taskCancel = p.stopper.WithCancelOnQuiesce(
		p.Config.AmbientContext.AnnotateCtx(context.Background()))

	// Note that callback registration must be performed before starting resolved
	// timestamp init because resolution posts resolvedTS event when it is done.
	if err := p.scheduler.Register(p.process, p.Priority); err != nil {
		p.cleanup()
		return err
	}

	// Launch an async task to scan over the resolved timestamp iterator and
	// initialize the unresolvedIntentQueue.
	if rtsIterFunc != nil {
		rtsIter := rtsIterFunc()
		initScan := newInitResolvedTSScan(p.Span, p, rtsIter)
		// TODO(oleg): we need to cap number of tasks that we can fire up across
		// all feeds as they could potentially generate O(n) tasks during start.
		err := stopper.RunAsyncTask(p.taskCtx, "rangefeed: init resolved ts", initScan.Run)
		if err != nil {
			initScan.Cancel()
			p.scheduler.StopProcessor()
			return err
		}
	} else {
		p.initResolvedTS(p.taskCtx, nil)
	}

	p.Metrics.RangeFeedProcessorsScheduler.Inc(1)
	return nil
}

// process is a scheduler callback that is processing scheduled events and
// requests.
func (p *ScheduledProcessor) process(e processorEventType) processorEventType {
	ctx := p.processCtx
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
			if p.pendingUnregistrations.Swap(false) {
				p.reg.unregisterMarkedRegistrations(ctx)
				// If we have no more registrations, we can stop this processor. Note
				// that stopInternal sets p.stopping to true so any register requests
				// being concurrenly enqueued will fail-fast before being added to the
				// registry.
				if p.reg.Len() == 0 {
					p.stopInternal(ctx, nil)
				}
			}
			return
		}
	}
}

// Transform and route pending events.
func (p *ScheduledProcessor) processEvents(ctx context.Context) {
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
	// NB: Len() check avoids hlc.Clock.Now() mutex acquisition in the common
	// case, which can be a significant source of contention.
	if !p.txnPushActive && p.rts.IsInit() && p.rts.intentQ.Len() > 0 {
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
			pushTxns := newTxnPushAttempt(p.Settings, p.Span, p.TxnPusher, p, toPush, now, func() {
				p.enqueueRequest(func(ctx context.Context) {
					p.txnPushActive = false
				})
			})
			p.txnPushActive = true
			// TODO(oleg): we need to cap number of tasks that we can fire up across
			// all feeds as they could potentially generate O(n) tasks for push.
			err := p.stopper.RunAsyncTask(p.taskCtx, "rangefeed: pushing old txns", pushTxns.Run)
			if err != nil {
				pushTxns.Cancel()
			}
		}
	}
}

func (p *ScheduledProcessor) processStop() {
	p.cleanup()
	p.Metrics.RangeFeedProcessorsScheduler.Dec(1)
}

func (p *ScheduledProcessor) cleanup() {
	ctx := p.AmbientContext.AnnotateCtx(context.Background())
	// Cleanup is normally called when all registrations are disconnected and
	// unregistered or were not created yet (processor start failure).
	// However, there's a case where processor is stopped by replica action while
	// registrations are still active. In that case registrations won't have a
	// chance to unregister themselves after their work loop terminates because
	// processor is already disconnected from scheduler.
	// To avoid leaking any registry resources and metrics, processor performs
	// explicit registry termination in that case.
	pErr := kvpb.NewError(&kvpb.NodeUnavailableError{})
	p.reg.DisconnectAllOnShutdown(ctx, pErr)

	// Unregister callback from scheduler
	p.scheduler.Unregister()

	p.taskCancel()
	close(p.stoppedC)
	p.MemBudget.Close(ctx)
	if p.UnregisterFromReplica != nil {
		p.UnregisterFromReplica(p)
	}

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
		p.reg.DisconnectWithErr(ctx, span, pErr)
	})
}

func (p *ScheduledProcessor) sendStop(pErr *kvpb.Error) {
	p.enqueueRequest(func(ctx context.Context) {
		p.stopInternal(ctx, pErr)
	})
}

func (p *ScheduledProcessor) stopInternal(ctx context.Context, pErr *kvpb.Error) {
	p.reg.DisconnectAllOnShutdown(ctx, pErr)
	// First set stopping flag to ensure that once all registrations are removed
	// processor should stop.
	p.stopping = true
	p.scheduler.StopProcessor()
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
	streamCtx context.Context,
	span roachpb.RSpan,
	startTS hlc.Timestamp,
	catchUpIter *CatchUpIterator,
	withDiff bool,
	withFiltering bool,
	withOmitRemote bool,
	stream Stream,
) (bool, Disconnector, *Filter) {
	// Synchronize the event channel so that this registration doesn't see any
	// events that were consumed before this registration was called. Instead,
	// it should see these events during its catch up scan.
	p.syncEventC()

	blockWhenFull := p.Config.EventChanTimeout == 0 // for testing

	var r registration
	bufferedStream, isBufferedStream := stream.(BufferedStream)
	if isBufferedStream {
		r = newUnbufferedRegistration(
			streamCtx, span.AsRawSpanWithNoLocals(), startTS, catchUpIter, withDiff, withFiltering, withOmitRemote,
			p.Config.EventChanCap, p.Metrics, bufferedStream, p.unregisterClientAsync)
	} else {
		r = newBufferedRegistration(
			streamCtx, span.AsRawSpanWithNoLocals(), startTS, catchUpIter, withDiff, withFiltering, withOmitRemote,
			p.Config.EventChanCap, blockWhenFull, p.Metrics, stream, p.unregisterClientAsync)
	}

	filter := runRequest(p, func(ctx context.Context, p *ScheduledProcessor) *Filter {
		if p.stopping {
			return nil
		}
		if !p.Span.AsRawSpanWithNoLocals().Contains(r.getSpan()) {
			log.Fatalf(ctx, "registration %s not in Processor's key range %v", r, p.Span)
		}

		// Add the new registration to the registry.
		p.reg.Register(ctx, r)

		// Prep response with filter that includes the new registration.
		f := p.reg.NewFilter()

		// Immediately publish a checkpoint event to the registry. This will be the first event
		// published to this registration after its initial catch-up scan completes. The resolved
		// timestamp might be empty but the checkpoint event is still useful to indicate that the
		// catch-up scan has completed. This allows clients to rely on stronger ordering semantics
		// once they observe the first checkpoint event.
		r.publish(ctx, p.newCheckpointEvent(), nil)

		// Run an output loop for the registry.
		runOutputLoop := func(ctx context.Context) { r.runOutputLoop(ctx, p.RangeID) }
		// NB: use ctx, not p.taskCtx, as the registry handles teardown itself.
		if err := p.Stopper.RunAsyncTask(ctx, "rangefeed: output loop", runOutputLoop); err != nil {
			// If we can't schedule internally, processor is already stopped which
			// could only happen on shutdown. Disconnect stream and just remove
			// registration.
			r.Disconnect(kvpb.NewError(err))
			// Normally, ubr.runOutputLoop is responsible for draining catch up
			// buffer. If it fails to start, we should drain it here.
			r.drainAllocations(ctx)
		}
		return f
	})
	if filter != nil {
		return true, r, filter
	}
	return false, nil, nil
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
		size := MemUsage(e)
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
			p.Metrics.RangefeedProcessorQueueTimeout.Inc(1)
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
				p.Metrics.RangefeedProcessorQueueTimeout.Inc(1)
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
		return newFilterFromRegistry(&p.reg)
	})
}

// runRequest will enqueue request to processor and wait for it to be complete.
// Function f will be executed on processor callback by scheduler worker. It
// is guaranteed that only single request is modifying processor at any given
// time. It is advisable to use provided processor reference for operations
// rather than using one within closure itself.
//
// If the processor is stopped concurrently with the request queueing, it may or
// may not be processed. If the request is ever processed, its return value is
// guaranteed to be returned here. Otherwise, the zero value is returned and the
// request is never processed.
func runRequest[T interface{}](
	p *ScheduledProcessor, f func(ctx context.Context, p *ScheduledProcessor) T,
) (r T) {
	result := make(chan T, 1)
	p.enqueueRequest(func(ctx context.Context) {
		result <- f(ctx, p)
		// Assert that we never process requests after stoppedC is closed. This is
		// necessary to coordinate catchup iter ownership and avoid double-closing.
		// Note that request/stop processing is always sequential, see process().
		if buildutil.CrdbTestBuild {
			select {
			case <-p.stoppedC:
				log.Fatalf(ctx, "processing request on stopped processor")
			default:
			}
		}
	})
	select {
	case r = <-result:
		return r
	case <-p.stoppedC:
		// If a request and stop were processed in rapid succession, and the node is
		// overloaded, this select may observe them happening at the same time and
		// take this branch instead of the result with 50% probability. Check again.
		select {
		case r = <-result:
		default:
		}
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

// unregisterClientAsync instructs the processor to unregister the given
// registration. This doesn't send an actual request to the request queue to
// ensure that it is non-blocking.
//
// Rather, the registration has its shouldUnregister flag set and the
// processor's pendingUnregistrations flag is set. During processRequests, if
// pendingUnregistrations is true, we remove any marked registrations from the
// registry.
//
// We are OK with these being processed out of order since these requests
// originate from a registration cleanup, so the registration in question is no
// longer processing events.
func (p *ScheduledProcessor) unregisterClientAsync(r registration) {
	select {
	case <-p.stoppedC:
		return
	default:
	}
	r.setShouldUnregister()
	p.pendingUnregistrations.Store(true)
	p.scheduler.Enqueue(RequestQueued)
}

func (p *ScheduledProcessor) consumeEvent(ctx context.Context, e *event) {
	switch {
	case e.ops != nil:
		p.consumeLogicalOps(ctx, e.ops, e.alloc)
	case !e.ct.IsEmpty():
		p.forwardClosedTS(ctx, e.ct.Timestamp, e.alloc)
	case bool(e.initRTS):
		p.initResolvedTS(ctx, e.alloc)
	case e.sst != nil:
		p.consumeSSTable(ctx, e.sst.data, e.sst.span, e.sst.ts, e.alloc)
	case e.sync != nil:
		if e.sync.testRegCatchupSpan != nil {
			if err := p.reg.waitForCaughtUp(ctx, *e.sync.testRegCatchupSpan); err != nil {
				log.Errorf(
					ctx,
					"error waiting for registries to catch up during test, results might be impacted: %s",
					err,
				)
			}
		}
		close(e.sync.c)
	default:
		log.Fatalf(ctx, "missing event variant: %+v", e)
	}
}

func (p *ScheduledProcessor) consumeLogicalOps(
	ctx context.Context, ops []enginepb.MVCCLogicalOp, alloc *SharedBudgetAllocation,
) {
	for _, op := range ops {
		// Publish RangeFeedValue updates, if necessary.
		switch t := op.GetValue().(type) {
		// OmitInRangefeeds is relevant only for transactional writes, so it's
		// propagated only in the case of a MVCCCommitIntentOp and
		// MVCCWriteValueOp (could be the result of a 1PC write).

		case *enginepb.MVCCWriteValueOp:
			// Publish the new value directly.
			p.publishValue(ctx, t.Key, t.Timestamp, t.Value, t.PrevValue, logicalOpMetadata{omitInRangefeeds: t.OmitInRangefeeds, originID: t.OriginID}, alloc)
		case *enginepb.MVCCDeleteRangeOp:
			// Publish the range deletion directly.
			p.publishDeleteRange(ctx, t.StartKey, t.EndKey, t.Timestamp, alloc)

		case *enginepb.MVCCWriteIntentOp:
			// No updates to publish.

		case *enginepb.MVCCUpdateIntentOp:
			// No updates to publish.

		case *enginepb.MVCCCommitIntentOp:
			// Publish the newly committed value.
			p.publishValue(ctx, t.Key, t.Timestamp, t.Value, t.PrevValue, logicalOpMetadata{omitInRangefeeds: t.OmitInRangefeeds, originID: t.OriginID}, alloc)

		case *enginepb.MVCCAbortIntentOp:
			// No updates to publish.

		case *enginepb.MVCCAbortTxnOp:
			// No updates to publish.

		default:
			log.Fatalf(ctx, "unknown logical op %T", t)
		}

		// Determine whether the operation caused the resolved timestamp to
		// move forward. If so, publish a RangeFeedCheckpoint notification.
		if p.rts.ConsumeLogicalOp(ctx, op) {
			p.publishCheckpoint(ctx, nil)
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

func (p *ScheduledProcessor) forwardClosedTS(
	ctx context.Context, newClosedTS hlc.Timestamp, alloc *SharedBudgetAllocation,
) {
	if p.rts.ForwardClosedTS(ctx, newClosedTS) {
		p.publishCheckpoint(ctx, alloc)
	}
}

func (p *ScheduledProcessor) initResolvedTS(ctx context.Context, alloc *SharedBudgetAllocation) {
	if p.rts.Init(ctx) {
		p.publishCheckpoint(ctx, alloc)
	}
}

func (p *ScheduledProcessor) publishValue(
	ctx context.Context,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value, prevValue []byte,
	valueMetadata logicalOpMetadata,
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
	p.reg.PublishToOverlapping(ctx, roachpb.Span{Key: key}, &event, valueMetadata, alloc)
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
	p.reg.PublishToOverlapping(ctx, span, &event, logicalOpMetadata{}, alloc)
}

func (p *ScheduledProcessor) publishSSTable(
	ctx context.Context,
	sst []byte,
	sstSpan roachpb.Span,
	sstWTS hlc.Timestamp,
	alloc *SharedBudgetAllocation,
) {
	if sstSpan.Equal(roachpb.Span{}) {
		log.Fatalf(ctx, "received SSTable without span")
	}
	if sstWTS.IsEmpty() {
		log.Fatalf(ctx, "received SSTable without write timestamp")
	}
	p.reg.PublishToOverlapping(ctx, sstSpan, &kvpb.RangeFeedEvent{
		SST: &kvpb.RangeFeedSSTable{
			Data:    sst,
			Span:    sstSpan,
			WriteTS: sstWTS,
		},
	}, logicalOpMetadata{}, alloc)
}

func (p *ScheduledProcessor) publishCheckpoint(ctx context.Context, alloc *SharedBudgetAllocation) {
	// TODO(nvanbenschoten): persist resolvedTimestamp. Give Processor a client.DB.
	// TODO(nvanbenschoten): rate limit these? send them periodically?

	event := p.newCheckpointEvent()
	p.reg.PublishToOverlapping(ctx, all, event, logicalOpMetadata{}, alloc)
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
