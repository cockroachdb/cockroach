// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/future"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

type LegacyProcessor struct {
	Config
	reg registry
	rts resolvedTimestamp

	regC       chan registration
	unregC     chan *registration
	lenReqC    chan struct{}
	lenResC    chan int
	filterReqC chan struct{}
	filterResC chan *Filter
	eventC     chan *event
	spanErrC   chan spanErr
	stopC      chan *kvpb.Error
	stoppedC   chan struct{}
}

// Start implements Processor interface.
// LegacyProcessor launches a goroutine with its internal workloop that processes
// requests for registrations, timestamp pushes etc as well as forwarding pending
// data to registrations.
func (p *LegacyProcessor) Start(stopper *stop.Stopper, newRtsIter IntentScannerConstructor) error {
	ctx := p.AnnotateCtx(context.Background())
	if err := stopper.RunAsyncTask(ctx, "rangefeed.LegacyProcessor", func(ctx context.Context) {
		p.run(ctx, p.RangeID, newRtsIter, stopper)
	}); err != nil {
		p.reg.DisconnectWithErr(all, kvpb.NewError(err))
		close(p.stoppedC)
		return err
	}
	return nil
}

// run is called from Start and runs the rangefeed.
func (p *LegacyProcessor) run(
	ctx context.Context,
	_forStacks roachpb.RangeID,
	rtsIterFunc IntentScannerConstructor,
	stopper *stop.Stopper,
) {
	// Close the memory budget last, or there will be a period of time during
	// which requests are still ongoing but will run into the closed budget,
	// causing shutdown noise and busy retries.
	// Closing the budget after stoppedC ensures that all other goroutines are
	// (very close to being) shut down by the time the budget goes away.
	defer p.MemBudget.Close(ctx)
	defer close(p.stoppedC)
	ctx, cancelOutputLoops := context.WithCancel(ctx)
	defer cancelOutputLoops()

	// Launch an async task to scan over the resolved timestamp iterator and
	// initialize the unresolvedIntentQueue. Ignore error if quiescing.
	if rtsIterFunc != nil {
		rtsIter := rtsIterFunc()
		initScan := newInitResolvedTSScan(p.Span, p, rtsIter)
		err := stopper.RunAsyncTask(ctx, "rangefeed: init resolved ts", initScan.Run)
		if err != nil {
			initScan.Cancel()
		}
	} else {
		p.initResolvedTS(ctx)
	}

	// txnPushTicker periodically pushes the transaction record of all
	// unresolved intents that are above a certain age, helping to ensure
	// that the resolved timestamp continues to make progress.
	var txnPushTicker *time.Ticker
	var txnPushTickerC <-chan time.Time
	var txnPushAttemptC chan struct{}
	if p.PushTxnsInterval > 0 {
		txnPushTicker = time.NewTicker(p.PushTxnsInterval)
		txnPushTickerC = txnPushTicker.C
		defer txnPushTicker.Stop()
	}

	for {
		select {

		// Handle new registrations.
		case r := <-p.regC:
			if !p.Span.AsRawSpanWithNoLocals().Contains(r.span) {
				log.Fatalf(ctx, "registration %s not in Processor's key range %v", r, p.Span)
			}

			// Construct the catchUpIter before notifying the registration that it
			// has been registered. Note that if the catchUpScan is never run, then
			// the iterator constructed here will be closed in disconnect.
			r.maybeConstructCatchUpIter()

			// Add the new registration to the registry.
			p.reg.Register(&r)

			// Publish an updated filter that includes the new registration.
			p.filterResC <- p.reg.NewFilter()

			// Immediately publish a checkpoint event to the registry. This will be the first event
			// published to this registration after its initial catch-up scan completes. The resolved
			// timestamp might be empty but the checkpoint event is still useful to indicate that the
			// catch-up scan has completed. This allows clients to rely on stronger ordering semantics
			// once they observe the first checkpoint event.
			r.publish(ctx, p.newCheckpointEvent(), nil)

			// Run an output loop for the registry.
			runOutputLoop := func(ctx context.Context) {
				r.runOutputLoop(ctx, p.RangeID)
				select {
				case p.unregC <- &r:
					if r.unreg != nil {
						r.unreg()
					}
				case <-p.stoppedC:
				}
			}
			if err := stopper.RunAsyncTask(ctx, "rangefeed: output loop", runOutputLoop); err != nil {
				r.disconnect(kvpb.NewError(err))
				p.reg.Unregister(ctx, &r)
			}

		// Respond to unregistration requests; these come from registrations that
		// encounter an error during their output loop.
		case r := <-p.unregC:
			p.reg.Unregister(ctx, r)

		// Send errors to registrations overlapping the span and disconnect them.
		// Requested via DisconnectSpanWithErr().
		case e := <-p.spanErrC:
			p.reg.DisconnectWithErr(e.span, e.pErr)

		// Respond to answers about the processor goroutine state.
		case <-p.lenReqC:
			p.lenResC <- p.reg.Len()

		// Respond to answers about which operations can be filtered before
		// reaching the Processor.
		case <-p.filterReqC:
			p.filterResC <- p.reg.NewFilter()

		// Transform and route events.
		case e := <-p.eventC:
			p.consumeEvent(ctx, e)
			e.alloc.Release(ctx)
			putPooledEvent(e)

		// Check whether any unresolved intents need a push.
		case <-txnPushTickerC:
			// Don't perform transaction push attempts until the resolved
			// timestamp has been initialized.
			if !p.rts.IsInit() {
				continue
			}

			now := p.Clock.Now()
			before := now.Add(-p.PushTxnsAge.Nanoseconds(), 0)
			oldTxns := p.rts.intentQ.Before(before)

			if len(oldTxns) > 0 {
				toPush := make([]enginepb.TxnMeta, len(oldTxns))
				for i, txn := range oldTxns {
					toPush[i] = txn.asTxnMeta()
				}

				// Set the ticker channel to nil so that it can't trigger a
				// second concurrent push. Create a push attempt response
				// channel that is closed when the push attempt completes.
				txnPushTickerC = nil
				txnPushAttemptC = make(chan struct{})

				// Launch an async transaction push attempt that pushes the
				// timestamp of all transactions beneath the push offset.
				// Ignore error if quiescing.
				pushTxns := newTxnPushAttempt(p.Span, p.TxnPusher, p, toPush, now, txnPushAttemptC)
				err := stopper.RunAsyncTask(ctx, "rangefeed: pushing old txns", pushTxns.Run)
				if err != nil {
					pushTxns.Cancel()
				}
			}

		// Update the resolved timestamp based on the push attempt.
		case <-txnPushAttemptC:
			// Reset the ticker channel so that it can trigger push attempts
			// again. Set the push attempt channel back to nil.
			txnPushTickerC = txnPushTicker.C
			txnPushAttemptC = nil

		// Close registrations and exit when signaled.
		case pErr := <-p.stopC:
			p.reg.DisconnectWithErr(all, pErr)
			return

		// Exit on stopper.
		case <-stopper.ShouldQuiesce():
			pErr := kvpb.NewError(&kvpb.NodeUnavailableError{})
			p.reg.DisconnectWithErr(all, pErr)
			return
		}
	}
}

// Stop implements Processor interface.
func (p *LegacyProcessor) Stop() {
	p.StopWithErr(nil)
}

// StopWithErr implements Processor interface.
func (p *LegacyProcessor) StopWithErr(pErr *kvpb.Error) {
	if p == nil {
		return
	}
	// Flush any remaining events before stopping.
	p.syncEventC()
	// Send the processor a stop signal.
	p.sendStop(pErr)
}

// DisconnectSpanWithErr implements Processor interface.
func (p *LegacyProcessor) DisconnectSpanWithErr(span roachpb.Span, pErr *kvpb.Error) {
	if p == nil {
		return
	}
	select {
	case p.spanErrC <- spanErr{span: span, pErr: pErr}:
	case <-p.stoppedC:
		// Already stopped. Do nothing.
	}
}

func (p *LegacyProcessor) sendStop(pErr *kvpb.Error) {
	select {
	case p.stopC <- pErr:
		// stopC has non-zero capacity so this should not block unless
		// multiple callers attempt to stop the Processor concurrently.
	case <-p.stoppedC:
		// Already stopped. Do nothing.
	}
}

// Register  implements Processor interface.
func (p *LegacyProcessor) Register(
	span roachpb.RSpan,
	startTS hlc.Timestamp,
	catchUpIterConstructor CatchUpIteratorConstructor,
	withDiff bool,
	stream Stream,
	disconnectFn func(),
	done *future.ErrorFuture,
) (bool, *Filter) {
	// Synchronize the event channel so that this registration doesn't see any
	// events that were consumed before this registration was called. Instead,
	// it should see these events during its catch up scan.
	p.syncEventC()

	blockWhenFull := p.Config.EventChanTimeout == 0 // for testing
	r := newRegistration(
		span.AsRawSpanWithNoLocals(), startTS, catchUpIterConstructor, withDiff,
		p.Config.EventChanCap, blockWhenFull, p.Metrics, stream, disconnectFn, done,
	)
	select {
	case p.regC <- r:
		// Wait for response.
		return true, <-p.filterResC
	case <-p.stoppedC:
		return false, nil
	}
}

// Len implements Processor interface.
func (p *LegacyProcessor) Len() int {
	if p == nil {
		return 0
	}

	// Ask the processor goroutine.
	select {
	case p.lenReqC <- struct{}{}:
		// Wait for response.
		return <-p.lenResC
	case <-p.stoppedC:
		return 0
	}
}

// Filter implements Processor interface.
func (p *LegacyProcessor) Filter() *Filter {
	if p == nil {
		return nil
	}

	// Ask the processor goroutine.
	select {
	case p.filterReqC <- struct{}{}:
		// Wait for response.
		return <-p.filterResC
	case <-p.stoppedC:
		return nil
	}
}

// ConsumeLogicalOps implements Processor interface.
func (p *LegacyProcessor) ConsumeLogicalOps(
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

// ConsumeSSTable implements Processor interface.
func (p *LegacyProcessor) ConsumeSSTable(
	ctx context.Context, sst []byte, sstSpan roachpb.Span, writeTS hlc.Timestamp,
) bool {
	if p == nil {
		return true
	}
	return p.sendEvent(ctx, event{sst: &sstEvent{sst, sstSpan, writeTS}}, p.EventChanTimeout)
}

// ForwardClosedTS implements Processor interface.
func (p *LegacyProcessor) ForwardClosedTS(ctx context.Context, closedTS hlc.Timestamp) bool {
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
func (p *LegacyProcessor) sendEvent(ctx context.Context, e event, timeout time.Duration) bool {
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
func (p *LegacyProcessor) setResolvedTSInitialized(ctx context.Context) {
	p.sendEvent(ctx, event{initRTS: true}, 0)
}

// syncEventC synchronizes access to the Processor goroutine, allowing the
// caller to establish causality with actions taken by the Processor goroutine.
// It does so by flushing the event pipeline.
func (p *LegacyProcessor) syncEventC() {
	syncC := make(chan struct{})
	ev := getPooledEvent(event{sync: &syncEvent{c: syncC}})
	select {
	case p.eventC <- ev:
		select {
		case <-syncC:
		// Synchronized.
		case <-p.stoppedC:
			// Already stopped. Do nothing.
		}
	case <-p.stoppedC:
		// Already stopped. Do nothing.
	}
}

func (p *LegacyProcessor) consumeEvent(ctx context.Context, e *event) {
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
		if e.sync.testRegCatchupSpan != nil {
			if err := p.reg.waitForCaughtUp(*e.sync.testRegCatchupSpan); err != nil {
				log.Errorf(
					ctx,
					"error waiting for registries to catch up during test, results might be impacted: %s",
					err,
				)
			}
		}
		close(e.sync.c)
	default:
		panic(fmt.Sprintf("missing event variant: %+v", e))
	}
}

func (p *LegacyProcessor) consumeLogicalOps(
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

func (p *LegacyProcessor) consumeSSTable(
	ctx context.Context,
	sst []byte,
	sstSpan roachpb.Span,
	sstWTS hlc.Timestamp,
	alloc *SharedBudgetAllocation,
) {
	p.publishSSTable(ctx, sst, sstSpan, sstWTS, alloc)
}

func (p *LegacyProcessor) forwardClosedTS(ctx context.Context, newClosedTS hlc.Timestamp) {
	if p.rts.ForwardClosedTS(newClosedTS) {
		p.publishCheckpoint(ctx)
	}
}

func (p *LegacyProcessor) initResolvedTS(ctx context.Context) {
	if p.rts.Init() {
		p.publishCheckpoint(ctx)
	}
}

func (p *LegacyProcessor) publishValue(
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

func (p *LegacyProcessor) publishDeleteRange(
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

func (p *LegacyProcessor) publishSSTable(
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

func (p *LegacyProcessor) publishCheckpoint(ctx context.Context) {
	// TODO(nvanbenschoten): persist resolvedTimestamp. Give Processor a client.DB.
	// TODO(nvanbenschoten): rate limit these? send them periodically?

	event := p.newCheckpointEvent()
	p.reg.PublishToOverlapping(ctx, all, event, nil)
}

func (p *LegacyProcessor) newCheckpointEvent() *kvpb.RangeFeedEvent {
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
