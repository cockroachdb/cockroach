// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package rangefeed

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

const (
	// defaultPushTxnsInterval is the default interval at which a Processor will
	// push all transactions in the unresolvedIntentQueue that are above the age
	// specified by PushTxnsAge.
	defaultPushTxnsInterval = 250 * time.Millisecond
	// defaultPushTxnsAge is the default age at which a Processor will begin to
	// consider a transaction old enough to push.
	defaultPushTxnsAge = 10 * time.Second
	// defaultCheckStreamsInterval is the default interval at which a Processor
	// will check all streams to make sure they have not been canceled.
	defaultCheckStreamsInterval = 1 * time.Second
)

// newErrBufferCapacityExceeded creates an error that is returned to subscribers
// if the rangefeed processor is not able to keep up with the flow of incoming
// events and is forced to drop events in order to not block. The error is
// usually associated with a slow consumer.
// TODO(nvanbenschoten): currently a single slow consumer can cause all
// rangefeeds on a Range to be shut down with this error. We should work
// on isolating buffering for individual consumers so that a slow consumer
// only affects itself. One idea for this is to replace Stream.Send with
// Stream.SendAsync and give each stream its own individual buffer. If
// an individual stream is unable to keep up, to should fail on its own.
func newErrBufferCapacityExceeded() *roachpb.Error {
	return roachpb.NewErrorf("rangefeed: buffer capacity exceeded due to slow consumer")
}

// Config encompasses the configuration required to create a Processor.
type Config struct {
	log.AmbientContext
	Clock *hlc.Clock
	Span  roachpb.RSpan

	TxnPusher TxnPusher
	// PushTxnsInterval specifies the interval at which a Processor will push
	// all transactions in the unresolvedIntentQueue that are above the age
	// specified by PushTxnsAge.
	PushTxnsInterval time.Duration
	// PushTxnsAge specifies the age at which a Processor will begin to consider
	// a transaction old enough to push.
	PushTxnsAge time.Duration

	// EventChanCap specifies the capacity to give to the Processor's input
	// channel.
	EventChanCap int
	// EventChanTimeout specifies the maximum duration that methods will
	// wait to send on the Processor's input channel before giving up and
	// shutting down the Processor. 0 for no timeout.
	EventChanTimeout time.Duration

	// CheckStreamsInterval specifies interval at which a Processor will check
	// all streams to make sure they have not been canceled.
	CheckStreamsInterval time.Duration
}

// SetDefaults initializes unset fields in Config to values
// suitable for use by a Processor.
func (sc *Config) SetDefaults() {
	if sc.TxnPusher == nil {
		if sc.PushTxnsInterval != 0 {
			panic("nil TxnPusher with non-zero PushTxnsInterval")
		}
		if sc.PushTxnsAge != 0 {
			panic("nil TxnPusher with non-zero PushTxnsAge")
		}
	} else {
		if sc.PushTxnsInterval == 0 {
			sc.PushTxnsInterval = defaultPushTxnsInterval
		}
		if sc.PushTxnsAge == 0 {
			sc.PushTxnsAge = defaultPushTxnsAge
		}
	}
	if sc.CheckStreamsInterval == 0 {
		sc.CheckStreamsInterval = defaultCheckStreamsInterval
	}
}

// Processor manages a set of rangefeed registrations and handles the routing of
// logical updates to these registrations. While routing logical updates to
// rangefeed registrations, the processor performs two important tasks:
// 1. it translates logical updates into rangefeed events.
// 2. it transforms a range-level closed timestamp to a rangefeed-level resolved
//    timestamp.
type Processor struct {
	Config
	reg registry
	rts resolvedTimestamp

	regC     chan registration
	catchUpC chan catchUpResult
	lenReqC  chan struct{}
	lenResC  chan int
	eventC   chan event
	stopC    chan *roachpb.Error
	stoppedC chan struct{}
}

// catchUpResult is delivered to the Processor goroutine when a catch up scan
// for a new registration has completed.
type catchUpResult struct {
	r    *registration
	pErr *roachpb.Error
}

// event is a union of different event types that the Processor goroutine needs
// to be informed of. It is used so that all events can be sent over the same
// channel, which is necessary to prevent reordering.
type event struct {
	ops     []enginepb.MVCCLogicalOp
	ct      hlc.Timestamp
	initRTS bool
	syncC   chan struct{}
}

// NewProcessor creates a new rangefeed Processor. The corresponding goroutine
// should be launched using the Start method.
func NewProcessor(cfg Config) *Processor {
	cfg.SetDefaults()
	cfg.AmbientContext.AddLogTag("rangefeed", nil)
	return &Processor{
		Config: cfg,
		reg:    makeRegistry(),
		rts:    makeResolvedTimestamp(),

		regC:     make(chan registration),
		catchUpC: make(chan catchUpResult),
		lenReqC:  make(chan struct{}),
		lenResC:  make(chan int),
		eventC:   make(chan event, cfg.EventChanCap),
		stopC:    make(chan *roachpb.Error, 1),
		stoppedC: make(chan struct{}),
	}
}

// Start launches a goroutine to process rangefeed events and send them to
// registrations.
//
// The provided iterator is used to initialize the rangefeed's resolved
// timestamp. It must obey the contract of an iterator used for an
// initResolvedTSScan. The Processor promises to clean up the iterator by
// calling its Close method when it is finished. If the iterator is nil then
// no initialization scan will be performed and the resolved timestamp will
// immediately be considered initialized.
func (p *Processor) Start(stopper *stop.Stopper, rtsIter engine.SimpleIterator) {
	ctx := p.AnnotateCtx(context.Background())
	stopper.RunWorker(ctx, func(ctx context.Context) {
		defer close(p.stoppedC)

		// Launch an async task to scan over the resolved timestamp iterator and
		// initialize the unresolvedIntentQueue. Ignore error if quiescing.
		if rtsIter != nil {
			initScan := newInitResolvedTSScan(p, rtsIter)
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

		// checkStreamsTicker periodically checks whether any streams have
		// disconnected. If so, the registration is unregistered.
		checkStreamsTicker := time.NewTicker(p.CheckStreamsInterval)
		defer checkStreamsTicker.Stop()

		for {
			select {
			// Handle new registrations.
			case r := <-p.regC:
				if !p.Span.AsRawSpanWithNoLocals().Contains(r.span) {
					log.Fatalf(ctx, "registration %s not in Processor's key range %v", r, p.Span)
				}

				// Add the new registration to the registry.
				p.reg.Register(&r)

				// Launch an async catch-up scan for the new registration.
				// Ignore error if quiescing.
				if r.catchUpIter != nil {
					catchUp := newCatchUpScan(p, &r)
					err := stopper.RunAsyncTask(ctx, "rangefeed: catch-up scan", catchUp.Run)
					if err != nil {
						catchUp.Cancel()
					}
				} else {
					p.handleCatchUpScanRes(ctx, catchUpResult{r: &r})
				}

			// React to registrations finishing their catch up scan.
			case res := <-p.catchUpC:
				p.handleCatchUpScanRes(ctx, res)

			// Respond to answers about the processor goroutine state.
			case <-p.lenReqC:
				p.lenResC <- p.reg.Len()

			// Transform and route events.
			case e := <-p.eventC:
				p.consumeEvent(ctx, e)

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
						toPush[i] = enginepb.TxnMeta{
							ID:        txn.txnID,
							Key:       txn.txnKey,
							Timestamp: txn.timestamp,
						}
					}

					// Set the ticker channel to nil so that it can't trigger a
					// second concurrent push. Create a push attempt response
					// channel that is closed when the push attempt completes.
					txnPushTickerC = nil
					txnPushAttemptC = make(chan struct{})

					// Launch an async transaction push attempt that pushes the
					// timestamp of all transactions beneath the push offset.
					// Ignore error if quiescing.
					pushTxns := newTxnPushAttempt(p, toPush, now, txnPushAttemptC)
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

			// Check whether any streams have disconnected.
			case <-checkStreamsTicker.C:
				p.reg.CheckStreams()

			// Close registrations and exit when signaled.
			case pErr := <-p.stopC:
				p.reg.DisconnectWithErr(all, pErr)
				return

			// Exit on stopper.
			case <-stopper.ShouldQuiesce():
				p.reg.Disconnect(all)
				return
			}
		}
	})
}

// Stop shuts down the processor and closes all registrations. Safe to call on
// nil Processor. It is not valid to restart a processor after it has been
// stopped.
func (p *Processor) Stop() {
	p.StopWithErr(nil)
}

// StopWithErr shuts down the processor and closes all registrations with the
// specified error. Safe to call on nil Processor. It is not valid to restart a
// processor after it has been stopped.
func (p *Processor) StopWithErr(pErr *roachpb.Error) {
	if p == nil {
		return
	}
	// Flush any remaining events before stopping.
	p.syncEventC()
	// Send the processor a stop signal.
	p.sendStop(pErr)
}

func (p *Processor) sendStop(pErr *roachpb.Error) {
	select {
	case p.stopC <- pErr:
		// stopC has non-zero capacity so this should not block unless
		// multiple callers attempt to stop the Processor concurrently.
	case <-p.stoppedC:
		// Already stopped. Do nothing.
	}
}

// Register registers the stream over the specified span of keys.
//
// The registration will not observe any events that were consumed before this
// method was called. It is undefined whether the registration will observe
// events that are consumed concurrently with this call. The channel will be
// provided an error when the registration closes.
//
// The provided iterator is used to catch the registration up from its starting
// timestamp with value events for all committed values. It must obey the
// contract of an iterator used for a catchUpScan. The Processor promises to
// clean up the iterator by calling its Close method when it is finished. If the
// iterator is nil then no catch-up scan will be performed.
//
// NOT safe to call on nil Processor.
func (p *Processor) Register(
	span roachpb.RSpan,
	startTS hlc.Timestamp,
	catchUpIter engine.SimpleIterator,
	stream Stream,
	errC chan<- *roachpb.Error,
) {
	// Synchronize the event channel so that this registration doesn't see any
	// events that were consumed before this registration was called. Instead,
	// it should see these events during its catch up scan.
	p.syncEventC()

	r := registration{
		span:        span.AsRawSpanWithNoLocals(),
		startTS:     startTS,
		catchUpIter: catchUpIter,
		stream:      stream,
		errC:        errC,
	}
	select {
	case p.regC <- r:
	case <-p.stoppedC:
		if catchUpIter != nil {
			catchUpIter.Close() // clean up
		}
		// errC has a capacity of 1. If it is already full, we don't need to send
		// another error.
		select {
		case errC <- roachpb.NewErrorf("rangefeed processor closed"):
		default:
		}
	}
}

// Len returns the number of registrations attached to the processor.
func (p *Processor) Len() int {
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

// deliverCatchUpScanRes informs the Processor of the results of a catch-up scan
// for a given registration.
func (p *Processor) deliverCatchUpScanRes(r *registration, pErr *roachpb.Error) {
	select {
	case p.catchUpC <- catchUpResult{r: r, pErr: pErr}:
	case <-p.stoppedC:
		// Already stopped. Do nothing.
	}
}

// ConsumeLogicalOps informs the rangefeed processor of the set of logical
// operations. It returns false if consuming the operations hit a timeout, as
// specified by the EventChanTimeout configuration. If the method returns false,
// the processor will have been stopped, so calling Stop is not necessary. Safe
// to call on nil Processor.
func (p *Processor) ConsumeLogicalOps(ops ...enginepb.MVCCLogicalOp) bool {
	if p == nil {
		return true
	}
	if len(ops) == 0 {
		return true
	}
	return p.sendEvent(event{ops: ops}, p.EventChanTimeout)
}

// ForwardClosedTS indicates that the closed timestamp that serves as the basis
// for the rangefeed processor's resolved timestamp has advanced. It returns
// false if forwarding the closed timestamp hit a timeout, as specified by the
// EventChanTimeout configuration. If the method returns false, the processor
// will have been stopped, so calling Stop is not necessary.  Safe to call on
// nil Processor.
func (p *Processor) ForwardClosedTS(closedTS hlc.Timestamp) bool {
	if p == nil {
		return true
	}
	if closedTS == (hlc.Timestamp{}) {
		return true
	}
	return p.sendEvent(event{ct: closedTS}, p.EventChanTimeout)
}

// sendEvent informs the Processor of a new event. If a timeout is specified,
// the method will wait for no longer than that duration before giving up,
// shutting down the Processor, and returning false. 0 for no timeout.
func (p *Processor) sendEvent(e event, timeout time.Duration) bool {
	if timeout == 0 {
		select {
		case p.eventC <- e:
		case <-p.stoppedC:
			// Already stopped. Do nothing.
		}
	} else {
		select {
		case p.eventC <- e:
		case <-p.stoppedC:
			// Already stopped. Do nothing.
		default:
			select {
			case p.eventC <- e:
			case <-p.stoppedC:
				// Already stopped. Do nothing.
			case <-time.After(timeout):
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
func (p *Processor) setResolvedTSInitialized() {
	select {
	case p.eventC <- event{initRTS: true}:
	case <-p.stoppedC:
		// Already stopped. Do nothing.
	}
}

// syncEventC synchronizes access to the Processor goroutine, allowing the
// caller to establish causality with actions taken by the Processor goroutine.
// It does so by flushing the event pipeline.
func (p *Processor) syncEventC() {
	syncC := make(chan struct{})
	select {
	case p.eventC <- event{syncC: syncC}:
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

//
// Methods called from Processor goroutine.
//

func (p *Processor) handleCatchUpScanRes(ctx context.Context, res catchUpResult) {
	if res.pErr == nil {
		res.r.SetCaughtUp()

		// Publish checkpoint to processor even if the resolved timestamp is
		// not initialized. In that case, the timestamp will be empty but the
		// checkpoint event is still useful to indicate that the catch-up scan
		// has completed. This allows clients to rely on stronger ordering
		// semantics once they observe the first checkpoint event.
		p.reg.PublishToReg(res.r, p.newCheckpointEvent())
	} else {
		p.reg.DisconnectRegWithError(res.r, res.pErr)
	}
}

func (p *Processor) consumeEvent(ctx context.Context, e event) {
	switch {
	case len(e.ops) > 0:
		p.consumeLogicalOps(ctx, e.ops)
	case e.ct != hlc.Timestamp{}:
		p.forwardClosedTS(ctx, e.ct)
	case e.initRTS:
		p.initResolvedTS(ctx)
	case e.syncC != nil:
		close(e.syncC)
	default:
		panic("missing event variant")
	}
}

func (p *Processor) consumeLogicalOps(ctx context.Context, ops []enginepb.MVCCLogicalOp) {
	for _, op := range ops {
		// Publish RangeFeedValue updates, if necessary.
		switch t := op.GetValue().(type) {
		case *enginepb.MVCCWriteValueOp:
			// Publish the new value directly.
			p.publishValue(ctx, t.Key, t.Timestamp, t.Value)

		case *enginepb.MVCCWriteIntentOp:
			// No updates to publish.

		case *enginepb.MVCCUpdateIntentOp:
			// No updates to publish.

		case *enginepb.MVCCCommitIntentOp:
			// Publish the newly committed value.
			p.publishValue(ctx, t.Key, t.Timestamp, t.Value)

		case *enginepb.MVCCAbortIntentOp:
			// No updates to publish.

		default:
			panic(fmt.Sprintf("unknown logical op %T", t))
		}

		// Determine whether the operation caused the resolved timestamp to
		// move forward. If so, publish a RangeFeedCheckpoint notification.
		if p.rts.ConsumeLogicalOp(op) {
			p.publishCheckpoint(ctx)
		}
	}
}

func (p *Processor) forwardClosedTS(ctx context.Context, newClosedTS hlc.Timestamp) {
	if p.rts.ForwardClosedTS(newClosedTS) {
		p.publishCheckpoint(ctx)
	}
}

func (p *Processor) initResolvedTS(ctx context.Context) {
	if p.rts.Init() {
		p.publishCheckpoint(ctx)
	}
}

func (p *Processor) publishValue(
	ctx context.Context, key roachpb.Key, timestamp hlc.Timestamp, value []byte,
) {
	if !p.Span.ContainsKey(roachpb.RKey(key)) {
		log.Fatalf(ctx, "key %v not in Processor's key range %v", key, p.Span)
	}

	span := roachpb.Span{Key: key}
	var event roachpb.RangeFeedEvent
	event.MustSetValue(&roachpb.RangeFeedValue{
		Key: key,
		Value: roachpb.Value{
			RawBytes:  value,
			Timestamp: timestamp,
		},
	})
	p.reg.PublishToOverlapping(span, &event)
}

func (p *Processor) publishCheckpoint(ctx context.Context) {
	// TODO(nvanbenschoten): persist resolvedTimestamp. Give Processor a client.DB.
	// TODO(nvanbenschoten): rate limit these? send them periodically?

	event := p.newCheckpointEvent()
	p.reg.PublishToOverlapping(all, event)
}

func (p *Processor) newCheckpointEvent() *roachpb.RangeFeedEvent {
	var event roachpb.RangeFeedEvent
	event.MustSetValue(&roachpb.RangeFeedCheckpoint{
		Span:       p.Span.AsRawSpanWithNoLocals(),
		ResolvedTS: p.rts.Get(),
	})
	return &event
}
