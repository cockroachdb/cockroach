// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/future"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

var (
	// DefaultPushTxnsInterval is the default interval at which a Processor will
	// push all transactions in the unresolvedIntentQueue that are above the age
	// specified by PushTxnsAge.
	DefaultPushTxnsInterval = envutil.EnvOrDefaultDuration(
		"COCKROACH_RANGEFEED_PUSH_TXNS_INTERVAL", time.Second)

	// defaultPushTxnsAge is the default age at which a Processor will begin to
	// consider a transaction old enough to push.
	defaultPushTxnsAge = envutil.EnvOrDefaultDuration(
		"COCKROACH_RANGEFEED_PUSH_TXNS_AGE", 10*time.Second)

	// PushTxnsEnabled can be used to disable rangefeed txn pushes, typically to
	// temporarily alleviate contention.
	PushTxnsEnabled = settings.RegisterBoolSetting(
		settings.SystemOnly,
		"kv.rangefeed.push_txns.enabled",
		"periodically push txn write timestamps to advance rangefeed resolved timestamps",
		true,
	)

	// PushTxnsBarrierEnabled is an escape hatch to disable the txn push barrier
	// command in case it causes unexpected problems. This can result in
	// violations of the rangefeed checkpoint guarantee, emitting premature
	// checkpoints before all writes below it have been emitted in rare cases.
	// See: https://github.com/cockroachdb/cockroach/issues/104309
	PushTxnsBarrierEnabled = settings.RegisterBoolSetting(
		settings.SystemOnly,
		"kv.rangefeed.push_txns.barrier.enabled",
		"flush and apply prior writes when a txn push returns an ambiguous abort "+
			"(disabling may emit premature checkpoints before writes in rare cases)",
		true,
	)
)

// newErrBufferCapacityExceeded creates an error that is returned to subscribers
// if the rangefeed processor is not able to keep up with the flow of incoming
// events and is forced to drop events in order to not block.
func newErrBufferCapacityExceeded() *kvpb.Error {
	return kvpb.NewError(
		kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_SLOW_CONSUMER),
	)
}

// Config encompasses the configuration required to create a Processor.
type Config struct {
	log.AmbientContext
	Clock    *hlc.Clock
	Stopper  *stop.Stopper
	Settings *cluster.Settings
	RangeID  roachpb.RangeID
	Span     roachpb.RSpan

	TxnPusher TxnPusher
	// PushTxnsInterval specifies the interval at which a Processor will push
	// all transactions in the unresolvedIntentQueue that are above the age
	// specified by PushTxnsAge.
	//
	// This option only applies to LegacyProcessor since ScheduledProcessor is
	// relying on store to push events to scheduler to initiate transaction push.
	PushTxnsInterval time.Duration
	// PushTxnsAge specifies the age at which a Processor will begin to consider
	// a transaction old enough to push.
	PushTxnsAge time.Duration

	// EventChanCap specifies the capacity to give to the Processor's input
	// channel.
	EventChanCap int
	// EventChanTimeout specifies the maximum time to wait when sending on the
	// Processor's input channel before giving up and shutting down the Processor.
	// 0 disables the timeout, backpressuring writers up through Raft (for tests).
	EventChanTimeout time.Duration

	// Metrics is for production monitoring of RangeFeeds.
	Metrics *Metrics

	// Optional Processor memory budget.
	MemBudget *FeedBudget

	// Rangefeed scheduler to use for processor. If set, SchedulerProcessor would
	// be instantiated.
	Scheduler *Scheduler

	// Priority marks this rangefeed as a priority rangefeed, which will run in a
	// separate scheduler shard with a dedicated worker pool. Should only be used
	// for low-volume system ranges, since the worker pool is small (default 2).
	// Only has an effect when Scheduler is used.
	Priority bool
}

// SetDefaults initializes unset fields in Config to values
// suitable for use by a Processor.
func (sc *Config) SetDefaults() {
	// Some tests don't set the TxnPusher, so we avoid setting a default push txn
	// interval in such cases #121429.
	if sc.TxnPusher == nil {
		if sc.PushTxnsInterval != 0 {
			panic("nil TxnPusher with non-zero PushTxnsInterval")
		}
		if sc.PushTxnsAge != 0 {
			panic("nil TxnPusher with non-zero PushTxnsAge")
		}
	} else {
		if sc.PushTxnsInterval == 0 {
			sc.PushTxnsInterval = DefaultPushTxnsInterval
		}
		if sc.PushTxnsAge == 0 {
			sc.PushTxnsAge = defaultPushTxnsAge
		}
	}
}

// Processor manages a set of rangefeed registrations and handles the routing of
// logical updates to these registrations. While routing logical updates to
// rangefeed registrations, the processor performs two important tasks:
//  1. it translates logical updates into rangefeed events.
//  2. it transforms a range-level closed timestamp to a rangefeed-level resolved
//     timestamp.
type Processor interface {
	// Lifecycle of processor.

	// Start processor will start internal tasks and background initializations.
	// It is ok to start registering streams before background initialization
	// completes.
	//
	// The provided iterator is used to initialize the rangefeed's resolved
	// timestamp. It must obey the contract of an iterator used for an
	// initResolvedTSScan. The Processor promises to clean up the iterator by
	// calling its Close method when it is finished.
	//
	// Note that newRtsIter must be called under the same lock as first
	// registration to ensure that all there would be no missing events.
	// This is currently achieved by Register function synchronizing with
	// the work loop before the lock is released.
	//
	// If the iterator is nil then no initialization scan will be performed and
	// the resolved timestamp will immediately be considered initialized.
	Start(stopper *stop.Stopper, newRtsIter IntentScannerConstructor) error
	// Stop processor and close all registrations.
	//
	// It is meant to be called by replica when it finds that all streams were
	// stopped before removing references to the processor.
	//
	// It is not valid to restart a processor after it has been stopped.
	Stop()
	// StopWithErr terminates all registrations with an error and then winds down
	// any internal processor resources.
	//
	// It is not valid to restart a processor after it has been stopped.
	StopWithErr(pErr *kvpb.Error)

	// Lifecycle of registrations.

	// Register registers the stream over the specified span of keys.
	//
	// The registration will not observe any events that were consumed before this
	// method was called. It is undefined whether the registration will observe
	// events that are consumed concurrently with this call. The channel will be
	// provided an error when the registration closes.
	//
	// The optionally provided "catch-up" iterator is used to read changes from the
	// engine which occurred after the provided start timestamp (exclusive). If
	// this method succeeds, registration must take ownership of iterator and
	// subsequently close it. If method fails, iterator must be kept intact and
	// would be closed by caller.
	//
	// If the method returns false, the processor will have been stopped, so calling
	// Stop is not necessary. If the method returns true, it will also return an
	// updated operation filter that includes the operations required by the new
	// registration.
	//
	// NB: startTS is exclusive; the first possible event will be at startTS.Next().
	Register(
		span roachpb.RSpan,
		startTS hlc.Timestamp, // exclusive
		catchUpIter *CatchUpIterator,
		withDiff bool,
		withFiltering bool,
		stream Stream,
		disconnectFn func(),
		done *future.ErrorFuture,
	) (bool, *Filter)
	// DisconnectSpanWithErr disconnects all rangefeed registrations that overlap
	// the given span with the given error.
	DisconnectSpanWithErr(span roachpb.Span, pErr *kvpb.Error)
	// Filter returns a new operation filter based on the registrations attached to
	// the processor. Returns nil if the processor has been stopped already.
	Filter() *Filter
	// Len returns the number of registrations attached to the processor.
	Len() int

	// Data flow.

	// ConsumeLogicalOps informs the rangefeed processor of the set of logical
	// operations. It returns false if consuming the operations hit a timeout, as
	// specified by the EventChanTimeout configuration. If the method returns false,
	// the processor will have been stopped, so calling Stop is not necessary.
	ConsumeLogicalOps(ctx context.Context, ops ...enginepb.MVCCLogicalOp) bool
	// ConsumeSSTable informs the rangefeed processor of an SSTable that was added
	// via AddSSTable. It returns false if consuming the SSTable hit a timeout, as
	// specified by the EventChanTimeout configuration. If the method returns false,
	// the processor will have been stopped, so calling Stop is not necessary.
	ConsumeSSTable(
		ctx context.Context, sst []byte, sstSpan roachpb.Span, writeTS hlc.Timestamp,
	) bool
	// ForwardClosedTS indicates that the closed timestamp that serves as the basis
	// for the rangefeed processor's resolved timestamp has advanced. It returns
	// false if forwarding the closed timestamp hit a timeout, as specified by the
	// EventChanTimeout configuration. If the method returns false, the processor
	// will have been stopped, so calling Stop is not necessary.
	ForwardClosedTS(ctx context.Context, closedTS hlc.Timestamp) bool

	// External notification integration.

	// ID returns scheduler ID of the processor that can be used to notify it
	// to do some type of work. If ID is 0 then processor doesn't support
	// external event scheduling.
	ID() int64
}

// NewProcessor creates a new rangefeed Processor. The corresponding processing
// loop should be launched using the Start method.
func NewProcessor(cfg Config) Processor {
	cfg.SetDefaults()
	cfg.AmbientContext.AddLogTag("rangefeed", nil)
	if cfg.Scheduler != nil {
		return NewScheduledProcessor(cfg)
	}
	return NewLegacyProcessor(cfg)
}

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

var eventSyncPool = sync.Pool{
	New: func() interface{} {
		return new(event)
	},
}

func getPooledEvent(ev event) *event {
	e := eventSyncPool.Get().(*event)
	*e = ev
	return e
}

func putPooledEvent(ev *event) {
	*ev = event{}
	eventSyncPool.Put(ev)
}

// event is a union of different event types that the Processor goroutine needs
// to be informed of. It is used so that all events can be sent over the same
// channel, which is necessary to prevent reordering.
type event struct {
	// Event variants. Only one set.
	ops     opsEvent
	ct      ctEvent
	initRTS initRTSEvent
	sst     *sstEvent
	sync    *syncEvent
	// Budget allocated to process the event.
	alloc *SharedBudgetAllocation
}

type opsEvent []enginepb.MVCCLogicalOp

type ctEvent struct {
	hlc.Timestamp
}

type initRTSEvent bool

type sstEvent struct {
	data []byte
	span roachpb.Span
	ts   hlc.Timestamp
}

type syncEvent struct {
	c chan struct{}
	// This setting is used in conjunction with c in tests in order to ensure that
	// all registrations have fully finished outputting their buffers. This has to
	// be done by the processor in order to avoid race conditions with the
	// registry. Should be used only in tests.
	testRegCatchupSpan *roachpb.Span
}

// spanErr is an error across a key span that will disconnect overlapping
// registrations.
type spanErr struct {
	span roachpb.Span
	pErr *kvpb.Error
}

func NewLegacyProcessor(cfg Config) *LegacyProcessor {
	p := &LegacyProcessor{
		Config: cfg,
		reg:    makeRegistry(cfg.Metrics),
		rts:    makeResolvedTimestamp(cfg.Settings),

		regC:       make(chan registration),
		unregC:     make(chan *registration),
		lenReqC:    make(chan struct{}),
		lenResC:    make(chan int),
		filterReqC: make(chan struct{}),
		filterResC: make(chan *Filter),
		eventC:     make(chan *event, cfg.EventChanCap),
		spanErrC:   make(chan spanErr),
		stopC:      make(chan *kvpb.Error, 1),
		stoppedC:   make(chan struct{}),
	}
	return p
}

// IntentScannerConstructor is used to construct an IntentScanner. It
// should be called from underneath a stopper task to ensure that the
// engine has not been closed.
type IntentScannerConstructor func() IntentScanner

// Start implements Processor interface.
//
// LegacyProcessor launches a goroutine to process rangefeed events and send
// them to registrations.
//
// Note that to fulfill newRtsIter contract, LegacyProcessor will create
// iterator at the start of its work loop prior to firing async task.
func (p *LegacyProcessor) Start(stopper *stop.Stopper, newRtsIter IntentScannerConstructor) error {
	ctx := p.AnnotateCtx(context.Background())
	if err := stopper.RunAsyncTask(ctx, "rangefeed.LegacyProcessor", func(ctx context.Context) {
		p.Metrics.RangeFeedProcessorsGO.Inc(1)
		defer p.Metrics.RangeFeedProcessorsGO.Dec(1)
		p.run(ctx, p.RangeID, newRtsIter, stopper)
	}); err != nil {
		p.reg.DisconnectWithErr(ctx, all, kvpb.NewError(err))
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

			// Add the new registration to the registry.
			p.reg.Register(ctx, &r)

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
			p.reg.DisconnectWithErr(ctx, e.span, e.pErr)

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
			// Don't perform transaction push attempts if disabled, until the resolved
			// timestamp has been initialized, or if we're not tracking any intents.
			if !PushTxnsEnabled.Get(&p.Settings.SV) || !p.rts.IsInit() || p.rts.intentQ.Len() == 0 {
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
				pushTxns := newTxnPushAttempt(p.Settings, p.Span, p.TxnPusher, p, toPush, now, func() {
					close(txnPushAttemptC)
				})
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
			p.reg.DisconnectAllOnShutdown(ctx, pErr)
			return

		// Exit on stopper.
		case <-stopper.ShouldQuiesce():
			pErr := kvpb.NewError(&kvpb.NodeUnavailableError{})
			p.reg.DisconnectAllOnShutdown(ctx, pErr)
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
	// Flush any remaining events before stopping.
	p.syncEventC()
	// Send the processor a stop signal.
	p.sendStop(pErr)
}

// DisconnectSpanWithErr implements Processor interface.
func (p *LegacyProcessor) DisconnectSpanWithErr(span roachpb.Span, pErr *kvpb.Error) {
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
	catchUpIter *CatchUpIterator,
	withDiff bool,
	withFiltering bool,
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
		span.AsRawSpanWithNoLocals(), startTS, catchUpIter, withDiff, withFiltering,
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
	if len(ops) == 0 {
		return true
	}
	return p.sendEvent(ctx, event{ops: ops}, p.EventChanTimeout)
}

// ConsumeSSTable implements Processor interface.
func (p *LegacyProcessor) ConsumeSSTable(
	ctx context.Context, sst []byte, sstSpan roachpb.Span, writeTS hlc.Timestamp,
) bool {
	return p.sendEvent(ctx, event{sst: &sstEvent{sst, sstSpan, writeTS}}, p.EventChanTimeout)
}

// ForwardClosedTS implements Processor interface.
func (p *LegacyProcessor) ForwardClosedTS(ctx context.Context, closedTS hlc.Timestamp) bool {
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
	p.syncEventCWithEvent(&syncEvent{c: make(chan struct{})})
}

// syncEventCWithEvent allows sync event to be sent and waited on its channel.
// Exposed to allow special test syncEvents that contain span to be sent.
func (p *LegacyProcessor) syncEventCWithEvent(se *syncEvent) {
	ev := getPooledEvent(event{sync: se})
	select {
	case p.eventC <- ev:
		select {
		case <-se.c:
		// Synchronized.
		case <-p.stoppedC:
			// Already stopped. Do nothing.
		}
	case <-p.stoppedC:
		// Already stopped. Return event back to the pool.
		putPooledEvent(ev)
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
		panic(fmt.Sprintf("missing event variant: %+v", e))
	}
}

func (p *LegacyProcessor) consumeLogicalOps(
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
			p.publishValue(ctx, t.Key, t.Timestamp, t.Value, t.PrevValue, t.OmitInRangefeeds, alloc)

		case *enginepb.MVCCDeleteRangeOp:
			// Publish the range deletion directly.
			p.publishDeleteRange(ctx, t.StartKey, t.EndKey, t.Timestamp, alloc)

		case *enginepb.MVCCWriteIntentOp:
			// No updates to publish.

		case *enginepb.MVCCUpdateIntentOp:
			// No updates to publish.

		case *enginepb.MVCCCommitIntentOp:
			// Publish the newly committed value.
			p.publishValue(ctx, t.Key, t.Timestamp, t.Value, t.PrevValue, t.OmitInRangefeeds, alloc)

		case *enginepb.MVCCAbortIntentOp:
			// No updates to publish.

		case *enginepb.MVCCAbortTxnOp:
			// No updates to publish.

		default:
			panic(errors.AssertionFailedf("unknown logical op %T", t))
		}

		// Determine whether the operation caused the resolved timestamp to
		// move forward. If so, publish a RangeFeedCheckpoint notification.
		if p.rts.ConsumeLogicalOp(ctx, op) {
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
	if p.rts.ForwardClosedTS(ctx, newClosedTS) {
		p.publishCheckpoint(ctx)
	}
}

func (p *LegacyProcessor) initResolvedTS(ctx context.Context) {
	if p.rts.Init(ctx) {
		p.publishCheckpoint(ctx)
	}
}

func (p *LegacyProcessor) publishValue(
	ctx context.Context,
	key roachpb.Key,
	timestamp hlc.Timestamp,
	value, prevValue []byte,
	omitInRangefeeds bool,
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
	p.reg.PublishToOverlapping(ctx, roachpb.Span{Key: key}, &event, omitInRangefeeds, alloc)
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
	p.reg.PublishToOverlapping(ctx, span, &event, false /* omitInRangefeeds */, alloc)
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
	}, false /* omitInRangefeeds */, alloc)
}

func (p *LegacyProcessor) publishCheckpoint(ctx context.Context) {
	// TODO(nvanbenschoten): persist resolvedTimestamp. Give Processor a client.DB.
	// TODO(nvanbenschoten): rate limit these? send them periodically?

	event := p.newCheckpointEvent()
	p.reg.PublishToOverlapping(ctx, all, event, false /* omitInRangefeeds */, nil)
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

// ID implements Processor interface.
func (p *LegacyProcessor) ID() int64 {
	return 0
}

// calculateDateEventSize returns estimated size of the event that contain actual
// data. We only account for logical ops and sst's. Those events come from raft
// and are budgeted. Other events come from processor jobs and update timestamps
// we don't take them into account as they are supposed to be small and to avoid
// complexity of having multiple producers getting from budget.
func calculateDateEventSize(e event) int64 {
	var size int64
	for _, op := range e.ops {
		size += int64(op.Size())
	}
	if e.sst != nil {
		size += int64(len(e.sst.data))
	}
	return size
}
