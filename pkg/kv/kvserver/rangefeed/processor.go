// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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

func newRetryErrBufferCapacityExceeded() error {
	return kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_SLOW_CONSUMER)
}

// newErrBufferCapacityExceeded creates an error that is returned to subscribers
// if the rangefeed processor is not able to keep up with the flow of incoming
// events and is forced to drop events in order to not block.
func newErrBufferCapacityExceeded() *kvpb.Error {
	return kvpb.NewError(newRetryErrBufferCapacityExceeded())
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

	// UnregisterFromReplica is a callback provided from the
	// replica that this processor can call when shutting down to
	// remove itself from the replica.
	UnregisterFromReplica func(Processor)
}

// SetDefaults initializes unset fields in Config to values
// suitable for use by a Processor.
func (sc *Config) SetDefaults() {
	// Some tests don't set the TxnPusher, so we avoid setting a default push txn
	// interval in such cases #121429.
	if sc.TxnPusher == nil {
		if sc.PushTxnsAge != 0 {
			panic("nil TxnPusher with non-zero PushTxnsAge")
		}
	} else {
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
		streamCtx context.Context,
		span roachpb.RSpan,
		startTS hlc.Timestamp, // exclusive
		catchUpIter *CatchUpIterator,
		withDiff bool,
		withFiltering bool,
		withOmitRemote bool,
		stream Stream,
	) (bool, Disconnector, *Filter)

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
	return NewScheduledProcessor(cfg)
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

// logicalOpMetadata is metadata associated with a logical Op.
type logicalOpMetadata struct {
	omitInRangefeeds bool
	originID         uint32
}

// IntentScannerConstructor is used to construct an IntentScanner. It
// should be called from underneath a stopper task to ensure that the
// engine has not been closed.
type IntentScannerConstructor func() IntentScanner
