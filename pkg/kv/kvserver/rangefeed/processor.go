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
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/future"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/sched"
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
	Clock   *hlc.Clock
	Stopper *stop.Stopper
	RangeID roachpb.RangeID
	Span    roachpb.RSpan

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

	// Metrics is for production monitoring of RangeFeeds.
	Metrics *Metrics

	// Optional Processor memory budget.
	MemBudget *FeedBudget

	// Scheduler
	UseNewProcessor bool
	Scheduler       sched.ClientScheduler
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
}

// Processor manages a set of rangefeed registrations and handles the routing of
// logical updates to these registrations. While routing logical updates to
// rangefeed registrations, the processor performs two important tasks:
//  1. it translates logical updates into rangefeed events.
//  2. it transforms a range-level closed timestamp to a rangefeed-level resolved
//     timestamp.
type Processor interface {
	// Lifecycle of processor.

	// Start processor with intent scanner.
	// Intent scanner factory should move to config for consistency.
	Start(stopper *stop.Stopper, rtsIterFunc IntentScannerConstructor) error
	Stop()
	StopWithErr(pErr *kvpb.Error)

	// Lifecycle of registrations.

	// Register will add new registration to processor.
	// NB: disconnectFn must not be called from work loop as it will currently
	// call back to processor to check len, filter to update replica state and
	// possibly remove this processor from replica. We can't make a local
	// decision before callback because we can have a concurrent request pending
	// in processor's work queue.
	Register(
		span roachpb.RSpan,
		startTS hlc.Timestamp,
		catchUpIterConstructor CatchUpIteratorConstructor,
		withDiff bool,
		stream Stream,
		sched sched.ClientScheduler,
		disconnectFn func(),
		done *future.ErrorFuture,
	) (bool, *Filter)
	DisconnectSpanWithErr(span roachpb.Span, pErr *kvpb.Error)
	Filter() *Filter
	Len() int

	// Data flow.

	// ConsumeLogicalOps returns false if logical ops were not consumed and
	// processor terminated. Caller must unset this processor as defunct.
	ConsumeLogicalOps(ctx context.Context, ops ...enginepb.MVCCLogicalOp) bool
	// ConsumeSSTable returns false if logical ops were not consumed and
	// processor terminated. Caller must unset this processor as defunct.
	ConsumeSSTable(
		ctx context.Context, sst []byte, sstSpan roachpb.Span, writeTS hlc.Timestamp,
	) bool
	// ForwardClosedTS returns false if logical ops were not consumed and
	// processor terminated. Caller must unset this processor as defunct.
	ForwardClosedTS(ctx context.Context, closedTS hlc.Timestamp) bool
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

// NewProcessor creates a new rangefeed Processor. The corresponding processing
// loop should be launched using the Start method.
func NewProcessor(cfg Config) Processor {
	cfg.SetDefaults()
	cfg.AmbientContext.AddLogTag("rangefeed", nil)
	if cfg.UseNewProcessor {
		// TODO(oleg): remove logging
		log.Infof(context.Background(), "creating new style processor for range feed on r%d", cfg.RangeID)
		return NewScheduledProcessor(cfg)
	}
	return NewLegacyProcessor(cfg)
}

// IntentScannerConstructor is used to construct an IntentScanner. It
// should be called from underneath a stopper task to ensure that the
// engine has not been closed.
type IntentScannerConstructor func() IntentScanner

// CatchUpIteratorConstructor is used to construct an iterator that can be used
// for catchup-scans. Takes the key span and exclusive start time to run the
// catchup scan for. It should be called from underneath a stopper task to
// ensure that the engine has not been closed.
type CatchUpIteratorConstructor func(roachpb.Span, hlc.Timestamp) *CatchUpIterator

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
