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
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeLogicalOp(val interface{}) enginepb.MVCCLogicalOp {
	var op enginepb.MVCCLogicalOp
	op.MustSetValue(val)
	return op
}

func writeValueOpWithKV(key roachpb.Key, ts hlc.Timestamp, val []byte) enginepb.MVCCLogicalOp {
	return makeLogicalOp(&enginepb.MVCCWriteValueOp{
		Key:       key,
		Timestamp: ts,
		Value:     val,
	})
}

func writeValueOp(ts hlc.Timestamp) enginepb.MVCCLogicalOp {
	return writeValueOpWithKV(roachpb.Key("a"), ts, []byte("val"))
}

func writeIntentOpWithDetails(
	txnID uuid.UUID, key []byte, minTS, ts hlc.Timestamp,
) enginepb.MVCCLogicalOp {
	return makeLogicalOp(&enginepb.MVCCWriteIntentOp{
		TxnID:           txnID,
		TxnKey:          key,
		TxnMinTimestamp: minTS,
		Timestamp:       ts,
	})
}

func writeIntentOpWithKey(txnID uuid.UUID, key []byte, ts hlc.Timestamp) enginepb.MVCCLogicalOp {
	return writeIntentOpWithDetails(txnID, key, ts /* minTS */, ts)
}

func writeIntentOp(txnID uuid.UUID, ts hlc.Timestamp) enginepb.MVCCLogicalOp {
	return writeIntentOpWithKey(txnID, nil /* key */, ts)
}

func updateIntentOp(txnID uuid.UUID, ts hlc.Timestamp) enginepb.MVCCLogicalOp {
	return makeLogicalOp(&enginepb.MVCCUpdateIntentOp{
		TxnID:     txnID,
		Timestamp: ts,
	})
}

func commitIntentOpWithKV(
	txnID uuid.UUID, key roachpb.Key, ts hlc.Timestamp, val []byte,
) enginepb.MVCCLogicalOp {
	return makeLogicalOp(&enginepb.MVCCCommitIntentOp{
		TxnID:     txnID,
		Key:       key,
		Timestamp: ts,
		Value:     val,
	})
}

func commitIntentOp(txnID uuid.UUID, ts hlc.Timestamp) enginepb.MVCCLogicalOp {
	return commitIntentOpWithKV(txnID, roachpb.Key("a"), ts, nil /* val */)
}

func abortIntentOp(txnID uuid.UUID) enginepb.MVCCLogicalOp {
	return makeLogicalOp(&enginepb.MVCCAbortIntentOp{
		TxnID: txnID,
	})
}

func abortTxnOp(txnID uuid.UUID) enginepb.MVCCLogicalOp {
	return makeLogicalOp(&enginepb.MVCCAbortTxnOp{
		TxnID: txnID,
	})
}

func makeRangeFeedEvent(val interface{}) *roachpb.RangeFeedEvent {
	var event roachpb.RangeFeedEvent
	event.MustSetValue(val)
	return &event
}

func rangeFeedValueWithPrev(key roachpb.Key, val, prev roachpb.Value) *roachpb.RangeFeedEvent {
	return makeRangeFeedEvent(&roachpb.RangeFeedValue{
		Key:       key,
		Value:     val,
		PrevValue: prev,
	})
}

func rangeFeedValue(key roachpb.Key, val roachpb.Value) *roachpb.RangeFeedEvent {
	return rangeFeedValueWithPrev(key, val, roachpb.Value{})
}

func rangeFeedCheckpoint(span roachpb.Span, ts hlc.Timestamp) *roachpb.RangeFeedEvent {
	return makeRangeFeedEvent(&roachpb.RangeFeedCheckpoint{
		Span:       span,
		ResolvedTS: ts,
	})
}

const testProcessorEventCCap = 16

func newTestProcessorWithTxnPusher(
	rtsIter storage.SimpleMVCCIterator, txnPusher TxnPusher,
) (*Processor, *stop.Stopper) {
	stopper := stop.NewStopper()

	var pushTxnInterval, pushTxnAge time.Duration = 0, 0 // disable
	if txnPusher != nil {
		pushTxnInterval = 10 * time.Millisecond
		pushTxnAge = 50 * time.Millisecond
	}

	p := NewProcessor(Config{
		AmbientContext:       log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:                hlc.NewClock(hlc.UnixNano, time.Nanosecond),
		Span:                 roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("z")},
		TxnPusher:            txnPusher,
		PushTxnsInterval:     pushTxnInterval,
		PushTxnsAge:          pushTxnAge,
		EventChanCap:         testProcessorEventCCap,
		CheckStreamsInterval: 10 * time.Millisecond,
	})
	p.Start(stopper, makeIteratorConstructor(rtsIter))
	return p, stopper
}

func makeIteratorConstructor(rtsIter storage.SimpleMVCCIterator) IteratorConstructor {
	if rtsIter == nil {
		return nil
	}
	return func() storage.SimpleMVCCIterator { return rtsIter }
}

func newTestProcessor(rtsIter storage.SimpleMVCCIterator) (*Processor, *stop.Stopper) {
	return newTestProcessorWithTxnPusher(rtsIter, nil /* pusher */)
}

func TestProcessorBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p, stopper := newTestProcessor(nil /* rtsIter */)
	defer stopper.Stop(context.Background())

	// Test processor without registrations.
	require.Equal(t, 0, p.Len())
	require.NotPanics(t, func() { p.ConsumeLogicalOps() })
	require.NotPanics(t, func() { p.ConsumeLogicalOps([]enginepb.MVCCLogicalOp{}...) })
	require.NotPanics(t, func() {
		txn1, txn2 := uuid.MakeV4(), uuid.MakeV4()
		p.ConsumeLogicalOps(
			writeValueOp(hlc.Timestamp{WallTime: 1}),
			writeIntentOp(txn1, hlc.Timestamp{WallTime: 2}),
			updateIntentOp(txn1, hlc.Timestamp{WallTime: 3}),
			commitIntentOp(txn1, hlc.Timestamp{WallTime: 4}),
			writeIntentOp(txn2, hlc.Timestamp{WallTime: 5}),
			abortIntentOp(txn2),
		)
		p.syncEventC()
		require.Equal(t, 0, p.rts.intentQ.Len())
	})
	require.NotPanics(t, func() { p.ForwardClosedTS(hlc.Timestamp{}) })
	require.NotPanics(t, func() { p.ForwardClosedTS(hlc.Timestamp{WallTime: 1}) })

	// Add a registration.
	r1Stream := newTestStream()
	r1ErrC := make(chan *roachpb.Error, 1)
	r1OK, r1Filter := p.Register(
		roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
		hlc.Timestamp{WallTime: 1},
		nil,   /* catchUpIter */
		false, /* withDiff */
		r1Stream,
		r1ErrC,
	)
	require.True(t, r1OK)
	p.syncEventAndRegistrations()
	require.Equal(t, 1, p.Len())
	require.Equal(t,
		[]*roachpb.RangeFeedEvent{rangeFeedCheckpoint(
			roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
			hlc.Timestamp{WallTime: 1},
		)},
		r1Stream.Events(),
	)

	// Test the processor's operation filter.
	require.True(t, r1Filter.NeedVal(roachpb.Span{Key: roachpb.Key("a")}))
	require.True(t, r1Filter.NeedVal(roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("r")}))
	require.False(t, r1Filter.NeedVal(roachpb.Span{Key: roachpb.Key("z")}))
	require.False(t, r1Filter.NeedPrevVal(roachpb.Span{Key: roachpb.Key("a")}))
	require.False(t, r1Filter.NeedPrevVal(roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("r")}))
	require.False(t, r1Filter.NeedPrevVal(roachpb.Span{Key: roachpb.Key("z")}))

	// Test checkpoint with one registration.
	p.ForwardClosedTS(hlc.Timestamp{WallTime: 5})
	p.syncEventAndRegistrations()
	require.Equal(t,
		[]*roachpb.RangeFeedEvent{rangeFeedCheckpoint(
			roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
			hlc.Timestamp{WallTime: 5},
		)},
		r1Stream.Events(),
	)

	// Test value with one registration.
	p.ConsumeLogicalOps(
		writeValueOpWithKV(roachpb.Key("c"), hlc.Timestamp{WallTime: 6}, []byte("val")),
	)
	p.syncEventAndRegistrations()
	require.Equal(t,
		[]*roachpb.RangeFeedEvent{rangeFeedValue(
			roachpb.Key("c"),
			roachpb.Value{
				RawBytes:  []byte("val"),
				Timestamp: hlc.Timestamp{WallTime: 6},
			},
		)},
		r1Stream.Events(),
	)

	// Test value to non-overlapping key with one registration.
	p.ConsumeLogicalOps(
		writeValueOpWithKV(roachpb.Key("s"), hlc.Timestamp{WallTime: 6}, []byte("val")),
	)
	p.syncEventAndRegistrations()
	require.Equal(t, []*roachpb.RangeFeedEvent(nil), r1Stream.Events())

	// Test intent that is aborted with one registration.
	txn1 := uuid.MakeV4()
	// Write intent.
	p.ConsumeLogicalOps(writeIntentOp(txn1, hlc.Timestamp{WallTime: 6}))
	p.syncEventAndRegistrations()
	require.Equal(t, []*roachpb.RangeFeedEvent(nil), r1Stream.Events())
	// Abort.
	p.ConsumeLogicalOps(abortIntentOp(txn1))
	p.syncEventC()
	require.Equal(t, []*roachpb.RangeFeedEvent(nil), r1Stream.Events())
	require.Equal(t, 0, p.rts.intentQ.Len())

	// Test intent that is committed with one registration.
	txn2 := uuid.MakeV4()
	// Write intent.
	p.ConsumeLogicalOps(writeIntentOp(txn2, hlc.Timestamp{WallTime: 10}))
	p.syncEventAndRegistrations()
	require.Equal(t, []*roachpb.RangeFeedEvent(nil), r1Stream.Events())
	// Forward closed timestamp. Should now be stuck on intent.
	p.ForwardClosedTS(hlc.Timestamp{WallTime: 15})
	p.syncEventAndRegistrations()
	require.Equal(t,
		[]*roachpb.RangeFeedEvent{rangeFeedCheckpoint(
			roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
			hlc.Timestamp{WallTime: 9},
		)},
		r1Stream.Events(),
	)
	// Update the intent. Should forward resolved timestamp.
	p.ConsumeLogicalOps(updateIntentOp(txn2, hlc.Timestamp{WallTime: 12}))
	p.syncEventAndRegistrations()
	require.Equal(t,
		[]*roachpb.RangeFeedEvent{rangeFeedCheckpoint(
			roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
			hlc.Timestamp{WallTime: 11},
		)},
		r1Stream.Events(),
	)
	// Commit intent. Should forward resolved timestamp to closed timestamp.
	p.ConsumeLogicalOps(
		commitIntentOpWithKV(txn2, roachpb.Key("e"), hlc.Timestamp{WallTime: 13}, []byte("ival")),
	)
	p.syncEventAndRegistrations()
	require.Equal(t,
		[]*roachpb.RangeFeedEvent{
			rangeFeedValue(
				roachpb.Key("e"),
				roachpb.Value{
					RawBytes:  []byte("ival"),
					Timestamp: hlc.Timestamp{WallTime: 13},
				},
			),
			rangeFeedCheckpoint(
				roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
				hlc.Timestamp{WallTime: 15},
			),
		},
		r1Stream.Events(),
	)

	// Add another registration with withDiff = true.
	r2Stream := newTestStream()
	r2ErrC := make(chan *roachpb.Error, 1)
	r2OK, r1And2Filter := p.Register(
		roachpb.RSpan{Key: roachpb.RKey("c"), EndKey: roachpb.RKey("z")},
		hlc.Timestamp{WallTime: 1},
		nil,  /* catchUpIter */
		true, /* withDiff */
		r2Stream,
		r2ErrC,
	)
	require.True(t, r2OK)
	p.syncEventAndRegistrations()
	require.Equal(t, 2, p.Len())
	require.Equal(t,
		[]*roachpb.RangeFeedEvent{rangeFeedCheckpoint(
			roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("z")},
			hlc.Timestamp{WallTime: 15},
		)},
		r2Stream.Events(),
	)

	// Test the processor's new operation filter.
	require.True(t, r1And2Filter.NeedVal(roachpb.Span{Key: roachpb.Key("a")}))
	require.True(t, r1And2Filter.NeedVal(roachpb.Span{Key: roachpb.Key("y")}))
	require.True(t, r1And2Filter.NeedVal(roachpb.Span{Key: roachpb.Key("y"), EndKey: roachpb.Key("zzz")}))
	require.False(t, r1And2Filter.NeedVal(roachpb.Span{Key: roachpb.Key("zzz")}))
	require.False(t, r1And2Filter.NeedPrevVal(roachpb.Span{Key: roachpb.Key("a")}))
	require.True(t, r1And2Filter.NeedPrevVal(roachpb.Span{Key: roachpb.Key("y")}))
	require.True(t, r1And2Filter.NeedPrevVal(roachpb.Span{Key: roachpb.Key("y"), EndKey: roachpb.Key("zzz")}))
	require.False(t, r1And2Filter.NeedPrevVal(roachpb.Span{Key: roachpb.Key("zzz")}))

	// Both registrations should see checkpoint.
	p.ForwardClosedTS(hlc.Timestamp{WallTime: 20})
	p.syncEventAndRegistrations()
	chEventAM := []*roachpb.RangeFeedEvent{rangeFeedCheckpoint(
		roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
		hlc.Timestamp{WallTime: 20},
	)}
	require.Equal(t, chEventAM, r1Stream.Events())
	chEventCZ := []*roachpb.RangeFeedEvent{rangeFeedCheckpoint(
		roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("z")},
		hlc.Timestamp{WallTime: 20},
	)}
	require.Equal(t, chEventCZ, r2Stream.Events())

	// Test value with two registration that overlaps both.
	p.ConsumeLogicalOps(
		writeValueOpWithKV(roachpb.Key("k"), hlc.Timestamp{WallTime: 22}, []byte("val2")),
	)
	p.syncEventAndRegistrations()
	valEvent := []*roachpb.RangeFeedEvent{rangeFeedValue(
		roachpb.Key("k"),
		roachpb.Value{
			RawBytes:  []byte("val2"),
			Timestamp: hlc.Timestamp{WallTime: 22},
		},
	)}
	require.Equal(t, valEvent, r1Stream.Events())
	require.Equal(t, valEvent, r2Stream.Events())

	// Test value that only overlaps the second registration.
	p.ConsumeLogicalOps(
		writeValueOpWithKV(roachpb.Key("v"), hlc.Timestamp{WallTime: 23}, []byte("val3")),
	)
	p.syncEventAndRegistrations()
	valEvent2 := []*roachpb.RangeFeedEvent{rangeFeedValue(
		roachpb.Key("v"),
		roachpb.Value{
			RawBytes:  []byte("val3"),
			Timestamp: hlc.Timestamp{WallTime: 23},
		},
	)}
	require.Equal(t, []*roachpb.RangeFeedEvent(nil), r1Stream.Events())
	require.Equal(t, valEvent2, r2Stream.Events())

	// Cancel the first registration.
	r1Stream.Cancel()
	require.NotNil(t, <-r1ErrC)

	// Stop the processor with an error.
	pErr := roachpb.NewErrorf("stop err")
	p.StopWithErr(pErr)
	require.NotNil(t, <-r2ErrC)

	// Adding another registration should fail.
	r3Stream := newTestStream()
	r3ErrC := make(chan *roachpb.Error, 1)
	r3OK, _ := p.Register(
		roachpb.RSpan{Key: roachpb.RKey("c"), EndKey: roachpb.RKey("z")},
		hlc.Timestamp{WallTime: 1},
		nil,   /* catchUpIter */
		false, /* withDiff */
		r3Stream,
		r3ErrC,
	)
	require.False(t, r3OK)
}

func TestNilProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var p *Processor

	// All of the following should be no-ops.
	require.Equal(t, 0, p.Len())
	require.NotPanics(t, func() { p.Stop() })
	require.NotPanics(t, func() { p.StopWithErr(nil) })
	require.NotPanics(t, func() { p.ConsumeLogicalOps() })
	require.NotPanics(t, func() { p.ConsumeLogicalOps(make([]enginepb.MVCCLogicalOp, 5)...) })
	require.NotPanics(t, func() { p.ForwardClosedTS(hlc.Timestamp{}) })
	require.NotPanics(t, func() { p.ForwardClosedTS(hlc.Timestamp{WallTime: 1}) })

	// The following should panic because they are not safe
	// to call on a nil Processor.
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	require.Panics(t, func() { p.Start(stopper, nil) })
	require.Panics(t, func() { p.Register(roachpb.RSpan{}, hlc.Timestamp{}, nil, false, nil, nil) })
}

func TestProcessorSlowConsumer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p, stopper := newTestProcessor(nil /* rtsIter */)
	defer stopper.Stop(context.Background())

	// Add a registration.
	r1Stream := newTestStream()
	r1ErrC := make(chan *roachpb.Error, 1)
	p.Register(
		roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
		hlc.Timestamp{WallTime: 1},
		nil,   /* catchUpIter */
		false, /* withDiff */
		r1Stream,
		r1ErrC,
	)
	r2Stream := newTestStream()
	r2ErrC := make(chan *roachpb.Error, 1)
	p.Register(
		roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("z")},
		hlc.Timestamp{WallTime: 1},
		nil,   /* catchUpIter */
		false, /* withDiff */
		r2Stream,
		r2ErrC,
	)
	p.syncEventAndRegistrations()
	require.Equal(t, 2, p.Len())
	require.Equal(t,
		[]*roachpb.RangeFeedEvent{rangeFeedCheckpoint(
			roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
			hlc.Timestamp{WallTime: 0},
		)},
		r1Stream.Events(),
	)
	require.Equal(t,
		[]*roachpb.RangeFeedEvent{rangeFeedCheckpoint(
			roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			hlc.Timestamp{WallTime: 0},
		)},
		r2Stream.Events(),
	)

	// Block its Send method and fill up the registration's input channel.
	unblock := r1Stream.BlockSend()
	defer func() {
		if unblock != nil {
			unblock()
		}
	}()
	// Need one more message to fill the channel because the first one will be
	// sent to the stream and block the registration outputLoop goroutine.
	toFill := testProcessorEventCCap + 1
	for i := 0; i < toFill; i++ {
		ts := hlc.Timestamp{WallTime: int64(i + 2)}
		p.ConsumeLogicalOps(
			writeValueOpWithKV(roachpb.Key("k"), ts, []byte("val")),
		)

		// Wait for just the unblocked registration to catch up. This prevents
		// the race condition where this registration overflows anyway due to
		// the rapid event consumption and small buffer size.
		p.syncEventAndRegistrationSpan(spXY)
	}

	// Consume one more event. Should not block, but should cause r1 to overflow
	// its registration buffer and drop the event.
	p.ConsumeLogicalOps(
		writeValueOpWithKV(roachpb.Key("k"), hlc.Timestamp{WallTime: 18}, []byte("val")),
	)

	// Wait for just the unblocked registration to catch up.
	p.syncEventAndRegistrationSpan(spXY)
	require.Equal(t, toFill+1, len(r2Stream.Events()))
	require.Equal(t, 2, p.reg.Len())

	// Unblock the send channel. The events should quickly be consumed.
	unblock()
	unblock = nil
	p.syncEventAndRegistrations()
	// At least one event should have been dropped due to overflow. We expect
	// exactly one event to be dropped, but it is possible that multiple events
	// were dropped due to rapid event consumption before the r1's outputLoop
	// began consuming from its event buffer.
	require.LessOrEqual(t, len(r1Stream.Events()), toFill)
	require.Equal(t, newErrBufferCapacityExceeded().GoError(), (<-r1ErrC).GoError())
	testutils.SucceedsSoon(t, func() error {
		if act, exp := p.Len(), 1; exp != act {
			return fmt.Errorf("processor had %d regs, wanted %d", act, exp)
		}
		return nil
	})
}

// TestProcessorInitializeResolvedTimestamp tests that when a Processor is given
// a resolved timestamp iterator, it doesn't initialize its resolved timestamp
// until it has consumed all intents in the iterator.
func TestProcessorInitializeResolvedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	txn1, txn2 := uuid.MakeV4(), uuid.MakeV4()
	rtsIter := newTestIterator([]storage.MVCCKeyValue{
		makeKV("a", "val1", 10),
		makeInline("b", "val2"),
		makeIntent("c", txn1, "txnKey1", 15),
		makeProvisionalKV("c", "txnKey1", 15),
		makeKV("c", "val3", 11),
		makeKV("c", "val4", 9),
		makeIntent("d", txn2, "txnKey2", 21),
		makeProvisionalKV("d", "txnKey2", 21),
		makeKV("d", "val5", 20),
		makeKV("d", "val6", 19),
		makeInline("g", "val7"),
		makeKV("m", "val8", 1),
		makeIntent("n", txn1, "txnKey1", 12),
		makeProvisionalKV("n", "txnKey1", 12),
		makeIntent("r", txn1, "txnKey1", 19),
		makeProvisionalKV("r", "txnKey1", 19),
		makeKV("r", "val9", 4),
		makeIntent("w", txn1, "txnKey1", 3),
		makeProvisionalKV("w", "txnKey1", 3),
		makeInline("x", "val10"),
		makeIntent("z", txn2, "txnKey2", 21),
		makeProvisionalKV("z", "txnKey2", 21),
		makeKV("z", "val11", 4),
	})
	rtsIter.block = make(chan struct{})

	p, stopper := newTestProcessor(rtsIter)
	defer stopper.Stop(context.Background())

	// The resolved timestamp should not be initialized.
	require.False(t, p.rts.IsInit())
	require.Equal(t, hlc.Timestamp{}, p.rts.Get())

	// Add a registration.
	r1Stream := newTestStream()
	p.Register(
		roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
		hlc.Timestamp{WallTime: 1},
		nil,   /* catchUpIter */
		false, /* withDiff */
		r1Stream,
		make(chan *roachpb.Error, 1),
	)
	p.syncEventAndRegistrations()
	require.Equal(t, 1, p.Len())

	// The registration should be provided a checkpoint immediately with an
	// empty resolved timestamp because it did not perform a catch-up scan.
	chEvent := []*roachpb.RangeFeedEvent{rangeFeedCheckpoint(
		roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
		hlc.Timestamp{},
	)}
	require.Equal(t, chEvent, r1Stream.Events())

	// The resolved timestamp should still not be initialized.
	require.False(t, p.rts.IsInit())
	require.Equal(t, hlc.Timestamp{}, p.rts.Get())

	// Forward the closed timestamp. The resolved timestamp should still
	// not be initialized.
	p.ForwardClosedTS(hlc.Timestamp{WallTime: 20})
	require.False(t, p.rts.IsInit())
	require.Equal(t, hlc.Timestamp{}, p.rts.Get())

	// Let the scan proceed.
	close(rtsIter.block)
	<-rtsIter.done
	require.True(t, rtsIter.closed)

	// Synchronize the event channel then verify that the resolved timestamp is
	// initialized and that it's blocked on the oldest unresolved intent's txn
	// timestamp. Txn1 has intents at many times but the unresolvedIntentQueue
	// tracks its latest, which is 19, so the resolved timestamp is
	// 19.FloorPrev() = 18.
	p.syncEventAndRegistrations()
	require.True(t, p.rts.IsInit())
	require.Equal(t, hlc.Timestamp{WallTime: 18}, p.rts.Get())

	// The registration should have been informed of the new resolved timestamp.
	chEvent = []*roachpb.RangeFeedEvent{rangeFeedCheckpoint(
		roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
		hlc.Timestamp{WallTime: 18},
	)}
	require.Equal(t, chEvent, r1Stream.Events())
}

func TestProcessorTxnPushAttempt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts10 := hlc.Timestamp{WallTime: 10}
	ts20 := hlc.Timestamp{WallTime: 20}
	ts25 := hlc.Timestamp{WallTime: 25}
	ts30 := hlc.Timestamp{WallTime: 30}
	ts50 := hlc.Timestamp{WallTime: 50}
	ts60 := hlc.Timestamp{WallTime: 60}
	ts70 := hlc.Timestamp{WallTime: 70}
	ts90 := hlc.Timestamp{WallTime: 90}

	// Create a set of transactions.
	txn1, txn2, txn3 := uuid.MakeV4(), uuid.MakeV4(), uuid.MakeV4()
	txn1Meta := enginepb.TxnMeta{ID: txn1, Key: keyA, WriteTimestamp: ts10, MinTimestamp: ts10}
	txn2Meta := enginepb.TxnMeta{ID: txn2, Key: keyB, WriteTimestamp: ts20, MinTimestamp: ts20}
	txn3Meta := enginepb.TxnMeta{ID: txn3, Key: keyC, WriteTimestamp: ts30, MinTimestamp: ts30}
	txn1Proto := &roachpb.Transaction{TxnMeta: txn1Meta, Status: roachpb.PENDING}
	txn2Proto := &roachpb.Transaction{TxnMeta: txn2Meta, Status: roachpb.PENDING}
	txn3Proto := &roachpb.Transaction{TxnMeta: txn3Meta, Status: roachpb.PENDING}

	// Modifications for test 2.
	txn1MetaT2Pre := enginepb.TxnMeta{ID: txn1, Key: keyA, WriteTimestamp: ts25, MinTimestamp: ts10}
	txn1MetaT2Post := enginepb.TxnMeta{ID: txn1, Key: keyA, WriteTimestamp: ts50, MinTimestamp: ts10}
	txn2MetaT2Post := enginepb.TxnMeta{ID: txn2, Key: keyB, WriteTimestamp: ts60, MinTimestamp: ts20}
	txn3MetaT2Post := enginepb.TxnMeta{ID: txn3, Key: keyC, WriteTimestamp: ts70, MinTimestamp: ts30}
	txn1ProtoT2 := &roachpb.Transaction{TxnMeta: txn1MetaT2Post, Status: roachpb.COMMITTED}
	txn2ProtoT2 := &roachpb.Transaction{TxnMeta: txn2MetaT2Post, Status: roachpb.PENDING}
	txn3ProtoT2 := &roachpb.Transaction{TxnMeta: txn3MetaT2Post, Status: roachpb.PENDING}

	// Modifications for test 3.
	txn2MetaT3Post := enginepb.TxnMeta{ID: txn2, Key: keyB, WriteTimestamp: ts60, MinTimestamp: ts20}
	txn3MetaT3Post := enginepb.TxnMeta{ID: txn3, Key: keyC, WriteTimestamp: ts90, MinTimestamp: ts30}
	txn2ProtoT3 := &roachpb.Transaction{TxnMeta: txn2MetaT3Post, Status: roachpb.ABORTED}
	txn3ProtoT3 := &roachpb.Transaction{TxnMeta: txn3MetaT3Post, Status: roachpb.PENDING}

	testNum := 0
	pausePushAttemptsC := make(chan struct{})
	resumePushAttemptsC := make(chan struct{})
	defer close(pausePushAttemptsC)
	defer close(resumePushAttemptsC)

	// Create a TxnPusher that performs assertions during the first 3 uses.
	var tp testTxnPusher
	tp.mockPushTxns(func(txns []enginepb.TxnMeta, ts hlc.Timestamp) ([]*roachpb.Transaction, error) {
		// The txns are not in a sorted order. Enforce one.
		sort.Slice(txns, func(i, j int) bool {
			return bytes.Compare(txns[i].Key, txns[j].Key) < 0
		})

		testNum++
		switch testNum {
		case 1:
			assert.Equal(t, 3, len(txns))
			assert.Equal(t, txn1Meta, txns[0])
			assert.Equal(t, txn2Meta, txns[1])
			assert.Equal(t, txn3Meta, txns[2])
			if t.Failed() {
				return nil, errors.New("test failed")
			}

			// Push does not succeed. Protos not at larger ts.
			return []*roachpb.Transaction{txn1Proto, txn2Proto, txn3Proto}, nil
		case 2:
			assert.Equal(t, 3, len(txns))
			assert.Equal(t, txn1MetaT2Pre, txns[0])
			assert.Equal(t, txn2Meta, txns[1])
			assert.Equal(t, txn3Meta, txns[2])
			if t.Failed() {
				return nil, errors.New("test failed")
			}

			// Push succeeds. Return new protos.
			return []*roachpb.Transaction{txn1ProtoT2, txn2ProtoT2, txn3ProtoT2}, nil
		case 3:
			assert.Equal(t, 2, len(txns))
			assert.Equal(t, txn2MetaT2Post, txns[0])
			assert.Equal(t, txn3MetaT2Post, txns[1])
			if t.Failed() {
				return nil, errors.New("test failed")
			}

			// Push succeeds. Return new protos.
			return []*roachpb.Transaction{txn2ProtoT3, txn3ProtoT3}, nil
		default:
			return nil, nil
		}
	})
	tp.mockResolveIntentsFn(func(ctx context.Context, intents []roachpb.LockUpdate) error {
		// There's nothing to assert here. We expect the intents to correspond to
		// transactions that had their LockSpans populated when we pushed them. This
		// test doesn't simulate that.

		if testNum > 3 {
			return nil
		}

		<-pausePushAttemptsC
		<-resumePushAttemptsC
		return nil
	})

	p, stopper := newTestProcessorWithTxnPusher(nil /* rtsIter */, &tp)
	defer stopper.Stop(context.Background())

	// Add a few intents and move the closed timestamp forward.
	writeIntentOpFromMeta := func(txn enginepb.TxnMeta) enginepb.MVCCLogicalOp {
		return writeIntentOpWithDetails(txn.ID, txn.Key, txn.MinTimestamp, txn.WriteTimestamp)
	}
	p.ConsumeLogicalOps(
		writeIntentOpFromMeta(txn1Meta),
		writeIntentOpFromMeta(txn2Meta),
		writeIntentOpFromMeta(txn2Meta),
		writeIntentOpFromMeta(txn3Meta),
	)
	p.ForwardClosedTS(hlc.Timestamp{WallTime: 40})
	p.syncEventC()
	require.Equal(t, hlc.Timestamp{WallTime: 9}, p.rts.Get())

	// Wait for the first txn push attempt to complete.
	pausePushAttemptsC <- struct{}{}

	// The resolved timestamp hasn't moved.
	p.syncEventC()
	require.Equal(t, hlc.Timestamp{WallTime: 9}, p.rts.Get())

	// Write another intent for one of the txns. This moves the resolved
	// timestamp forward.
	p.ConsumeLogicalOps(writeIntentOpFromMeta(txn1MetaT2Pre))
	p.syncEventC()
	require.Equal(t, hlc.Timestamp{WallTime: 19}, p.rts.Get())

	// Unblock the second txn push attempt and wait for it to complete.
	resumePushAttemptsC <- struct{}{}
	pausePushAttemptsC <- struct{}{}

	// The resolved timestamp should have moved forwards to the closed
	// timestamp.
	p.syncEventC()
	require.Equal(t, hlc.Timestamp{WallTime: 40}, p.rts.Get())

	// Forward the closed timestamp.
	p.ForwardClosedTS(hlc.Timestamp{WallTime: 80})
	p.syncEventC()
	require.Equal(t, hlc.Timestamp{WallTime: 49}, p.rts.Get())

	// Txn1's first intent is committed. Resolved timestamp doesn't change.
	p.ConsumeLogicalOps(
		commitIntentOp(txn1MetaT2Post.ID, txn1MetaT2Post.WriteTimestamp),
	)
	p.syncEventC()
	require.Equal(t, hlc.Timestamp{WallTime: 49}, p.rts.Get())

	// Txn1's second intent is committed. Resolved timestamp moves forward.
	p.ConsumeLogicalOps(
		commitIntentOp(txn1MetaT2Post.ID, txn1MetaT2Post.WriteTimestamp),
	)
	p.syncEventC()
	require.Equal(t, hlc.Timestamp{WallTime: 59}, p.rts.Get())

	// Unblock the third txn push attempt and wait for it to complete.
	resumePushAttemptsC <- struct{}{}
	pausePushAttemptsC <- struct{}{}

	// The resolved timestamp should have moved forwards to the closed
	// timestamp.
	p.syncEventC()
	require.Equal(t, hlc.Timestamp{WallTime: 80}, p.rts.Get())

	// Forward the closed timestamp.
	p.ForwardClosedTS(hlc.Timestamp{WallTime: 100})
	p.syncEventC()
	require.Equal(t, hlc.Timestamp{WallTime: 89}, p.rts.Get())

	// Commit txn3's only intent. Resolved timestamp moves forward.
	p.ConsumeLogicalOps(
		commitIntentOp(txn3MetaT3Post.ID, txn3MetaT3Post.WriteTimestamp),
	)
	p.syncEventC()
	require.Equal(t, hlc.Timestamp{WallTime: 100}, p.rts.Get())

	// Release push attempt to avoid deadlock.
	resumePushAttemptsC <- struct{}{}
}

// TestProcessorConcurrentStop tests that all methods in Processor's API
// correctly handle the processor concurrently shutting down. If they did
// not then it would be possible for them to deadlock.
func TestProcessorConcurrentStop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const trials = 10
	for i := 0; i < trials; i++ {
		p, stopper := newTestProcessor(nil /* rtsIter */)

		var wg sync.WaitGroup
		wg.Add(6)
		go func() {
			defer wg.Done()
			runtime.Gosched()
			s := newTestStream()
			errC := make(chan<- *roachpb.Error, 1)
			p.Register(p.Span, hlc.Timestamp{}, nil, false, s, errC)
		}()
		go func() {
			defer wg.Done()
			runtime.Gosched()
			p.Len()
		}()
		go func() {
			defer wg.Done()
			runtime.Gosched()
			p.ConsumeLogicalOps(
				writeValueOpWithKV(roachpb.Key("s"), hlc.Timestamp{WallTime: 6}, []byte("val")),
			)
		}()
		go func() {
			defer wg.Done()
			runtime.Gosched()
			p.ForwardClosedTS(hlc.Timestamp{WallTime: 2})
		}()
		go func() {
			defer wg.Done()
			runtime.Gosched()
			p.Stop()
		}()
		go func() {
			defer wg.Done()
			runtime.Gosched()
			stopper.Stop(context.Background())
		}()
		wg.Wait()
	}
}

// TestProcessorRegistrationObservesOnlyNewEvents tests that a registration
// observes only operations that are consumed after it has registered.
func TestProcessorRegistrationObservesOnlyNewEvents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p, stopper := newTestProcessor(nil /* rtsIter */)
	defer stopper.Stop(context.Background())

	firstC := make(chan int64)
	regDone := make(chan struct{})
	regs := make(map[*testStream]int64)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := int64(1); i < 250; i++ {
			// Add a new registration every 10 ops.
			if i%10 == 0 {
				firstC <- i
				<-regDone
			}

			// Consume the logical op. Encode the index in the timestamp.
			p.ConsumeLogicalOps(writeValueOp(hlc.Timestamp{WallTime: i}))
		}
		p.syncEventC()
		close(firstC)
	}()
	go func() {
		defer wg.Done()
		for firstIdx := range firstC {
			// For each index, create a new registration. The first
			// operation is should see is firstIdx.
			s := newTestStream()
			regs[s] = firstIdx
			errC := make(chan *roachpb.Error, 1)
			p.Register(p.Span, hlc.Timestamp{}, nil, false, s, errC)
			regDone <- struct{}{}
		}
	}()
	wg.Wait()
	p.syncEventAndRegistrations()

	// Verify that no registrations were given operations
	// from before they registered.
	for s, expFirstIdx := range regs {
		events := s.Events()
		require.IsType(t, &roachpb.RangeFeedCheckpoint{}, events[0].GetValue())
		require.IsType(t, &roachpb.RangeFeedValue{}, events[1].GetValue())

		firstVal := events[1].GetValue().(*roachpb.RangeFeedValue)
		firstIdx := firstVal.Value.Timestamp.WallTime
		require.Equal(t, expFirstIdx, firstIdx)
	}
}

// syncEventAndRegistrations waits for all previously sent events to be
// processed *and* for all registration output loops to fully process their own
// internal buffers.
func (p *Processor) syncEventAndRegistrations() {
	p.syncEventAndRegistrationSpan(all)
}

// syncEventAndRegistrations waits for all previously sent events to be
// processed *and* for all registration output loops for registrations
// overlapping the given span to fully process their own internal buffers.
func (p *Processor) syncEventAndRegistrationSpan(span roachpb.Span) {
	syncC := make(chan struct{})
	ev := getPooledEvent(event{syncC: syncC, testRegCatchupSpan: span})
	select {
	case p.eventC <- ev:
		select {
		case <-syncC:
		// Synchronized.
		case <-p.stoppedC:
			// Already stopped. Do nothing.
		}
	case <-p.stoppedC:
		putPooledEvent(ev)
		// Already stopped. Do nothing.
	}
}
