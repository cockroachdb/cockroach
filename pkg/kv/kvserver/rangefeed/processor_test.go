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
	"math"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/assert"
)

func TestProcessorBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutils.RunValues(t, "proc type", testTypes, func(t *testing.T, pt procType) {
		p, h, stopper := newTestProcessor(t, withProcType(pt))
		ctx := context.Background()

		serverStream := newTestServerStream()
		streamMuxer := NewStreamMuxer(serverStream, newTestRangefeedCounter())
		var wg sync.WaitGroup
		wg.Add(1)
		defer wg.Wait()
		// Make sure to shut down the muxer before wg.Wait().
		defer stopper.Stop(ctx)
		if err := stopper.RunAsyncTask(ctx, "mux-term-forwarder", func(ctx context.Context) {
			defer wg.Done()
			streamMuxer.Run(ctx, stopper)
		}); err != nil {
			wg.Done()
		}

		fmt.Println("HERE1")
		// Test processor without registrations.
		require.Equal(t, 0, p.Len())
		require.NotPanics(t, func() { p.ConsumeLogicalOps(ctx) })
		require.NotPanics(t, func() { p.ConsumeLogicalOps(ctx, []enginepb.MVCCLogicalOp{}...) })
		require.NotPanics(t, func() {
			txn1, txn2 := uuid.MakeV4(), uuid.MakeV4()
			p.ConsumeLogicalOps(ctx,
				writeValueOp(hlc.Timestamp{WallTime: 1}),
				writeIntentOp(txn1, hlc.Timestamp{WallTime: 2}),
				updateIntentOp(txn1, hlc.Timestamp{WallTime: 3}),
				commitIntentOp(txn1, hlc.Timestamp{WallTime: 4}),
				writeIntentOp(txn2, hlc.Timestamp{WallTime: 5}),
				abortIntentOp(txn2))
			h.syncEventC()
			require.Equal(t, 0, h.rts.intentQ.Len())
		})
		require.NotPanics(t, func() { p.ForwardClosedTS(ctx, hlc.Timestamp{}) })
		require.NotPanics(t, func() { p.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 1}) })

		fmt.Println("HERE2")

		// Add a registration.
		const r1StreamId, r2StreamId, r3StreamID = int64(1), int64(2), int64(3)
		r1Stream := newTestSingleFeedStream(streamMuxer, r1StreamId, serverStream)
		r1OK, r1Filter := p.Register(
			roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
			hlc.Timestamp{WallTime: 1},
			nil,   /* catchUpIter */
			false, /* withDiff */
			false, /* withFiltering */
			r1Stream,
			func() {},
		)
		require.True(t, r1OK)
		h.syncEventAndRegistrations()
		require.Equal(t, 1, p.Len())
		require.Equal(t,
			[]*kvpb.RangeFeedEvent{
				rangeFeedCheckpoint(
					roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
					hlc.Timestamp{WallTime: 1},
				),
			},
			serverStream.rangefeedEventsSentById(r1StreamId),
		)

		fmt.Println("HERE3")

		// Test the processor's operation filter.
		require.True(t, r1Filter.NeedVal(roachpb.Span{Key: roachpb.Key("a")}))
		require.True(t, r1Filter.NeedVal(roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("r")}))
		require.False(t, r1Filter.NeedVal(roachpb.Span{Key: roachpb.Key("z")}))
		require.False(t, r1Filter.NeedPrevVal(roachpb.Span{Key: roachpb.Key("a")}))
		require.False(t,
			r1Filter.NeedPrevVal(roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("r")}))
		require.False(t, r1Filter.NeedPrevVal(roachpb.Span{Key: roachpb.Key("z")}))

		// Test checkpoint with one registration.
		p.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 5})
		h.syncEventAndRegistrations()
		require.Equal(t,
			[]*kvpb.RangeFeedEvent{
				rangeFeedCheckpoint(
					roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
					hlc.Timestamp{WallTime: 5},
				),
			},
			serverStream.rangefeedEventsSentById(r1StreamId),
		)

		// Test value with one registration.
		p.ConsumeLogicalOps(ctx,
			writeValueOpWithKV(roachpb.Key("c"), hlc.Timestamp{WallTime: 6}, []byte("val")))
		h.syncEventAndRegistrations()
		require.Equal(t,
			[]*kvpb.RangeFeedEvent{
				rangeFeedValue(
					roachpb.Key("c"),
					roachpb.Value{
						RawBytes:  []byte("val"),
						Timestamp: hlc.Timestamp{WallTime: 6},
					},
				),
			},
			serverStream.rangefeedEventsSentById(r1StreamId),
		)

		// Test value to non-overlapping key with one registration.
		p.ConsumeLogicalOps(ctx,
			writeValueOpWithKV(roachpb.Key("s"), hlc.Timestamp{WallTime: 6}, []byte("val")))
		h.syncEventAndRegistrations()
		require.Equal(t, []*kvpb.RangeFeedEvent(nil), serverStream.rangefeedEventsSentById(r1StreamId))

		// Test intent that is aborted with one registration.
		txn1 := uuid.MakeV4()
		// Write intent.
		p.ConsumeLogicalOps(ctx, writeIntentOp(txn1, hlc.Timestamp{WallTime: 6}))
		h.syncEventAndRegistrations()
		require.Equal(t, []*kvpb.RangeFeedEvent(nil), serverStream.rangefeedEventsSentById(r1StreamId))
		// Abort.
		p.ConsumeLogicalOps(ctx, abortIntentOp(txn1))
		h.syncEventC()
		require.Equal(t, []*kvpb.RangeFeedEvent(nil), serverStream.rangefeedEventsSentById(r1StreamId))
		require.Equal(t, 0, h.rts.intentQ.Len())

		// Test intent that is committed with one registration.
		txn2 := uuid.MakeV4()
		// Write intent.
		p.ConsumeLogicalOps(ctx, writeIntentOp(txn2, hlc.Timestamp{WallTime: 10}))
		h.syncEventAndRegistrations()
		require.Equal(t, []*kvpb.RangeFeedEvent(nil), serverStream.rangefeedEventsSentById(r1StreamId))
		// Forward closed timestamp. Should now be stuck on intent.
		p.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 15})
		h.syncEventAndRegistrations()
		require.Equal(t,
			[]*kvpb.RangeFeedEvent{
				rangeFeedCheckpoint(
					roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
					hlc.Timestamp{WallTime: 9},
				),
			},
			serverStream.rangefeedEventsSentById(r1StreamId),
		)
		// Update the intent. Should forward resolved timestamp.
		p.ConsumeLogicalOps(ctx, updateIntentOp(txn2, hlc.Timestamp{WallTime: 12}))
		h.syncEventAndRegistrations()
		require.Equal(t,
			[]*kvpb.RangeFeedEvent{
				rangeFeedCheckpoint(
					roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
					hlc.Timestamp{WallTime: 11},
				),
			},
			serverStream.rangefeedEventsSentById(r1StreamId),
		)
		// Commit intent. Should forward resolved timestamp to closed timestamp.
		p.ConsumeLogicalOps(ctx,
			commitIntentOpWithKV(txn2, roachpb.Key("e"), hlc.Timestamp{WallTime: 13},
				[]byte("ival"), false /* omitInRangefeeds */))
		h.syncEventAndRegistrations()
		require.Equal(t,
			[]*kvpb.RangeFeedEvent{
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
			serverStream.rangefeedEventsSentById(r1StreamId),
		)

		// Add another registration with withDiff = true and withFiltering = true.
		r2Stream := newTestSingleFeedStream(streamMuxer, r2StreamId, serverStream)
		r2OK, r1And2Filter := p.Register(
			roachpb.RSpan{Key: roachpb.RKey("c"), EndKey: roachpb.RKey("z")},
			hlc.Timestamp{WallTime: 1},
			nil,  /* catchUpIter */
			true, /* withDiff */
			true, /* withFiltering */
			r2Stream,
			func() {},
		)
		require.True(t, r2OK)
		h.syncEventAndRegistrations()
		require.Equal(t, 2, p.Len())
		require.Equal(t,
			[]*kvpb.RangeFeedEvent{
				rangeFeedCheckpoint(
					roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("z")},
					hlc.Timestamp{WallTime: 15},
				),
			},
			serverStream.rangefeedEventsSentById(r2StreamId),
		)

		// Test the processor's new operation filter.
		require.True(t, r1And2Filter.NeedVal(roachpb.Span{Key: roachpb.Key("a")}))
		require.True(t, r1And2Filter.NeedVal(roachpb.Span{Key: roachpb.Key("y")}))
		require.True(t,
			r1And2Filter.NeedVal(roachpb.Span{Key: roachpb.Key("y"), EndKey: roachpb.Key("zzz")}))
		require.False(t, r1And2Filter.NeedVal(roachpb.Span{Key: roachpb.Key("zzz")}))
		require.False(t, r1And2Filter.NeedPrevVal(roachpb.Span{Key: roachpb.Key("a")}))
		require.True(t, r1And2Filter.NeedPrevVal(roachpb.Span{Key: roachpb.Key("y")}))
		require.True(t,
			r1And2Filter.NeedPrevVal(roachpb.Span{Key: roachpb.Key("y"), EndKey: roachpb.Key("zzz")}))
		require.False(t, r1And2Filter.NeedPrevVal(roachpb.Span{Key: roachpb.Key("zzz")}))

		// Both registrations should see checkpoint.
		p.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 20})
		h.syncEventAndRegistrations()
		chEventAM := []*kvpb.RangeFeedEvent{
			rangeFeedCheckpoint(
				roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
				hlc.Timestamp{WallTime: 20},
			),
		}
		require.Equal(t, chEventAM, serverStream.rangefeedEventsSentById(r1StreamId))
		chEventCZ := []*kvpb.RangeFeedEvent{
			rangeFeedCheckpoint(
				roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("z")},
				hlc.Timestamp{WallTime: 20},
			),
		}
		require.Equal(t, chEventCZ, serverStream.rangefeedEventsSentById(r2StreamId))

		// Test value with two registration that overlaps both.
		p.ConsumeLogicalOps(ctx,
			writeValueOpWithKV(roachpb.Key("k"), hlc.Timestamp{WallTime: 22}, []byte("val2")))
		h.syncEventAndRegistrations()
		valEvent := []*kvpb.RangeFeedEvent{
			rangeFeedValue(
				roachpb.Key("k"),
				roachpb.Value{
					RawBytes:  []byte("val2"),
					Timestamp: hlc.Timestamp{WallTime: 22},
				},
			),
		}
		require.Equal(t, valEvent, serverStream.rangefeedEventsSentById(r1StreamId))
		require.Equal(t, valEvent, serverStream.rangefeedEventsSentById(r2StreamId))

		// Test value that only overlaps the second registration.
		p.ConsumeLogicalOps(ctx,
			writeValueOpWithKV(roachpb.Key("v"), hlc.Timestamp{WallTime: 23}, []byte("val3")))
		h.syncEventAndRegistrations()
		valEvent2 := []*kvpb.RangeFeedEvent{
			rangeFeedValue(
				roachpb.Key("v"),
				roachpb.Value{
					RawBytes:  []byte("val3"),
					Timestamp: hlc.Timestamp{WallTime: 23},
				},
			),
		}
		require.Equal(t, []*kvpb.RangeFeedEvent(nil), serverStream.rangefeedEventsSentById(r1StreamId))
		require.Equal(t, valEvent2, serverStream.rangefeedEventsSentById(r2StreamId))

		// Test committing intent with OmitInRangefeeds that overlaps two
		// registration (one withFiltering = true and one withFiltering = false).
		p.ConsumeLogicalOps(ctx,
			commitIntentOpWithKV(txn2, roachpb.Key("k"), hlc.Timestamp{WallTime: 22},
				[]byte("val3"), true /* omitInRangefeeds */))
		h.syncEventAndRegistrations()
		valEvent3 := []*kvpb.RangeFeedEvent{
			rangeFeedValue(
				roachpb.Key("k"),
				roachpb.Value{
					RawBytes:  []byte("val3"),
					Timestamp: hlc.Timestamp{WallTime: 22},
				},
			),
		}
		require.Equal(t, valEvent3, serverStream.rangefeedEventsSentById(r1StreamId))
		// r2Stream should not see the event.

		fmt.Println("HERE5")

		// Cancel the first registration.
		r1Stream.Disconnect(kvpb.NewError(context.Canceled))
		require.NotNil(t, r1Stream.Err(t))

		// Stop the processor with an error.
		pErr := kvpb.NewErrorf("stop err")
		p.StopWithErr(pErr)
		require.NotNil(t, r2Stream.Err(t))

		fmt.Println("HERE6")

		// Adding another registration should fail.
		r3Stream := newTestSingleFeedStream(streamMuxer, r3StreamID, serverStream)
		r3OK, _ := p.Register(
			roachpb.RSpan{Key: roachpb.RKey("c"), EndKey: roachpb.RKey("z")},
			hlc.Timestamp{WallTime: 1},
			nil,   /* catchUpIter */
			false, /* withDiff */
			false, /* withFiltering */
			r3Stream,
			func() {},
		)
		require.False(t, r3OK)
	})
}

func TestProcessorSlowConsumer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutils.RunValues(t, "proc type", testTypes, func(t *testing.T, pt procType) {
		p, h, stopper := newTestProcessor(t, withProcType(pt))
		ctx := context.Background()

		serverStream := newTestServerStream()
		streamMuxer := NewStreamMuxer(serverStream, newTestRangefeedCounter())
		var wg sync.WaitGroup
		wg.Add(1)
		defer wg.Wait()
		// Make sure to shut down the muxer before wg.Wait().
		defer stopper.Stop(ctx)
		if err := stopper.RunAsyncTask(ctx, "mux-term-forwarder", func(ctx context.Context) {
			defer wg.Done()
			streamMuxer.Run(ctx, stopper)
		}); err != nil {
			wg.Done()
		}

		// Add a registration.
		const r1StreamId, r2StreamId = int64(1), int64(2)
		r1Stream := newTestSingleFeedStream(streamMuxer, r1StreamId, serverStream)
		_, _ = p.Register(
			roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
			hlc.Timestamp{WallTime: 1},
			nil,   /* catchUpIter */
			false, /* withDiff */
			false, /* withFiltering */
			r1Stream,
			func() {},
		)
		r2Stream := newTestSingleFeedStream(streamMuxer, r2StreamId, serverStream)
		p.Register(
			roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("z")},
			hlc.Timestamp{WallTime: 1},
			nil,   /* catchUpIter */
			false, /* withDiff */
			false, /* withFiltering */
			r2Stream,
			func() {},
		)
		h.syncEventAndRegistrations()
		require.Equal(t, 2, p.Len())
		require.Equal(t,
			[]*kvpb.RangeFeedEvent{
				rangeFeedCheckpoint(
					roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
					hlc.Timestamp{WallTime: 0},
				),
			},
			serverStream.rangefeedEventsSentById(r1StreamId),
		)
		require.Equal(t,
			[]*kvpb.RangeFeedEvent{
				rangeFeedCheckpoint(
					roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
					hlc.Timestamp{WallTime: 0},
				),
			},
			serverStream.rangefeedEventsSentById(r2StreamId),
		)

		// Block its Send method and fill up the registration's input channel.
		unblock := r1Stream.blockSend()
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
			p.ConsumeLogicalOps(ctx, writeValueOpWithKV(roachpb.Key("k"), ts, []byte("val")))

			// Wait for just the unblocked registration to catch up. This prevents
			// the race condition where this registration overflows anyway due to
			// the rapid event consumption and small buffer size.
			h.syncEventAndRegistrationsSpan(spXY)
		}

		// Consume one more event. Should not block, but should cause r1 to overflow
		// its registration buffer and drop the event.
		p.ConsumeLogicalOps(ctx,
			writeValueOpWithKV(roachpb.Key("k"), hlc.Timestamp{WallTime: 18}, []byte("val")))

		// Wait for just the unblocked registration to catch up.
		h.syncEventAndRegistrationsSpan(spXY)
		require.Equal(t, toFill+1, len(serverStream.rangefeedEventsSentById(r2StreamId)))
		require.Equal(t, 2, p.Len())

		// Unblock the send channel. The events should quickly be consumed.
		unblock()
		unblock = nil
		h.syncEventAndRegistrations()
		// At least one event should have been dropped due to overflow. We expect
		// exactly one event to be dropped, but it is possible that multiple events
		// were dropped due to rapid event consumption before the r1's outputLoop
		// began consuming from its event buffer.
		normalEventsSentById := serverStream.filterEventsSentById(r1StreamId, func(e *kvpb.MuxRangeFeedEvent) bool {
			return e.Error != nil
		})
		require.LessOrEqual(t, len(normalEventsSentById), toFill)
		require.Equal(t, newErrBufferCapacityExceeded().GoError(), r1Stream.Err(t))
		testutils.SucceedsSoon(t, func() error {
			if act, exp := p.Len(), 1; exp != act {
				return fmt.Errorf("processor had %d regs, wanted %d", act, exp)
			}
			return nil
		})
	})
}

// TestProcessorMemoryBudgetExceeded tests that memory budget will limit amount
// of data buffered for the feed and result in a registration being removed as a
// result of budget exhaustion.
func TestProcessorMemoryBudgetExceeded(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutils.RunValues(t, "proc type", testTypes, func(t *testing.T, pt procType) {

		fb := newTestBudget(40)
		m := NewMetrics()
		p, h, stopper := newTestProcessor(t, withBudget(fb), withChanTimeout(time.Millisecond),
			withMetrics(m), withProcType(pt))
		ctx := context.Background()

		serverStream := newTestServerStream()
		streamMuxer := NewStreamMuxer(serverStream, newTestRangefeedCounter())
		var wg sync.WaitGroup
		wg.Add(1)
		defer wg.Wait()
		// Make sure to shut down the muxer before wg.Wait().
		defer stopper.Stop(ctx)

		if err := stopper.RunAsyncTask(ctx, "mux-term-forwarder", func(ctx context.Context) {
			defer wg.Done()
			streamMuxer.Run(ctx, stopper)
		}); err != nil {
			wg.Done()
		}

		// Add a registration.
		const streamId = int64(1)
		stream := newTestSingleFeedStream(streamMuxer, streamId, serverStream)
		_, _ = p.Register(
			roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
			hlc.Timestamp{WallTime: 1},
			nil,   /* catchUpIter */
			false, /* withDiff */
			false, /* withFiltering */
			stream,
			func() {},
		)
		h.syncEventAndRegistrations()

		// Block it.
		unblock := stream.blockSend()
		defer func() {
			if unblock != nil {
				unblock()
			}
		}()

		// Write entries till budget is exhausted
		for i := 0; i < 10; i++ {
			if !p.ConsumeLogicalOps(ctx, writeValueOpWithKV(
				roachpb.Key("k"),
				hlc.Timestamp{WallTime: int64(i + 2)},
				[]byte(fmt.Sprintf("this is big value %02d", i)))) {
				break
			}
		}
		// Ensure that stop event generated by memory budget error is processed.
		h.syncEventC()

		// Unblock the 'send' channel. The events should quickly be consumed.
		unblock()
		unblock = nil
		h.syncEventAndRegistrations()

		require.Equal(t, newErrBufferCapacityExceeded().GoError(), stream.Err(t))
		require.Equal(t, 0, p.Len(), "registration was not removed")
		require.Equal(t, int64(1), m.RangeFeedBudgetExhausted.Count())
	})
}

// TestProcessorMemoryBudgetReleased that memory budget is correctly released.
func TestProcessorMemoryBudgetReleased(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutils.RunValues(t, "proc type", testTypes, func(t *testing.T, pt procType) {
		fb := newTestBudget(250)
		p, h, stopper := newTestProcessor(t, withBudget(fb), withChanTimeout(15*time.Minute),
			withProcType(pt))
		ctx := context.Background()

		serverStream := newTestServerStream()
		streamMuxer := NewStreamMuxer(serverStream, newTestRangefeedCounter())
		var wg sync.WaitGroup
		wg.Add(1)
		defer wg.Wait()
		// Make sure to shut down the muxer before wg.Wait().
		defer stopper.Stop(ctx)
		if err := stopper.RunAsyncTask(ctx, "mux-term-forwarder", func(ctx context.Context) {
			defer wg.Done()
			streamMuxer.Run(ctx, stopper)
		}); err != nil {
			wg.Done()
		}

		// Add a registration.
		const streamId = int64(1)
		stream := newTestSingleFeedStream(streamMuxer, streamId, serverStream)
		p.Register(
			roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
			hlc.Timestamp{WallTime: 1},
			nil,   /* catchUpIter */
			false, /* withDiff */
			false, /* withFiltering */
			stream,
			func() {},
		)
		h.syncEventAndRegistrations()

		// Write entries and check they are consumed so that we could write more
		// data than total budget if inflight messages are within budget.
		const eventCount = 10
		for i := 0; i < eventCount; i++ {
			p.ConsumeLogicalOps(ctx, writeValueOpWithKV(
				roachpb.Key("k"),
				hlc.Timestamp{WallTime: int64(i + 2)},
				[]byte("value")))
		}
		h.syncEventAndRegistrations()

		// Count consumed values
		consumedOps := 0
		for _, e := range serverStream.rangefeedEventsSentById(streamId) {
			if e.Val != nil {
				consumedOps++
			}
		}
		require.Equal(t, 1, p.Len(), "registration was removed")
		require.Equal(t, 10, consumedOps)
	})
}

// TestProcessorInitializeResolvedTimestamp tests that when a Processor is given
// a resolved timestamp iterator, it doesn't initialize its resolved timestamp
// until it has consumed all intents in the iterator.
func TestProcessorInitializeResolvedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunValues(t, "proc type", testTypes, func(t *testing.T, pt procType) {
		txn1 := makeTxn("txn1", uuid.MakeV4(), isolation.Serializable, hlc.Timestamp{})
		txn2 := makeTxn("txn2", uuid.MakeV4(), isolation.Serializable, hlc.Timestamp{})
		txnWithTs := func(txn roachpb.Transaction, ts int64) *roachpb.Transaction {
			txnTs := hlc.Timestamp{WallTime: ts}
			txn.TxnMeta.MinTimestamp = txnTs
			txn.TxnMeta.WriteTimestamp = txnTs
			txn.ReadTimestamp = txnTs
			return &txn
		}
		data := []storeOp{
			{kv: makeKV("a", "val1", 10)},
			{kv: makeKV("c", "val4", 9)},
			{kv: makeKV("c", "val3", 11)},
			{kv: makeProvisionalKV("c", "txnKey1", 15), txn: txnWithTs(txn1, 15)},
			{kv: makeKV("d", "val6", 19)},
			{kv: makeKV("d", "val5", 20)},
			{kv: makeProvisionalKV("d", "txnKey2", 21), txn: txnWithTs(txn2, 21)},
			{kv: makeKV("m", "val8", 1)},
			{kv: makeProvisionalKV("n", "txnKey1", 12), txn: txnWithTs(txn1, 12)},
			{kv: makeKV("r", "val9", 4)},
			{kv: makeProvisionalKV("r", "txnKey1", 19), txn: txnWithTs(txn1, 19)},
			{kv: makeProvisionalKV("w", "txnKey1", 3), txn: txnWithTs(txn1, 3)},
			{kv: makeKV("z", "val11", 4)},
			{kv: makeProvisionalKV("z", "txnKey2", 21), txn: txnWithTs(txn2, 21)},
		}
		scanner, cleanup, err := makeIntentScanner(data, roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("zz")})
		require.NoError(t, err, "failed to prepare test data")
		defer cleanup()

		p, h, stopper := newTestProcessor(t, withRtsScanner(scanner), withProcType(pt))
		ctx := context.Background()

		serverStream := newTestServerStream()
		streamMuxer := NewStreamMuxer(serverStream, newTestRangefeedCounter())
		var wg sync.WaitGroup
		wg.Add(1)
		defer wg.Wait()
		// Make sure to shut down the muxer before wg.Wait().
		defer stopper.Stop(ctx)
		if err := stopper.RunAsyncTask(ctx, "mux-term-forwarder", func(ctx context.Context) {
			defer wg.Done()
			streamMuxer.Run(ctx, stopper)
		}); err != nil {
			wg.Done()
		}

		// The resolved timestamp should not be initialized.
		require.False(t, h.rts.IsInit())
		require.Equal(t, hlc.Timestamp{}, h.rts.Get())

		// Add a registration.
		const r1StreamId = int64(1)
		r1Stream := newTestSingleFeedStream(streamMuxer, r1StreamId, serverStream)
		p.Register(
			roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
			hlc.Timestamp{WallTime: 1},
			nil,   /* catchUpIter */
			false, /* withDiff */
			false, /* withFiltering */
			r1Stream,
			func() {},
		)
		h.syncEventAndRegistrations()
		require.Equal(t, 1, p.Len())

		// The registration should be provided a checkpoint immediately with an
		// empty resolved timestamp because it did not perform a catch-up scan.
		chEvent := []*kvpb.RangeFeedEvent{
			rangeFeedCheckpoint(
				roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
				hlc.Timestamp{},
			),
		}
		require.Equal(t, chEvent, serverStream.rangefeedEventsSentById(r1StreamId))

		// The resolved timestamp should still not be initialized.
		require.False(t, h.rts.IsInit())
		require.Equal(t, hlc.Timestamp{}, h.rts.Get())

		// Forward the closed timestamp. The resolved timestamp should still
		// not be initialized.
		p.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 20})
		require.False(t, h.rts.IsInit())
		require.Equal(t, hlc.Timestamp{}, h.rts.Get())

		// Let the scan proceed.
		close(scanner.block)
		<-scanner.done

		// Synchronize the event channel then verify that the resolved timestamp is
		// initialized and that it's blocked on the oldest unresolved intent's txn
		// timestamp. Txn1 has intents at many times but the unresolvedIntentQueue
		// tracks its latest, which is 19, so the resolved timestamp is
		// 19.FloorPrev() = 18.
		h.syncEventAndRegistrations()
		require.True(t, h.rts.IsInit())
		require.Equal(t, hlc.Timestamp{WallTime: 18}, h.rts.Get())

		// The registration should have been informed of the new resolved timestamp.
		chEvent = []*kvpb.RangeFeedEvent{
			rangeFeedCheckpoint(
				roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
				hlc.Timestamp{WallTime: 18},
			),
		}
		require.Equal(t, chEvent, serverStream.rangefeedEventsSentById(r1StreamId))
	})
}

func TestProcessorTxnPushAttempt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunValues(t, "proc type", testTypes, func(t *testing.T, pt procType) {
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
		txn1Meta := enginepb.TxnMeta{ID: txn1, Key: keyA, IsoLevel: isolation.Serializable, WriteTimestamp: ts10, MinTimestamp: ts10}
		txn2Meta := enginepb.TxnMeta{ID: txn2, Key: keyB, IsoLevel: isolation.Snapshot, WriteTimestamp: ts20, MinTimestamp: ts20}
		txn3Meta := enginepb.TxnMeta{ID: txn3, Key: keyC, IsoLevel: isolation.ReadCommitted, WriteTimestamp: ts30, MinTimestamp: ts30}
		txn1Proto := &roachpb.Transaction{TxnMeta: txn1Meta, Status: roachpb.PENDING}
		txn2Proto := &roachpb.Transaction{TxnMeta: txn2Meta, Status: roachpb.PENDING}
		txn3Proto := &roachpb.Transaction{TxnMeta: txn3Meta, Status: roachpb.PENDING}

		// Modifications for test 2.
		txn1MetaT2Pre := enginepb.TxnMeta{ID: txn1, Key: keyA, IsoLevel: isolation.Serializable, WriteTimestamp: ts25, MinTimestamp: ts10}
		txn1MetaT2Post := enginepb.TxnMeta{ID: txn1, Key: keyA, IsoLevel: isolation.Serializable, WriteTimestamp: ts50, MinTimestamp: ts10}
		txn2MetaT2Post := enginepb.TxnMeta{ID: txn2, Key: keyB, IsoLevel: isolation.Snapshot, WriteTimestamp: ts60, MinTimestamp: ts20}
		txn3MetaT2Post := enginepb.TxnMeta{ID: txn3, Key: keyC, IsoLevel: isolation.ReadCommitted, WriteTimestamp: ts70, MinTimestamp: ts30}
		txn1ProtoT2 := &roachpb.Transaction{TxnMeta: txn1MetaT2Post, Status: roachpb.COMMITTED}
		txn2ProtoT2 := &roachpb.Transaction{TxnMeta: txn2MetaT2Post, Status: roachpb.PENDING}
		txn3ProtoT2 := &roachpb.Transaction{TxnMeta: txn3MetaT2Post, Status: roachpb.PENDING}

		// Modifications for test 3.
		txn2MetaT3Post := enginepb.TxnMeta{ID: txn2, Key: keyB, IsoLevel: isolation.Snapshot, WriteTimestamp: ts60, MinTimestamp: ts20}
		txn3MetaT3Post := enginepb.TxnMeta{ID: txn3, Key: keyC, IsoLevel: isolation.ReadCommitted, WriteTimestamp: ts90, MinTimestamp: ts30}
		txn2ProtoT3 := &roachpb.Transaction{TxnMeta: txn2MetaT3Post, Status: roachpb.ABORTED}
		txn3ProtoT3 := &roachpb.Transaction{TxnMeta: txn3MetaT3Post, Status: roachpb.PENDING}

		testNum := 0
		pausePushAttemptsC := make(chan struct{})
		resumePushAttemptsC := make(chan struct{})
		defer close(pausePushAttemptsC)
		defer close(resumePushAttemptsC)

		// Create a TxnPusher that performs assertions during the first 3 uses.
		var tp testTxnPusher
		tp.mockPushTxns(func(
			ctx context.Context, txns []enginepb.TxnMeta, ts hlc.Timestamp,
		) ([]*roachpb.Transaction, bool, error) {
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
					return nil, false, errors.New("test failed")
				}

				// Push does not succeed. Protos not at larger ts.
				return []*roachpb.Transaction{txn1Proto, txn2Proto, txn3Proto}, false, nil
			case 2:
				assert.Equal(t, 3, len(txns))
				assert.Equal(t, txn1MetaT2Pre, txns[0])
				assert.Equal(t, txn2Meta, txns[1])
				assert.Equal(t, txn3Meta, txns[2])
				if t.Failed() {
					return nil, false, errors.New("test failed")
				}

				// Push succeeds. Return new protos.
				return []*roachpb.Transaction{txn1ProtoT2, txn2ProtoT2, txn3ProtoT2}, false, nil
			case 3:
				assert.Equal(t, 2, len(txns))
				assert.Equal(t, txn2MetaT2Post, txns[0])
				assert.Equal(t, txn3MetaT2Post, txns[1])
				if t.Failed() {
					return nil, false, errors.New("test failed")
				}

				// Push succeeds. Return new protos.
				return []*roachpb.Transaction{txn2ProtoT3, txn3ProtoT3}, false, nil
			default:
				return nil, false, nil
			}
		})
		tp.mockResolveIntentsFn(func(ctx context.Context, intents []roachpb.LockUpdate) error {
			// There's nothing to assert here. We expect the intents to correspond to
			// transactions that had their LockSpans populated when we pushed them. This
			// test doesn't simulate that.

			if testNum > 3 {
				return nil
			}

			pausePushAttemptsC <- struct{}{}
			<-resumePushAttemptsC
			return nil
		})

		p, h, stopper := newTestProcessor(t, withPusher(&tp), withProcType(pt))
		ctx := context.Background()
		defer stopper.Stop(ctx)

		// Add a few intents and move the closed timestamp forward.
		p.ConsumeLogicalOps(ctx,
			writeIntentOpFromMeta(txn1Meta),
			writeIntentOpFromMeta(txn2Meta),
			writeIntentOpFromMeta(txn2Meta),
			writeIntentOpFromMeta(txn3Meta),
		)
		p.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 40})
		h.syncEventC()
		require.Equal(t, hlc.Timestamp{WallTime: 9}, h.rts.Get())

		// Wait for the first txn push attempt to complete.
		h.triggerTxnPushUntilPushed(t, pausePushAttemptsC)

		// The resolved timestamp hasn't moved.
		h.syncEventC()
		require.Equal(t, hlc.Timestamp{WallTime: 9}, h.rts.Get())

		// Write another intent for one of the txns. This moves the resolved
		// timestamp forward.
		p.ConsumeLogicalOps(ctx, writeIntentOpFromMeta(txn1MetaT2Pre))
		h.syncEventC()
		require.Equal(t, hlc.Timestamp{WallTime: 19}, h.rts.Get())

		// Unblock the second txn push attempt and wait for it to complete.
		resumePushAttemptsC <- struct{}{}
		h.triggerTxnPushUntilPushed(t, pausePushAttemptsC)

		// The resolved timestamp should have moved forwards to the closed
		// timestamp.
		h.syncEventC()
		require.Equal(t, hlc.Timestamp{WallTime: 40}, h.rts.Get())

		// Forward the closed timestamp.
		p.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 80})
		h.syncEventC()
		require.Equal(t, hlc.Timestamp{WallTime: 49}, h.rts.Get())

		// Txn1's first intent is committed. Resolved timestamp doesn't change.
		p.ConsumeLogicalOps(ctx, commitIntentOp(txn1MetaT2Post.ID, txn1MetaT2Post.WriteTimestamp))
		h.syncEventC()
		require.Equal(t, hlc.Timestamp{WallTime: 49}, h.rts.Get())

		// Txn1's second intent is committed. Resolved timestamp moves forward.
		p.ConsumeLogicalOps(ctx, commitIntentOp(txn1MetaT2Post.ID, txn1MetaT2Post.WriteTimestamp))
		h.syncEventC()
		require.Equal(t, hlc.Timestamp{WallTime: 59}, h.rts.Get())

		// Unblock the third txn push attempt and wait for it to complete.
		resumePushAttemptsC <- struct{}{}
		h.triggerTxnPushUntilPushed(t, pausePushAttemptsC)

		// The resolved timestamp should have moved forwards to the closed
		// timestamp.
		h.syncEventC()
		require.Equal(t, hlc.Timestamp{WallTime: 80}, h.rts.Get())

		// Forward the closed timestamp.
		p.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 100})
		h.syncEventC()
		require.Equal(t, hlc.Timestamp{WallTime: 89}, h.rts.Get())

		// Commit txn3's only intent. Resolved timestamp moves forward.
		p.ConsumeLogicalOps(ctx, commitIntentOp(txn3MetaT3Post.ID, txn3MetaT3Post.WriteTimestamp))
		h.syncEventC()
		require.Equal(t, hlc.Timestamp{WallTime: 100}, h.rts.Get())

		// Release push attempt to avoid deadlock.
		resumePushAttemptsC <- struct{}{}
	})
}

// TestProcessorTxnPushDisabled tests that processors don't attempt txn pushes
// when disabled.
func TestProcessorTxnPushDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const pushInterval = 10 * time.Millisecond

	// Set up a txn to write intents.
	ts := hlc.Timestamp{WallTime: 10}
	txnID := uuid.MakeV4()
	txnMeta := enginepb.TxnMeta{
		ID:             txnID,
		Key:            keyA,
		IsoLevel:       isolation.Serializable,
		WriteTimestamp: ts,
		MinTimestamp:   ts,
	}

	// Disable txn pushes.
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	PushTxnsEnabled.Override(ctx, &st.SV, false)

	// Set up a txn pusher and processor that errors on any pushes.
	//
	// TODO(kv): We don't test the scheduled processor here, since the setting
	// instead controls the Store.startRangefeedTxnPushNotifier() loop which sits
	// outside of the processor and can't be tested with this test harness. Write
	// a new test when the legacy processor is removed and the scheduled processor
	// is used by default.
	var tp testTxnPusher
	tp.mockPushTxns(func(ctx context.Context, txns []enginepb.TxnMeta, ts hlc.Timestamp) ([]*roachpb.Transaction, bool, error) {
		err := errors.Errorf("unexpected txn push for txns=%v ts=%s", txns, ts)
		t.Errorf("%v", err)
		return nil, false, err
	})

	p, h, stopper := newTestProcessor(t, withSettings(st), withPusher(&tp),
		withPushTxnsIntervalAge(pushInterval, time.Millisecond))
	defer stopper.Stop(ctx)

	// Move the resolved ts forward to just before the txn timestamp.
	rts := ts.Add(-1, 0)
	require.True(t, p.ForwardClosedTS(ctx, rts))
	h.syncEventC()
	require.Equal(t, rts, h.rts.Get())

	// Add a few intents and move the closed timestamp forward.
	p.ConsumeLogicalOps(ctx, writeIntentOpFromMeta(txnMeta))
	p.ForwardClosedTS(ctx, ts)
	h.syncEventC()
	require.Equal(t, rts, h.rts.Get())

	// Wait for 10x the push txns interval, to make sure pushes are disabled.
	// Waiting for something to not happen is a bit smelly, but gets the job done.
	time.Sleep(10 * pushInterval)
}

// TestProcessorConcurrentStop tests that all methods in Processor's API
// correctly handle the processor concurrently shutting down. If they did
// not then it would be possible for them to deadlock.
func TestProcessorConcurrentStop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutils.RunValues(t, "proc type", testTypes, func(t *testing.T, pt procType) {
		nodeStopper := stop.NewStopper()
		ctx := context.Background()

		serverStream := newTestServerStream()
		streamMuxer := NewStreamMuxer(serverStream, newTestRangefeedCounter())

		var wg sync.WaitGroup
		wg.Add(1)
		defer wg.Wait()
		// Make sure to shut down the muxer before wg.Wait().
		defer nodeStopper.Stop(ctx)

		if err := nodeStopper.RunAsyncTask(ctx, "mux-term-forwarder", func(ctx context.Context) {
			defer wg.Done()
			streamMuxer.Run(ctx, nodeStopper)
		}); err != nil {
			wg.Done()
		}

		const trials = 10
		for i := 0; i < trials; i++ {
			p, h, stopper := newTestProcessor(t, withProcType(pt))

			var wg sync.WaitGroup
			wg.Add(6)
			go func(id int) {
				defer wg.Done()
				runtime.Gosched()
				s := newTestSingleFeedStream(streamMuxer, int64(id), serverStream)
				p.Register(h.span, hlc.Timestamp{}, nil, /* catchUpIter */
					false /* withDiff */, false /* withFiltering */, s, func() {})
			}(i)
			go func() {
				defer wg.Done()
				runtime.Gosched()
				p.Len()
			}()
			go func() {
				defer wg.Done()
				runtime.Gosched()
				p.ConsumeLogicalOps(ctx,
					writeValueOpWithKV(roachpb.Key("s"), hlc.Timestamp{WallTime: 6}, []byte("val")))
			}()
			go func() {
				defer wg.Done()
				runtime.Gosched()
				p.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 2})
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
	})
}

// TestProcessorRegistrationObservesOnlyNewEvents tests that a registration
// observes only operations that are consumed after it has registered.
func TestProcessorRegistrationObservesOnlyNewEvents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutils.RunValues(t, "proc type", testTypes, func(t *testing.T, pt procType) {

		p, h, stopper := newTestProcessor(t, withProcType(pt))
		ctx := context.Background()

		serverStream := newTestServerStream()
		streamMuxer := NewStreamMuxer(serverStream, newTestRangefeedCounter())

		var nodeWg sync.WaitGroup
		nodeWg.Add(1)
		defer nodeWg.Wait()
		// Make sure to shut down the muxer before wg.Wait().
		defer stopper.Stop(ctx)
		if err := stopper.RunAsyncTask(ctx, "mux-term-forwarder", func(ctx context.Context) {
			defer nodeWg.Done()
			streamMuxer.Run(ctx, stopper)
		}); err != nil {
			nodeWg.Done()
		}

		firstC := make(chan int64)
		regDone := make(chan struct{})
		regs := make(map[int64]int64)

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
				p.ConsumeLogicalOps(ctx, writeValueOp(hlc.Timestamp{WallTime: i}))
			}
			h.syncEventC()
			close(firstC)
		}()
		id := int64(0)
		go func() {
			defer wg.Done()
			for firstIdx := range firstC {
				// For each index, create a new registration. The first
				// operation is should see is firstIdx.
				s := newTestSingleFeedStream(streamMuxer, id, serverStream)
				regs[id] = firstIdx
				p.Register(h.span, hlc.Timestamp{}, nil, /* catchUpIter */
					false /* withDiff */, false /* withFiltering */, s, func() {})
				regDone <- struct{}{}
				id++
			}
		}()
		wg.Wait()
		h.syncEventAndRegistrations()

		// Verify that no registrations were given operations
		// from before they registered.
		for id, expFirstIdx := range regs {
			events := serverStream.rangefeedEventsSentById(id)
			require.IsType(t, &kvpb.RangeFeedCheckpoint{}, events[0].GetValue())
			require.IsType(t, &kvpb.RangeFeedValue{}, events[1].GetValue())

			firstVal := events[1].GetValue().(*kvpb.RangeFeedValue)
			firstIdx := firstVal.Value.Timestamp.WallTime
			require.Equal(t, expFirstIdx, firstIdx)
		}
	})
}

func TestBudgetReleaseOnProcessorStop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const totalEvents = 100

	// Channel capacity is used in two places, processor channel and registration
	// channel. By having each of them half the events could we could fit
	// everything in. Additional elements is a slack for checkpoint events as well
	// as sync events used to flush queues.
	const channelCapacity = totalEvents/2 + 10

	testutils.RunValues(t, "proc type", testTypes, func(t *testing.T, pt procType) {
		s := cluster.MakeTestingClusterSettings()
		m := mon.NewMonitor(mon.Options{
			Name:      "rangefeed",
			Res:       mon.MemoryResource,
			Increment: 1,
			Limit:     math.MaxInt64,
		})
		m.Start(context.Background(), nil, mon.NewStandaloneBudget(math.MaxInt64))

		b := m.MakeBoundAccount()
		fb := NewFeedBudget(&b, 0, &s.SV)

		p, h, stopper := newTestProcessor(t, withBudget(fb), withChanCap(channelCapacity),
			withEventTimeout(100*time.Millisecond), withProcType(pt))
		ctx := context.Background()

		muxer, cleanUp := NewTestStremMuxer(stopper)
		defer cleanUp()
		defer stopper.Stop(ctx)

		// Add a registration.
		const r1streamID = int64(1)
		rStream := newConsumer(r1streamID, 50, muxer)
		defer func() { rStream.Resume() }()
		_, _ = p.Register(
			roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
			hlc.Timestamp{WallTime: 1},
			nil,   /* catchUpIter */
			false, /* withDiff */
			false, /* withFiltering */
			rStream,
			func() {},
		)
		h.syncEventAndRegistrations()

		for i := 0; i < totalEvents; i++ {
			p.ConsumeLogicalOps(ctx, writeValueOpWithKV(
				roachpb.Key("k"),
				hlc.Timestamp{WallTime: int64(i + 2)},
				[]byte(fmt.Sprintf("this is value %04d", i))))
		}

		// Wait for half of the event to be processed by stream then stop processor.
		select {
		case <-rStream.blocked:
		case err := <-rStream.done:
			t.Fatal("stream failed with error before all data was consumed", err)
		}

		// Since stop is blocking and needs to flush events we need to do that in
		// parallel.
		stopped := make(chan interface{})
		go func() {
			p.Stop()
			stopped <- struct{}{}
		}()

		// Resume event loop in consumer to unblock any internal loops of processor or
		// registrations.
		rStream.Resume()

		// Wait for top function to finish processing before verifying that we
		// consumed all events.
		<-stopped

		// We need to wait for budget to drain as Stop would only post stop event
		// after flushing the queue, but couldn't determine when main processor loop
		// is actually closed.
		testutils.SucceedsSoon(t, func() error {
			fmt.Printf("Budget now: %d bytes remained, %d events processed\n",
				m.AllocBytes(), rStream.Consumed())
			if m.AllocBytes() != 0 {
				return errors.Errorf(
					"Failed to release all budget after stop: %d bytes remained, %d events processed",
					m.AllocBytes(), rStream.Consumed())
			}
			return nil
		})
	})
}

// TestBudgetReleaseOnLastStreamError verifies that when stream fails memory
// budget for discarded pending events is returned.
func TestBudgetReleaseOnLastStreamError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const totalEvents = 100

	// Add an extra capacity in channel to accommodate for checkpoint and sync
	// objects. Ideally it would be nice to have
	const channelCapacity = totalEvents + 5

	testutils.RunValues(t, "proc type", testTypes, func(t *testing.T, pt procType) {
		fb := newTestBudget(math.MaxInt64)

		p, h, stopper := newTestProcessor(t, withBudget(fb), withChanCap(channelCapacity),
			withEventTimeout(time.Millisecond), withProcType(pt))
		ctx := context.Background()

		muxer, cleanUp := NewTestStremMuxer(stopper)
		defer cleanUp()
		defer stopper.Stop(ctx)

		// Add a registration.
		const r1streamID = int64(1)
		rStream := newConsumer(r1streamID, 90, muxer)
		defer func() { rStream.Resume() }()
		_, _ = p.Register(
			roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
			hlc.Timestamp{WallTime: 1},
			nil,   /* catchUpIter */
			false, /* withDiff */
			false, /* withFiltering */
			rStream,
			func() {},
		)
		h.syncEventAndRegistrations()

		for i := 0; i < totalEvents; i++ {
			p.ConsumeLogicalOps(ctx, writeValueOpWithKV(
				roachpb.Key("k"),
				hlc.Timestamp{WallTime: int64(i + 2)},
				[]byte(fmt.Sprintf("this is value %04d", i))))
		}

		// Wait for half of the event to be processed then raise error.
		select {
		case <-rStream.blocked:
		case err := <-rStream.done:
			t.Fatal("stream failed with error before stream blocked: ", err)
		}

		// Resume event loop in consumer and fail Stream to remove registration.
		rStream.ResumeWithFailure(errors.Errorf("Closing down stream"))

		// We need to wait for budget to drain as all pending events are processed
		// or dropped.
		requireBudgetDrainedSoon(t, fb, rStream)
	})
}

func newTestBudget(limit int64) *FeedBudget {
	s := cluster.MakeTestingClusterSettings()
	m := getMemoryMonitor(s)
	m.Start(context.Background(), nil, mon.NewStandaloneBudget(limit))
	b := m.MakeBoundAccount()
	fb := NewFeedBudget(&b, 0, &s.SV)
	return fb
}

// TestBudgetReleaseOnOneStreamError verifies that if one stream fails while
// other keeps running, accounting correctly releases memory budget for shared
// events.
func TestBudgetReleaseOnOneStreamError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const totalEvents = 100

	// Channel capacity is used in two places, processor channel and registration
	// channel. By having each of them half the events could we could fit
	// everything in. Additional elements is a slack for checkpoint events as well
	// as sync events used to flush queues.
	const channelCapacity = totalEvents/2 + 10

	testutils.RunValues(t, "proc type", testTypes, func(t *testing.T, pt procType) {

		fb := newTestBudget(math.MaxInt64)

		p, h, stopper := newTestProcessor(t, withBudget(fb), withChanCap(channelCapacity),
			withEventTimeout(100*time.Millisecond), withProcType(pt))
		ctx := context.Background()

		muxer, cleanUp := NewTestStremMuxer(stopper)
		defer cleanUp()
		defer stopper.Stop(ctx)

		// Add a registration.
		const r1streamID, r2streamID = int64(1), int64(2)
		r1Stream := newConsumer(r1streamID, 50, muxer)
		defer func() { r1Stream.Resume() }()
		_, _ = p.Register(
			roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
			hlc.Timestamp{WallTime: 1},
			nil,   /* catchUpIter */
			false, /* withDiff */
			false, /* withFiltering */
			r1Stream,
			func() {},
		)

		// Non-blocking registration that would consume all events.
		r2Stream := newConsumer(r2streamID, 0, muxer)
		p.Register(
			roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
			hlc.Timestamp{WallTime: 1},
			nil,   /* catchUpIter */
			false, /* withDiff */
			false, /* withFiltering */
			r2Stream,
			func() {},
		)
		h.syncEventAndRegistrations()

		for i := 0; i < totalEvents; i++ {
			p.ConsumeLogicalOps(ctx, writeValueOpWithKV(
				roachpb.Key("k"),
				hlc.Timestamp{WallTime: int64(i + 2)},
				[]byte(fmt.Sprintf("this is value %04d", i))))
		}

		// Wait for half of the event to be processed then stop processor.
		select {
		case <-r1Stream.blocked:
		case err := <-r1Stream.done:
			t.Fatal("stream failed with error before all data was consumed", err)
		}

		// Resume event loop in consumer and fail Stream to remove registration.
		r1Stream.ResumeWithFailure(errors.Errorf("Closing down stream"))

		// We need to wait for budget to drain as all pending events are processed
		// or dropped.
		requireBudgetDrainedSoon(t, fb, r1Stream)
	})
}

// requireBudgetDrainedSoon checks that memory budget drains to zero soon.
// Since we don't stop the processor we can't rely on on stop operation syncing
// all registrations and we resort to waiting for registration work loops to
// stop and drain remaining allocations.
// We use account and not a monitor for those checks because monitor doesn't
// necessary return all data to the pool until processor is stopped.
func requireBudgetDrainedSoon(t *testing.T, b *FeedBudget, stream *consumer) {
	testutils.SucceedsSoon(t, func() error {
		b.mu.Lock()
		used := b.mu.memBudget.Used()
		b.mu.Unlock()
		fmt.Printf("Budget used: %d bytes, %d events processed\n",
			used, stream.Consumed())
		if used != 0 {
			return errors.Errorf(
				"Failed to release all budget after stream stop: %d bytes remained, %d events processed",
				used, stream.Consumed())
		}
		return nil
	})
}

type consumer struct {
	ctx        context.Context
	ctxDone    func()
	sentValues int32
	done       chan *kvpb.Error
	streamID   int64

	blockAfter int
	blocked    chan interface{}
	resume     chan error
	muxer      *TestStreamMuxer
}

func newConsumer(streamID int64, blockAfter int, muxer *TestStreamMuxer) *consumer {
	ctx, done := context.WithCancel(context.Background())
	return &consumer{
		ctx:        ctx,
		ctxDone:    done,
		blockAfter: blockAfter,
		blocked:    make(chan interface{}),
		resume:     make(chan error),
		done:       make(chan *kvpb.Error, 1),
		muxer:      muxer,
		streamID:   streamID,
	}
}

func (c *consumer) Send(e *kvpb.RangeFeedEvent) error {
	if e.Val != nil {
		v := int(atomic.AddInt32(&c.sentValues, 1))
		if v == c.blockAfter {
			// Resume test if it was waiting for stream to block.
			close(c.blocked)
			// Wait for resume signal with an optional error.
			err, ok := <-c.resume
			if ok {
				return err
			}
		}
	}
	return nil
}

func (c *consumer) Context() context.Context {
	return c.ctx
}

func (c *consumer) Disconnect(error *kvpb.Error) {
	c.done <- error
	c.muxer.DisconnectRangefeedWithError(c.streamID)
}

func (c *consumer) Err(t *testing.T) error {
	select {
	case err := <-c.done:
		return err.GoError()
	case <-time.After(30 * time.Second):
		t.Fatalf("time out waiting for rangefeed completion")
		return nil
	}
}

func (c *consumer) RegisterRangefeedCleanUp(f func()) {
	c.muxer.RegisterRangefeedCleanUp(c.streamID, f)
}

func (c *consumer) Cancel() {
	c.ctxDone()
}

func (c *consumer) WaitBlock() {
	<-c.blocked
}

// Resume resumes stream by closing its wait channel.
// If there was a pending err for resuming then it would be discarded and
// channel closed.
func (c *consumer) Resume() {
	select {
	case _, ok := <-c.resume:
		if ok {
			close(c.resume)
		}
	default:
		close(c.resume)
	}
}

// Resume resumes stream by posting an error and then closing stream.
// Method would block until error is posted.
func (c *consumer) ResumeWithFailure(err error) {
	c.resume <- err
}

func (c *consumer) Consumed() int {
	return int(atomic.LoadInt32(&c.sentValues))
}

// TestSizeOfEvent tests the size of the event struct. It is fine if this struct
// changes in size, as long as this is done consciously.
func TestSizeOfEvent(t *testing.T) {
	var e event
	size := int(unsafe.Sizeof(e))
	require.Equal(t, 72, size)
}

// TestProcessorBackpressure tests that a processor with EventChanTimeout set to
// 0 will backpressure senders when a consumer isn't keeping up.
func TestProcessorBackpressure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	span := roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("z")}

	p, h, stopper := newTestProcessor(t, withSpan(span), withBudget(newTestBudget(math.MaxInt64)),
		withChanCap(1), withEventTimeout(0), withProcType(legacyProcessor))
	defer p.Stop()

	serverStream := newTestServerStream()
	streamMuxer := NewStreamMuxer(serverStream, newTestRangefeedCounter())
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()
	// Make sure to shut down the muxer before wg.Wait().
	defer stopper.Stop(ctx)
	if err := stopper.RunAsyncTask(ctx, "mux-term-forwarder", func(ctx context.Context) {
		defer wg.Done()
		streamMuxer.Run(ctx, stopper)
	}); err != nil {
		wg.Done()
	}

	// Add a registration.
	const streamId = int64(1)
	stream := newTestSingleFeedStream(streamMuxer, streamId, serverStream)
	ok, _ := p.Register(span, hlc.MinTimestamp, nil, /* catchUpIter */
		false /* withDiff */, false /* withFiltering */, stream, nil)
	require.True(t, ok)

	// Wait for the initial checkpoint.
	h.syncEventAndRegistrations()
	require.Len(t, serverStream.rangefeedEventsSentById(streamId), 1)

	// Block the registration consumer, and spawn a goroutine to post events to
	// the stream, which should block. The rangefeed pipeline buffers a few
	// additional events in intermediate goroutines between channels, so post 10
	// events to be sure.
	unblock := stream.blockSend()
	defer unblock()

	const numEvents = 10
	doneC := make(chan struct{})
	go func() {
		for i := 0; i < numEvents; i++ {
			assert.True(t, p.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: int64(i + 1)}))
		}
		close(doneC)
	}()

	// The sender should be blocked for at least 3 seconds.
	select {
	case <-doneC:
		t.Fatal("send unexpectely succeeded")
	case <-time.After(3 * time.Second):
	case <-ctx.Done():
	}

	// Unblock the sender, and wait for it to complete.
	unblock()
	select {
	case <-doneC:
	case <-time.After(time.Second):
		t.Fatal("sender did not complete")
	}

	// Wait for the final checkpoint event.
	h.syncEventAndRegistrations()
	events := serverStream.rangefeedEventsSentById(streamId)
	require.Equal(t, &kvpb.RangeFeedEvent{
		Checkpoint: &kvpb.RangeFeedCheckpoint{
			Span:       span.AsRawSpanWithNoLocals(),
			ResolvedTS: hlc.Timestamp{WallTime: numEvents},
		},
	}, events[len(events)-1])
}

// TestProcessorContextCancellation tests that the processor cancels the
// contexts of async tasks when stopped. It does not, however, cancel the
// process() context -- it probably should, but this should arguably also be
// handled by the scheduler.
func TestProcessorContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Try stopping both via the stopper and via Processor.Stop().
	testutils.RunTrueAndFalse(t, "stopper", func(t *testing.T, useStopper bool) {

		// Set up a transaction to push.
		txnTS := hlc.Timestamp{WallTime: 10} // after resolved timestamp
		txnMeta := enginepb.TxnMeta{
			ID: uuid.MakeV4(), Key: keyA, WriteTimestamp: txnTS, MinTimestamp: txnTS}

		// Set up a transaction pusher that will block until the context cancels.
		pushReadyC := make(chan struct{})
		pushDoneC := make(chan struct{})

		var pusher testTxnPusher
		pusher.mockPushTxns(func(
			ctx context.Context, txns []enginepb.TxnMeta, ts hlc.Timestamp,
		) ([]*roachpb.Transaction, bool, error) {
			pushReadyC <- struct{}{}
			<-ctx.Done()
			close(pushDoneC)
			return nil, false, ctx.Err()
		})
		pusher.mockResolveIntentsFn(func(ctx context.Context, intents []roachpb.LockUpdate) error {
			return nil
		})

		// Start a test processor.
		p, h, stopper := newTestProcessor(t, withPusher(&pusher))
		ctx := context.Background()
		defer stopper.Stop(ctx)

		// Add an intent and move the closed timestamp past it. This should trigger a
		// txn push attempt, wait for that to happen.
		p.ConsumeLogicalOps(ctx, writeIntentOpFromMeta(txnMeta))
		p.ForwardClosedTS(ctx, txnTS.Add(1, 0))
		h.syncEventC()
		h.triggerTxnPushUntilPushed(t, pushReadyC)

		// Now, stop the processor, and wait for the txn pusher to exit.
		if useStopper {
			stopper.Stop(ctx)
		} else {
			p.Stop()
		}
		select {
		case <-pushDoneC:
		case <-time.After(3 * time.Second):
			t.Fatal("txn pusher did not exit")
		}
	})
}
