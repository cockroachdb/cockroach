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
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
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
		r1Stream := newTestStream(streamMuxer, r1StreamId)
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
		r2Stream := newTestStream(streamMuxer, r2StreamId)
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
		require.NotNil(t, r1Stream.Err(t, serverStream))

		// Stop the processor with an error.
		pErr := kvpb.NewErrorf("stop err")
		p.StopWithErr(pErr)
		require.NotNil(t, r2Stream.Err(t, serverStream))

		fmt.Println("HERE6")

		// Adding another registration should fail.
		r3Stream := newTestStream(streamMuxer, r3StreamID)
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
