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
	"testing"

	_ "github.com/cockroachdb/cockroach/pkg/keys" // hook up pretty printer
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestRegistrationBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	val := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
	ev1, ev2 := new(kvpb.RangeFeedEvent), new(kvpb.RangeFeedEvent)
	ev1.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val})
	ev2.MustSetValue(&kvpb.RangeFeedValue{Key: keyB, Value: val})

	testutils.RunValues(t, "registration type=", registrationTestTypes, func(t *testing.T, rt registrationType) {
		t.Run("registration with no catchup scan specified", func(t *testing.T) {
			s := newTestStream()
			noCatchupReg := newTestRegistration(s, withRSpan(spAB), withRegistrationType(rt))
			noCatchupReg.publish(ctx, ev1, nil /* alloc */)
			noCatchupReg.publish(ctx, ev2, nil /* alloc */)
			if noCatchupReg, ok := noCatchupReg.(*bufferedRegistration); ok {
				require.Equal(t, len(noCatchupReg.buf), 2)
			}
			go noCatchupReg.runOutputLoop(ctx, 0)
			require.NoError(t, noCatchupReg.waitForCaughtUp(ctx))
			require.Equal(t, []*kvpb.RangeFeedEvent{ev1, ev2}, s.Events())
			noCatchupReg.disconnect(nil)
		})
		t.Run("registration with catchup scan", func(t *testing.T) {
			s := newTestStream()
			catchupReg := newTestRegistration(s, withRSpan(spBC),
				withStartTs(hlc.Timestamp{WallTime: 1}),
				withCatchUpIter(newTestIterator([]storage.MVCCKeyValue{
					makeKV("b", "val1", 10),
					makeKV("bc", "val3", 11),
					makeKV("bd", "val4", 9),
				}, nil)), withRegistrationType(rt))
			catchupReg.publish(ctx, ev1, nil /* alloc */)
			catchupReg.publish(ctx, ev2, nil /* alloc */)
			if r, ok := catchupReg.(*bufferedRegistration); ok {
				require.Equal(t, len(r.buf), 2)
			}
			go catchupReg.runOutputLoop(ctx, 0)
			require.NoError(t, catchupReg.waitForCaughtUp(ctx))
			events := s.Events()
			require.Equal(t, 5, len(events))
			require.Equal(t, []*kvpb.RangeFeedEvent{ev1, ev2}, events[3:])
			catchupReg.disconnect(nil)
		})
		t.Run("external disconnect after output loop", func(t *testing.T) {
			s := newTestStream()
			disconnectReg := newTestRegistration(s, withRSpan(spAB), withRegistrationType(rt))
			disconnectReg.publish(ctx, ev1, nil /* alloc */)
			disconnectReg.publish(ctx, ev2, nil /* alloc */)
			go disconnectReg.runOutputLoop(ctx, 0)
			require.NoError(t, disconnectReg.waitForCaughtUp(ctx))
			discErr := kvpb.NewError(fmt.Errorf("disconnection error"))
			disconnectReg.disconnect(discErr)
			require.Equal(t, discErr.GoError(), s.WaitForError(t))
			require.Equal(t, 2, len(s.Events()))
		})
		t.Run("external disconnect before output loop", func(t *testing.T) {
			s := newTestStream()
			disconnectEarlyReg := newTestRegistration(s, withRSpan(spAB), withRegistrationType(rt))
			disconnectEarlyReg.publish(ctx, ev1, nil /* alloc */)
			disconnectEarlyReg.publish(ctx, ev2, nil /* alloc */)
			discErr := kvpb.NewError(fmt.Errorf("disconnection error"))
			disconnectEarlyReg.disconnect(discErr)
			go disconnectEarlyReg.runOutputLoop(ctx, 0)
			require.Equal(t, discErr.GoError(), s.WaitForError(t))
			if rt == buffered {
				require.Equal(t, 0, len(s.Events()))
			} else {
				// For unbuffered registration, the events are sent to the stream
				// directly without going through runOutputLoop buffer.
				require.Equal(t, []*kvpb.RangeFeedEvent{ev1, ev2}, s.Events())
			}
		})
		t.Run("overflow", func(t *testing.T) {
			s := newTestStream()
			var capOfBuf int
			var overflowReg registration
			switch rt {
			case buffered:
				overflowReg = newTestRegistration(s, withRSpan(spAB), withRegistrationType(rt))
				capOfBuf = cap(overflowReg.(*bufferedRegistration).buf)
			case unbuffered:
				overflowReg = newTestRegistration(s,
					withRegistrationType(true),
					withRSpan(spBC),
					withStartTs(hlc.Timestamp{WallTime: 1}),
					// Initialize a noop catch-up iterator to force catch up buffer to be used
					// for events publish.
					withCatchUpIter(newTestIterator([]storage.MVCCKeyValue{}, nil)),
					withRegistrationType(rt))
				// Safe to access without locking since catchupReg is not used elsewhere
				// yet.
				capOfBuf = cap(overflowReg.(*unbufferedRegistration).mu.catchUpBuf)
			}
			for i := 0; i < capOfBuf+3; i++ {
				overflowReg.publish(ctx, ev1, nil /* alloc */)
			}
			go overflowReg.runOutputLoop(ctx, 0)
			require.Equal(t, kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_SLOW_CONSUMER), s.WaitForError(t))
			require.Equal(t, capOfBuf, len(s.Events()))
		})
		t.Run("stream error", func(t *testing.T) {
			s := newTestStream()
			streamErrReg := newTestRegistration(s, withRSpan(spAB), withRegistrationType(rt))
			streamErr := fmt.Errorf("stream error")
			s.SetSendErr(streamErr)
			go streamErrReg.runOutputLoop(ctx, 0)
			streamErrReg.publish(ctx, ev1, nil /* alloc */)
			require.Equal(t, streamErr, s.WaitForError(t))
		})
		t.Run("stream context canceled", func(t *testing.T) {
			s := newTestStream()
			streamCancelReg := newTestRegistration(s, withRSpan(spAB), withRegistrationType(rt))
			streamCancelReg.disconnect(kvpb.NewError(context.Canceled))
			go streamCancelReg.runOutputLoop(ctx, 0)
			require.NoError(t, streamCancelReg.waitForCaughtUp(ctx))
			require.Equal(t, context.Canceled, s.WaitForError(t))
		})
	})
}

func TestRegistrationCatchUpScan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunValues(t, "registration type=", registrationTestTypes, func(t *testing.T, rt registrationType) {
		testutils.RunTrueAndFalse(t, "filtering", func(t *testing.T, filtering bool) {
			// Run a catch-up scan for a registration over a test
			// iterator with the following keys.
			txn1, txn2 := uuid.MakeV4(), uuid.MakeV4()
			iter := newTestIterator([]storage.MVCCKeyValue{
				makeKV("a", "valA1", 10),
				makeIntent("c", txn1, "txnKeyC", 15),
				makeProvisionalKV("c", "txnKeyC", 15),
				makeKV("c", "valC2", 11),
				makeKV("c", "valC1", 9),
				makeIntent("d", txn2, "txnKeyD", 21),
				makeProvisionalKV("d", "txnKeyD", 21),
				makeKV("d", "valD5", 20),
				makeKV("d", "valD4", 19),
				makeKV("d", "valD3", 16),
				makeKV("d", "valD2", 3),
				makeKV("d", "valD1", 1),
				makeKV("e", "valE3", 6),
				makeKV("e", "valE2", 5),
				makeKV("e", "valE1", 4),
				makeKV("f", "valF3", 7),
				makeKV("f", "valF2", 6),
				makeKV("f", "valF1", 5),
				makeKV("h", "valH1", 15),
				makeKV("m", "valM1", 1),
				makeIntent("n", txn1, "txnKeyN", 12),
				makeProvisionalKV("n", "txnKeyN", 12),
				makeIntent("r", txn1, "txnKeyR", 19),
				makeProvisionalKV("r", "txnKeyR", 19),
				makeKV("r", "valR1", 4),
				makeKV("s", "valS3", 21),
				makeKVWithHeader("s", "valS2", 20, enginepb.MVCCValueHeader{OmitInRangefeeds: true}),
				makeKV("s", "valS1", 19),
				makeIntent("w", txn1, "txnKeyW", 3),
				makeProvisionalKV("w", "txnKeyW", 3),
				makeIntent("z", txn2, "txnKeyZ", 21),
				makeProvisionalKV("z", "txnKeyZ", 21),
				makeKV("z", "valZ1", 4),
			}, roachpb.Key("w"))

			metrics := NewMetrics()
			s := newTestStream()
			r := newTestRegistration(s, withRSpan(roachpb.Span{
				Key:    roachpb.Key("d"),
				EndKey: roachpb.Key("w"),
			}), withStartTs(hlc.Timestamp{WallTime: 4}), withCatchUpIter(iter), withDiff(true),
				withFiltering(filtering), withRMetrics(metrics), withRegistrationType(rt))
			require.Zero(t, metrics.RangeFeedCatchUpScanNanos.Count())
			switch r := r.(type) {
			case *bufferedRegistration:
				require.NoError(t, r.maybeRunCatchUpScan(context.Background()))
			case *unbufferedRegistration:
				require.NoError(t, r.maybeRunCatchUpScan(context.Background()))
			}
			require.True(t, iter.closed)
			require.NotZero(t, metrics.RangeFeedCatchUpScanNanos.Count())

			// Compare the events sent on the registration's Stream to the expected events.
			expEvents := []*kvpb.RangeFeedEvent{
				rangeFeedValueWithPrev(
					roachpb.Key("d"),
					makeValWithTs("valD3", 16),
					makeVal("valD2"),
				),
				rangeFeedValueWithPrev(
					roachpb.Key("d"),
					makeValWithTs("valD4", 19),
					makeVal("valD3"),
				),
				rangeFeedValueWithPrev(
					roachpb.Key("d"),
					makeValWithTs("valD5", 20),
					makeVal("valD4"),
				),
				rangeFeedValueWithPrev(
					roachpb.Key("e"),
					makeValWithTs("valE2", 5),
					makeVal("valE1"),
				),
				rangeFeedValueWithPrev(
					roachpb.Key("e"),
					makeValWithTs("valE3", 6),
					makeVal("valE2"),
				),
				rangeFeedValue(
					roachpb.Key("f"),
					makeValWithTs("valF1", 5),
				),
				rangeFeedValueWithPrev(
					roachpb.Key("f"),
					makeValWithTs("valF2", 6),
					makeVal("valF1"),
				),
				rangeFeedValueWithPrev(
					roachpb.Key("f"),
					makeValWithTs("valF3", 7),
					makeVal("valF2"),
				),
				rangeFeedValue(
					roachpb.Key("h"),
					makeValWithTs("valH1", 15),
				),
				rangeFeedValue(
					roachpb.Key("s"),
					makeValWithTs("valS1", 19),
				),
			}
			if !filtering {
				expEvents = append(expEvents,
					rangeFeedValueWithPrev(
						roachpb.Key("s"),
						makeValWithTs("valS2", 20),
						makeVal("valS1"),
					))
			}
			expEvents = append(expEvents, rangeFeedValueWithPrev(
				roachpb.Key("s"),
				makeValWithTs("valS3", 21),
				// Even though the event that wrote val2 is filtered out, we want to keep
				// val2 as a previous value of the next event.
				makeVal("valS2"),
			))
			require.Equal(t, expEvents, s.Events())
		})
	})
}

// TestRegistryWithOmitOrigin verifies that when a registration is created with
// withOmitRemote = true, it will not publish values with originID != 0.
func TestRegistryWithOmitOrigin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	testutils.RunValues(t, "registration type=", registrationTestTypes, func(t *testing.T, rt registrationType) {
		noPrev := func(ev *kvpb.RangeFeedEvent) *kvpb.RangeFeedEvent {
			ev = ev.ShallowCopy()
			ev.GetValue().(*kvpb.RangeFeedValue).PrevValue = roachpb.Value{}
			return ev
		}

		val := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
		ev1, ev2 := new(kvpb.RangeFeedEvent), new(kvpb.RangeFeedEvent)
		ev1.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val, PrevValue: val})
		ev2.MustSetValue(&kvpb.RangeFeedValue{Key: keyB, Value: val, PrevValue: val})

		reg := makeRegistry(NewMetrics())

		sAC := newTestStream()
		rAC := newTestRegistration(sAC, withRSpan(spAC), withRegistrationType(rt))
		originFilteringStream := newTestStream()
		originFiltering := newTestRegistration(originFilteringStream, withRSpan(spAC),
			withOmitRemote(true), withRegistrationType(rt))

		go rAC.runOutputLoop(ctx, 0)
		go originFiltering.runOutputLoop(ctx, 0)

		defer rAC.disconnect(nil)
		defer originFiltering.disconnect(nil)

		reg.Register(ctx, rAC)
		reg.Register(ctx, originFiltering)

		reg.PublishToOverlapping(ctx, spAC, ev1, logicalOpMetadata{}, nil /* alloc */)
		reg.PublishToOverlapping(ctx, spAC, ev2, logicalOpMetadata{originID: 1}, nil /* alloc */)

		require.NoError(t, reg.waitForCaughtUp(ctx, all))

		require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev1), noPrev(ev2)}, sAC.Events())
		require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev1)}, originFilteringStream.Events())
		require.Nil(t, sAC.Error())
		require.Nil(t, originFilteringStream.Error())
	})
}

func TestRegistryBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	testutils.RunValues(t, "registration type=", registrationTestTypes, func(t *testing.T, rt registrationType) {
		val := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
		ev1, ev2 := new(kvpb.RangeFeedEvent), new(kvpb.RangeFeedEvent)
		ev3, ev4, ev5 := new(kvpb.RangeFeedEvent), new(kvpb.RangeFeedEvent), new(kvpb.RangeFeedEvent)
		ev1.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val, PrevValue: val})
		ev2.MustSetValue(&kvpb.RangeFeedValue{Key: keyB, Value: val, PrevValue: val})
		ev3.MustSetValue(&kvpb.RangeFeedValue{Key: keyC, Value: val, PrevValue: val})
		ev4.MustSetValue(&kvpb.RangeFeedValue{Key: keyD, Value: val, PrevValue: val})
		ev5.MustSetValue(&kvpb.RangeFeedValue{Key: keyD, Value: val, PrevValue: val})
		err1 := kvpb.NewErrorf("error1")
		noPrev := func(ev *kvpb.RangeFeedEvent) *kvpb.RangeFeedEvent {
			ev = ev.ShallowCopy()
			ev.GetValue().(*kvpb.RangeFeedValue).PrevValue = roachpb.Value{}
			return ev
		}

		reg := makeRegistry(NewMetrics())
		require.Equal(t, 0, reg.Len())
		reg.PublishToOverlapping(ctx, spAB, ev1, logicalOpMetadata{}, nil /* alloc */)
		reg.Disconnect(ctx, spAB)
		reg.DisconnectWithErr(ctx, spAB, err1)

		sAB := newTestStream()
		rAB := newTestRegistration(sAB, withRSpan(spAB), withRegistrationType(rt))
		sBC := newTestStream()
		rBC := newTestRegistration(sBC, withRSpan(spBC), withDiff(true), withRegistrationType(rt))
		sCD := newTestStream()
		rCD := newTestRegistration(sCD, withRSpan(spCD), withDiff(true), withRegistrationType(rt))
		sAC := newTestStream()
		rAC := newTestRegistration(sAC, withRSpan(spAC), withRegistrationType(rt))
		sACFiltering := newTestStream()
		rACFiltering := newTestRegistration(sACFiltering, withRSpan(spAC), withFiltering(true), withRegistrationType(rt))
		go rAB.runOutputLoop(ctx, 0)
		go rBC.runOutputLoop(ctx, 0)
		go rCD.runOutputLoop(ctx, 0)
		go rAC.runOutputLoop(ctx, 0)
		go rACFiltering.runOutputLoop(ctx, 0)
		defer rAB.disconnect(nil)
		defer rBC.disconnect(nil)
		defer rCD.disconnect(nil)
		defer rAC.disconnect(nil)
		defer rACFiltering.disconnect(nil)

		// Register 6 registrations.
		reg.Register(ctx, rAB)
		require.Equal(t, 1, reg.Len())
		reg.Register(ctx, rBC)
		require.Equal(t, 2, reg.Len())
		reg.Register(ctx, rCD)
		require.Equal(t, 3, reg.Len())
		reg.Register(ctx, rAC)
		require.Equal(t, 4, reg.Len())
		reg.Register(ctx, rACFiltering)
		require.Equal(t, 5, reg.Len())

		// Publish to different spans.
		reg.PublishToOverlapping(ctx, spAB, ev1, logicalOpMetadata{}, nil /* alloc */)
		reg.PublishToOverlapping(ctx, spBC, ev2, logicalOpMetadata{}, nil /* alloc */)
		reg.PublishToOverlapping(ctx, spCD, ev3, logicalOpMetadata{}, nil /* alloc */)
		reg.PublishToOverlapping(ctx, spAC, ev4, logicalOpMetadata{}, nil /* alloc */)
		reg.PublishToOverlapping(ctx, spAC, ev5, logicalOpMetadata{omitInRangefeeds: true}, nil /* alloc */)

		require.NoError(t, reg.waitForCaughtUp(ctx, all))
		require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev1), noPrev(ev4), noPrev(ev5)}, sAB.Events())
		require.Equal(t, []*kvpb.RangeFeedEvent{ev2, ev4, ev5}, sBC.Events())
		require.Equal(t, []*kvpb.RangeFeedEvent{ev3}, sCD.Events())
		require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev1), noPrev(ev2), noPrev(ev4), noPrev(ev5)}, sAC.Events())
		// Registration rACFiltering doesn't receive ev5 because both withFiltering
		// (for the registration) and OmitInRangefeeds (for the event) are true.
		require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev1), noPrev(ev2), noPrev(ev4)}, sACFiltering.Events())
		require.Nil(t, sAB.Error())
		require.Nil(t, sBC.Error())
		require.Nil(t, sCD.Error())
		require.Nil(t, sAC.Error())
		require.Nil(t, sACFiltering.Error())

		// Check the registry's operation filter.
		f := reg.NewFilter()
		// Testing NeedVal.
		require.True(t, f.NeedVal(spAB))
		require.True(t, f.NeedVal(spBC))
		require.True(t, f.NeedVal(spCD))
		require.True(t, f.NeedVal(spAC))
		require.False(t, f.NeedVal(spXY))
		require.True(t, f.NeedVal(roachpb.Span{Key: keyA}))
		require.True(t, f.NeedVal(roachpb.Span{Key: keyB}))
		require.True(t, f.NeedVal(roachpb.Span{Key: keyC}))
		require.False(t, f.NeedVal(roachpb.Span{Key: keyX}))
		// Testing NeedPrevVal.
		require.False(t, f.NeedPrevVal(spAB))
		require.True(t, f.NeedPrevVal(spBC))
		require.True(t, f.NeedPrevVal(spCD))
		require.True(t, f.NeedPrevVal(spAC))
		require.False(t, f.NeedPrevVal(spXY))
		require.False(t, f.NeedPrevVal(roachpb.Span{Key: keyA}))
		require.True(t, f.NeedPrevVal(roachpb.Span{Key: keyB}))
		require.True(t, f.NeedPrevVal(roachpb.Span{Key: keyC}))
		require.False(t, f.NeedPrevVal(roachpb.Span{Key: keyX}))

		// Disconnect span that overlaps with rCD.
		reg.DisconnectWithErr(ctx, spCD, err1)
		require.Equal(t, 4, reg.Len())
		require.Equal(t, err1.GoError(), sCD.WaitForError(t))

		// Can still publish to rAB.
		reg.PublishToOverlapping(ctx, spAB, ev4, logicalOpMetadata{}, nil /* alloc */)
		reg.PublishToOverlapping(ctx, spBC, ev3, logicalOpMetadata{}, nil /* alloc */)
		reg.PublishToOverlapping(ctx, spCD, ev2, logicalOpMetadata{}, nil /* alloc */)
		reg.PublishToOverlapping(ctx, spAC, ev1, logicalOpMetadata{}, nil /* alloc */)
		require.NoError(t, reg.waitForCaughtUp(ctx, all))
		require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev4), noPrev(ev1)}, sAB.Events())

		// Disconnect from rAB without error.
		reg.Disconnect(ctx, spAB)
		require.Nil(t, sAC.WaitForError(t))
		require.Nil(t, sAB.WaitForError(t))
		require.Equal(t, 1, reg.Len())

		// Check the registry's operation filter again.
		f = reg.NewFilter()
		// Testing NeedVal.
		require.False(t, f.NeedVal(spAB))
		require.True(t, f.NeedVal(spBC))
		require.False(t, f.NeedVal(spCD))
		require.True(t, f.NeedVal(spAC))
		require.False(t, f.NeedVal(spXY))
		require.False(t, f.NeedVal(roachpb.Span{Key: keyA}))
		require.True(t, f.NeedVal(roachpb.Span{Key: keyB}))
		require.False(t, f.NeedVal(roachpb.Span{Key: keyC}))
		require.False(t, f.NeedVal(roachpb.Span{Key: keyX}))
		// Testing NeedPrevVal.
		require.False(t, f.NeedPrevVal(spAB))
		require.True(t, f.NeedPrevVal(spBC))
		require.False(t, f.NeedPrevVal(spCD))
		require.True(t, f.NeedPrevVal(spAC))
		require.False(t, f.NeedPrevVal(spXY))
		require.False(t, f.NeedPrevVal(roachpb.Span{Key: keyA}))
		require.True(t, f.NeedPrevVal(roachpb.Span{Key: keyB}))
		require.False(t, f.NeedPrevVal(roachpb.Span{Key: keyC}))
		require.False(t, f.NeedPrevVal(roachpb.Span{Key: keyX}))

		// Unregister the rBC registration.
		reg.Unregister(ctx, rBC)
		require.Equal(t, 0, reg.Len())
	})
}

func TestRegistryPublishBeneathStartTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	testutils.RunValues(t, "registration type=", registrationTestTypes, func(t *testing.T, rt registrationType) {
		reg := makeRegistry(NewMetrics())
		s := newTestStream()
		r := newTestRegistration(s, withRSpan(spAB), withStartTs(hlc.Timestamp{WallTime: 10}), withRegistrationType(rt))
		go r.runOutputLoop(ctx, 0)
		reg.Register(ctx, r)

		// Publish a value with a timestamp beneath the registration's start
		// timestamp. Should be ignored.
		ev := new(kvpb.RangeFeedEvent)
		ev.MustSetValue(&kvpb.RangeFeedValue{
			Value: roachpb.Value{Timestamp: hlc.Timestamp{WallTime: 5}},
		})
		reg.PublishToOverlapping(ctx, spAB, ev, logicalOpMetadata{}, nil /* alloc */)
		require.NoError(t, reg.waitForCaughtUp(ctx, all))
		require.Nil(t, s.Events())

		// Publish a value with a timestamp equal to the registration's start
		// timestamp. Should be ignored.
		ev.MustSetValue(&kvpb.RangeFeedValue{
			Value: roachpb.Value{Timestamp: hlc.Timestamp{WallTime: 10}},
		})
		reg.PublishToOverlapping(ctx, spAB, ev, logicalOpMetadata{}, nil /* alloc */)
		require.NoError(t, reg.waitForCaughtUp(ctx, all))
		require.Nil(t, s.Events())

		// Publish a checkpoint with a timestamp beneath the registration's. Should
		// be delivered.
		ev.MustSetValue(&kvpb.RangeFeedCheckpoint{
			Span: spAB, ResolvedTS: hlc.Timestamp{WallTime: 5},
		})
		reg.PublishToOverlapping(ctx, spAB, ev, logicalOpMetadata{}, nil /* alloc */)
		require.NoError(t, reg.waitForCaughtUp(ctx, all))
		require.Equal(t, []*kvpb.RangeFeedEvent{ev}, s.Events())

		r.disconnect(nil)
	})
}

func TestRegistrationString(t *testing.T) {
	testCases := []struct {
		r   baseRegistration
		exp string
	}{
		{
			r: baseRegistration{
				span: roachpb.Span{Key: roachpb.Key("a")},
			},
			exp: `[a @ 0,0+]`,
		},
		{
			r: baseRegistration{span: roachpb.Span{
				Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
			},
			exp: `[{a-c} @ 0,0+]`,
		},
		{
			r: baseRegistration{
				span:             roachpb.Span{Key: roachpb.Key("d")},
				catchUpTimestamp: hlc.Timestamp{WallTime: 10, Logical: 1},
			},
			exp: `[d @ 0.000000010,1+]`,
		},
		{
			r: baseRegistration{span: roachpb.Span{
				Key: roachpb.Key("d"), EndKey: roachpb.Key("z")},
				catchUpTimestamp: hlc.Timestamp{WallTime: 40, Logical: 9},
			},
			exp: `[{d-z} @ 0.000000040,9+]`,
		},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.exp, tc.r.String())
	}
}

// TestRegistryShutdown test verifies that when we shutdown registry with
// existing registration, registration won't try to update any metrics
// implicitly.
func TestRegistryShutdownMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	testutils.RunValues(t, "registration type=", registrationTestTypes, func(t *testing.T, rt registrationType) {
		reg := makeRegistry(NewMetrics())
		regDoneC := make(chan interface{})
		r := newTestRegistration(newTestStream(), withRSpan(spAB),
			withStartTs(hlc.Timestamp{WallTime: 10}), withRegistrationType(rt))
		go func() {
			r.runOutputLoop(ctx, 0)
			close(regDoneC)
		}()
		reg.Register(ctx, r)

		reg.DisconnectAllOnShutdown(ctx, nil)
		<-regDoneC
		require.Zero(t, reg.metrics.RangeFeedRegistrations.Value(), "metric is not zero on stop")
	})
}

// TestBaseRegistration tests base registration implementation methods.
func TestBaseRegistration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r := newTestRegistration(newTestStream(), withRSpan(spAB), withStartTs(hlc.Timestamp{WallTime: 10}), withDiff(true), withFiltering(true))
	require.Equal(t, spAB, r.getSpan())
	require.Equal(t, hlc.Timestamp{WallTime: 10}, r.getCatchUpTimestamp())
	r.setSpanAsKeys()
	require.Equal(t, r.Range(), spAB.AsRange())
	require.Equal(t, r.Range(), r.Range())
	r.setID(10)
	require.Equal(t, uintptr(10), r.ID())
	require.True(t, r.getWithDiff())
	require.True(t, r.getWithFiltering())
	require.False(t, r.getWithOmitRemote())
}
