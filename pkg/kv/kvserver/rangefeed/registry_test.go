// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"fmt"
	"testing"

	_ "github.com/cockroachdb/cockroach/pkg/keys" // hook up pretty printer
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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
			eventSent := len(noCatchupReg.getBuf())
			switch rt {
			case unbuffered:
				eventSent += len(s.GetEvents())
				require.Nil(t, noCatchupReg.getBuf())
			}
			go noCatchupReg.runOutputLoop(ctx, 0)
			require.NoError(t, noCatchupReg.waitForCaughtUp(ctx))
			require.Equal(t, []*kvpb.RangeFeedEvent{ev1, ev2}, s.GetAndClearEvents())
			noCatchupReg.Disconnect(nil)
			require.Nil(t, s.GetAndClearEvents())
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
			require.Equal(t, 2, len(catchupReg.getBuf()))
			go catchupReg.runOutputLoop(ctx, 0)
			require.NoError(t, catchupReg.waitForCaughtUp(ctx))
			events := s.GetAndClearEvents()
			require.Equal(t, 5, len(events))
			require.Equal(t, []*kvpb.RangeFeedEvent{ev1, ev2}, events[3:])
			catchupReg.Disconnect(nil)
			require.Nil(t, s.GetAndClearEvents())
		})
		t.Run("external disconnect after output loop", func(t *testing.T) {
			s := newTestStream()
			disconnectReg := newTestRegistration(s, withRSpan(spAB), withRegistrationType(rt))
			disconnectReg.publish(ctx, ev1, nil /* alloc */)
			disconnectReg.publish(ctx, ev2, nil /* alloc */)
			go disconnectReg.runOutputLoop(ctx, 0)
			require.NoError(t, disconnectReg.waitForCaughtUp(ctx))
			discErr := kvpb.NewError(fmt.Errorf("disconnection error"))
			disconnectReg.Disconnect(discErr)
			require.Equal(t, discErr.GoError(), s.WaitForError(t))
			require.Equal(t, 2, len(s.GetAndClearEvents()))
			disconnectReg.publish(ctx, ev1, nil /* alloc */)
			require.Equal(t, 0, len(s.GetAndClearEvents()))
		})
		t.Run("external disconnect before output loop without catch up iter", func(t *testing.T) {
			s := newTestStream()
			disconnectEarlyReg := newTestRegistration(s, withRSpan(spAB), withRegistrationType(rt))
			disconnectEarlyReg.publish(ctx, ev1, nil /* alloc */)
			disconnectEarlyReg.publish(ctx, ev2, nil /* alloc */)
			discErr := kvpb.NewError(fmt.Errorf("disconnection error"))
			disconnectEarlyReg.Disconnect(discErr)
			go disconnectEarlyReg.runOutputLoop(ctx, 0)
			require.Equal(t, discErr.GoError(), s.WaitForError(t))
			// This is the main behaviour change between buffered and unbuffered
			// registration. For buffered registration, events buffered are
			// discarded after disconnection. For unbuffered registration,
			// events buffered already are still sent to the stream after
			// disconnection.
			if rt == buffered {
				require.Equal(t, 0, len(s.GetAndClearEvents()))
			} else {
				require.Equal(t, []*kvpb.RangeFeedEvent{ev1, ev2}, s.GetAndClearEvents())
			}
			// Repeatedly disconnect should be idempotent.
			disconnectEarlyReg.Disconnect(kvpb.NewError(nil))
			require.Nil(t, s.GetAndClearEvents())
		})
		t.Run("external disconnect before output loop with catch up iter", func(t *testing.T) {
			s := newTestStream()
			iter := newTestIterator(keyValues, roachpb.Key("w"))
			disconnectEarlyReg := newTestRegistration(s, withRSpan(spAB), withRegistrationType(rt), withCatchUpIter(iter))
			disconnectEarlyReg.publish(ctx, ev1, nil /* alloc */)
			disconnectEarlyReg.publish(ctx, ev2, nil /* alloc */)
			discErr := kvpb.NewError(fmt.Errorf("disconnection error"))
			disconnectEarlyReg.Disconnect(discErr)
			require.Equal(t, discErr.GoError(), s.WaitForError(t))
			disconnectEarlyReg.runOutputLoop(ctx, 0)
			// If there is a catch up iterator, the buffered events should not
			// be sent to the stream.
			// Test the effects rather than the implementation
			require.Nil(t, s.GetAndClearEvents())
		})
		t.Run("overflow", func(t *testing.T) {
			s := newTestStream()
			// Unbuffered registration can only overflow during a catch up scan,
			// so we initialize a catch up scan.
			overflowReg := newTestRegistration(s,
				withRegistrationType(rt),
				withRSpan(spBC),
				withStartTs(hlc.Timestamp{WallTime: 1}),
				withCatchUpIter(newTestIterator([]storage.MVCCKeyValue{}, nil)),
				withRegistrationType(rt))
			capOfBuf := cap(overflowReg.getBuf())
			for i := 0; i < capOfBuf+3; i++ {
				overflowReg.publish(ctx, ev1, nil /* alloc */)
			}
			go overflowReg.runOutputLoop(ctx, 0)
			require.Equal(t, kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_SLOW_CONSUMER), s.WaitForError(t))
			require.Equal(t, capOfBuf, len(s.GetAndClearEvents()))
			require.NoError(t, overflowReg.waitForCaughtUp(ctx))
			require.True(t, overflowReg.getOverflowed())
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
			streamCancelReg.Disconnect(kvpb.NewError(context.Canceled))
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
			iter := newTestIterator(keyValues, roachpb.Key("w"))
			metrics := NewMetrics()
			s := newTestStream()
			r := newTestRegistration(s, withRSpan(roachpb.Span{
				Key:    roachpb.Key("d"),
				EndKey: roachpb.Key("w"),
			}), withStartTs(hlc.Timestamp{WallTime: 4}), withCatchUpIter(iter), withDiff(true),
				withFiltering(filtering), withRMetrics(metrics), withRegistrationType(rt))
			require.Zero(t, metrics.RangeFeedCatchUpScanNanos.Count())
			require.NoError(t, r.maybeRunCatchUpScan(context.Background()))
			require.True(t, iter.closed)
			require.NotZero(t, metrics.RangeFeedCatchUpScanNanos.Count())
			// Compare the events sent on the registration's Stream to the
			// expected events.
			require.Equal(t, expEvents(filtering), s.GetAndClearEvents())
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

		defer rAC.Disconnect(nil)
		defer originFiltering.Disconnect(nil)

		reg.Register(ctx, rAC)
		reg.Register(ctx, originFiltering)

		reg.PublishToOverlapping(ctx, spAC, ev1, logicalOpMetadata{}, nil /* alloc */)
		reg.PublishToOverlapping(ctx, spAC, ev2, logicalOpMetadata{originID: 1}, nil /* alloc */)

		require.NoError(t, reg.waitForCaughtUp(ctx, all))

		require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev1), noPrev(ev2)}, sAC.GetAndClearEvents())
		require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev1)}, originFilteringStream.GetAndClearEvents())
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
		defer rAB.Disconnect(nil)
		defer rBC.Disconnect(nil)
		defer rCD.Disconnect(nil)
		defer rAC.Disconnect(nil)
		defer rACFiltering.Disconnect(nil)

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
		require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev1), noPrev(ev4), noPrev(ev5)}, sAB.GetAndClearEvents())
		require.Equal(t, []*kvpb.RangeFeedEvent{ev2, ev4, ev5}, sBC.GetAndClearEvents())
		require.Equal(t, []*kvpb.RangeFeedEvent{ev3}, sCD.GetAndClearEvents())
		require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev1), noPrev(ev2), noPrev(ev4), noPrev(ev5)}, sAC.GetAndClearEvents())
		// Registration rACFiltering doesn't receive ev5 because both withFiltering
		// (for the registration) and OmitInRangefeeds (for the event) are true.
		require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev1), noPrev(ev2), noPrev(ev4)}, sACFiltering.GetAndClearEvents())
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
		require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev4), noPrev(ev1)}, sAB.GetAndClearEvents())

		// Disconnect from rAB without error.
		reg.DisconnectWithErr(ctx, spAB, nil)
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

		// Unregister the rBC registration as if it was being unregistered via the
		// processor.
		rBC.setShouldUnregister()
		reg.unregisterMarkedRegistrations(ctx)
		require.Equal(t, 0, reg.Len())
		require.Equal(t, 0, int(reg.metrics.RangeFeedRegistrations.Value()),
			"RangefeedRegistrations metric not zero after all registrations have been removed")
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
		require.Nil(t, s.GetAndClearEvents())

		// Publish a value with a timestamp equal to the registration's start
		// timestamp. Should be ignored.
		ev.MustSetValue(&kvpb.RangeFeedValue{
			Value: roachpb.Value{Timestamp: hlc.Timestamp{WallTime: 10}},
		})
		reg.PublishToOverlapping(ctx, spAB, ev, logicalOpMetadata{}, nil /* alloc */)
		require.NoError(t, reg.waitForCaughtUp(ctx, all))
		require.Nil(t, s.GetAndClearEvents())

		// Publish a checkpoint with a timestamp beneath the registration's. Should
		// be delivered.
		ev.MustSetValue(&kvpb.RangeFeedCheckpoint{
			Span: spAB, ResolvedTS: hlc.Timestamp{WallTime: 5},
		})
		reg.PublishToOverlapping(ctx, spAB, ev, logicalOpMetadata{}, nil /* alloc */)
		require.NoError(t, reg.waitForCaughtUp(ctx, all))
		require.Equal(t, []*kvpb.RangeFeedEvent{ev}, s.GetAndClearEvents())

		r.Disconnect(nil)
	})
}

func TestRegistrationString(t *testing.T) {
	testCases := []struct {
		r   *baseRegistration
		exp string
	}{
		{
			r: &baseRegistration{
				span: roachpb.Span{Key: roachpb.Key("a")},
			},
			exp: `[a @ 0,0+]`,
		},
		{
			r: &baseRegistration{span: roachpb.Span{
				Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
			},
			exp: `[{a-c} @ 0,0+]`,
		},
		{
			r: &baseRegistration{
				span:             roachpb.Span{Key: roachpb.Key("d")},
				catchUpTimestamp: hlc.Timestamp{WallTime: 10, Logical: 1},
			},
			exp: `[d @ 0.000000010,1+]`,
		},
		{
			r: &baseRegistration{span: roachpb.Span{
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

// TODO(wenyihu6): Should catch up events also get stripped events? They current'y don't.
func TestPublishStrippedEvents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	val := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
	prevVal := roachpb.Value{RawBytes: []byte("prevVal"), Timestamp: hlc.Timestamp{WallTime: 1}}
	ev1 := new(kvpb.RangeFeedEvent)
	ev1.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val, PrevValue: prevVal})
	expectedEv := new(kvpb.RangeFeedEvent)
	// maybeStripEvent should strip the PrevValue from the event since withDiff is
	// false.
	expectedEv.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val})

	err := kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_NO_LEASEHOLDER))
	testutils.RunValues(t, "registration type=", registrationTestTypes, func(t *testing.T, rt registrationType) {
		s := newTestStream()
		noCatchupReg := newTestRegistration(s, withRSpan(spAB), withRegistrationType(rt), withDiff(false))
		noCatchupReg.publish(ctx, ev1, nil /* alloc */)
		go noCatchupReg.runOutputLoop(ctx, 0)
		require.NoError(t, noCatchupReg.waitForCaughtUp(ctx))
		require.Equal(t, []*kvpb.RangeFeedEvent{expectedEv}, s.GetAndClearEvents())
		noCatchupReg.Disconnect(err)
		require.Equal(t, err, kvpb.NewError(s.Error()))
	})
}
