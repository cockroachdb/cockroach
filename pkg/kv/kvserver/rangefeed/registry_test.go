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

	// Registration with no catchup scan specified.
	noCatchupReg := newTestRegistration(spAB, hlc.Timestamp{}, nil, /* catchup */
		false /* withDiff */, false /* withFiltering */, false /* withOmitRemote */)
	noCatchupReg.publish(ctx, ev1, nil /* alloc */)
	noCatchupReg.publish(ctx, ev2, nil /* alloc */)
	require.Equal(t, len(noCatchupReg.buf), 2)
	go noCatchupReg.runOutputLoop(ctx, 0)
	require.NoError(t, noCatchupReg.waitForCaughtUp(ctx))
	require.Equal(t, []*kvpb.RangeFeedEvent{ev1, ev2}, noCatchupReg.Events())
	noCatchupReg.disconnect(nil)

	// Registration with catchup scan.
	catchupReg := newTestRegistration(spBC, hlc.Timestamp{WallTime: 1},
		newTestIterator([]storage.MVCCKeyValue{
			makeKV("b", "val1", 10),
			makeKV("bc", "val3", 11),
			makeKV("bd", "val4", 9),
		}, nil),
		false /* withDiff */, false /* withFiltering */, false /* withOmitRemote */)
	catchupReg.publish(ctx, ev1, nil /* alloc */)
	catchupReg.publish(ctx, ev2, nil /* alloc */)
	require.Equal(t, len(catchupReg.buf), 2)
	go catchupReg.runOutputLoop(ctx, 0)
	require.NoError(t, catchupReg.waitForCaughtUp(ctx))
	events := catchupReg.Events()
	require.Equal(t, 5, len(events))
	require.Equal(t, []*kvpb.RangeFeedEvent{ev1, ev2}, events[3:])
	catchupReg.disconnect(nil)

	// EXIT CONDITIONS
	// External Disconnect.
	disconnectReg := newTestRegistration(spAB, hlc.Timestamp{}, nil, /* catchup */
		false /* withDiff */, false /* withFiltering */, false /* withOmitRemote */)
	disconnectReg.publish(ctx, ev1, nil /* alloc */)
	disconnectReg.publish(ctx, ev2, nil /* alloc */)
	go disconnectReg.runOutputLoop(ctx, 0)
	require.NoError(t, disconnectReg.waitForCaughtUp(ctx))
	discErr := kvpb.NewError(fmt.Errorf("disconnection error"))
	disconnectReg.disconnect(discErr)
	require.Equal(t, discErr.GoError(), disconnectReg.WaitForError(t))
	require.Equal(t, 2, len(disconnectReg.Events()))

	// External Disconnect before output loop.
	disconnectEarlyReg := newTestRegistration(spAB, hlc.Timestamp{}, nil, /* catchup */
		false /* withDiff */, false /* withFiltering */, false /* withOmitRemote */)
	disconnectEarlyReg.publish(ctx, ev1, nil /* alloc */)
	disconnectEarlyReg.publish(ctx, ev2, nil /* alloc */)
	disconnectEarlyReg.disconnect(discErr)
	go disconnectEarlyReg.runOutputLoop(ctx, 0)
	require.Equal(t, discErr.GoError(), disconnectEarlyReg.WaitForError(t))
	require.Equal(t, 0, len(disconnectEarlyReg.Events()))

	// Overflow.
	overflowReg := newTestRegistration(spAB, hlc.Timestamp{}, nil, /* catchup */
		false /* withDiff */, false /* withFiltering */, false /* withOmitRemote */)
	for i := 0; i < cap(overflowReg.buf)+3; i++ {
		overflowReg.publish(ctx, ev1, nil /* alloc */)
	}
	go overflowReg.runOutputLoop(ctx, 0)
	require.Equal(t, newErrBufferCapacityExceeded().GoError(), overflowReg.WaitForError(t))
	require.Equal(t, cap(overflowReg.buf), len(overflowReg.Events()))

	// Stream Error.
	streamErrReg := newTestRegistration(spAB, hlc.Timestamp{}, nil, /* catchup */
		false /* withDiff */, false /* withFiltering */, false /* withOmitRemote */)
	streamErr := fmt.Errorf("stream error")
	streamErrReg.SetSendErr(streamErr)
	go streamErrReg.runOutputLoop(ctx, 0)
	streamErrReg.publish(ctx, ev1, nil /* alloc */)
	require.Equal(t, streamErr, streamErrReg.WaitForError(t))

	// Stream Context Canceled.
	streamCancelReg := newTestRegistration(spAB, hlc.Timestamp{}, nil, /* catchup */
		false /* withDiff */, false /* withFiltering */, false /* withOmitRemote */)

	streamCancelReg.Cancel()
	go streamCancelReg.runOutputLoop(ctx, 0)
	require.NoError(t, streamCancelReg.waitForCaughtUp(ctx))
	require.Equal(t, streamCancelReg.stream.Context().Err(), streamCancelReg.WaitForError(t))
}

func TestRegistrationCatchUpScan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "withFiltering", func(t *testing.T, withFiltering bool) {
		// Run a catch-up scan for a registration over a test
		// iterator with the following keys.
		iter := newTestIterator(keyValues, roachpb.Key("w"))
		r := newTestRegistration(roachpb.Span{
			Key:    roachpb.Key("d"),
			EndKey: roachpb.Key("w"),
		}, hlc.Timestamp{WallTime: 4}, iter, true /* withDiff */, withFiltering, false /* withOmitRemote */)

		require.Zero(t, r.metrics.RangeFeedCatchUpScanNanos.Count())
		require.NoError(t, r.maybeRunCatchUpScan(context.Background()))
		require.True(t, iter.closed)
		require.NotZero(t, r.metrics.RangeFeedCatchUpScanNanos.Count())

		// Compare the events sent on the registration's Stream to the expected events.
		require.Equal(t, expEvents(withFiltering), r.Events())
	})
}

// TestRegistryWithOmitOrigin verifies that when a registration is created with
// withOmitRemote = true, it will not publish values with originID != 0.
func TestRegistryWithOmitOrigin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

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
	rAC := newTestRegistration(spAC, hlc.Timestamp{}, nil, false /* withDiff */, false /* withFiltering */, false /* withOmitRemote */)
	originFiltering := newTestRegistration(spAC, hlc.Timestamp{}, nil, false /* withDiff */, false /* withFiltering */, true /* withOmitRemote */)

	go rAC.runOutputLoop(ctx, 0)
	go originFiltering.runOutputLoop(ctx, 0)

	defer rAC.disconnect(nil)
	defer originFiltering.disconnect(nil)

	reg.Register(ctx, rAC.bufferedRegistration)
	reg.Register(ctx, originFiltering.bufferedRegistration)

	reg.PublishToOverlapping(ctx, spAC, ev1, logicalOpMetadata{}, nil /* alloc */)
	reg.PublishToOverlapping(ctx, spAC, ev2, logicalOpMetadata{originID: 1}, nil /* alloc */)

	require.NoError(t, reg.waitForCaughtUp(ctx, all))

	require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev1), noPrev(ev2)}, rAC.Events())
	require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev1)}, originFiltering.Events())
	require.Nil(t, rAC.Error())
	require.Nil(t, originFiltering.Error())
}

func TestRegistryBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

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

	rAB := newTestRegistration(spAB, hlc.Timestamp{}, nil, false /* withDiff */, false /* withFiltering */, false /* withOmitRemote */)
	rBC := newTestRegistration(spBC, hlc.Timestamp{}, nil, true /* withDiff */, false /* withFiltering */, false /* withOmitRemote */)
	rCD := newTestRegistration(spCD, hlc.Timestamp{}, nil, true /* withDiff */, false /* withFiltering */, false /* withOmitRemote */)
	rAC := newTestRegistration(spAC, hlc.Timestamp{}, nil, false /* withDiff */, false /* withFiltering */, false /* withOmitRemote */)
	rACFiltering := newTestRegistration(spAC, hlc.Timestamp{}, nil, false /* withDiff */, true /* withFiltering */, false /* withOmitRemote */)
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
	reg.Register(ctx, rAB.bufferedRegistration)
	require.Equal(t, 1, reg.Len())
	reg.Register(ctx, rBC.bufferedRegistration)
	require.Equal(t, 2, reg.Len())
	reg.Register(ctx, rCD.bufferedRegistration)
	require.Equal(t, 3, reg.Len())
	reg.Register(ctx, rAC.bufferedRegistration)
	require.Equal(t, 4, reg.Len())
	reg.Register(ctx, rACFiltering.bufferedRegistration)
	require.Equal(t, 5, reg.Len())

	// Publish to different spans.
	reg.PublishToOverlapping(ctx, spAB, ev1, logicalOpMetadata{}, nil /* alloc */)
	reg.PublishToOverlapping(ctx, spBC, ev2, logicalOpMetadata{}, nil /* alloc */)
	reg.PublishToOverlapping(ctx, spCD, ev3, logicalOpMetadata{}, nil /* alloc */)
	reg.PublishToOverlapping(ctx, spAC, ev4, logicalOpMetadata{}, nil /* alloc */)
	reg.PublishToOverlapping(ctx, spAC, ev5, logicalOpMetadata{omitInRangefeeds: true}, nil /* alloc */)

	require.NoError(t, reg.waitForCaughtUp(ctx, all))
	require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev1), noPrev(ev4), noPrev(ev5)}, rAB.Events())
	require.Equal(t, []*kvpb.RangeFeedEvent{ev2, ev4, ev5}, rBC.Events())
	require.Equal(t, []*kvpb.RangeFeedEvent{ev3}, rCD.Events())
	require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev1), noPrev(ev2), noPrev(ev4), noPrev(ev5)}, rAC.Events())
	// Registration rACFiltering doesn't receive ev5 because both withFiltering
	// (for the registration) and OmitInRangefeeds (for the event) are true.
	require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev1), noPrev(ev2), noPrev(ev4)}, rACFiltering.Events())
	require.Nil(t, rAB.Error())
	require.Nil(t, rBC.Error())
	require.Nil(t, rCD.Error())
	require.Nil(t, rAC.Error())
	require.Nil(t, rACFiltering.Error())

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
	require.Equal(t, err1.GoError(), rCD.WaitForError(t))

	// Can still publish to rAB.
	reg.PublishToOverlapping(ctx, spAB, ev4, logicalOpMetadata{}, nil /* alloc */)
	reg.PublishToOverlapping(ctx, spBC, ev3, logicalOpMetadata{}, nil /* alloc */)
	reg.PublishToOverlapping(ctx, spCD, ev2, logicalOpMetadata{}, nil /* alloc */)
	reg.PublishToOverlapping(ctx, spAC, ev1, logicalOpMetadata{}, nil /* alloc */)
	require.NoError(t, reg.waitForCaughtUp(ctx, all))
	require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev4), noPrev(ev1)}, rAB.Events())

	// Disconnect from rAB without error.
	reg.Disconnect(ctx, spAB)
	require.Nil(t, rAC.WaitForError(t))
	require.Nil(t, rAB.WaitForError(t))
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
	reg.Unregister(ctx, rBC.bufferedRegistration)
	require.Equal(t, 0, reg.Len())
}

func TestRegistryPublishBeneathStartTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	reg := makeRegistry(NewMetrics())

	r := newTestRegistration(spAB, hlc.Timestamp{WallTime: 10}, nil, /* catchup */
		false /* withDiff */, false /* withFiltering */, false /* withOmitRemote */)
	go r.runOutputLoop(ctx, 0)
	reg.Register(ctx, r.bufferedRegistration)

	// Publish a value with a timestamp beneath the registration's start
	// timestamp. Should be ignored.
	ev := new(kvpb.RangeFeedEvent)
	ev.MustSetValue(&kvpb.RangeFeedValue{
		Value: roachpb.Value{Timestamp: hlc.Timestamp{WallTime: 5}},
	})
	reg.PublishToOverlapping(ctx, spAB, ev, logicalOpMetadata{}, nil /* alloc */)
	require.NoError(t, reg.waitForCaughtUp(ctx, all))
	require.Nil(t, r.Events())

	// Publish a value with a timestamp equal to the registration's start
	// timestamp. Should be ignored.
	ev.MustSetValue(&kvpb.RangeFeedValue{
		Value: roachpb.Value{Timestamp: hlc.Timestamp{WallTime: 10}},
	})
	reg.PublishToOverlapping(ctx, spAB, ev, logicalOpMetadata{}, nil /* alloc */)
	require.NoError(t, reg.waitForCaughtUp(ctx, all))
	require.Nil(t, r.Events())

	// Publish a checkpoint with a timestamp beneath the registration's. Should
	// be delivered.
	ev.MustSetValue(&kvpb.RangeFeedCheckpoint{
		Span: spAB, ResolvedTS: hlc.Timestamp{WallTime: 5},
	})
	reg.PublishToOverlapping(ctx, spAB, ev, logicalOpMetadata{}, nil /* alloc */)
	require.NoError(t, reg.waitForCaughtUp(ctx, all))
	require.Equal(t, []*kvpb.RangeFeedEvent{ev}, r.Events())

	r.disconnect(nil)
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
	reg := makeRegistry(NewMetrics())

	regDoneC := make(chan interface{})
	r := newTestRegistration(spAB, hlc.Timestamp{WallTime: 10}, nil, /*catchup */
		false /* withDiff */, false /* withFiltering */, false /* withOmitRemote */)
	go func() {
		r.runOutputLoop(ctx, 0)
		close(regDoneC)
	}()
	reg.Register(ctx, r.bufferedRegistration)

	reg.DisconnectAllOnShutdown(ctx, nil)
	<-regDoneC
	require.Zero(t, reg.metrics.RangeFeedRegistrations.Value(), "metric is not zero on stop")
}

// TestBaseRegistration tests base registration implementation methods.
func TestBaseRegistration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r := newTestRegistration(spAB, hlc.Timestamp{WallTime: 10}, nil, /*catchup */
		true /* withDiff */, true /* withFiltering */, false /* withOmitRemote */)
	require.Equal(t, spAB, r.getSpan())
	require.Equal(t, hlc.Timestamp{WallTime: 10}, r.getCatchUpTimestamp())
	r.setSpanAsKeys()
	require.Equal(t, r.keys, spAB.AsRange())
	require.Equal(t, r.keys, r.Range())
	r.setID(10)
	require.Equal(t, uintptr(10), r.ID())
	require.True(t, r.getWithDiff())
	require.True(t, r.getWithFiltering())
	require.False(t, r.getWithOmitRemote())
}
