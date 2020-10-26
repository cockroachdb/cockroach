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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

var (
	keyA, keyB = roachpb.Key("a"), roachpb.Key("b")
	keyC, keyD = roachpb.Key("c"), roachpb.Key("d")
	keyX, keyY = roachpb.Key("x"), roachpb.Key("y")

	spAB = roachpb.Span{Key: keyA, EndKey: keyB}
	spBC = roachpb.Span{Key: keyB, EndKey: keyC}
	spCD = roachpb.Span{Key: keyC, EndKey: keyD}
	spAC = roachpb.Span{Key: keyA, EndKey: keyC}
	spXY = roachpb.Span{Key: keyX, EndKey: keyY}
)

type testStream struct {
	ctx     context.Context
	ctxDone func()
	mu      struct {
		syncutil.Mutex
		sendErr error
		events  []*roachpb.RangeFeedEvent
	}
}

func newTestStream() *testStream {
	ctx, done := context.WithCancel(context.Background())
	return &testStream{ctx: ctx, ctxDone: done}
}

func (s *testStream) Context() context.Context {
	return s.ctx
}

func (s *testStream) Cancel() {
	s.ctxDone()
}

func (s *testStream) Send(e *roachpb.RangeFeedEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.sendErr != nil {
		return s.mu.sendErr
	}
	s.mu.events = append(s.mu.events, e)
	return nil
}

func (s *testStream) SetSendErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.sendErr = err
}

func (s *testStream) Events() []*roachpb.RangeFeedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	es := s.mu.events
	s.mu.events = nil
	return es
}

func (s *testStream) BlockSend() func() {
	s.mu.Lock()
	return s.mu.Unlock
}

type testRegistration struct {
	registration
	stream *testStream
	errC   <-chan *roachpb.Error
}

func newTestRegistration(
	span roachpb.Span, ts hlc.Timestamp, catchup storage.SimpleMVCCIterator, withDiff bool,
) *testRegistration {
	s := newTestStream()
	errC := make(chan *roachpb.Error, 1)
	return &testRegistration{
		registration: newRegistration(
			span,
			ts,
			makeIteratorConstructor(catchup),
			withDiff,
			5,
			NewMetrics(),
			s,
			errC,
		),
		stream: s,
		errC:   errC,
	}
}

func (r *testRegistration) Events() []*roachpb.RangeFeedEvent {
	return r.stream.Events()
}

func (r *testRegistration) Err() *roachpb.Error {
	select {
	case pErr := <-r.errC:
		return pErr
	default:
		return nil
	}
}

func TestRegistrationBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	val := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
	ev1, ev2 := new(roachpb.RangeFeedEvent), new(roachpb.RangeFeedEvent)
	ev1.MustSetValue(&roachpb.RangeFeedValue{Key: keyA, Value: val})
	ev2.MustSetValue(&roachpb.RangeFeedValue{Key: keyB, Value: val})

	// Registration with no catchup scan specified.
	noCatchupReg := newTestRegistration(spAB, hlc.Timestamp{}, nil, false)
	noCatchupReg.publish(ev1)
	noCatchupReg.publish(ev2)
	require.Equal(t, len(noCatchupReg.buf), 2)
	go noCatchupReg.runOutputLoop(context.Background())
	require.NoError(t, noCatchupReg.waitForCaughtUp())
	require.Equal(t, []*roachpb.RangeFeedEvent{ev1, ev2}, noCatchupReg.stream.Events())
	noCatchupReg.disconnect(nil)
	<-noCatchupReg.errC

	// Registration with catchup scan.
	catchupReg := newTestRegistration(spBC, hlc.Timestamp{WallTime: 1}, newTestIterator([]storage.MVCCKeyValue{
		makeKV("b", "val1", 10),
		makeInline("ba", "val2"),
		makeKV("bc", "val3", 11),
		makeKV("bd", "val4", 9),
	}), false)
	catchupReg.publish(ev1)
	catchupReg.publish(ev2)
	require.Equal(t, len(catchupReg.buf), 2)
	go catchupReg.runOutputLoop(context.Background())
	require.NoError(t, catchupReg.waitForCaughtUp())
	events := catchupReg.stream.Events()
	require.Equal(t, 6, len(events))
	require.Equal(t, []*roachpb.RangeFeedEvent{ev1, ev2}, events[4:])
	catchupReg.disconnect(nil)
	<-catchupReg.errC

	// EXIT CONDITIONS
	// External Disconnect.
	disconnectReg := newTestRegistration(spAB, hlc.Timestamp{}, nil, false)
	disconnectReg.publish(ev1)
	disconnectReg.publish(ev2)
	go disconnectReg.runOutputLoop(context.Background())
	require.NoError(t, disconnectReg.waitForCaughtUp())
	discErr := roachpb.NewError(fmt.Errorf("disconnection error"))
	disconnectReg.disconnect(discErr)
	err := <-disconnectReg.errC
	require.Equal(t, discErr, err)

	// Overflow.
	overflowReg := newTestRegistration(spAB, hlc.Timestamp{}, nil, false)
	for i := 0; i < cap(overflowReg.buf)+3; i++ {
		overflowReg.publish(ev1)
	}
	go overflowReg.runOutputLoop(context.Background())
	err = <-overflowReg.errC
	require.Equal(t, newErrBufferCapacityExceeded(), err)
	require.Equal(t, cap(overflowReg.buf), len(overflowReg.Events()))

	// Stream Error.
	streamErrReg := newTestRegistration(spAB, hlc.Timestamp{}, nil, false)
	streamErr := fmt.Errorf("stream error")
	streamErrReg.stream.SetSendErr(streamErr)
	go streamErrReg.runOutputLoop(context.Background())
	streamErrReg.publish(ev1)
	err = <-streamErrReg.errC
	require.Equal(t, streamErr.Error(), err.GoError().Error())

	// Stream Context Canceled.
	streamCancelReg := newTestRegistration(spAB, hlc.Timestamp{}, nil, false)
	streamCancelReg.stream.Cancel()
	go streamCancelReg.runOutputLoop(context.Background())
	require.NoError(t, streamCancelReg.waitForCaughtUp())
	err = <-streamCancelReg.errC
	require.Equal(t, streamCancelReg.stream.Context().Err().Error(), err.GoError().Error())
}

func TestRegistrationCatchUpScan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Run a catch-up scan for a registration over a test
	// iterator with the following keys.
	txn1, txn2 := uuid.MakeV4(), uuid.MakeV4()
	iter := newTestIterator([]storage.MVCCKeyValue{
		makeKV("a", "valA1", 10),
		makeInline("b", "valB1"),
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
		makeInline("g", "valG1"),
		makeKV("h", "valH1", 15),
		makeKV("m", "valM1", 1),
		makeIntent("n", txn1, "txnKeyN", 12),
		makeProvisionalKV("n", "txnKeyN", 12),
		makeIntent("r", txn1, "txnKeyR", 19),
		makeProvisionalKV("r", "txnKeyR", 19),
		makeKV("r", "valR1", 4),
		makeIntent("w", txn1, "txnKeyW", 3),
		makeProvisionalKV("w", "txnKeyW", 3),
		makeInline("x", "valX1"),
		makeIntent("z", txn2, "txnKeyZ", 21),
		makeProvisionalKV("z", "txnKeyZ", 21),
		makeKV("z", "valZ1", 4),
	})
	r := newTestRegistration(roachpb.Span{
		Key:    roachpb.Key("d"),
		EndKey: roachpb.Key("w"),
	}, hlc.Timestamp{WallTime: 4}, iter, true /* withDiff */)

	require.Zero(t, r.metrics.RangeFeedCatchupScanNanos.Count())
	require.NoError(t, r.maybeRunCatchupScan())
	require.True(t, iter.closed)
	require.NotZero(t, r.metrics.RangeFeedCatchupScanNanos.Count())

	// Compare the events sent on the registration's Stream to the expected events.
	expEvents := []*roachpb.RangeFeedEvent{
		rangeFeedValueWithPrev(
			roachpb.Key("d"),
			roachpb.Value{RawBytes: []byte("valD3"), Timestamp: hlc.Timestamp{WallTime: 16}},
			roachpb.Value{RawBytes: []byte("valD2")},
		),
		rangeFeedValueWithPrev(
			roachpb.Key("d"),
			roachpb.Value{RawBytes: []byte("valD4"), Timestamp: hlc.Timestamp{WallTime: 19}},
			roachpb.Value{RawBytes: []byte("valD3")},
		),
		rangeFeedValueWithPrev(
			roachpb.Key("d"),
			roachpb.Value{RawBytes: []byte("valD5"), Timestamp: hlc.Timestamp{WallTime: 20}},
			roachpb.Value{RawBytes: []byte("valD4")},
		),
		rangeFeedValueWithPrev(
			roachpb.Key("e"),
			roachpb.Value{RawBytes: []byte("valE2"), Timestamp: hlc.Timestamp{WallTime: 5}},
			roachpb.Value{RawBytes: []byte("valE1")},
		),
		rangeFeedValueWithPrev(
			roachpb.Key("e"),
			roachpb.Value{RawBytes: []byte("valE3"), Timestamp: hlc.Timestamp{WallTime: 6}},
			roachpb.Value{RawBytes: []byte("valE2")},
		),
		rangeFeedValue(
			roachpb.Key("f"),
			roachpb.Value{RawBytes: []byte("valF1"), Timestamp: hlc.Timestamp{WallTime: 5}},
		),
		rangeFeedValueWithPrev(
			roachpb.Key("f"),
			roachpb.Value{RawBytes: []byte("valF2"), Timestamp: hlc.Timestamp{WallTime: 6}},
			roachpb.Value{RawBytes: []byte("valF1")},
		),
		rangeFeedValueWithPrev(
			roachpb.Key("f"),
			roachpb.Value{RawBytes: []byte("valF3"), Timestamp: hlc.Timestamp{WallTime: 7}},
			roachpb.Value{RawBytes: []byte("valF2")},
		),
		rangeFeedValue(
			roachpb.Key("g"),
			roachpb.Value{RawBytes: []byte("valG1"), Timestamp: hlc.Timestamp{WallTime: 0}},
		),
		rangeFeedValue(
			roachpb.Key("h"),
			roachpb.Value{RawBytes: []byte("valH1"), Timestamp: hlc.Timestamp{WallTime: 15}},
		),
	}
	require.Equal(t, expEvents, r.Events())
}

func TestRegistryBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	val := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
	ev1, ev2 := new(roachpb.RangeFeedEvent), new(roachpb.RangeFeedEvent)
	ev3, ev4 := new(roachpb.RangeFeedEvent), new(roachpb.RangeFeedEvent)
	ev1.MustSetValue(&roachpb.RangeFeedValue{Key: keyA, Value: val, PrevValue: val})
	ev2.MustSetValue(&roachpb.RangeFeedValue{Key: keyB, Value: val, PrevValue: val})
	ev3.MustSetValue(&roachpb.RangeFeedValue{Key: keyC, Value: val, PrevValue: val})
	ev4.MustSetValue(&roachpb.RangeFeedValue{Key: keyD, Value: val, PrevValue: val})
	err1 := roachpb.NewErrorf("error1")
	noPrev := func(ev *roachpb.RangeFeedEvent) *roachpb.RangeFeedEvent {
		ev = ev.ShallowCopy()
		ev.GetValue().(*roachpb.RangeFeedValue).PrevValue = roachpb.Value{}
		return ev
	}

	reg := makeRegistry()
	require.Equal(t, 0, reg.Len())
	require.NotPanics(t, func() { reg.PublishToOverlapping(spAB, ev1) })
	require.NotPanics(t, func() { reg.Disconnect(spAB) })
	require.NotPanics(t, func() { reg.DisconnectWithErr(spAB, err1) })

	rAB := newTestRegistration(spAB, hlc.Timestamp{}, nil, false /* withDiff */)
	rBC := newTestRegistration(spBC, hlc.Timestamp{}, nil, true /* withDiff */)
	rCD := newTestRegistration(spCD, hlc.Timestamp{}, nil, true /* withDiff */)
	rAC := newTestRegistration(spAC, hlc.Timestamp{}, nil, false /* withDiff */)
	go rAB.runOutputLoop(context.Background())
	go rBC.runOutputLoop(context.Background())
	go rCD.runOutputLoop(context.Background())
	go rAC.runOutputLoop(context.Background())
	defer rAB.disconnect(nil)
	defer rBC.disconnect(nil)
	defer rCD.disconnect(nil)
	defer rAC.disconnect(nil)

	// Register 4 registrations.
	reg.Register(&rAB.registration)
	require.Equal(t, 1, reg.Len())
	reg.Register(&rBC.registration)
	require.Equal(t, 2, reg.Len())
	reg.Register(&rCD.registration)
	require.Equal(t, 3, reg.Len())
	reg.Register(&rAC.registration)
	require.Equal(t, 4, reg.Len())

	// Publish to different spans.
	reg.PublishToOverlapping(spAB, ev1)
	reg.PublishToOverlapping(spBC, ev2)
	reg.PublishToOverlapping(spCD, ev3)
	reg.PublishToOverlapping(spAC, ev4)
	require.NoError(t, reg.waitForCaughtUp(all))
	require.Equal(t, []*roachpb.RangeFeedEvent{noPrev(ev1), noPrev(ev4)}, rAB.Events())
	require.Equal(t, []*roachpb.RangeFeedEvent{ev2, ev4}, rBC.Events())
	require.Equal(t, []*roachpb.RangeFeedEvent{ev3}, rCD.Events())
	require.Equal(t, []*roachpb.RangeFeedEvent{noPrev(ev1), noPrev(ev2), noPrev(ev4)}, rAC.Events())
	require.Nil(t, rAB.Err())
	require.Nil(t, rBC.Err())
	require.Nil(t, rCD.Err())
	require.Nil(t, rAC.Err())

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
	reg.DisconnectWithErr(spCD, err1)
	require.Equal(t, 3, reg.Len())
	require.Equal(t, err1.GoError(), rCD.Err().GoError())

	// Can still publish to rAB.
	reg.PublishToOverlapping(spAB, ev4)
	reg.PublishToOverlapping(spBC, ev3)
	reg.PublishToOverlapping(spCD, ev2)
	reg.PublishToOverlapping(spAC, ev1)
	require.NoError(t, reg.waitForCaughtUp(all))
	require.Equal(t, []*roachpb.RangeFeedEvent{noPrev(ev4), noPrev(ev1)}, rAB.Events())

	// Disconnect from rAB without error.
	reg.Disconnect(spAB)
	require.Nil(t, rAC.Err())
	require.Nil(t, rAB.Err())
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
	reg.Unregister(&rBC.registration)
	require.Equal(t, 0, reg.Len())
}

func TestRegistryPublishAssertsPopulatedInformation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	reg := makeRegistry()

	rNoDiff := newTestRegistration(spAB, hlc.Timestamp{}, nil, false /* withDiff */)
	go rNoDiff.runOutputLoop(context.Background())
	reg.Register(&rNoDiff.registration)

	rWithDiff := newTestRegistration(spCD, hlc.Timestamp{}, nil, true /* withDiff */)
	go rWithDiff.runOutputLoop(context.Background())
	reg.Register(&rWithDiff.registration)

	key := roachpb.Key("a")
	val := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
	noVal := roachpb.Value{Timestamp: hlc.Timestamp{WallTime: 1}}
	ev := new(roachpb.RangeFeedEvent)

	// Both registrations require RangeFeedValue events to have a Key.
	ev.MustSetValue(&roachpb.RangeFeedValue{
		Key:       nil,
		Value:     val,
		PrevValue: val,
	})
	require.Panics(t, func() { reg.PublishToOverlapping(spAB, ev) })
	require.Panics(t, func() { reg.PublishToOverlapping(spCD, ev) })
	require.NoError(t, reg.waitForCaughtUp(all))

	// Both registrations require RangeFeedValue events to have a Value.
	ev.MustSetValue(&roachpb.RangeFeedValue{
		Key:       key,
		Value:     noVal,
		PrevValue: val,
	})
	require.Panics(t, func() { reg.PublishToOverlapping(spAB, ev) })
	require.Panics(t, func() { reg.PublishToOverlapping(spCD, ev) })
	require.NoError(t, reg.waitForCaughtUp(all))

	// Neither registrations require RangeFeedValue events to have a PrevValue.
	// Even when they are requested, the previous value can always be nil.
	ev.MustSetValue(&roachpb.RangeFeedValue{
		Key:       key,
		Value:     val,
		PrevValue: roachpb.Value{},
	})
	require.NotPanics(t, func() { reg.PublishToOverlapping(spAB, ev) })
	require.NotPanics(t, func() { reg.PublishToOverlapping(spCD, ev) })
	require.NoError(t, reg.waitForCaughtUp(all))

	rNoDiff.disconnect(nil)
	rWithDiff.disconnect(nil)
}

func TestRegistryPublishBeneathStartTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	reg := makeRegistry()

	r := newTestRegistration(spAB, hlc.Timestamp{WallTime: 10}, nil, false)
	go r.runOutputLoop(context.Background())
	reg.Register(&r.registration)

	// Publish a value with a timestamp beneath the registration's start
	// timestamp. Should be ignored.
	ev := new(roachpb.RangeFeedEvent)
	ev.MustSetValue(&roachpb.RangeFeedValue{
		Value: roachpb.Value{Timestamp: hlc.Timestamp{WallTime: 5}},
	})
	reg.PublishToOverlapping(spAB, ev)
	require.NoError(t, reg.waitForCaughtUp(all))
	require.Nil(t, r.Events())

	// Publish a value with a timestamp equal to the registration's start
	// timestamp. Should be ignored.
	ev.MustSetValue(&roachpb.RangeFeedValue{
		Value: roachpb.Value{Timestamp: hlc.Timestamp{WallTime: 10}},
	})
	reg.PublishToOverlapping(spAB, ev)
	require.NoError(t, reg.waitForCaughtUp(all))
	require.Nil(t, r.Events())

	// Publish a checkpoint with a timestamp beneath the registration's. Should
	// be delivered.
	ev.MustSetValue(&roachpb.RangeFeedCheckpoint{
		Span: spAB, ResolvedTS: hlc.Timestamp{WallTime: 5},
	})
	reg.PublishToOverlapping(spAB, ev)
	require.NoError(t, reg.waitForCaughtUp(all))
	require.Equal(t, []*roachpb.RangeFeedEvent{ev}, r.Events())

	r.disconnect(nil)
	<-r.errC
}

func TestRegistrationString(t *testing.T) {
	testCases := []struct {
		r   registration
		exp string
	}{
		{
			r: registration{
				span: roachpb.Span{Key: roachpb.Key("a")},
			},
			exp: `[a @ 0,0+]`,
		},
		{
			r: registration{span: roachpb.Span{
				Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
			},
			exp: `[{a-c} @ 0,0+]`,
		},
		{
			r: registration{
				span:             roachpb.Span{Key: roachpb.Key("d")},
				catchupTimestamp: hlc.Timestamp{WallTime: 10, Logical: 1},
			},
			exp: `[d @ 0.000000010,1+]`,
		},
		{
			r: registration{span: roachpb.Span{
				Key: roachpb.Key("d"), EndKey: roachpb.Key("z")},
				catchupTimestamp: hlc.Timestamp{WallTime: 40, Logical: 9},
			},
			exp: `[{d-z} @ 0.000000040,9+]`,
		},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.exp, tc.r.String())
	}
}
