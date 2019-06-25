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
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
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
	span roachpb.Span, ts hlc.Timestamp, catchup engine.SimpleIterator,
) *testRegistration {
	s := newTestStream()
	errC := make(chan *roachpb.Error, 1)
	return &testRegistration{
		registration: newRegistration(
			span,
			ts,
			catchup,
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

	val := roachpb.Value{Timestamp: hlc.Timestamp{WallTime: 1}}
	ev1, ev2 := new(roachpb.RangeFeedEvent), new(roachpb.RangeFeedEvent)
	ev1.MustSetValue(&roachpb.RangeFeedValue{Value: val})
	ev2.MustSetValue(&roachpb.RangeFeedValue{Value: val})

	// Registration with no catchup scan specified.
	noCatchupReg := newTestRegistration(spAB, hlc.Timestamp{}, nil)
	noCatchupReg.publish(ev1)
	noCatchupReg.publish(ev2)
	require.Equal(t, len(noCatchupReg.buf), 2)
	go noCatchupReg.runOutputLoop(context.Background())
	require.NoError(t, noCatchupReg.waitForCaughtUp())
	require.Equal(t, []*roachpb.RangeFeedEvent{ev1, ev2}, noCatchupReg.stream.Events())
	noCatchupReg.disconnect(nil)
	<-noCatchupReg.errC

	// Registration with catchup scan.
	catchupReg := newTestRegistration(spBC, hlc.Timestamp{WallTime: 1}, newTestIterator([]engine.MVCCKeyValue{
		makeKV("b", "val1", 10),
		makeInline("ba", "val2"),
		makeKV("bc", "val3", 11),
		makeKV("bd", "val4", 9),
	}))
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
	disconnectReg := newTestRegistration(spAB, hlc.Timestamp{}, nil)
	disconnectReg.publish(ev1)
	disconnectReg.publish(ev2)
	go disconnectReg.runOutputLoop(context.Background())
	require.NoError(t, disconnectReg.waitForCaughtUp())
	discErr := roachpb.NewError(fmt.Errorf("disconnection error"))
	disconnectReg.disconnect(discErr)
	err := <-disconnectReg.errC
	require.Equal(t, discErr, err)

	// Overflow.
	overflowReg := newTestRegistration(spAB, hlc.Timestamp{}, nil)
	for i := 0; i < cap(overflowReg.buf)+3; i++ {
		overflowReg.publish(ev1)
	}
	go overflowReg.runOutputLoop(context.Background())
	err = <-overflowReg.errC
	require.Equal(t, newErrBufferCapacityExceeded(), err)
	require.Equal(t, cap(overflowReg.buf), len(overflowReg.Events()))

	// Stream Error.
	streamErrReg := newTestRegistration(spAB, hlc.Timestamp{}, nil)
	streamErr := fmt.Errorf("stream error")
	streamErrReg.stream.SetSendErr(streamErr)
	go streamErrReg.runOutputLoop(context.Background())
	streamErrReg.publish(ev1)
	err = <-streamErrReg.errC
	require.Equal(t, streamErr.Error(), err.GoError().Error())

	// Stream Context Canceled.
	streamCancelReg := newTestRegistration(spAB, hlc.Timestamp{}, nil)
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
	iter := newTestIterator([]engine.MVCCKeyValue{
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
	r := newTestRegistration(roachpb.Span{
		Key:    roachpb.Key("d"),
		EndKey: roachpb.Key("w"),
	}, hlc.Timestamp{WallTime: 4}, iter)

	require.Zero(t, r.metrics.RangeFeedCatchupScanNanos.Count())
	require.NoError(t, r.runCatchupScan())
	require.True(t, iter.closed)
	require.NotZero(t, r.metrics.RangeFeedCatchupScanNanos.Count())

	// Compare the events sent on the registration's Stream to the expected events.
	expEvents := []*roachpb.RangeFeedEvent{
		rangeFeedValue(
			roachpb.Key("d"),
			roachpb.Value{RawBytes: []byte("val6"), Timestamp: hlc.Timestamp{WallTime: 19}},
		),
		rangeFeedValue(
			roachpb.Key("d"),
			roachpb.Value{RawBytes: []byte("val5"), Timestamp: hlc.Timestamp{WallTime: 20}},
		),
		rangeFeedValue(
			roachpb.Key("g"),
			roachpb.Value{RawBytes: []byte("val7"), Timestamp: hlc.Timestamp{WallTime: 0}},
		),
	}
	require.Equal(t, expEvents, r.Events())
}

func TestRegistryBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	val := roachpb.Value{Timestamp: hlc.Timestamp{WallTime: 1}}
	ev1, ev2 := new(roachpb.RangeFeedEvent), new(roachpb.RangeFeedEvent)
	ev3, ev4 := new(roachpb.RangeFeedEvent), new(roachpb.RangeFeedEvent)
	ev1.MustSetValue(&roachpb.RangeFeedValue{Value: val})
	ev2.MustSetValue(&roachpb.RangeFeedValue{Value: val})
	ev3.MustSetValue(&roachpb.RangeFeedValue{Value: val})
	ev4.MustSetValue(&roachpb.RangeFeedValue{Value: val})
	err1 := roachpb.NewErrorf("error1")

	reg := makeRegistry()
	require.Equal(t, 0, reg.Len())
	require.NotPanics(t, func() { reg.PublishToOverlapping(spAB, ev1) })
	require.NotPanics(t, func() { reg.Disconnect(spAB) })
	require.NotPanics(t, func() { reg.DisconnectWithErr(spAB, err1) })

	rAB := newTestRegistration(spAB, hlc.Timestamp{}, nil)
	rBC := newTestRegistration(spBC, hlc.Timestamp{}, nil)
	rCD := newTestRegistration(spCD, hlc.Timestamp{}, nil)
	rAC := newTestRegistration(spAC, hlc.Timestamp{}, nil)
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
	require.Equal(t, []*roachpb.RangeFeedEvent{ev1, ev4}, rAB.Events())
	require.Equal(t, []*roachpb.RangeFeedEvent{ev2, ev4}, rBC.Events())
	require.Equal(t, []*roachpb.RangeFeedEvent{ev3}, rCD.Events())
	require.Equal(t, []*roachpb.RangeFeedEvent{ev1, ev2, ev4}, rAC.Events())
	require.Nil(t, rAB.Err())
	require.Nil(t, rBC.Err())
	require.Nil(t, rCD.Err())
	require.Nil(t, rAC.Err())

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
	require.Equal(t, []*roachpb.RangeFeedEvent{ev4, ev1}, rAB.Events())

	// Disconnect from rAB without error.
	reg.Disconnect(spAB)
	require.Nil(t, rAC.Err())
	require.Nil(t, rAB.Err())
	require.Equal(t, 1, reg.Len())

	// Register and unregister.
	reg.Unregister(&rBC.registration)
	require.Equal(t, 0, reg.Len())
}

func TestRegistryPublishBeneathStartTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	reg := makeRegistry()

	r := newTestRegistration(spAB, hlc.Timestamp{WallTime: 10}, nil)
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
		ResolvedTS: hlc.Timestamp{WallTime: 5},
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
			exp: `[a @ 0.000000000,0+]`,
		},
		{
			r: registration{span: roachpb.Span{
				Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
			},
			exp: `[{a-c} @ 0.000000000,0+]`,
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
