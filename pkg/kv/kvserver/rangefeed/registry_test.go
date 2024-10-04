// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"fmt"
	"sync"
	"testing"

	_ "github.com/cockroachdb/cockroach/pkg/keys" // hook up pretty printer
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/future"
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
		events  []*kvpb.RangeFeedEvent
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

func (s *testStream) Send(e *kvpb.RangeFeedEvent) error {
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

func (s *testStream) Events() []*kvpb.RangeFeedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	es := s.mu.events
	s.mu.events = nil
	return es
}

func (s *testStream) BlockSend() func() {
	s.mu.Lock()
	var once sync.Once
	return func() {
		once.Do(s.mu.Unlock) // safe to call multiple times, e.g. defer and explicit
	}
}

type testRegistration struct {
	registration
	stream *testStream
}

func makeCatchUpIterator(
	iter storage.SimpleMVCCIterator, span roachpb.Span, startTime hlc.Timestamp,
) *CatchUpIterator {
	if iter == nil {
		return nil
	}
	return &CatchUpIterator{
		simpleCatchupIter: simpleCatchupIterAdapter{iter},
		span:              span,
		startTime:         startTime,
	}
}

func newTestRegistration(
	span roachpb.Span,
	ts hlc.Timestamp,
	catchup storage.SimpleMVCCIterator,
	withDiff bool,
	withFiltering bool,
) *testRegistration {
	s := newTestStream()
	r := newRegistration(
		span,
		ts,
		makeCatchUpIterator(catchup, span, ts),
		withDiff,
		withFiltering,
		5,
		false, /* blockWhenFull */
		NewMetrics(),
		s,
		func() {},
		&future.ErrorFuture{},
	)
	return &testRegistration{
		registration: r,
		stream:       s,
	}
}

func (r *testRegistration) Events() []*kvpb.RangeFeedEvent {
	return r.stream.Events()
}

func (r *testRegistration) Err() error {
	err, _ := future.Wait(context.Background(), r.done)
	return err
}

func (r *testRegistration) TryErr() error {
	return future.MakeAwaitableFuture(r.done).Get()
}

func TestRegistrationBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	val := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
	ev1, ev2 := new(kvpb.RangeFeedEvent), new(kvpb.RangeFeedEvent)
	ev1.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val})
	ev2.MustSetValue(&kvpb.RangeFeedValue{Key: keyB, Value: val})

	// Registration with no catchup scan specified.
	noCatchupReg := newTestRegistration(spAB, hlc.Timestamp{}, nil, /* catchup */
		false /* withDiff */, false /* withFiltering */)
	noCatchupReg.publish(ctx, ev1, nil /* alloc */)
	noCatchupReg.publish(ctx, ev2, nil /* alloc */)
	require.Equal(t, len(noCatchupReg.buf), 2)
	go noCatchupReg.runOutputLoop(context.Background(), 0)
	require.NoError(t, noCatchupReg.waitForCaughtUp())
	require.Equal(t, []*kvpb.RangeFeedEvent{ev1, ev2}, noCatchupReg.stream.Events())
	noCatchupReg.disconnect(nil)

	// Registration with catchup scan.
	catchupReg := newTestRegistration(spBC, hlc.Timestamp{WallTime: 1},
		newTestIterator([]storage.MVCCKeyValue{
			makeKV("b", "val1", 10),
			makeKV("bc", "val3", 11),
			makeKV("bd", "val4", 9),
		}, nil),
		false /* withDiff */, false /* withFiltering */)
	catchupReg.publish(ctx, ev1, nil /* alloc */)
	catchupReg.publish(ctx, ev2, nil /* alloc */)
	require.Equal(t, len(catchupReg.buf), 2)
	go catchupReg.runOutputLoop(context.Background(), 0)
	require.NoError(t, catchupReg.waitForCaughtUp())
	events := catchupReg.stream.Events()
	require.Equal(t, 5, len(events))
	require.Equal(t, []*kvpb.RangeFeedEvent{ev1, ev2}, events[3:])
	catchupReg.disconnect(nil)

	// EXIT CONDITIONS
	// External Disconnect.
	disconnectReg := newTestRegistration(spAB, hlc.Timestamp{}, nil, /* catchup */
		false /* withDiff */, false /* withFiltering */)
	disconnectReg.publish(ctx, ev1, nil /* alloc */)
	disconnectReg.publish(ctx, ev2, nil /* alloc */)
	go disconnectReg.runOutputLoop(context.Background(), 0)
	require.NoError(t, disconnectReg.waitForCaughtUp())
	discErr := kvpb.NewError(fmt.Errorf("disconnection error"))
	disconnectReg.disconnect(discErr)
	require.Equal(t, discErr.GoError(), disconnectReg.Err())
	require.Equal(t, 2, len(disconnectReg.stream.Events()))

	// External Disconnect before output loop.
	disconnectEarlyReg := newTestRegistration(spAB, hlc.Timestamp{}, nil, /* catchup */
		false /* withDiff */, false /* withFiltering */)
	disconnectEarlyReg.publish(ctx, ev1, nil /* alloc */)
	disconnectEarlyReg.publish(ctx, ev2, nil /* alloc */)
	disconnectEarlyReg.disconnect(discErr)
	go disconnectEarlyReg.runOutputLoop(context.Background(), 0)
	require.Equal(t, discErr.GoError(), disconnectEarlyReg.Err())
	require.Equal(t, 0, len(disconnectEarlyReg.stream.Events()))

	// Overflow.
	overflowReg := newTestRegistration(spAB, hlc.Timestamp{}, nil, /* catchup */
		false /* withDiff */, false /* withFiltering */)
	for i := 0; i < cap(overflowReg.buf)+3; i++ {
		overflowReg.publish(ctx, ev1, nil /* alloc */)
	}
	go overflowReg.runOutputLoop(context.Background(), 0)
	require.Equal(t, newErrBufferCapacityExceeded().GoError(), overflowReg.Err())
	require.Equal(t, cap(overflowReg.buf), len(overflowReg.Events()))

	// Stream Error.
	streamErrReg := newTestRegistration(spAB, hlc.Timestamp{}, nil, /* catchup */
		false /* withDiff */, false /* withFiltering */)
	streamErr := fmt.Errorf("stream error")
	streamErrReg.stream.SetSendErr(streamErr)
	go streamErrReg.runOutputLoop(context.Background(), 0)
	streamErrReg.publish(ctx, ev1, nil /* alloc */)
	require.Equal(t, streamErr.Error(), streamErrReg.Err().Error())

	// Stream Context Canceled.
	streamCancelReg := newTestRegistration(spAB, hlc.Timestamp{}, nil, /* catchup */
		false /* withDiff */, false /* withFiltering */)
	streamCancelReg.stream.Cancel()
	go streamCancelReg.runOutputLoop(context.Background(), 0)
	require.NoError(t, streamCancelReg.waitForCaughtUp())
	require.Equal(t, streamCancelReg.stream.Context().Err(), streamCancelReg.Err())
}

func TestRegistrationCatchUpScan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "withFiltering", func(t *testing.T, withFiltering bool) {
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

		r := newTestRegistration(roachpb.Span{
			Key:    roachpb.Key("d"),
			EndKey: roachpb.Key("w"),
		}, hlc.Timestamp{WallTime: 4}, iter, true /* withDiff */, withFiltering)

		require.Zero(t, r.metrics.RangeFeedCatchUpScanNanos.Count())
		require.NoError(t, r.maybeRunCatchUpScan(context.Background()))
		require.True(t, iter.closed)
		require.NotZero(t, r.metrics.RangeFeedCatchUpScanNanos.Count())

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
		if !withFiltering {
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
		require.Equal(t, expEvents, r.Events())
	})
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
	require.NotPanics(t, func() { reg.PublishToOverlapping(ctx, spAB, ev1, false /* omitInRangefeeds */, nil /* alloc */) })
	require.NotPanics(t, func() { reg.Disconnect(spAB) })
	require.NotPanics(t, func() { reg.DisconnectWithErr(spAB, err1) })

	rAB := newTestRegistration(spAB, hlc.Timestamp{}, nil, false /* withDiff */, false /* withFiltering */)
	rBC := newTestRegistration(spBC, hlc.Timestamp{}, nil, true /* withDiff */, false /* withFiltering */)
	rCD := newTestRegistration(spCD, hlc.Timestamp{}, nil, true /* withDiff */, false /* withFiltering */)
	rAC := newTestRegistration(spAC, hlc.Timestamp{}, nil, false /* withDiff */, false /* withFiltering */)
	rACFiltering := newTestRegistration(spAC, hlc.Timestamp{}, nil, false /* withDiff */, true /* withFiltering */)
	go rAB.runOutputLoop(context.Background(), 0)
	go rBC.runOutputLoop(context.Background(), 0)
	go rCD.runOutputLoop(context.Background(), 0)
	go rAC.runOutputLoop(context.Background(), 0)
	go rACFiltering.runOutputLoop(context.Background(), 0)
	defer rAB.disconnect(nil)
	defer rBC.disconnect(nil)
	defer rCD.disconnect(nil)
	defer rAC.disconnect(nil)
	defer rACFiltering.disconnect(nil)

	// Register 4 registrations.
	reg.Register(&rAB.registration)
	require.Equal(t, 1, reg.Len())
	reg.Register(&rBC.registration)
	require.Equal(t, 2, reg.Len())
	reg.Register(&rCD.registration)
	require.Equal(t, 3, reg.Len())
	reg.Register(&rAC.registration)
	require.Equal(t, 4, reg.Len())
	reg.Register(&rACFiltering.registration)
	require.Equal(t, 5, reg.Len())

	// Publish to different spans.
	reg.PublishToOverlapping(ctx, spAB, ev1, false /* omitInRangefeeds */, nil /* alloc */)
	reg.PublishToOverlapping(ctx, spBC, ev2, false /* omitInRangefeeds */, nil /* alloc */)
	reg.PublishToOverlapping(ctx, spCD, ev3, false /* omitInRangefeeds */, nil /* alloc */)
	reg.PublishToOverlapping(ctx, spAC, ev4, false /* omitInRangefeeds */, nil /* alloc */)
	reg.PublishToOverlapping(ctx, spAC, ev5, true /* omitInRangefeeds */, nil /* alloc */)
	require.NoError(t, reg.waitForCaughtUp(all))
	require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev1), noPrev(ev4), noPrev(ev5)}, rAB.Events())
	require.Equal(t, []*kvpb.RangeFeedEvent{ev2, ev4, ev5}, rBC.Events())
	require.Equal(t, []*kvpb.RangeFeedEvent{ev3}, rCD.Events())
	require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev1), noPrev(ev2), noPrev(ev4), noPrev(ev5)}, rAC.Events())
	// Registration rACFiltering doesn't receive ev5 because both withFiltering
	// (for the registration) and OmitInRangefeeds (for the event) are true.
	require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev1), noPrev(ev2), noPrev(ev4)}, rACFiltering.Events())
	require.Nil(t, rAB.TryErr())
	require.Nil(t, rBC.TryErr())
	require.Nil(t, rCD.TryErr())
	require.Nil(t, rAC.TryErr())
	require.Nil(t, rACFiltering.TryErr())

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
	require.Equal(t, 4, reg.Len())
	require.Equal(t, err1.GoError(), rCD.Err())

	// Can still publish to rAB.
	reg.PublishToOverlapping(ctx, spAB, ev4, false /* omitInRangefeeds */, nil /* alloc */)
	reg.PublishToOverlapping(ctx, spBC, ev3, false /* omitInRangefeeds */, nil /* alloc */)
	reg.PublishToOverlapping(ctx, spCD, ev2, false /* omitInRangefeeds */, nil /* alloc */)
	reg.PublishToOverlapping(ctx, spAC, ev1, false /* omitInRangefeeds */, nil /* alloc */)
	require.NoError(t, reg.waitForCaughtUp(all))
	require.Equal(t, []*kvpb.RangeFeedEvent{noPrev(ev4), noPrev(ev1)}, rAB.Events())

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
	reg.Unregister(ctx, &rBC.registration)
	require.Equal(t, 0, reg.Len())
}

func TestRegistryPublishAssertsPopulatedInformation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	reg := makeRegistry(NewMetrics())

	rNoDiff := newTestRegistration(spAB, hlc.Timestamp{}, nil, false /* withDiff */, false /* withFiltering */)
	go rNoDiff.runOutputLoop(context.Background(), 0)
	reg.Register(&rNoDiff.registration)

	rWithDiff := newTestRegistration(spCD, hlc.Timestamp{}, nil, true /* withDiff */, false /* withFiltering */)
	go rWithDiff.runOutputLoop(context.Background(), 0)
	reg.Register(&rWithDiff.registration)

	key := roachpb.Key("a")
	val := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
	noVal := roachpb.Value{Timestamp: hlc.Timestamp{WallTime: 1}}
	ev := new(kvpb.RangeFeedEvent)

	// Both registrations require RangeFeedValue events to have a Key.
	ev.MustSetValue(&kvpb.RangeFeedValue{
		Key:       nil,
		Value:     val,
		PrevValue: val,
	})
	require.Panics(t, func() { reg.PublishToOverlapping(ctx, spAB, ev, false /* omitInRangefeeds */, nil /* alloc */) })
	require.Panics(t, func() { reg.PublishToOverlapping(ctx, spCD, ev, false /* omitInRangefeeds */, nil /* alloc */) })
	require.NoError(t, reg.waitForCaughtUp(all))

	// Both registrations require RangeFeedValue events to have a Value.
	ev.MustSetValue(&kvpb.RangeFeedValue{
		Key:       key,
		Value:     noVal,
		PrevValue: val,
	})
	require.Panics(t, func() { reg.PublishToOverlapping(ctx, spAB, ev, false /* omitInRangefeeds */, nil /* alloc */) })
	require.Panics(t, func() { reg.PublishToOverlapping(ctx, spCD, ev, false /* omitInRangefeeds */, nil /* alloc */) })
	require.NoError(t, reg.waitForCaughtUp(all))

	// Neither registrations require RangeFeedValue events to have a PrevValue.
	// Even when they are requested, the previous value can always be nil.
	ev.MustSetValue(&kvpb.RangeFeedValue{
		Key:       key,
		Value:     val,
		PrevValue: roachpb.Value{},
	})
	require.NotPanics(t, func() { reg.PublishToOverlapping(ctx, spAB, ev, false /* omitInRangefeeds */, nil /* alloc */) })
	require.NotPanics(t, func() { reg.PublishToOverlapping(ctx, spCD, ev, false /* omitInRangefeeds */, nil /* alloc */) })
	require.NoError(t, reg.waitForCaughtUp(all))

	rNoDiff.disconnect(nil)
	rWithDiff.disconnect(nil)
}

func TestRegistryPublishBeneathStartTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	reg := makeRegistry(NewMetrics())

	r := newTestRegistration(spAB, hlc.Timestamp{WallTime: 10}, nil, /* catchup */
		false /* withDiff */, false /* withFiltering */)
	go r.runOutputLoop(context.Background(), 0)
	reg.Register(&r.registration)

	// Publish a value with a timestamp beneath the registration's start
	// timestamp. Should be ignored.
	ev := new(kvpb.RangeFeedEvent)
	ev.MustSetValue(&kvpb.RangeFeedValue{
		Value: roachpb.Value{Timestamp: hlc.Timestamp{WallTime: 5}},
	})
	reg.PublishToOverlapping(ctx, spAB, ev, false /* omitInRangefeeds */, nil /* alloc */)
	require.NoError(t, reg.waitForCaughtUp(all))
	require.Nil(t, r.Events())

	// Publish a value with a timestamp equal to the registration's start
	// timestamp. Should be ignored.
	ev.MustSetValue(&kvpb.RangeFeedValue{
		Value: roachpb.Value{Timestamp: hlc.Timestamp{WallTime: 10}},
	})
	reg.PublishToOverlapping(ctx, spAB, ev, false /* omitInRangefeeds */, nil /* alloc */)
	require.NoError(t, reg.waitForCaughtUp(all))
	require.Nil(t, r.Events())

	// Publish a checkpoint with a timestamp beneath the registration's. Should
	// be delivered.
	ev.MustSetValue(&kvpb.RangeFeedCheckpoint{
		Span: spAB, ResolvedTS: hlc.Timestamp{WallTime: 5},
	})
	reg.PublishToOverlapping(ctx, spAB, ev, false /* omitInRangefeeds */, nil /* alloc */)
	require.NoError(t, reg.waitForCaughtUp(all))
	require.Equal(t, []*kvpb.RangeFeedEvent{ev}, r.Events())

	r.disconnect(nil)
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
				catchUpTimestamp: hlc.Timestamp{WallTime: 10, Logical: 1},
			},
			exp: `[d @ 0.000000010,1+]`,
		},
		{
			r: registration{span: roachpb.Span{
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
	reg := makeRegistry(NewMetrics())

	regDoneC := make(chan interface{})
	r := newTestRegistration(spAB, hlc.Timestamp{WallTime: 10}, nil, /*catchup */
		false /* withDiff */, false /* withFiltering */)
	go func() {
		r.runOutputLoop(context.Background(), 0)
		close(regDoneC)
	}()
	reg.Register(&r.registration)

	reg.DisconnectAllOnShutdown(nil)
	<-regDoneC
	require.Zero(t, reg.metrics.RangeFeedRegistrations.Value(), "metric is not zero on stop")
}
