// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestUnbufferedRegOnConcurrentDisconnect tests that BufferedSender can handle
// concurrent stream disconnects.
func TestUnbufferedRegWithStreamManager(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	p, h, stopper := newTestProcessor(t, withRangefeedTestType(scheduledProcessorWithBufferedSender))
	defer stopper.Stop(ctx)
	testServerStream := newTestServerStream()
	testRangefeedCounter := newTestRangefeedCounter()
	bs := NewBufferedSender(testServerStream)
	sm := NewStreamManager(bs, testRangefeedCounter)
	require.NoError(t, sm.Start(ctx, stopper))

	const r1 = 1
	t.Run("no events sent before registering streams", func(t *testing.T) {
		p.ConsumeLogicalOps(ctx, writeValueOp(hlc.Timestamp{WallTime: 1}))
		require.Equal(t, 0, testServerStream.totalEventsSent())
	})
	t.Run("register 50 streams", func(t *testing.T) {
		for id := int64(0); id < 50; id++ {
			registered, d, _ := p.Register(ctx, h.span, hlc.Timestamp{}, nil, /* catchUpIter */
				false /* withDiff */, false /* withFiltering */, false, /* withOmitRemote */
				sm.NewStream(id, r1))
			require.True(t, registered)
			sm.AddStream(id, d)
		}
		require.Equal(t, 50, testRangefeedCounter.get())
		require.Equal(t, 50, p.Len())
	})
	t.Run("publish a 0-valued checkpoint to signal catch-up completion", func(t *testing.T) {
		testServerStream.waitForEventCount(t, 50)
		testServerStream.iterateEventsByStreamID(func(_ int64, events []*kvpb.MuxRangeFeedEvent) {
			require.Equal(t, 1, len(events))
			require.NotNil(t, events[0].Checkpoint)
		})
	})
	testServerStream.reset()
	t.Run("publish 20 logical ops to 50 registrations", func(t *testing.T) {
		for i := 0; i < 20; i++ {
			p.ConsumeLogicalOps(ctx, writeValueOp(hlc.Timestamp{WallTime: 1}))
		}
		testServerStream.waitForEventCount(t, 20*50)
		testServerStream.iterateEventsByStreamID(func(_ int64, events []*kvpb.MuxRangeFeedEvent) {
			require.Equal(t, 20, len(events))
			require.NotNil(t, events[0].RangeFeedEvent.Val)
		})
	})

	t.Run("disconnect 50 streams concurrently", func(t *testing.T) {
		var wg sync.WaitGroup
		for id := int64(0); id < 50; id++ {
			wg.Add(1)
			go func(id int64) {
				defer wg.Done()
				sm.DisconnectStream(id, kvpb.NewError(fmt.Errorf("disconnection error")))
			}(id)
		}
		wg.Wait()
		require.NoError(t, bs.waitForEmptyBuffer(ctx))
		testRangefeedCounter.waitForRangefeedCount(t, 0)
		testutils.SucceedsSoon(t, func() error {
			if p.Len() == 0 {
				return nil
			}
			return errors.Newf("expected 0 registrations, found %d", p.Len())
		})
	})
}

// TestUnbufferedRegOnDisconnect tests that BufferedSender can handle
// disconnects properly and send events in the correct order.
func TestUnbufferedRegCorrectnessOnDisconnect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	p, h, stopper := newTestProcessor(t, withRangefeedTestType(scheduledProcessorWithBufferedSender))
	defer stopper.Stop(ctx)
	testServerStream := newTestServerStream()
	testRangefeedCounter := newTestRangefeedCounter()
	bs := NewBufferedSender(testServerStream)
	sm := NewStreamManager(bs, testRangefeedCounter)
	require.NoError(t, sm.Start(ctx, stopper))
	defer sm.Stop(ctx)

	startTs := hlc.Timestamp{WallTime: 4}
	span := roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("w")}
	catchUpIter := newTestIterator(keyValues, roachpb.Key("w"))
	const r1 = 1
	const s1 = 1

	key := roachpb.Key("d")
	val1 := roachpb.Value{RawBytes: []byte("val1"), Timestamp: hlc.Timestamp{WallTime: 5}}
	val2 := roachpb.Value{RawBytes: []byte("val2"), Timestamp: hlc.Timestamp{WallTime: 6}}
	op1 := writeValueOpWithKV(key, val1.Timestamp, val1.RawBytes)
	op2 := writeValueOpWithKV(key, val2.Timestamp, val2.RawBytes)
	ev1, ev2 := new(kvpb.RangeFeedEvent), new(kvpb.RangeFeedEvent)
	ev1.MustSetValue(&kvpb.RangeFeedValue{Key: key, Value: val1})
	ev2.MustSetValue(&kvpb.RangeFeedValue{Key: key, Value: val2})

	// Add checkpoint event
	checkpointEvent := &kvpb.RangeFeedEvent{}
	checkpointEvent.MustSetValue(&kvpb.RangeFeedCheckpoint{
		Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
	})

	discErr := kvpb.NewError(fmt.Errorf("disconnection error"))
	evErr := &kvpb.RangeFeedEvent{}
	evErr.MustSetValue(&kvpb.RangeFeedError{Error: *discErr})

	// Register one stream.
	registered, d, _ := p.Register(ctx, h.span, startTs,
		makeCatchUpIterator(catchUpIter, span, startTs), /* catchUpIter */
		true /* withDiff */, false /* withFiltering */, false, /* withOmitRemote */
		sm.NewStream(s1, r1))
	sm.AddStream(s1, d)
	require.True(t, registered)
	require.Equal(t, 1, testRangefeedCounter.get())

	// Publish two real live events to the stream.
	p.ConsumeLogicalOps(ctx, op1)
	p.ConsumeLogicalOps(ctx, op2)

	catchUpEvents := expEvents(false)
	// catch up events + checkpoint + ev1 + evv2
	testServerStream.waitForEventCount(t, len(catchUpEvents)+1+2)

	// Disconnection error should be buffered and sent after the events.
	p.DisconnectSpanWithErr(spBC, discErr)
	testServerStream.waitForEventCount(t, len(catchUpEvents)+1+2+1)

	numExpectedEvents := len(catchUpEvents) + 4
	expectedEvents := make([]*kvpb.RangeFeedEvent, numExpectedEvents)
	// Copy catch-up events into expected events
	copy(expectedEvents, catchUpEvents)
	// Add checkpoint event.
	expectedEvents[len(catchUpEvents)] = checkpointEvent
	// Add disconnection error.
	expectedEvents[len(catchUpEvents)+1] = ev1
	expectedEvents[len(catchUpEvents)+2] = ev2
	expectedEvents[len(catchUpEvents)+3] = evErr

	require.Equal(t, testServerStream.getEventsByStreamID(s1), expectedEvents)
	testRangefeedCounter.waitForRangefeedCount(t, 0)
}

// TestCatchUpBufDrain tests that the catchUpBuf is drained after all events are
// sent or after being disconnected.
func TestUnbufferedRegWithCatchUpBufCleanUpAfterRunOutputLoop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	rng, _ := randutil.NewTestRand()
	ev1 := new(kvpb.RangeFeedEvent)
	val := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
	ev1.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val})
	numReg := rng.Intn(1000)
	regs := make([]*unbufferedRegistration, numReg)

	for i := 0; i < numReg; i++ {
		s := newTestStream()
		iter := newTestIterator(keyValues, roachpb.Key("w"))
		catchUpReg := newTestRegistration(s, withRSpan(spAB), withRegistrationType(unbuffered),
			withDiff(false), withCatchUpIter(iter)).(*unbufferedRegistration)
		catchUpReg.publish(ctx, ev1, nil /* alloc */)
		go catchUpReg.runOutputLoop(ctx, 0)
		regs[i] = catchUpReg
	}

	// For each registration, publish events (higher chance) and disconnect
	// randomly.
	for j := 0; j < numReg; j++ {
		if rng.Intn(5) < 4 {
			for i := 0; i < 100; i++ {
				regs[j].publish(ctx, ev1, nil /* alloc */)
			}
		} else {
			regs[j].Disconnect(kvpb.NewError(nil))
		}
	}

	// All registrations should be caught up or disconnected. CatchUpBuf should
	// be emptied.
	for _, reg := range regs {
		require.NoError(t, reg.waitForCaughtUp(ctx))
		require.Nil(t, reg.getBuf())
	}
}

// TestUnbufferedReg tests for unbuffered registrations specifically. Note that
// a lot of tests are already covered in registry_test.go.
// TODO(wenyihu6): figure out better tests for this (add memory accounting
// tests here as well, add specific tests for draining, add a testing hook
// specific to catch up switch over point)
func TestUnbufferedRegOnCatchUpSwitchOver(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	val1 := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
	val2 := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 5}}
	ev1, ev2, ev3, ev4, ev5 := new(kvpb.RangeFeedEvent), new(kvpb.RangeFeedEvent),
		new(kvpb.RangeFeedEvent), new(kvpb.RangeFeedEvent), new(kvpb.RangeFeedEvent)
	ev1.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val1})
	ev2.MustSetValue(&kvpb.RangeFeedValue{Key: keyB, Value: val1})
	ev3.MustSetValue(&kvpb.RangeFeedValue{Key: keyC, Value: val2})
	ev4.MustSetValue(&kvpb.RangeFeedCheckpoint{
		Span: roachpb.Span{
			Key:    roachpb.Key("d"),
			EndKey: roachpb.Key("w")},
		ResolvedTS: hlc.Timestamp{WallTime: 5},
	})
	ev5.MustSetValue(&kvpb.RangeFeedDeleteRange{
		Span: roachpb.Span{
			Key:    roachpb.Key("d"),
			EndKey: roachpb.Key("w")},
		Timestamp: hlc.Timestamp{WallTime: 6},
	})
	t.Run("disconnect before catch up scan starts", func(t *testing.T) {
		s := newTestStream()
		iter := newTestIterator(keyValues, roachpb.Key("w"))
		catchUpReg := newTestRegistration(s, withRSpan(spAB), withRegistrationType(unbuffered),
			withCatchUpIter(iter)).(*unbufferedRegistration)
		catchUpReg.publish(ctx, ev1, nil /* alloc */)
		catchUpReg.Disconnect(kvpb.NewError(nil))
		require.Nil(t, catchUpReg.mu.catchUpIter)
		// Catch up scan should not be initiated.
		go catchUpReg.runOutputLoop(ctx, 0)
		require.NoError(t, catchUpReg.waitForCaughtUp(ctx))
		require.Nil(t, catchUpReg.mu.catchUpIter)
		// No events should be sent since the registration has catch up buffer and
		// is disconnected before catch up scan starts.
		require.Nil(t, s.GetAndClearEvents())
		// Repeatedly disconnect should be fine.
		catchUpReg.Disconnect(kvpb.NewError(nil))
	})
	t.Run("disconnect after maybeRunCatchUpScan and before publishCatchUpBuffer",
		func(t *testing.T) {
			s := newTestStream()
			iter := newTestIterator(keyValues, roachpb.Key("w"))
			catchUpReg := newTestRegistration(s, withRSpan(roachpb.Span{
				Key:    roachpb.Key("d"),
				EndKey: roachpb.Key("w"),
			}), withStartTs(hlc.Timestamp{WallTime: 4}), withCatchUpIter(iter), withDiff(true),
				withRegistrationType(unbuffered)).(*unbufferedRegistration)
			for i := 0; i < 10000; i++ {
				catchUpReg.publish(ctx, ev1, nil /* alloc */)
			}
			// No events should be sent since the registration has catch up buffer.
			require.Nil(t, s.GetAndClearEvents())
			ctx, catchUpReg.mu.catchUpScanCancelFn = context.WithCancel(ctx)
			require.NoError(t, catchUpReg.maybeRunCatchUpScan(ctx))
			// Disconnected before catch up overflowed.
			catchUpReg.Disconnect(kvpb.NewError(nil))
			require.Equal(t, context.Canceled, catchUpReg.publishCatchUpBuffer(ctx))
			require.NotNil(t, catchUpReg.mu.catchUpBuf)
			require.True(t, catchUpReg.mu.catchUpOverflowed)
			catchUpReg.drainAllocations(ctx)
			require.Nil(t, catchUpReg.mu.catchUpBuf)
		})
	t.Run("disconnect during publishCatchUpBuffer",
		func(t *testing.T) {
			s := newTestStream()
			iter := newTestIterator(keyValues, roachpb.Key("w"))
			catchUpReg := newTestRegistration(s, withRSpan(roachpb.Span{
				Key:    roachpb.Key("d"),
				EndKey: roachpb.Key("w"),
			}), withStartTs(hlc.Timestamp{WallTime: 4}), withCatchUpIter(iter), withDiff(true),
				withRegistrationType(unbuffered)).(*unbufferedRegistration)
			for i := 0; i < 100000; i++ {
				catchUpReg.publish(ctx, ev1, nil /* alloc */)
			}
			// No events should be sent since the registration has catch up buffer.
			require.Nil(t, s.GetAndClearEvents())
			ctx, catchUpReg.mu.catchUpScanCancelFn = context.WithCancel(ctx)
			require.NoError(t, catchUpReg.maybeRunCatchUpScan(ctx))
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = catchUpReg.publishCatchUpBuffer(ctx)
			}()
			catchUpReg.Disconnect(kvpb.NewError(nil))
			require.NotNil(t, catchUpReg.mu.catchUpBuf)
			require.True(t, catchUpReg.mu.catchUpOverflowed)
			// Wait for the publishCatchUpBuffer to finish to avoid race conditions.
			// The first publish() during publishCatchUpBuffer does not hold locks.
			wg.Wait()
			catchUpReg.drainAllocations(ctx)
			require.Nil(t, catchUpReg.mu.catchUpBuf)
		})

	t.Run("disconnect after publishCatchUpBuffer",
		func(t *testing.T) {
			s := newTestStream()
			iter := newTestIterator(keyValues, roachpb.Key("w"))
			catchUpReg := newTestRegistration(s, withRSpan(roachpb.Span{
				Key:    roachpb.Key("d"),
				EndKey: roachpb.Key("w"),
			}), withStartTs(hlc.Timestamp{WallTime: 4}), withCatchUpIter(iter), withDiff(true),
				withRegistrationType(unbuffered)).(*unbufferedRegistration)
			for i := 0; i < 100000; i++ {
				catchUpReg.publish(ctx, ev1, nil /* alloc */)
			}
			// No events should be sent since the registration has catch up buffer.
			require.Nil(t, s.GetAndClearEvents())
			ctx, catchUpReg.mu.catchUpScanCancelFn = context.WithCancel(ctx)
			require.NoError(t, catchUpReg.maybeRunCatchUpScan(ctx))
			require.Error(t, newRetryErrBufferCapacityExceeded(), catchUpReg.publishCatchUpBuffer(ctx))
			catchUpReg.Disconnect(kvpb.NewError(newRetryErrBufferCapacityExceeded()))
			require.NotNil(t, catchUpReg.mu.catchUpBuf)
			require.True(t, catchUpReg.mu.catchUpOverflowed)
			catchUpReg.drainAllocations(ctx)
			require.Nil(t, catchUpReg.mu.catchUpBuf)
		})
	t.Run("disconnect after runOutputLoop", func(t *testing.T) {
		// Run a catch-up scan for a registration over a test
		// iterator with the following keys.
		s := newTestStream()
		ctx := context.Background()
		iter := newTestIterator(keyValues, roachpb.Key("w"))
		r := newTestRegistration(s, withRSpan(roachpb.Span{
			Key:    roachpb.Key("d"),
			EndKey: roachpb.Key("w"),
		}), withStartTs(hlc.Timestamp{WallTime: 4}), withCatchUpIter(iter), withDiff(true),
			withRegistrationType(unbuffered)).(*unbufferedRegistration)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.runOutputLoop(ctx, 0)
		}()
		capOfBuf := cap(r.mu.catchUpBuf)
		r.publish(ctx, ev1, &SharedBudgetAllocation{refCount: 1})
		r.publish(ctx, ev2, &SharedBudgetAllocation{refCount: 1})
		r.publish(ctx, ev3, &SharedBudgetAllocation{refCount: 1})
		r.publish(ctx, ev4, &SharedBudgetAllocation{refCount: 1})
		r.publish(ctx, ev5, &SharedBudgetAllocation{refCount: 1})
		catchUpEvents := expEvents(false)
		wg.Wait()

		require.False(t, r.getOverflowed())
		require.Nil(t, r.getBuf())
		s.waitForEventCount(t, capOfBuf+len(catchUpEvents))
		require.Equal(t,
			[]*kvpb.RangeFeedEvent{ev1, ev2, ev3, ev4, ev5}, s.mu.events[len(catchUpEvents):])
	})
}
