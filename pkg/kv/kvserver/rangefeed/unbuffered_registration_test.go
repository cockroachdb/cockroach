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
	"reflect"
	"sync"
	"testing"

	_ "github.com/cockroachdb/cockroach/pkg/keys" // hook up pretty printer
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

// TestStreamMuxerWithConcurrentDisconnect tests that StreamMuxer can handle
// concurrent stream disconnects.
func TestStreamMuxerWithConcurrentDisconnect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	p, h, stopper := newTestProcessor(t, withRangefeedTestType(scheduledProcessorWithUnbufferedReg))
	defer stopper.Stop(ctx)
	testServerStream := newTestServerStream()
	testRangefeedCounter := newTestRangefeedCounter()
	muxer := NewStreamMuxerWithOpts(testServerStream, testRangefeedCounter, unbuffered)
	require.NoError(t, muxer.Start(ctx, stopper))
	defer func() {
		muxer.Stop()
		require.Equal(t, 0, p.Len())
	}()

	p.ConsumeLogicalOps(ctx, writeValueOp(hlc.Timestamp{WallTime: 1}))
	require.Equal(t, 0, testServerStream.totalEventsSent())

	const r1 = 1
	var wg sync.WaitGroup
	for id := int64(0); id < 50; id++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			ctx, done := context.WithCancel(context.Background())
			muxer.AddStream(id, r1, done)
			p.Register(h.span, hlc.Timestamp{}, nil, /* catchUpIter */
				false /* withDiff */, false /* withFiltering */, false, /* withOmitRemote */
				NewTestPerRangeEventSink(ctx, id, r1, muxer, unbuffered), func() {})
		}(id)
	}
	wg.Wait()
	require.Equal(t, int32(50), testRangefeedCounter.get())
	require.Equal(t, 50, p.Len())

	check := func(f func(e *kvpb.MuxRangeFeedEvent) bool, expected int, expectedEachStreamEventCount int, expectedTotal int) error {
		if actual := testServerStream.totalEventsFilterBy(f); actual != expected {
			return errors.Newf("expected %d events filtered, but got %v", expected, actual)
		}
		if actualTotal := testServerStream.totalEventsSent(); actualTotal != expectedTotal {
			return errors.Newf("expected %d events sent, but got %v", expectedTotal, actualTotal)
		}
		var err error
		testServerStream.iterateEvents(func(events []*kvpb.MuxRangeFeedEvent) bool {
			if len(events) != expectedEachStreamEventCount {
				err = errors.Newf("expected %d events sent by every stream, but got %v for a stream", expectedEachStreamEventCount, len(events))
				return false
			}
			return true
		})
		return err
	}

	// Make sure events consumed before p.Register are not sent.
	testutils.SucceedsSoon(t, func() error {
		f := func(e *kvpb.MuxRangeFeedEvent) bool {
			return e.Checkpoint != nil
		}
		return check(f, 50, 1, 50)
	})

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.ConsumeLogicalOps(ctx, writeValueOp(hlc.Timestamp{WallTime: 1}))
		}()
	}
	testutils.SucceedsSoon(t, func() error {
		f := func(e *kvpb.MuxRangeFeedEvent) bool {
			return e.Val != nil
		}
		return check(f, 20*50, 21, 21*50)
	})

	for id := int64(0); id < 50; id++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			muxer.DisconnectStreamWithError(id, r1, kvpb.NewError(nil))
		}(id)
	}
	wg.Wait()
	require.Equal(t, int32(0), testRangefeedCounter.get())
}

// TODO(wenyihu6): add memory accounting tests here as well
// TestCatchUpBufDrain tests that the catchUpBuf is drained after all events are
// sent.
func TestCatchUpBufDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
		catchUpReg := newTestRegistration(s, withRSpan(spAB), withRegistrationType(unbuffered), withDiff(false),
			withCatchUpIter(iter)).(*unbufferedRegistration)
		catchUpReg.publish(ctx, ev1, nil /* alloc */)
		go catchUpReg.runOutputLoop(ctx, 0)
		regs[i] = catchUpReg
	}

	// For each registration, publish events (higher chance) and disconnect
	// randomly.
	for j := 0; j < numReg; j++ {
		if rng.Intn(5) != 4 {
			for i := 0; i < 100; i++ {
				regs[j].publish(ctx, ev1, nil /* alloc */)
			}
		} else {
			regs[j].disconnect(kvpb.NewError(nil))
		}
	}

	// Wait for all registrations to catch up and drain their catchUpBuf.
	for _, reg := range regs {
		testutils.SucceedsSoon(t, func() error {
			if reg.waitForCaughtUp(ctx) != nil {
				return errors.Newf("not caught up")
			}
			reg.mu.Lock()
			defer reg.mu.Unlock()
			if reg.mu.catchUpBuf != nil {
				return errors.Newf("catchUpBuf not drained")
			}
			return nil
		})
	}
}

// A lot of tests are already covered in registry_test.go. This test is for
// unbuffered registrations specifically.
func TestUnbufferedRegistration(t *testing.T) {
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
		catchUpReg.disconnect(kvpb.NewError(nil))
		require.Nil(t, catchUpReg.mu.catchUpIter)
		// Catch up scan should not be initiated.
		go catchUpReg.runOutputLoop(ctx, 0)
		require.NoError(t, catchUpReg.waitForCaughtUp(ctx))
		require.Nil(t, catchUpReg.mu.catchUpIter)
		// No events should be sent since the registration has catch up buffer and
		// is disconnected before catch up scan starts.
		require.Nil(t, s.Events())
		// Repeatedly disconnect should be fine.
		catchUpReg.disconnect(kvpb.NewError(nil))
	})
	t.Run("disconnect before publishCatchUpBuffer", func(t *testing.T) {
		s := newTestStream()
		iter := newTestIterator(keyValues, roachpb.Key("w"))
		catchUpReg := newTestRegistration(s, withRSpan(spAB), withRegistrationType(unbuffered),
			withCatchUpIter(iter)).(*unbufferedRegistration)
		for i := 0; i < 10000; i++ {
			catchUpReg.publish(ctx, ev1, nil /* alloc */)
		}
		// No events should be sent since the registration has catch up buffer.
		require.Nil(t, s.Events())
		require.NoError(t, catchUpReg.maybeRunCatchUpScan(context.Background()))
		// Disconnected before catch up overflowed.
		catchUpReg.disconnect(kvpb.NewError(nil))
		require.Equal(t, context.Canceled,
			catchUpReg.publishCatchUpBuffer(context.Background()))
		require.NotNil(t, catchUpReg.mu.catchUpBuf)
		require.True(t, catchUpReg.mu.catchUpOverflowed)
		catchUpReg.discardCatchUpBuffer()
		require.Nil(t, catchUpReg.mu.catchUpBuf)
	})
	t.Run("catch up scan + publish updates correctness testing", func(t *testing.T) {
		// Run a catch-up scan for a registration over a test
		// iterator with the following keys.
		s := newTestStream()
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
		r.publish(ctx, ev1, nil /* alloc */)
		r.publish(ctx, ev2, nil /* alloc */)
		r.publish(ctx, ev3, nil /* alloc */)
		r.publish(ctx, ev4, nil /* alloc */)
		r.publish(ctx, ev5, nil /* alloc */)
		catchUpEvents := expEvents(false)
		testutils.SucceedsSoon(t, func() error {
			s.mu.Lock()
			defer s.mu.Unlock()
			if len(s.mu.events) < len(catchUpEvents) || !reflect.DeepEqual(catchUpEvents, s.mu.events[:len(catchUpEvents)]) {
				return errors.Newf("expected %v in %v", catchUpEvents, s.mu.events)
			}
			return nil
		})
		wg.Wait()

		func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			require.False(t, r.mu.catchUpOverflowed)
			require.Nil(t, r.mu.catchUpBuf)
		}()

		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			require.Equal(t, capOfBuf+len(catchUpEvents), len(s.mu.events))
			require.Equal(t, []*kvpb.RangeFeedEvent{ev1, ev2, ev3, ev4, ev5}, s.mu.events[len(catchUpEvents):])
		}()
	})
}
