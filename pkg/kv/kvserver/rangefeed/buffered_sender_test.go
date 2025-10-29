// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestBufferedSenderWithSendBufferedError tests that BufferedSender can handle stream
// disconnects properly including context canceled, metrics updates, rangefeed
// cleanup.
func TestBufferedSenderDisconnectStream(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	testServerStream := newTestServerStream()
	smMetrics := NewStreamManagerMetrics()
	st := cluster.MakeTestingClusterSettings()
	bs := NewBufferedSender(testServerStream, st, NewBufferedSenderMetrics())
	sm := NewStreamManager(bs, smMetrics)
	require.NoError(t, sm.Start(ctx, stopper))
	defer sm.Stop(ctx)

	const streamID = 0
	err := kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_NO_LEASEHOLDER))
	errEvent := makeMuxRangefeedErrorEvent(int64(streamID), 1, err)

	t.Run("basic operation", func(t *testing.T) {
		var num atomic.Int32
		sm.RegisteringStream(streamID)
		sm.AddStream(int64(streamID), &cancelCtxDisconnector{
			cancel: func() {
				num.Add(1)
				require.NoError(t, sm.sender.sendBuffered(errEvent, nil))
			},
		})
		require.Equal(t, int64(1), smMetrics.ActiveMuxRangeFeed.Value())
		require.Equal(t, 0, bs.len())
		sm.DisconnectStream(int64(streamID), err)
		testServerStream.waitForEvent(t, errEvent)
		require.Equal(t, int32(1), num.Load())
		require.Equal(t, 1, testServerStream.totalEventsSent())
		waitForRangefeedCount(t, smMetrics, 0)
		testServerStream.reset()
	})
	t.Run("disconnect stream on the same stream is idempotent", func(t *testing.T) {
		sm.RegisteringStream(streamID)
		sm.AddStream(int64(streamID), &cancelCtxDisconnector{
			cancel: func() {
				require.NoError(t, sm.sender.sendBuffered(errEvent, nil))
			},
		})
		require.Equal(t, int64(1), smMetrics.ActiveMuxRangeFeed.Value())
		sm.DisconnectStream(int64(streamID), err)
		require.NoError(t, bs.waitForEmptyBuffer(ctx))
		sm.DisconnectStream(int64(streamID), err)
		require.NoError(t, bs.waitForEmptyBuffer(ctx))
		require.Equalf(t, 1, testServerStream.totalEventsSent(),
			"expected only 1 error event in %s", testServerStream.String())
		waitForRangefeedCount(t, smMetrics, 0)
	})
}

func TestBufferedSenderChaosWithStop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	testServerStream := newTestServerStream()

	smMetrics := NewStreamManagerMetrics()
	st := cluster.MakeTestingClusterSettings()
	bs := NewBufferedSender(testServerStream, st, NewBufferedSenderMetrics())
	sm := NewStreamManager(bs, smMetrics)
	require.NoError(t, sm.Start(ctx, stopper))

	rng, _ := randutil.NewTestRand()

	// [activeStreamStart,activeStreamEnd) are in the active streams.
	// activeStreamStart <= activeStreamEnd. If activeStreamStart ==
	// activeStreamEnd, no streams are active yet. [0, activeStreamStart) are
	// disconnected.
	var actualSum atomic.Int32
	activeStreamStart := int64(0)
	activeStreamEnd := int64(0)

	t.Run("mixed operations of add and disconnect stream", func(t *testing.T) {
		const ops = 1000
		var wg sync.WaitGroup
		for i := 0; i < ops; i++ {
			addStream := rng.Intn(2) == 0
			require.LessOrEqualf(t, activeStreamStart, activeStreamEnd, "test programming error")
			if addStream || activeStreamStart == activeStreamEnd {
				streamID := activeStreamEnd
				sm.RegisteringStream(streamID)
				sm.AddStream(streamID, &cancelCtxDisconnector{
					cancel: func() {
						actualSum.Add(1)
						_ = sm.sender.sendBuffered(
							makeMuxRangefeedErrorEvent(streamID, 1, newErrBufferCapacityExceeded()), nil)
					},
				})
				activeStreamEnd++
			} else {
				wg.Add(1)
				go func(id int64) {
					defer wg.Done()
					sm.DisconnectStream(id, newErrBufferCapacityExceeded())
				}(activeStreamStart)
				activeStreamStart++
			}
		}

		wg.Wait()
		require.Equal(t, int32(activeStreamStart), actualSum.Load())

		require.NoError(t, bs.waitForEmptyBuffer(ctx))
		// We stop the sender as a way to syncronize the send
		// loop. While we've waiting for the buffer to be
		// empty, we also need to know that the sender is done
		// handling the last message that it processed before
		// we observe any of the counters on the test stream.
		stopper.Stop(ctx)
		require.Equal(t, activeStreamStart, int64(testServerStream.totalEventsSent()))
		expectedActiveStreams := activeStreamEnd - activeStreamStart
		require.Equal(t, int(expectedActiveStreams), sm.activeStreamCount())
		waitForRangefeedCount(t, smMetrics, int(expectedActiveStreams))
	})

	t.Run("stream manager on stop", func(t *testing.T) {
		sm.Stop(ctx)
		require.Equal(t, int64(0), smMetrics.ActiveMuxRangeFeed.Value())
		require.Equal(t, 0, sm.activeStreamCount())
		// Cleanup functions should be called for all active streams.
		require.Equal(t, int32(activeStreamEnd), actualSum.Load())
		// No error events should be sent during Stop().
		require.Equal(t, activeStreamStart, int64(testServerStream.totalEventsSent()))

	})

	t.Run("stream manager after stop", func(t *testing.T) {
		// No events should be buffered after stopped.
		val1 := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
		ev1 := new(kvpb.RangeFeedEvent)
		ev1.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val1})
		muxEv := &kvpb.MuxRangeFeedEvent{RangeFeedEvent: *ev1, RangeID: 0, StreamID: 1}
		require.Equal(t, bs.sendBuffered(muxEv, nil).Error(), errors.New("stream sender is stopped").Error())
		require.Equal(t, 0, bs.len())
	})
}

// TestBufferedSenderOnOverflow tests that BufferedSender handles overflow
// properly. It does not test the shutdown flow with stream manager.
func TestBufferedSenderOnOverflow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	testServerStream := newTestServerStream()
	st := cluster.MakeTestingClusterSettings()

	queueCap := int64(24)
	streamID := int64(1)

	RangefeedSingleBufferedSenderQueueMaxPerReg.Override(ctx, &st.SV, queueCap)
	bs := NewBufferedSender(testServerStream, st, NewBufferedSenderMetrics())
	smMetrics := NewStreamManagerMetrics()
	sm := NewStreamManager(bs, smMetrics)
	sm.RegisteringStream(streamID)

	require.Equal(t, queueCap, bs.queueMu.perStreamCapacity)

	val1 := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
	ev1 := new(kvpb.RangeFeedEvent)
	ev1.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val1})
	muxEv := &kvpb.MuxRangeFeedEvent{RangeFeedEvent: *ev1, RangeID: 0, StreamID: streamID}

	for range queueCap {
		require.NoError(t, bs.sendBuffered(muxEv, nil))
	}
	require.Equal(t, queueCap, int64(bs.len()))
	e, success := bs.popFront()
	require.Equal(t, sharedMuxEvent{
		ev:    muxEv,
		alloc: nil,
	}, e)
	require.True(t, success)
	require.Equal(t, queueCap-1, int64(bs.len()))
	require.NoError(t, bs.sendBuffered(muxEv, nil))
	require.Equal(t, queueCap, int64(bs.len()))

	// Overflow now.
	t.Logf(bs.TestingBufferSummary())
	err := bs.sendBuffered(muxEv, nil)
	require.EqualError(t, err, newRetryErrBufferCapacityExceeded().Error())
	t.Logf(bs.TestingBufferSummary())

	// Start the stream manager now, which will start to drain the overflowed stream.
	require.NoError(t, sm.Start(ctx, stopper))
	defer sm.Stop(ctx)

	testServerStream.waitForEventCount(t, int(queueCap))
	testServerStream.reset()

	// The stream should now be in state overflowing. Any non-error events are
	// silently dropped, but the next error event is delivered to the client.
	ev2 := kvpb.RangeFeedEvent{Error: &kvpb.RangeFeedError{Error: *newErrBufferCapacityExceeded()}}
	muxErrEvent := &kvpb.MuxRangeFeedEvent{RangeFeedEvent: ev2, RangeID: 0, StreamID: streamID}

	t.Logf(bs.TestingBufferSummary())
	require.NoError(t, bs.sendBuffered(muxEv, nil))
	require.NoError(t, bs.sendBuffered(muxErrEvent, nil))
	t.Logf(bs.TestingBufferSummary())

	testServerStream.waitForEvent(t, muxErrEvent)
	require.Equal(t, 1, testServerStream.totalEventsSent())

	// Once the error event has been delivered to the client, all additional
	// requests should be dropped.
	//
	// We use a second stream "flush" the buffer and assert we don't see these
	// events. This assumes buffered sender orders unrelated streams.
	muxEv2 := *muxEv
	muxEv2.StreamID = 2
	sm.RegisteringStream(2)
	testServerStream.reset()
	require.NoError(t, bs.sendBuffered(muxEv, nil))
	require.NoError(t, bs.sendBuffered(muxErrEvent, nil))
	require.NoError(t, bs.sendBuffered(&muxEv2, nil))
	testServerStream.waitForEvent(t, &muxEv2)
	require.Equal(t, 1, testServerStream.totalEventsSent())
}

// TestBufferedSenderOnOverflowWithErrorEvent tests the special case of a stream
// hitting an overflow on an event that is itself an error.
func TestBufferedSenderOnOverflowWithErrorEvent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	testServerStream := newTestServerStream()
	st := cluster.MakeTestingClusterSettings()

	queueCap := int64(24)
	streamID := int64(1)

	RangefeedSingleBufferedSenderQueueMaxPerReg.Override(ctx, &st.SV, queueCap)
	bs := NewBufferedSender(testServerStream, st, NewBufferedSenderMetrics())
	smMetrics := NewStreamManagerMetrics()
	sm := NewStreamManager(bs, smMetrics)
	sm.RegisteringStream(streamID)

	require.Equal(t, queueCap, bs.queueMu.perStreamCapacity)

	val1 := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
	ev1 := new(kvpb.RangeFeedEvent)
	ev1.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val1})
	muxEv := &kvpb.MuxRangeFeedEvent{RangeFeedEvent: *ev1, RangeID: 0, StreamID: streamID}

	for range queueCap {
		require.NoError(t, bs.sendBuffered(muxEv, nil))
	}

	// Overflow now.
	ev2 := kvpb.RangeFeedEvent{Error: &kvpb.RangeFeedError{Error: *newErrBufferCapacityExceeded()}}
	muxErrEvent := &kvpb.MuxRangeFeedEvent{RangeFeedEvent: ev2, RangeID: 0, StreamID: streamID}
	// We admit the error event.
	require.NoError(t, bs.sendBuffered(muxErrEvent, nil))
	t.Logf(bs.TestingBufferSummary())

	// Start the stream manager now, which will start to drain the overflowed stream.
	require.NoError(t, sm.Start(ctx, stopper))
	defer sm.Stop(ctx)

	testServerStream.waitForEventCount(t, int(queueCap+1))
	// We should see the event we sent to the stream.
	require.True(t, testServerStream.hasEvent(muxErrEvent))
	testServerStream.reset()

	// All additional requests should be dropped, including errors because the
	// first error should have moved us to overflowed.
	//
	// We use a second stream "flush" the buffer and assert we don't see these
	// events. This assumes buffered sender orders unrelated streams.
	muxEv2 := *muxEv
	muxEv2.StreamID = 2
	sm.RegisteringStream(2)
	testServerStream.reset()
	require.NoError(t, bs.sendBuffered(muxEv, nil))
	require.NoError(t, bs.sendBuffered(muxErrEvent, nil))
	require.NoError(t, bs.sendBuffered(&muxEv2, nil))
	testServerStream.waitForEvent(t, &muxEv2)
	require.Equal(t, 1, testServerStream.totalEventsSent())
}

// TestBufferedSenderOnOverflowMultiStream tests that BufferedSender and
// StreamManager handle overflow and stream removal.
func TestBufferedSenderOnOverflowMultiStream(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	testServerStream := newTestServerStream()
	smMetrics := NewStreamManagerMetrics()
	st := cluster.MakeTestingClusterSettings()

	queueCap := int64(24)
	RangefeedSingleBufferedSenderQueueMaxPerReg.Override(ctx, &st.SV, queueCap)
	bs := NewBufferedSender(testServerStream, st, NewBufferedSenderMetrics())
	require.Equal(t, queueCap, bs.queueMu.perStreamCapacity)

	sm := NewStreamManager(bs, smMetrics)
	require.NoError(t, sm.Start(ctx, stopper))
	defer sm.Stop(ctx)

	p, h, pStopper := newTestProcessor(t, withRangefeedTestType(scheduledProcessorWithBufferedSender))
	defer pStopper.Stop(ctx)

	streamID1 := int64(42)
	streamID2 := streamID1 + 1

	val1 := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
	ev1 := new(kvpb.RangeFeedEvent)
	ev1.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val1})
	muxEv := &kvpb.MuxRangeFeedEvent{RangeFeedEvent: *ev1, RangeID: 0, StreamID: streamID1}

	// Block the stream so that we can overflow later.
	unblock := testServerStream.BlockSend()
	defer unblock()

	waitForQueueLen := func(len int) {
		testutils.SucceedsSoon(t, func() error {
			if bs.len() == len {
				return nil
			}
			return errors.Newf("expected %d events, found %d", len, bs.len())
		})
	}

	// Add our stream to the stream manager.
	sm.RegisteringStream(streamID1)
	registered, d, _ := p.Register(ctx, h.span, hlc.Timestamp{}, nil, /* catchUpIter */
		false /* withDiff */, false /* withFiltering */, false /* withOmitRemote */, noBulkDelivery,
		sm.NewStream(streamID1, 1 /*rangeID*/))
	require.True(t, registered)
	sm.AddStream(streamID1, d)

	// Add a second stream to the stream manager.
	sm.RegisteringStream(streamID2)
	registered, d, _ = p.Register(ctx, h.span, hlc.Timestamp{}, nil, /* catchUpIter */
		false /* withDiff */, false /* withFiltering */, false /* withOmitRemote */, noBulkDelivery,
		sm.NewStream(streamID2, 1 /*rangeID*/))
	require.True(t, registered)
	sm.AddStream(streamID2, d)

	// At this point we actually have sent 2 events, one for each checkpoint sent
	// by the registrations. One of these should get pulled off the queue and block.
	waitForQueueLen(1)
	// Now fill the rest of the queue.
	for range queueCap {
		require.NoError(t, sm.sender.sendBuffered(muxEv, nil))
	}

	// The next write should overflow.
	capExceededErrStr := newRetryErrBufferCapacityExceeded().Error()
	err := sm.sender.sendBuffered(muxEv, nil)
	require.EqualError(t, err, capExceededErrStr)

	// A write to a different stream should be fine
	muxEv.StreamID = streamID2
	err = sm.sender.sendBuffered(muxEv, nil)
	require.NoError(t, err)

	unblock()
	waitForQueueLen(0)
}
