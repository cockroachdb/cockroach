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
	RangefeedSingleBufferedSenderQueueMaxSize.Override(ctx, &st.SV, queueCap)
	bs := NewBufferedSender(testServerStream, st, NewBufferedSenderMetrics())
	require.Equal(t, queueCap, bs.queueMu.capacity)

	val1 := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
	ev1 := new(kvpb.RangeFeedEvent)
	ev1.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val1})
	muxEv := &kvpb.MuxRangeFeedEvent{RangeFeedEvent: *ev1, RangeID: 0, StreamID: 1}

	for range queueCap {
		require.NoError(t, bs.sendBuffered(muxEv, nil))
	}
	require.Equal(t, queueCap, int64(bs.len()))
	e, success, overflowed, remains := bs.popFront()
	require.Equal(t, sharedMuxEvent{
		ev:    muxEv,
		alloc: nil,
	}, e)
	require.True(t, success)
	require.False(t, overflowed)
	require.Equal(t, queueCap-1, remains)
	require.Equal(t, queueCap-1, int64(bs.len()))
	require.NoError(t, bs.sendBuffered(muxEv, nil))
	require.Equal(t, queueCap, int64(bs.len()))

	// Overflow now.
	require.Equal(t, bs.sendBuffered(muxEv, nil).Error(),
		newRetryErrBufferCapacityExceeded().Error())
}

// TestBufferedSenderOnStreamShutdown tests that BufferedSender and
// StreamManager handle overflow and shutdown properly.
func TestBufferedSenderOnStreamShutdown(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	testServerStream := newTestServerStream()
	smMetrics := NewStreamManagerMetrics()
	st := cluster.MakeTestingClusterSettings()

	queueCap := int64(24)
	RangefeedSingleBufferedSenderQueueMaxSize.Override(ctx, &st.SV, queueCap)
	bs := NewBufferedSender(testServerStream, st, NewBufferedSenderMetrics())
	require.Equal(t, queueCap, bs.queueMu.capacity)

	sm := NewStreamManager(bs, smMetrics)
	require.NoError(t, sm.Start(ctx, stopper))
	defer sm.Stop(ctx)

	p, h, pStopper := newTestProcessor(t, withRangefeedTestType(scheduledProcessorWithBufferedSender))
	defer pStopper.Stop(ctx)

	streamID := int64(42)

	val1 := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
	ev1 := new(kvpb.RangeFeedEvent)
	ev1.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val1})
	muxEv := &kvpb.MuxRangeFeedEvent{RangeFeedEvent: *ev1, RangeID: 0, StreamID: streamID}

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
	registered, d, _ := p.Register(ctx, h.span, hlc.Timestamp{}, nil, /* catchUpIter */
		false /* withDiff */, false /* withFiltering */, false /* withOmitRemote */, noBulkDelivery,
		sm.NewStream(streamID, 1 /*rangeID*/))
	require.True(t, registered)
	sm.AddStream(streamID, d)

	require.NoError(t, sm.sender.sendBuffered(muxEv, nil))
	// At this point we actually have sent 2 events. 1 checkpoint event sent by
	// register and 1 event sent on the line above. We wait for 1 of these events
	// to be pulled off the queue and block in the sender, leaving 1 in the queue.
	waitForQueueLen(1)
	// Now fill the rest of the queue.
	for range queueCap - 1 {
		require.NoError(t, sm.sender.sendBuffered(muxEv, nil))
	}

	// The next write should overflow.
	capExceededErrStr := newRetryErrBufferCapacityExceeded().Error()
	err := sm.sender.sendBuffered(muxEv, nil)
	require.EqualError(t, err, capExceededErrStr)
	require.True(t, bs.overflowed())

	unblock()
	waitForQueueLen(0)
	// Overflow cleanup.
	err = <-sm.Error()
	require.EqualError(t, err, capExceededErrStr)
	// Note that we expect the stream manager to shut down here, but no actual
	// error events would be sent during the shutdown.
	err = sm.sender.sendBuffered(muxEv, nil)
	require.EqualError(t, err, capExceededErrStr)
}
