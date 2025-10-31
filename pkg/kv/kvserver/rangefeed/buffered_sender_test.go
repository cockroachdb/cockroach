// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

func TestBufferedSenderReturnsErrorAfterManagerStop(t *testing.T) {
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
	sm.Stop(ctx)

	// No events should be buffered after the manager is stopped.
	val1 := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
	ev1 := new(kvpb.RangeFeedEvent)
	ev1.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val1})
	muxEv := &kvpb.MuxRangeFeedEvent{RangeFeedEvent: *ev1, RangeID: 0, StreamID: 1}
	require.Equal(t, bs.sendBuffered(muxEv, nil).Error(), errors.New("stream sender is stopped").Error())
	require.Equal(t, 0, bs.len())
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
	bs.addStream(streamID)
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
	err := bs.sendBuffered(muxEv, nil)
	require.EqualError(t, err, newRetryErrBufferCapacityExceeded().Error())
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
