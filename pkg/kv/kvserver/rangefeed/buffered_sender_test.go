// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
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
	dest := bs.popEvents(nil, 1)
	require.Equal(t, 1, len(dest))
	e := dest[0]
	require.Equal(t, sharedMuxEvent{
		ev:    muxEv,
		alloc: nil,
	}, e)

	require.Equal(t, queueCap-1, int64(bs.len()))
	require.NoError(t, bs.sendBuffered(muxEv, nil))
	require.Equal(t, queueCap, int64(bs.len()))

	// Overflow now.
	t.Log(bs.TestingBufferSummary())
	err := bs.sendBuffered(muxEv, nil)
	require.EqualError(t, err, newRetryErrBufferCapacityExceeded().Error())
	t.Log(bs.TestingBufferSummary())

	// Start the stream manager now, which will start to drain the overflowed stream.
	require.NoError(t, sm.Start(ctx, stopper))
	defer sm.Stop(ctx)

	testServerStream.waitForEventCount(t, int(queueCap))
	testServerStream.reset()

	// The stream should now be in state overflowing. The next error event will
	// remove the stream from our state map. Any subsequent events will be
	// dropped.
	ev2 := kvpb.RangeFeedEvent{Error: &kvpb.RangeFeedError{Error: *newErrBufferCapacityExceeded()}}
	muxErrEvent := &kvpb.MuxRangeFeedEvent{RangeFeedEvent: ev2, RangeID: 0, StreamID: streamID}

	t.Log(bs.TestingBufferSummary())
	require.NoError(t, bs.sendBuffered(muxErrEvent, nil))
	require.ErrorIs(t, bs.sendBuffered(muxEv, nil), errNoSuchStream)
	require.ErrorIs(t, bs.sendBuffered(muxErrEvent, nil), errNoSuchStream)
	t.Log(bs.TestingBufferSummary())

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
	require.ErrorIs(t, bs.sendBuffered(muxEv, nil), errNoSuchStream)
	require.ErrorIs(t, bs.sendBuffered(muxErrEvent, nil), errNoSuchStream)
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
	t.Log(bs.TestingBufferSummary())

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
	require.ErrorIs(t, bs.sendBuffered(muxEv, nil), errNoSuchStream)
	require.ErrorIs(t, bs.sendBuffered(muxErrEvent, nil), errNoSuchStream)
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
	bs.sendBufSize = 1 // Test requires knowing exactly when the queue will fill.
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
	registered, d, _ := p.Register(ctx, h.span, hlc.Timestamp{}, nil, /* catchUpSnap */
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
