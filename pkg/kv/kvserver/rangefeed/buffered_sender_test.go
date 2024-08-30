// Copyright 2024 The Cockroach Authors.
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
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestBufferedSenderWithDisconnect tests that BufferedSender can handle stream
// disconnects properly including context canceled, metrics updates, rangefeed
// cleanup after SendBufferedError.
func TestBufferedSenderWithSendBufferedError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	testServerStream := newTestServerStream()
	testRangefeedCounter := newTestRangefeedCounter()
	bs := NewBufferedSender(testServerStream, testRangefeedCounter)
	require.NoError(t, bs.Start(ctx, stopper))
	defer bs.Stop()

	t.Run("basic operation", func(t *testing.T) {
		const streamID = 0
		var num atomic.Int32
		streamCtx, cancel := context.WithCancel(context.Background())
		bs.AddStream(int64(streamID), cancel)
		bs.RegisterRangefeedCleanUp(int64(streamID), func() {
			num.Add(1)
		})
		bs.SendBufferedError(makeMuxRangefeedErrorEvent(int64(streamID), 1, kvpb.NewError(nil)))
		require.NoError(t, bs.waitForEmptyBuffer(ctx))
		// Ensure that the rangefeed clean up is called.
		require.Equal(t, int32(1), num.Load())
		// Ensure that the stream is properly disconnected.
		require.Equal(t, context.Canceled, streamCtx.Err())
		require.Equal(t, int32(0), testRangefeedCounter.get())
	})
	t.Run("cleanup map and active streams map out of sync", func(t *testing.T) {
		const streamID = 0
		var num atomic.Int32
		streamCtx, cancel := context.WithCancel(context.Background())
		bs.AddStream(int64(streamID), cancel)
		require.Equal(t, int32(1), testRangefeedCounter.get())

		// Disconnect stream without registering clean up should still work.
		bs.SendBufferedError(makeMuxRangefeedErrorEvent(int64(streamID), 1, kvpb.NewError(nil)))
		require.Equal(t, context.Canceled, streamCtx.Err())
		require.NoError(t, bs.waitForEmptyBuffer(ctx))
		require.Equal(t, int32(0), num.Load())
		require.Equal(t, int32(0), testRangefeedCounter.get())
	})

	t.Run("multiple clean up should do nothing", func(t *testing.T) {
		const streamID = 0
		var num atomic.Int32
		_, cancel := context.WithCancel(context.Background())
		bs.AddStream(int64(streamID), cancel)
		bs.RegisterRangefeedCleanUp(int64(streamID), func() {
			num.Add(1)
		})

		bs.SendBufferedError(makeMuxRangefeedErrorEvent(int64(streamID), 1, kvpb.NewError(nil)))
		require.NoError(t, bs.waitForEmptyBuffer(ctx))
		require.Equal(t, int32(1), num.Load())
		require.Equal(t, int32(0), testRangefeedCounter.get())

		// Disconnecting the stream again should do nothing.
		bs.SendBufferedError(makeMuxRangefeedErrorEvent(int64(streamID), 1, kvpb.NewError(nil)))
		require.NoError(t, bs.waitForEmptyBuffer(ctx))
		require.Equal(t, int32(1), num.Load())
		require.Equal(t, int32(0), testRangefeedCounter.get())
	})
}

func TestBufferedSenderOnStop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	testServerStream := newTestServerStream()
	testRangefeedCounter := newTestRangefeedCounter()
	bs := NewBufferedSender(testServerStream, testRangefeedCounter)
	require.NoError(t, bs.Start(ctx, stopper))

	rng, _ := randutil.NewTestRand()
	var actualSum atomic.Int32

	// [streamIdStart,streamIDEnd) are in the active streams. streamIdStart <=
	// streamIDEnd. If streamIDStart == streamIDEnd, no elements yet. [0,
	// streamIdStart) are disconnected.
	streamIdStart := int64(0)
	streamIdEnd := int64(0)

	defer func() {
		bs.Stop()
		// Ensure that all streams are disconnected and cleanups are properly
		// called.
		require.Equal(t, int32(0), testRangefeedCounter.get())
		require.Equal(t, int32(streamIdEnd), actualSum.Load())
		require.Equal(t, streamIdStart, int64(testServerStream.totalEventsSent()))
		val1 := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
		ev1 := new(kvpb.RangeFeedEvent)
		ev1.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val1})
		muxEv := &kvpb.MuxRangeFeedEvent{RangeFeedEvent: *ev1, RangeID: 0, StreamID: 1}
		require.Equal(t, bs.SendBuffered(muxEv, nil).Error(),
			errors.New("stream sender is stopped").Error())
	}()

	for i := 0; i < 100000; i++ {
		randBool := rng.Intn(2) == 0
		require.LessOrEqualf(t, streamIdStart, streamIdEnd, "test programming error")
		if randBool || streamIdStart == streamIdEnd {
			_, cancel := context.WithCancel(context.Background())
			bs.AddStream(streamIdEnd, cancel)
			bs.RegisterRangefeedCleanUp(streamIdEnd, func() {
				actualSum.Add(1)
			})
			streamIdEnd++
		} else {
			bs.SendBufferedError(makeMuxRangefeedErrorEvent(streamIdStart, 1, kvpb.NewError(nil)))
			streamIdStart++
		}
	}
	require.NoError(t, bs.waitForEmptyBuffer(ctx))
	require.Equal(t, int32(streamIdStart), actualSum.Load())
	require.Equal(t, streamIdStart, int64(testServerStream.totalEventsSent()))
	require.Equal(t, int32(streamIdEnd-streamIdStart), testRangefeedCounter.get())
}

func TestBufferedSenderOnOverflow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx, cancel := context.WithCancel(context.Background())

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	testServerStream := newTestServerStream()
	testRangefeedCounter := newTestRangefeedCounter()
	bufferedSenderCapacity = 10
	bs := NewBufferedSender(testServerStream, testRangefeedCounter)
	cancel()
	
	val1 := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
	ev1 := new(kvpb.RangeFeedEvent)
	ev1.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val1})
	muxEv := &kvpb.MuxRangeFeedEvent{RangeFeedEvent: *ev1, RangeID: 0, StreamID: 1}
	require.NoError(t, bs.Start(ctx, stopper))
	defer func() {
		bs.Stop()
		require.True(t, bs.queueMu.buffer.Empty())
		require.Equal(t, bs.SendBuffered(muxEv, nil).Error(),
			errors.New("stream sender is stopped").Error())
	}()

	getLen := func() int64 {
		bs.queueMu.Lock()
		defer bs.queueMu.Unlock()
		return bs.queueMu.buffer.Len()
	}

	for i := 0; i < 10; i++ {
		require.NoError(t, bs.SendBuffered(muxEv, nil))
	}
	require.Equal(t, int64(10), getLen())
	e, success, overflowed, remains := bs.popFront()
	require.Equal(t, &sharedMuxEvent{
		event: muxEv,
		alloc: nil,
	}, e)
	require.True(t, success)
	require.False(t, overflowed)
	require.Equal(t, int64(9), remains)
	require.Equal(t, int64(9), getLen())
	require.NoError(t, bs.SendBuffered(muxEv, nil))
	require.Equal(t, int64(10), getLen())

	// Overflow now.
	require.Equal(t, bs.SendBuffered(muxEv, nil).Error(),
		newRetryErrBufferCapacityExceeded().Error())
}
