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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestStreamMuxer tests that correctly forwards rangefeed completion errors to
// the server stream.
func TestStreamMuxer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	testServerStream := newTestServerStream()
	testRangefeedCounter := newTestRangefeedCounter()
	muxer := NewStreamMuxer(testServerStream, testRangefeedCounter)
	require.NoError(t, muxer.Start(ctx, stopper))
	defer muxer.Stop()

	t.Run("nil handling", func(t *testing.T) {
		const streamID = 0
		const rangeID = 1
		streamCtx, cancel := context.WithCancel(context.Background())
		muxer.AddStream(streamID, rangeID, cancel)
		// Note that kvpb.NewError(nil) == nil.
		require.Equal(t, testRangefeedCounter.get(), int32(1))
		muxer.DisconnectStreamWithError(streamID, rangeID, kvpb.NewError(nil))
		require.Equal(t, testRangefeedCounter.get(), int32(0))
		require.Equal(t, context.Canceled, streamCtx.Err())
		expectedErrEvent := &kvpb.MuxRangeFeedEvent{
			StreamID: streamID,
			RangeID:  rangeID,
		}
		expectedErrEvent.MustSetValue(&kvpb.RangeFeedError{
			Error: *kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED)),
		})
		time.Sleep(10 * time.Millisecond)
		require.Equal(t, 1, testServerStream.totalEventsSent())
		require.True(t, testServerStream.hasEvent(expectedErrEvent))

		// Repeatedly closing the stream does nothing.
		muxer.DisconnectStreamWithError(streamID, rangeID,
			kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED)))
		time.Sleep(10 * time.Millisecond)
		require.Equal(t, 1, testServerStream.totalEventsSent())
	})

	t.Run("send rangefeed completion error", func(t *testing.T) {
		testRangefeedCompletionErrors := []struct {
			streamID int64
			rangeID  roachpb.RangeID
			Error    error
		}{
			{0, 1, kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED)},
			{1, 1, context.Canceled},
			{2, 2, &kvpb.NodeUnavailableError{}},
		}

		require.Equal(t, testRangefeedCounter.get(), int32(0))

		for _, muxError := range testRangefeedCompletionErrors {
			muxer.AddStream(muxError.streamID, muxError.rangeID, func() {})
		}

		require.Equal(t, testRangefeedCounter.get(), int32(3))

		var wg sync.WaitGroup
		for _, muxError := range testRangefeedCompletionErrors {
			wg.Add(1)
			go func(streamID int64, rangeID roachpb.RangeID, err error) {
				defer wg.Done()
				muxer.DisconnectStreamWithError(streamID, rangeID, kvpb.NewError(err))
			}(muxError.streamID, muxError.rangeID, muxError.Error)
		}
		wg.Wait()

		for _, muxError := range testRangefeedCompletionErrors {
			testutils.SucceedsSoon(t, func() error {
				ev := &kvpb.MuxRangeFeedEvent{
					StreamID: muxError.streamID,
					RangeID:  muxError.rangeID,
				}
				ev.MustSetValue(&kvpb.RangeFeedError{
					Error: *kvpb.NewError(muxError.Error),
				})
				if testServerStream.hasEvent(ev) {
					return nil
				}
				return errors.Newf("expected error %v not found", muxError)
			})
		}
		require.Equal(t, testRangefeedCounter.get(), int32(0))
	})
}

// TestStreamMuxerOnBlockingIO tests that the
// StreamMuxer.DisconnectStreamWithError doesn't block on IO.
func TestStreamMuxerOnBlockingIO(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	testServerStream := newTestServerStream()
	testRangefeedCounter := newTestRangefeedCounter()
	muxer := NewStreamMuxer(testServerStream, testRangefeedCounter)
	require.NoError(t, muxer.Start(ctx, stopper))
	defer muxer.Stop()

	const streamID = 0
	const rangeID = 1
	streamCtx, streamCancel := context.WithCancel(context.Background())
	muxer.AddStream(0, rangeID, streamCancel)

	ev := &kvpb.MuxRangeFeedEvent{
		StreamID: streamID,
		RangeID:  rangeID,
	}
	ev.MustSetValue(&kvpb.RangeFeedCheckpoint{
		Span:       roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
		ResolvedTS: hlc.Timestamp{WallTime: 1},
	})
	require.NoError(t, muxer.sender.SendUnbuffered(ev))
	require.Truef(t, testServerStream.hasEvent(ev),
		"expected event %v not found in %v", ev, testServerStream)

	// Block the stream.
	unblock := testServerStream.BlockSend()

	// Although stream is blocked, we should be able to disconnect the stream
	// without blocking.
	muxer.DisconnectStreamWithError(streamID, rangeID,
		kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_NO_LEASEHOLDER)))
	require.Equal(t, streamCtx.Err(), context.Canceled)
	unblock()
	time.Sleep(100 * time.Millisecond)
	expectedErrEvent := &kvpb.MuxRangeFeedEvent{
		StreamID: streamID,
		RangeID:  rangeID,
	}
	expectedErrEvent.MustSetValue(&kvpb.RangeFeedError{
		Error: *kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_NO_LEASEHOLDER)),
	})
	// Receive the event after getting unblocked.
	require.Truef(t, testServerStream.hasEvent(expectedErrEvent),
		"expected event %v not found in %v", ev, testServerStream)
}

// TestStreamMuxerWithDisconnect tests that StreamMuxer can handle stream
// disconnects properly including context canceled, metrics updates, rangefeed
// cleanup.
func TestStreamMuxerWithDisconnect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	testServerStream := newTestServerStream()
	testRangefeedCounter := newTestRangefeedCounter()
	muxer := NewStreamMuxer(testServerStream, testRangefeedCounter)
	require.NoError(t, muxer.Start(ctx, stopper))
	defer muxer.Stop()

	t.Run("basic operation", func(t *testing.T) {
		const streamID = 0
		var num atomic.Int32
		streamCtx, cancel := context.WithCancel(context.Background())
		muxer.AddStream(int64(streamID), 1, cancel)
		muxer.RegisterRangefeedCleanUp(int64(streamID), func() {
			num.Add(1)
		})
		muxer.DisconnectStreamWithError(int64(streamID), 1, kvpb.NewError(nil))
		time.Sleep(10 * time.Millisecond)
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
		muxer.AddStream(int64(streamID), 1, cancel)
		require.Equal(t, int32(1), testRangefeedCounter.get())

		// Disconnect stream without registering clean up should still work.
		muxer.DisconnectStreamWithError(int64(streamID), 1, kvpb.NewError(nil))
		require.Equal(t, context.Canceled, streamCtx.Err())
		require.Equal(t, int32(0), num.Load())
		require.Equal(t, int32(0), testRangefeedCounter.get())

		// Although not possible theoretically, register clean up with an already
		// disconnected stream should still work if the stream is disconnected
		// again. This is more robust.
		muxer.RegisterRangefeedCleanUp(int64(streamID), func() {
			num.Add(1)
		})
		muxer.DisconnectStreamWithError(int64(streamID), 1, kvpb.NewError(nil))
		time.Sleep(10 * time.Millisecond)
		require.Equal(t, int32(1), num.Load())
		require.Equal(t, int32(0), testRangefeedCounter.get())
	})

	t.Run("multiple clean up should do nothing", func(t *testing.T) {
		const streamID = 0
		var num atomic.Int32
		_, cancel := context.WithCancel(context.Background())
		muxer.AddStream(int64(streamID), 1, cancel)
		muxer.RegisterRangefeedCleanUp(int64(streamID), func() {
			num.Add(1)
		})

		muxer.DisconnectStreamWithError(int64(streamID), 1, kvpb.NewError(nil))
		time.Sleep(10 * time.Millisecond)
		require.Equal(t, int32(1), num.Load())
		require.Equal(t, int32(0), testRangefeedCounter.get())

		// Disconnecting the stream again should do nothing.
		muxer.DisconnectStreamWithError(int64(streamID), 1, kvpb.NewError(nil))
		time.Sleep(10 * time.Millisecond)
		require.Equal(t, int32(1), num.Load())
		require.Equal(t, int32(0), testRangefeedCounter.get())
	})
}

func TestStreamMuxerOnStop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	testServerStream := newTestServerStream()
	testRangefeedCounter := newTestRangefeedCounter()
	muxer := NewStreamMuxer(testServerStream, testRangefeedCounter)
	require.NoError(t, muxer.Start(ctx, stopper))

	rng, _ := randutil.NewTestRand()
	var actualSum atomic.Int32

	// [streamIdStart,streamIDEnd) are in the active streams. streamIdStart <=
	// streamIDEnd. If streamIDStart == streamIDEnd, no elements yet. [0,
	// streamIdStart) are disconnected.
	streamIdStart := int64(0)
	streamIdEnd := int64(0)

	defer func() {
		muxer.Stop()
		// Ensure that all streams are disconnected and cleanups are properly
		// called.
		require.Equal(t, int32(0), testRangefeedCounter.get())
		require.Equal(t, int32(streamIdEnd), actualSum.Load())
		require.Equal(t, streamIdStart, int64(testServerStream.totalEventsSent()))
	}()

	for i := 0; i < 100000; i++ {
		randBool := rng.Intn(2) == 0
		require.LessOrEqualf(t, streamIdStart, streamIdEnd, "test programming error")
		if randBool || streamIdStart == streamIdEnd {
			_, cancel := context.WithCancel(context.Background())
			muxer.AddStream(streamIdEnd, 1, cancel)
			muxer.RegisterRangefeedCleanUp(streamIdEnd, func() {
				actualSum.Add(1)
			})
			streamIdEnd++
		} else {
			muxer.DisconnectStreamWithError(streamIdStart, 1, kvpb.NewError(nil))
			streamIdStart++
		}
	}
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int32(streamIdStart), actualSum.Load())
	require.Equal(t, streamIdStart, int64(testServerStream.totalEventsSent()))
	require.Equal(t, int32(streamIdEnd-streamIdStart), testRangefeedCounter.get())
}
