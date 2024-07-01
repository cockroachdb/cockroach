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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestStreamMuxerBasic tests basic StreamMuxer functionality.
func TestStreamMuxerBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()

	testServerStream := newTestServerStream()
	testRangefeedCounter := newTestRangefeedCounter()
	muxer, cleanUp := newTestStreamMuxerWithRangefeedCounter(t, ctx, stopper, testServerStream, testRangefeedCounter)
	defer cleanUp()

	// Note that this also tests that the StreamMuxer stops when the stopper is
	// stopped. If not, the test will hang.
	defer stopper.Stop(ctx)

	t.Run("nil handling", func(t *testing.T) {
		const streamID = 0
		const rangeID = 1
		streamCtx, cancel := context.WithCancel(context.Background())
		muxer.AddStream(streamID, rangeID, cancel)
		require.Equal(t, testRangefeedCounter.get(), int32(1))
		muxer.DisconnectRangefeedWithError(streamID, rangeID, kvpb.NewError(nil))
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

		// Repeat closing the stream does nothing.
		muxer.DisconnectRangefeedWithError(streamID, rangeID,
			kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED)))
		time.Sleep(10 * time.Millisecond)
		require.Equal(t, 1, testServerStream.totalEventsSent())
		require.Equal(t, testRangefeedCounter.get(), int32(0))
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
			func(streamID int64, rangeID roachpb.RangeID, err error) {
				muxer.DisconnectRangefeedWithError(streamID, rangeID, kvpb.NewError(err))
				wg.Done()
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

// TestStreamMuxerWithConcurrentDisconnect tests that StreamMuxer can handle
// concurrent stream disconnects.
func TestStreamMuxerWithConcurrentDisconnect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "proc type", testTypes, func(t *testing.T, pt procType) {
		ctx := context.Background()

		p, h, stopper := newTestProcessor(t, withProcType(pt))
		serverStream := newTestServerStream()
		testRangefeedCounter := newTestRangefeedCounter()
		muxer, cleanUp := newTestStreamMuxerWithRangefeedCounter(t, ctx, stopper, serverStream, testRangefeedCounter)
		defer cleanUp()
		defer stopper.Stop(ctx)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := 0; id < 50; id++ {
				s := newTestSingleFeedStream(int64(id), muxer, serverStream)
				p.Register(h.span, hlc.Timestamp{}, nil, /* catchUpIter */
					false /* withDiff */, false /* withFiltering */, false /* withOmitRemote */, s, func() {})
			}
		}()
		wg.Wait()

		wg.Add(2)
		require.Equal(t, 50, p.Len())
		require.Equal(t, int32(50), testRangefeedCounter.get())
		go func() {
			defer wg.Done()
			for i := int64(1); i < 50; i++ {
				p.ConsumeLogicalOps(ctx, writeValueOp(hlc.Timestamp{WallTime: i}))
			}
		}()
		h.syncEventAndRegistrations()
		go func() {
			defer wg.Done()
			for id := 0; id < 50; id++ {
				muxer.DisconnectRangefeedWithError(int64(id), 1, kvpb.NewError(nil))
			}
		}()
		wg.Wait()
		time.Sleep(1 * time.Second)

		require.Equal(t, 0, p.Len())
		require.Equal(t, int32(0), testRangefeedCounter.get())
	})
}

// TestStreamMuxerWithDisconnect tests that StreamMuxer can handle stream
// disconnects properly including context canceled, metrics updates, rangefeed
// cleanup.
func TestStreamMuxerWithDisconnect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stopper := stop.NewStopper()
	serverStream := newTestServerStream()
	testRangefeedCounter := newTestRangefeedCounter()
	muxer, cleanUp := newTestStreamMuxerWithRangefeedCounter(t, ctx, stopper, serverStream, testRangefeedCounter)
	defer cleanUp()
	defer stopper.Stop(ctx)

	t.Run("basic", func(t *testing.T) {
		const streamID = 0
		var num atomic.Int32
		streamCtx, cancel := context.WithCancel(context.Background())
		muxer.AddStream(int64(streamID), 1, cancel)
		muxer.RegisterRangefeedCleanUp(int64(streamID), func() {
			num.Add(1)
		})
		muxer.DisconnectRangefeedWithError(int64(streamID), 1, kvpb.NewError(nil))
		time.Sleep(10 * time.Millisecond)
		require.Equal(t, int32(1), num.Load())
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
		muxer.DisconnectRangefeedWithError(int64(streamID), 1, kvpb.NewError(nil))
		require.Equal(t, context.Canceled, streamCtx.Err())
		require.Equal(t, int32(0), num.Load())
		require.Equal(t, int32(0), testRangefeedCounter.get())

		// Register clean up with disconnected stream should still work
		muxer.RegisterRangefeedCleanUp(int64(streamID), func() {
			num.Add(1)
		})
		muxer.DisconnectRangefeedWithError(int64(streamID), 1, kvpb.NewError(nil))
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

		// Disconnect stream without registering clean up should still work.
		muxer.DisconnectRangefeedWithError(int64(streamID), 1, kvpb.NewError(nil))
		time.Sleep(10 * time.Millisecond)
		require.Equal(t, int32(1), num.Load())
		require.Equal(t, int32(0), testRangefeedCounter.get())

		muxer.DisconnectRangefeedWithError(int64(streamID), 1, kvpb.NewError(nil))
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

		// Disconnect stream without registering clean up should still work.
		muxer.DisconnectRangefeedWithError(int64(streamID), 1, kvpb.NewError(nil))
		time.Sleep(10 * time.Millisecond)
		require.Equal(t, int32(1), num.Load())
		require.Equal(t, int32(0), testRangefeedCounter.get())

		muxer.DisconnectRangefeedWithError(int64(streamID), 1, kvpb.NewError(nil))
		time.Sleep(10 * time.Millisecond)
		require.Equal(t, int32(1), num.Load())
		require.Equal(t, int32(0), testRangefeedCounter.get())
	})
}

// TestStreamMuxerOnDisconnectAll tests that streams are properly disconnected
// when StreamMuxer is stopped.
func TestStreamMuxerOnDisconnectAll(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	stopper := stop.NewStopper()

	testServerStream := newTestServerStream()
	testRangefeedCounter := newTestRangefeedCounter()
	muxer, cleanUp := newTestStreamMuxerWithRangefeedCounter(t, ctx, stopper, testServerStream, testRangefeedCounter)
	defer cleanUp()
	defer stopper.Stop(ctx)

	_, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	var num atomic.Int32
	for id := 0; id < 10; id++ {
		muxer.AddStream(int64(id), 1, streamCancel)
		muxer.RegisterRangefeedCleanUp(int64(id), func() {
			num.Add(1)
		})
	}
	require.Equal(t, int32(10), testRangefeedCounter.get())
	require.Equal(t, int32(0), num.Load())

	cancel()
	time.Sleep(10 * time.Millisecond)

	for id := 0; id < 10; id++ {
		expectedErrEvent := &kvpb.MuxRangeFeedEvent{
			StreamID: int64(id),
			RangeID:  1,
		}
		expectedErrEvent.MustSetValue(&kvpb.RangeFeedError{
			Error: *kvpb.NewError(context.Canceled),
		})
		require.True(t, testServerStream.hasEvent(expectedErrEvent))
	}
	require.Equal(t, 10, testServerStream.totalEventsSent())
	require.Equal(t, int32(10), num.Load())
}
