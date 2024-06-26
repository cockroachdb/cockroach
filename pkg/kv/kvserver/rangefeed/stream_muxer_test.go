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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestNodeStreamMuxer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()

	testServerStream := newTestServerStream()
	testRangefeedCounter := newTestRangefeedCounter()
	streamMuxer := NewStreamMuxer(testServerStream, testRangefeedCounter)
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()
	// Make sure to shut down the muxer before wg.Wait().
	defer stopper.Stop(ctx)
	if err := stopper.RunAsyncTask(ctx, "mux-term-forwarder", func(ctx context.Context) {
		defer wg.Done()
		streamMuxer.Run(ctx, stopper)
	}); err != nil {
		wg.Done()
	}

	wrapReasonInError := func(reason kvpb.RangeFeedRetryError_Reason) *kvpb.Error {
		return kvpb.NewError(kvpb.NewRangeFeedRetryError(reason))
	}

	_, streamCancel := context.WithCancel(context.Background())
	streamMuxer.AddStream(0, streamCancel)
	require.Equal(t, testRangefeedCounter.get(), int32(1))

	rangefeedStreams := []struct {
		streamID            int64
		rangeID             roachpb.RangeID
		serverDisconnectErr *kvpb.Error
		clientErr           *kvpb.Error
	}{
		{1, 1,
			wrapReasonInError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED),
			wrapReasonInError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED)},

		{2, 1,
			wrapReasonInError(kvpb.RangeFeedRetryError_REASON_RANGE_SPLIT),
			wrapReasonInError(kvpb.RangeFeedRetryError_REASON_RANGE_SPLIT)},

		{3, 2,
			wrapReasonInError(kvpb.RangeFeedRetryError_REASON_RAFT_SNAPSHOT),
			wrapReasonInError(kvpb.RangeFeedRetryError_REASON_RAFT_SNAPSHOT)},

		// Nil error should be converted into
		// RangeFeedRetryError_REASON_RANGEFEED_CLOSED.
		{4, 2,
			nil,
			wrapReasonInError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED)},
	}

	t.Run("disconnect stream cancels stream context", func(t *testing.T) {
		for _, stream := range rangefeedStreams {
			streamCtx, streamCancel := context.WithCancel(context.Background())
			require.Equal(t, testRangefeedCounter.get(), int32(1))

			streamMuxer.AddStream(stream.streamID, streamCancel)
			require.Equal(t, testRangefeedCounter.get(), int32(2))

			streamMuxer.DisconnectRangefeedWithError(stream.streamID, stream.rangeID, stream.serverDisconnectErr)
			require.Error(t, streamCtx.Err(), context.Canceled)
		}

		// Pause for one second to make sure muxer has time to process all the errors.
		time.Sleep(1 * time.Second)

		// Check client errors sent to stream.
		for _, stream := range rangefeedStreams {
			require.True(t, testServerStream.hasEvent(makeRangefeedErrorEvent(
				stream.streamID, stream.rangeID, stream.clientErr)))
		}
		require.Equal(t, testRangefeedCounter.get(), int32(1))
	})

	t.Run("concurrently disconnect streams", func(t *testing.T) {
		_, noop := context.WithCancel(context.Background())
		defer noop()
		for _, stream := range rangefeedStreams {
			streamMuxer.AddStream(stream.streamID, noop)
		}
		require.Equal(t, testRangefeedCounter.get(), int32(5))

		var wg sync.WaitGroup
		for _, stream := range rangefeedStreams {
			wg.Add(1)
			go func(streamID int64, rangeID roachpb.RangeID, serverDisconnectErr *kvpb.Error) {
				defer wg.Done()
				streamMuxer.DisconnectRangefeedWithError(streamID, rangeID, serverDisconnectErr)
			}(stream.streamID, stream.rangeID, stream.serverDisconnectErr)
		}
		wg.Wait()

		// Pause for one second to make sure muxer has time to process all the errors.
		time.Sleep(1 * time.Second)

		// Check client errors sent to stream.
		for _, stream := range rangefeedStreams {
			require.True(t, testServerStream.hasEvent(makeRangefeedErrorEvent(
				stream.streamID, stream.rangeID, stream.clientErr)))
		}
		require.Equal(t, testRangefeedCounter.get(), int32(1))
	})

	t.Run("repeatedly closing streams does nothing", func(t *testing.T) {
		prevNum := testServerStream.eventSentCount()
		streamMuxer.DisconnectRangefeedWithError(1, 1,
			wrapReasonInError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED))
		// Pause for one second to make sure muxer has time to process all the errors.
		time.Sleep(1 * time.Second)
		require.Equal(t, prevNum, testServerStream.eventSentCount())
		require.Equal(t, testRangefeedCounter.get(), int32(1))
	})
}
