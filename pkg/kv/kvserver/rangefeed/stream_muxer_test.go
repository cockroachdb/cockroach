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
	"fmt"
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

func wrapReasonInError(reason kvpb.RangeFeedRetryError_Reason) *kvpb.Error {
	return kvpb.NewError(kvpb.NewRangeFeedRetryError(reason))
}

func TestTransformToClientErr(t *testing.T) {
	rangefeedStreams := []struct {
		serverErr         kvpb.RangeFeedRetryError_Reason
		expectedClientErr kvpb.RangeFeedRetryError_Reason
	}{
		{kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED,
			kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED,
		},
		{kvpb.RangeFeedRetryError_REASON_RAFT_SNAPSHOT,
			kvpb.RangeFeedRetryError_REASON_RAFT_SNAPSHOT,
		},
		{kvpb.RangeFeedRetryError_REASON_RANGE_SPLIT,
			kvpb.RangeFeedRetryError_REASON_RANGE_SPLIT,
		},
		{-1,
			kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED,
		},
	}
	for _, stream := range rangefeedStreams {
		t.Run(fmt.Sprintf("server:%s", stream.serverErr), func(t *testing.T) {
			var serverErr *kvpb.Error
			if stream.serverErr == -1 {
				serverErr = nil
			} else {
				serverErr = wrapReasonInError(stream.serverErr)
			}
			expectedClientErr := wrapReasonInError(stream.expectedClientErr)
			require.Equal(t, expectedClientErr.GoError(), transformToClientErr(serverErr).GoError())
		})
	}
}

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

	t.Run("disconnect stream cancels stream context", func(t *testing.T) {
		streamCtx, streamCancel := context.WithCancel(context.Background())
		const streamID = int64(0)
		const rangeID = roachpb.RangeID(1)
		streamMuxer.AddStream(streamID, streamCancel)
		require.Equal(t, testRangefeedCounter.get(), int32(1))

		streamMuxer.DisconnectRangefeedWithError(streamID, rangeID,
			wrapReasonInError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED))
		require.Error(t, streamCtx.Err(), context.Canceled)

		time.Sleep(1 * time.Second)
		require.True(t, testServerStream.hasEvent(makeRangefeedErrorEvent(
			streamID, rangeID, wrapReasonInError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED))))
		require.Equal(t, testRangefeedCounter.get(), int32(0))
	})

	t.Run("repeatedly closing streams does nothing", func(t *testing.T) {
		const streamID = int64(0)
		const rangeID = roachpb.RangeID(1)
		eventSentBefore := testServerStream.eventSentCount()
		streamMuxer.DisconnectRangefeedWithError(streamID, rangeID,
			wrapReasonInError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED))
		// Pause for one second to make sure muxer has time to process all the errors.
		time.Sleep(1 * time.Second)
		require.Equal(t, eventSentBefore, testServerStream.eventSentCount())
		require.Equal(t, testRangefeedCounter.get(), int32(0))
	})

	t.Run("concurrently disconnect streams", func(t *testing.T) {
		const totalStreams = 10
		const rangeID = roachpb.RangeID(0)
		for streamID := 1; streamID <= totalStreams; streamID++ {
			_, noop := context.WithCancel(context.Background())
			streamMuxer.AddStream(int64(streamID), noop)
		}
		require.Equal(t, testRangefeedCounter.get(), int32(totalStreams))

		var wg sync.WaitGroup
		for streamID := 1; streamID <= totalStreams; streamID++ {
			wg.Add(1)
			go func(streamID int64, rangeID roachpb.RangeID, serverDisconnectErr *kvpb.Error) {
				defer wg.Done()
				streamMuxer.DisconnectRangefeedWithError(streamID, rangeID, serverDisconnectErr)
			}(int64(streamID), rangeID, wrapReasonInError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED))
		}
		wg.Wait()

		// Pause for one second to make sure muxer has time to process all the errors.
		time.Sleep(1 * time.Second)

		// Check client errors sent to stream.
		for streamID := 1; streamID <= totalStreams; streamID++ {
			require.True(t, testServerStream.hasEvent(makeRangefeedErrorEvent(
				int64(streamID), rangeID, wrapReasonInError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED))))
		}
		require.Equal(t, testRangefeedCounter.get(), int32(0))
	})
}
