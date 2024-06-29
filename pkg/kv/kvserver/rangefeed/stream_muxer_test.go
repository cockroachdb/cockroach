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
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestStreamMuxerOnStop tests that the StreamMuxer stops when the context is cancelled.
func TestStreamMuxerOnContextCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	stopper := stop.NewStopper()

	testServerStream := newTestServerStream()
	testRangefeedCounter := newTestRangefeedCounter()
	muxer, cleanUp := NewTestStreamMuxer(t, ctx, stopper, testServerStream, testRangefeedCounter)
	defer cleanUp()
	defer stopper.Stop(ctx)

	cancel()
	time.Sleep(10 * time.Millisecond)
	expectedErrEvent := &kvpb.MuxRangeFeedEvent{
		StreamID: 0,
		RangeID:  1,
	}
	expectedErrEvent.MustSetValue(&kvpb.RangeFeedError{
		Error: *kvpb.NewError(context.Canceled),
	})
	muxer.appendMuxError(expectedErrEvent)
	time.Sleep(10 * time.Millisecond)
	require.False(t, testServerStream.hasEvent(expectedErrEvent))
	require.Equal(t, 0, testServerStream.totalEventsSent())
}

func TestStreamMuxer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()

	testServerStream := newTestServerStream()
	testRangefeedCounter := newTestRangefeedCounter()
	muxer, cleanUp := NewTestStreamMuxer(t, ctx, stopper, testServerStream, testRangefeedCounter)
	defer cleanUp()

	// Note that this also tests that the StreamMuxer stops when the stopper is
	// stopped. If not, the test will hang.
	defer stopper.Stop(ctx)

	t.Run("nil handling", func(t *testing.T) {
		const streamID = 0
		const rangeID = 1
		streamCtx, cancel := context.WithCancel(context.Background())
		muxer.AddStream(0, cancel)
		// Note that kvpb.NewError(nil) == nil.
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
			muxer.AddStream(muxError.streamID, func() {})
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

func TestStreamMuxerOnBlockingIO(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()

	testServerStream := newTestServerStream()
	testRangefeedCounter := newTestRangefeedCounter()
	muxer, cleanUp := NewTestStreamMuxer(t, ctx, stopper, testServerStream, testRangefeedCounter)
	defer cleanUp()
	defer stopper.Stop(ctx)

	const streamID = 0
	const rangeID = 1
	_, cancel := context.WithCancel(context.Background())
	muxer.AddStream(0, cancel)
	ev := &kvpb.MuxRangeFeedEvent{
		StreamID: streamID,
		RangeID:  rangeID,
	}
	ev.SetValue(
		*rangeFeedCheckpoint(
			roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
			hlc.Timestamp{WallTime: 1}),
	)
	err := muxer.sender.Send(ev)
	require.NoError(t, err)
	require.True(t, testServerStream.hasEvent(ev))

	// Block the stream.
	unblock := testServerStream.BlockSend()

	// Although stream is blocked, we should be able to disconnect the stream
	// without blocking.
	muxer.DisconnectRangefeedWithError(streamID, rangeID,
		kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_NO_LEASEHOLDER)))
	expectedErrEvent := &kvpb.MuxRangeFeedEvent{
		StreamID: streamID,
		RangeID:  rangeID,
	}
	expectedErrEvent.MustSetValue(&kvpb.RangeFeedError{
		Error: *kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_NO_LEASEHOLDER)),
	})
	unblock()
	time.Sleep(10 * time.Millisecond)
	// Receive the event after unblocking.
	require.True(t, testServerStream.hasEvent(expectedErrEvent))
}
