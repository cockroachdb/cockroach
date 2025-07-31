// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

func makeMuxRangefeedErrorEvent(
	streamID int64, rangeID roachpb.RangeID, err *kvpb.Error,
) *kvpb.MuxRangeFeedEvent {
	ev := &kvpb.MuxRangeFeedEvent{
		StreamID: streamID,
		RangeID:  rangeID,
	}
	ev.MustSetValue(&kvpb.RangeFeedError{
		Error: *transformRangefeedErrToClientError(err),
	})
	return ev
}

// TestUnbufferedSenderDisconnect tests that correctly forwards rangefeed
// completion errors to the server stream.
func TestUnbufferedSenderDisconnect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	testServerStream := newTestServerStream()
	testRangefeedCounter := newTestRangefeedCounter()
	ubs := NewUnbufferedSender(testServerStream, testRangefeedCounter)
	require.NoError(t, ubs.Start(ctx, stopper))
	defer ubs.Stop()

	t.Run("nil handling", func(t *testing.T) {
		const streamID = 0
		const rangeID = 1
		streamCtx, cancel := context.WithCancel(context.Background())
		ubs.AddStream(streamID, cancel)
		// Note that kvpb.NewError(nil) == nil.
		require.Equal(t, testRangefeedCounter.get(), int32(1))
		ubs.SendBufferedError(makeMuxRangefeedErrorEvent(streamID, rangeID,
			kvpb.NewError(nil)))
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
		ubs.SendBufferedError(makeMuxRangefeedErrorEvent(streamID, rangeID,
			kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED))))
		time.Sleep(10 * time.Millisecond)
		require.Equalf(t, 1, testServerStream.totalEventsSent(), testServerStream.String())
	})

	t.Run("send rangefeed completion error concurrently", func(t *testing.T) {
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
			ubs.AddStream(muxError.streamID, func() {})
		}

		require.Equal(t, testRangefeedCounter.get(), int32(3))

		var wg sync.WaitGroup
		for _, muxError := range testRangefeedCompletionErrors {
			wg.Add(1)
			go func(streamID int64, rangeID roachpb.RangeID, err error) {
				defer wg.Done()
				ubs.SendBufferedError(makeMuxRangefeedErrorEvent(streamID, rangeID, kvpb.NewError(err)))
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

// TestUnbufferedSenderOnBlockingIO tests that the
// UnbufferedSender.SendBufferedError doesn't block on IO.
func TestUnbufferedSenderOnBlockingIO(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	testServerStream := newTestServerStream()
	testRangefeedCounter := newTestRangefeedCounter()
	ubs := NewUnbufferedSender(testServerStream, testRangefeedCounter)
	require.NoError(t, ubs.Start(ctx, stopper))
	defer ubs.Stop()

	const streamID = 0
	const rangeID = 1
	streamCtx, streamCancel := context.WithCancel(context.Background())
	ubs.AddStream(0, streamCancel)

	ev := &kvpb.MuxRangeFeedEvent{
		StreamID: streamID,
		RangeID:  rangeID,
	}
	ev.MustSetValue(&kvpb.RangeFeedCheckpoint{
		Span:       roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
		ResolvedTS: hlc.Timestamp{WallTime: 1},
	})
	require.NoError(t, ubs.sender.Send(ev))
	require.Truef(t, testServerStream.hasEvent(ev),
		"expected event %v not found in %v", ev, testServerStream)

	// Block the stream.
	unblock := testServerStream.BlockSend()

	// Although stream is blocked, we should be able to disconnect the stream
	// without blocking.
	ubs.SendBufferedError(makeMuxRangefeedErrorEvent(streamID, rangeID,
		kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_NO_LEASEHOLDER))))
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

// TestUnbufferedSenderConcurrentSend tests that UnbufferedSender.SendUnbuffered
// is thread-safe.
func TestUnbufferedSenderWithConcurrentSend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	testServerStream := newTestServerStream()
	testRangefeedCounter := newTestRangefeedCounter()
	ubs := NewUnbufferedSender(testServerStream, testRangefeedCounter)
	require.NoError(t, ubs.Start(ctx, stopper))
	defer ubs.Stop()

	ubs.AddStream(1, func() {})
	require.Equal(t, testRangefeedCounter.get(), int32(1))

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			val := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
			ev1 := new(kvpb.RangeFeedEvent)
			ev1.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val, PrevValue: val})
			require.NoError(t, ubs.SendUnbuffered(&kvpb.MuxRangeFeedEvent{
				StreamID:       1,
				RangeID:        1,
				RangeFeedEvent: *ev1,
			}))
		}()
	}
	wg.Wait()

	require.Equal(t, 10, testServerStream.eventsSent)
}
