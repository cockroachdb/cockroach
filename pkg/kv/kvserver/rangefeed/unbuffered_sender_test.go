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
	ubs := NewUnbufferedSender(testServerStream)
	sm := NewStreamManager(ubs, testRangefeedCounter)
	require.NoError(t, sm.Start(ctx, stopper))
	defer sm.Stop(ctx)
	t.Run("nil handling", func(t *testing.T) {
		const streamID = 0
		const rangeID = 1
		streamCtx, streamCtxCancel := context.WithCancel(context.Background())
		ev := &kvpb.MuxRangeFeedEvent{
			StreamID: streamID,
			RangeID:  rangeID,
		}
		err := transformRangefeedErrToClientError(kvpb.NewError(nil))
		ev.MustSetValue(&kvpb.RangeFeedError{
			Error: *err,
		})
		sm.AddStream(streamID, &cancelCtxDisconnector{
			cancel: func() {
				streamCtxCancel()
				require.NoError(t, ubs.sendBuffered(makeMuxRangefeedErrorEvent(streamID, rangeID, nil), nil))
			},
		})
		// Note that kvpb.NewError(nil) == nil.
		require.Equal(t, 1, testRangefeedCounter.get())
		sm.DisconnectStream(streamID, err)
		// todo(wait for error)
		testServerStream.waitForEvent(t, ev)
		require.Equal(t, 0, testRangefeedCounter.get())
		require.Equal(t, context.Canceled, streamCtx.Err())
		require.Equal(t, 1, testServerStream.totalEventsSent())

		// Repeat closing the stream does nothing.
		sm.DisconnectStream(streamID, err)
		testServerStream.waitForEvent(t, ev)
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

		require.Equal(t, 0, testRangefeedCounter.get())

		for _, muxError := range testRangefeedCompletionErrors {
			streamID := muxError.streamID
			rangeID := muxError.rangeID
			err := muxError.Error
			sm.AddStream(muxError.streamID, &cancelCtxDisconnector{
				cancel: func() {
					require.NoError(t, ubs.sendBuffered(makeMuxRangefeedErrorEvent(streamID, rangeID, kvpb.NewError(err)), nil))
				},
			})
		}

		require.Equal(t, 3, testRangefeedCounter.get())

		var wg sync.WaitGroup
		for _, muxError := range testRangefeedCompletionErrors {
			wg.Add(1)
			go func(streamID int64, rangeID roachpb.RangeID, err error) {
				defer wg.Done()
				sm.DisconnectStream(streamID, kvpb.NewError(err))
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
				return errors.Newf("expected error %v not found in %s",
					muxError, testServerStream.String())
			})
		}
		testRangefeedCounter.waitForRangefeedCount(t, 0)
	})
}

// TestUnbufferedSenderDisconnectOnBlockingIO tests that the
// UnbufferedSender.Disconnect doesn't block on IO.
func TestUnbufferedSenderDisconnectBlockingIO(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	const streamID = 0
	const rangeID = 1
	sp := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")}
	testServerStream := newTestServerStream()
	testRangefeedCounter := newTestRangefeedCounter()
	ubs := NewUnbufferedSender(testServerStream)
	sm := NewStreamManager(ubs, testRangefeedCounter)
	require.NoError(t, sm.Start(ctx, stopper))
	defer sm.Stop(ctx)

	disconnectErr := makeMuxRangefeedErrorEvent(streamID, rangeID,
		kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_NO_LEASEHOLDER)))

	streamCtx, streamCancel := context.WithCancel(context.Background())
	sm.AddStream(0, &cancelCtxDisconnector{
		cancel: func() {
			streamCancel()
			require.NoError(t, ubs.sendBuffered(disconnectErr, nil))
		},
	})

	ev := &kvpb.MuxRangeFeedEvent{
		StreamID: streamID,
		RangeID:  rangeID,
	}
	ev.MustSetValue(&kvpb.RangeFeedCheckpoint{
		Span:       sp,
		ResolvedTS: hlc.Timestamp{WallTime: 1},
	})
	require.NoError(t, sm.sender.sendUnbuffered(ev))
	require.Truef(t, testServerStream.hasEvent(ev),
		"expected event %v not found in %v", ev, testServerStream)

	// Block the stream.
	unblock := testServerStream.BlockSend()

	// Although stream is blocked, we should be able to disconnect the stream
	// without blocking.
	sm.DisconnectStream(streamID,
		kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_NO_LEASEHOLDER)))
	require.Equal(t, streamCtx.Err(), context.Canceled)
	unblock()
	time.Sleep(100 * time.Millisecond)
	// Receive the event after getting unblocked.

	require.Truef(t, testServerStream.hasEvent(disconnectErr),
		"expected event %v not found in %v", disconnectErr, testServerStream)
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
	sm := NewStreamManager(NewUnbufferedSender(testServerStream), testRangefeedCounter)
	require.NoError(t, sm.Start(ctx, stopper))
	defer sm.Stop(ctx)

	sm.AddStream(1, &cancelCtxDisconnector{
		cancel: func() {},
	})
	require.Equal(t, 1, testRangefeedCounter.get())

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			val := roachpb.Value{RawBytes: []byte("val"), Timestamp: hlc.Timestamp{WallTime: 1}}
			ev1 := new(kvpb.RangeFeedEvent)
			ev1.MustSetValue(&kvpb.RangeFeedValue{Key: keyA, Value: val, PrevValue: val})
			require.NoError(t, sm.sender.sendUnbuffered(&kvpb.MuxRangeFeedEvent{
				StreamID:       1,
				RangeID:        1,
				RangeFeedEvent: *ev1,
			}))
		}()
	}
	wg.Wait()

	require.Equal(t, 10, testServerStream.eventsSent)
}
