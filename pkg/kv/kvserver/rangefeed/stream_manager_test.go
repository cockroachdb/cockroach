// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestStreamManagerDisconnectStream tests that StreamManager can handle stream
// disconnects properly including context canceled, metrics updates, rangefeed
// cleanup.
func TestStreamManagerDisconnectStream(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	testutils.RunValues(t, "feed type", testTypes, func(t *testing.T, rt rangefeedTestType) {
		testServerStream := newTestServerStream()
		testRangefeedCounter := newTestRangefeedCounter()
		var s sender
		switch rt {
		case scheduledProcessorWithUnbufferedSender:
			s = NewUnbufferedSender(testServerStream)
		default:
			t.Fatalf("unknown rangefeed test type %v", rt)
		}

		sm := NewStreamManager(s, testRangefeedCounter)
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
			require.Equal(t, 1, testRangefeedCounter.get())
			require.Equal(t, 0, testServerStream.totalEventsSent())
			sm.DisconnectStream(int64(streamID), err)
			testServerStream.waitForEvent(t, errEvent)
			require.Equal(t, int32(1), num.Load())
			require.Equal(t, 1, testServerStream.totalEventsSent())
			testRangefeedCounter.waitForRangefeedCount(t, 0)
			testServerStream.reset()
		})
		t.Run("disconnect stream on the same stream is idempotent", func(t *testing.T) {
			sm.AddStream(int64(streamID), &cancelCtxDisconnector{
				cancel: func() {
					require.NoError(t, sm.sender.sendBuffered(errEvent, nil))
				},
			})
			require.Equal(t, 1, testRangefeedCounter.get())
			sm.DisconnectStream(int64(streamID), err)
			sm.DisconnectStream(int64(streamID), err)
			testServerStream.waitForEvent(t, errEvent)
			require.Equalf(t, 1, testServerStream.totalEventsSent(),
				"expected only 1 error event but got %s", testServerStream.String())
			testRangefeedCounter.waitForRangefeedCount(t, 0)
		})
	})
}

// TestStreamManagerChaosWithStop tests that StreamManager can handle a mix of
// AddStream, DisconnectStream with Stop properly.
func TestStreamManagerChaosWithStop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	testutils.RunValues(t, "feed type", testTypes, func(t *testing.T, rt rangefeedTestType) {
		testServerStream := newTestServerStream()
		testRangefeedCounter := newTestRangefeedCounter()
		var s sender
		switch rt {
		case scheduledProcessorWithUnbufferedSender:
			s = NewUnbufferedSender(testServerStream)
		default:
			t.Fatalf("unknown rangefeed test type %v", rt)
		}
		sm := NewStreamManager(s, testRangefeedCounter)
		require.NoError(t, sm.Start(ctx, stopper))

		rng, _ := randutil.NewTestRand()

		// [activeStreamStart,activeStreamEnd) are in the active streams.
		// activeStreamStart <= activeStreamEnd. If activeStreamStart ==
		// activeStreamEnd, no streams are active yet. [0, activeStreamStart) are
		// disconnected.
		var actualSum atomic.Int32
		activeStreamStart := int64(0)
		activeStreamEnd := int64(0)

		t.Run("mixed operations of add and disconnect stream", func(t *testing.T) {
			const ops = 1000
			var wg sync.WaitGroup
			for i := 0; i < ops; i++ {
				addStream := rng.Intn(2) == 0
				require.LessOrEqualf(t, activeStreamStart, activeStreamEnd, "test programming error")
				if addStream || activeStreamStart == activeStreamEnd {
					streamID := activeStreamEnd
					sm.AddStream(streamID, &cancelCtxDisconnector{
						cancel: func() {
							actualSum.Add(1)
							_ = sm.sender.sendBuffered(
								makeMuxRangefeedErrorEvent(streamID, 1, newErrBufferCapacityExceeded()), nil)
						},
					})
					activeStreamEnd++
				} else {
					wg.Add(1)
					go func(id int64) {
						defer wg.Done()
						sm.DisconnectStream(id, newErrBufferCapacityExceeded())
					}(activeStreamStart)
					activeStreamStart++
				}
			}

			wg.Wait()
			require.Equal(t, int32(activeStreamStart), actualSum.Load())
			testServerStream.waitForEventCount(t, int(activeStreamStart))
			expectedActiveStreams := activeStreamEnd - activeStreamStart
			require.Equal(t, int(expectedActiveStreams), sm.activeStreamCount())
			testRangefeedCounter.waitForRangefeedCount(t, int(expectedActiveStreams))
		})

		t.Run("stream manager on stop", func(t *testing.T) {
			sm.Stop(ctx)
			require.Equal(t, 0, testRangefeedCounter.get())
			require.Equal(t, 0, sm.activeStreamCount())
			// Cleanup functions should be called for all active streams.
			require.Equal(t, int32(activeStreamEnd), actualSum.Load())
			// No error events should be sent during Stop().
			require.Equal(t, activeStreamStart, int64(testServerStream.totalEventsSent()))
		})
	})
}

// TestStreamManagerErrorHandling tests that StreamManager can handle different
// ways of errors properly.
func TestStreamManagerErrorHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testutils.RunValues(t, "feed type", testTypes, func(t *testing.T, rt rangefeedTestType) {
		testServerStream := newTestServerStream()
		testRangefeedCounter := newTestRangefeedCounter()
		var s sender
		switch rt {
		case scheduledProcessorWithUnbufferedSender:
			s = NewUnbufferedSender(testServerStream)
		case scheduledProcessorWithBufferedSender:
			s = NewBufferedSender(testServerStream)
		default:
			t.Fatalf("unknown rangefeed test type %v", rt)
		}

		sm := NewStreamManager(s, testRangefeedCounter)
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		require.NoError(t, sm.Start(ctx, stopper))
		const sID, rID = int64(0), 1
		disconnectErr := kvpb.NewError(fmt.Errorf("disconnection error"))

		expectErrorHandlingInvariance := func(p Processor) {
			testRangefeedCounter.waitForRangefeedCount(t, 0)
			testutils.SucceedsSoon(t, func() error {
				if p.Len() == 0 {
					return nil
				}
				return errors.Newf("expected 0 registrations, found %d", p.Len())
			})
			testServerStream.waitForEvent(t, makeMuxRangefeedErrorEvent(sID, rID, disconnectErr))
			require.Equalf(t, 1, testServerStream.totalEventsFilterBy(
				func(e *kvpb.MuxRangeFeedEvent) bool {
					return e.Error != nil
				}), "expected only 1 error event in %s", testServerStream.String())
		}
		t.Run("Fail to register rangefeed with the processor", func(t *testing.T) {
			p, _, stopper := newTestProcessor(t, withRangefeedTestType(rt))
			defer stopper.Stop(ctx)
			sm.NewStream(sID, rID)
			// We mock failed registration by not calling p.Register.
			// node.MuxRangefeed would call sendBuffered with error event.
			require.NoError(t, sm.sender.sendBuffered(makeMuxRangefeedErrorEvent(sID, rID, disconnectErr), nil))
			expectErrorHandlingInvariance(p)
			testServerStream.reset()
		})
		t.Run("Disconnect stream after registration with processor but before adding to stream manager",
			func(t *testing.T) {
				p, h, stopper := newTestProcessor(t, withRangefeedTestType(rt))
				defer stopper.Stop(ctx)
				stream := sm.NewStream(sID, rID)
				registered, d, _ := p.Register(ctx, h.span, hlc.Timestamp{}, nil, /* catchUpIter */
					false /* withDiff */, false /* withFiltering */, false, /* withOmitRemote */
					stream)
				require.True(t, registered)
				go p.StopWithErr(disconnectErr)
				require.Equal(t, 0, testRangefeedCounter.get())
				sm.AddStream(sID, d)
				expectErrorHandlingInvariance(p)
				testServerStream.reset()
			})
		t.Run("Disconnect stream after registration with processor and stream manager", func(t *testing.T) {
			stream := sm.NewStream(sID, rID)
			p, h, stopper := newTestProcessor(t, withRangefeedTestType(rt))
			defer stopper.Stop(ctx)
			registered, d, _ := p.Register(ctx, h.span, hlc.Timestamp{}, nil, /* catchUpIter */
				false /* withDiff */, false /* withFiltering */, false, /* withOmitRemote */
				stream)
			require.True(t, registered)
			sm.AddStream(sID, d)
			require.Equal(t, 1, p.Len())
			require.Equal(t, 1, testRangefeedCounter.get())
			sm.DisconnectStream(sID, disconnectErr)
			expectErrorHandlingInvariance(p)
			testServerStream.reset()
		})
		t.Run("Stream manager disconnects everything", func(t *testing.T) {
			stream := sm.NewStream(sID, rID)
			p, h, stopper := newTestProcessor(t, withRangefeedTestType(rt))
			defer stopper.Stop(ctx)
			registered, d, _ := p.Register(ctx, h.span, hlc.Timestamp{}, nil, /* catchUpIter */
				false /* withDiff */, false /* withFiltering */, false, /* withOmitRemote */
				stream)
			require.True(t, registered)
			sm.AddStream(sID, d)
			require.Equal(t, 1, testRangefeedCounter.get())
			require.Equal(t, 1, p.Len())
			sm.Stop(ctx)
			// No disconnect events should be sent during Stop().
			testRangefeedCounter.waitForRangefeedCount(t, 0)
			testutils.SucceedsSoon(t, func() error {
				if p.Len() == 0 {
					return nil
				}
				return errors.Newf("expected 0 registrations, found %d", p.Len())
			})
		})
	})
}
