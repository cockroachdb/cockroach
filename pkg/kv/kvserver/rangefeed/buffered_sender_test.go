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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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
	// TODO(wenyihu6): change disconnectAll to Stop later on.
	defer bs.disconnectAll()

	t.Run("basic operation", func(t *testing.T) {
		const streamID = 0
		var num atomic.Int32
		streamCtx, cancel := context.WithCancel(context.Background())
		bs.AddStream(int64(streamID), cancel)
		bs.RegisterRangefeedCleanUp(int64(streamID), func() {
			num.Add(1)
		})
		bs.SendBufferedError(makeMuxRangefeedErrorEvent(int64(streamID), 1, kvpb.NewError(nil)))
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
		bs.AddStream(int64(streamID), cancel)
		require.Equal(t, int32(1), testRangefeedCounter.get())

		// Disconnect stream without registering clean up should still work.
		bs.SendBufferedError(makeMuxRangefeedErrorEvent(int64(streamID), 1, kvpb.NewError(nil)))
		require.Equal(t, context.Canceled, streamCtx.Err())
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
		time.Sleep(10 * time.Millisecond)
		require.Equal(t, int32(1), num.Load())
		require.Equal(t, int32(0), testRangefeedCounter.get())

		// Disconnecting the stream again should do nothing.
		bs.SendBufferedError(makeMuxRangefeedErrorEvent(int64(streamID), 1, kvpb.NewError(nil)))
		time.Sleep(10 * time.Millisecond)
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

	rng, _ := randutil.NewTestRand()
	var actualSum atomic.Int32

	// [streamIdStart,streamIDEnd) are in the active streams. streamIdStart <=
	// streamIDEnd. If streamIDStart == streamIDEnd, no elements yet. [0,
	// streamIdStart) are disconnected.
	streamIdStart := int64(0)
	streamIdEnd := int64(0)

	defer func() {
		bs.disconnectAll()
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
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int32(streamIdStart), actualSum.Load())
	require.Equal(t, streamIdStart, int64(testServerStream.totalEventsSent()))
	require.Equal(t, int32(streamIdEnd-streamIdStart), testRangefeedCounter.get())
}
