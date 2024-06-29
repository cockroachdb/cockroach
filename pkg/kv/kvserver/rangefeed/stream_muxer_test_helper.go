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
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// testRangefeedCounter mocks nodeMetrics for testing.
type testRangefeedCounter struct {
	count atomic.Int32
}

func newTestRangefeedCounter() *testRangefeedCounter {
	return &testRangefeedCounter{}
}

func (c *testRangefeedCounter) IncrementRangefeedCounter() {
	c.count.Add(1)
}

func (c *testRangefeedCounter) DecrementRangefeedCounter() {
	c.count.Add(-1)
}

func (c *testRangefeedCounter) get() int32 {
	return c.count.Load()
}

// testServerStream mocks grpc server stream for testing.
type testServerStream struct {
	syncutil.Mutex
	// eventsSent is the total number of events sent.
	eventsSent int
	// streamEvents is a map of streamID to a list of events sent to that stream.
	streamEvents map[int64][]*kvpb.MuxRangeFeedEvent
}

func newTestServerStream() *testServerStream {
	return &testServerStream{
		streamEvents: make(map[int64][]*kvpb.MuxRangeFeedEvent),
	}
}

func (s *testServerStream) totalEventsSent() int {
	s.Lock()
	defer s.Unlock()
	return s.eventsSent
}

// hasEvent returns true if the event is found in the streamEvents map. Note
// that it does a deep equal comparison.
func (s *testServerStream) hasEvent(e *kvpb.MuxRangeFeedEvent) bool {
	if e == nil {
		return false
	}
	s.Lock()
	defer s.Unlock()
	for _, streamEvent := range s.streamEvents[e.StreamID] {
		if reflect.DeepEqual(e, streamEvent) {
			return true
		}
	}
	return false
}

// String returns a string representation of the events sent in the stream.
func (s *testServerStream) String() string {
	str := strings.Builder{}
	for streamID, eventList := range s.streamEvents {
		str.WriteString(
			fmt.Sprintf("StreamID:%d, Len:%d\n", streamID, len(eventList)))
	}
	return str.String()
}

func (s *testServerStream) SendIsThreadSafe() {}

func (s *testServerStream) Send(e *kvpb.MuxRangeFeedEvent) error {
	s.Lock()
	defer s.Unlock()
	s.eventsSent++
	s.streamEvents[e.StreamID] = append(s.streamEvents[e.StreamID], e)
	return nil
}

func (s *testServerStream) BlockSend() func() {
	s.Lock()
	var once sync.Once
	return func() {
		// safe to call multiple times, e.g. defer and explicit
		once.Do(s.Unlock) //nolint:deferunlockcheck
	}
}

// NewTestStreamMuxer is a helper function to create a StreamMuxer for testing.
// It uses the actual muxer. Example usage:
//
// serverStream := newTestServerStream()
// streamMuxer, cleanUp := NewTestStreamMuxer(t, ctx, stopper, serverStream)
// defer cleanUp()
// defer stopper.Stop(ctx) // or defer cancel() - important to stop the muxer before cleanUp()
func NewTestStreamMuxer(
	t *testing.T,
	ctx context.Context,
	stopper *stop.Stopper,
	sender severStreamSender,
	metrics rangefeedMetricsRecorder,
) (muxer *StreamMuxer, cleanUp func()) {
	muxer = NewStreamMuxer(sender, metrics)
	var wg sync.WaitGroup
	wg.Add(1)
	if err := stopper.RunAsyncTask(ctx, "mux-term-forwarder", func(ctx context.Context) {
		defer wg.Done()
		muxer.Run(ctx, stopper)
	}); err != nil {
		wg.Done()
		t.Fatal(err)
	}
	return muxer, wg.Wait
}
