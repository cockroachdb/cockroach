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

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// testRangefeedCounter mocks rangefeed metrics for testing.
type testRangefeedCounter struct {
	count atomic.Int32
}

var _ RangefeedMetricsRecorder = &testRangefeedCounter{}

func newTestRangefeedCounter() *testRangefeedCounter {
	return &testRangefeedCounter{}
}

func (c *testRangefeedCounter) UpdateMetricsOnRangefeedConnect() {
	c.count.Add(1)
}

func (c *testRangefeedCounter) UpdateMetricsOnRangefeedDisconnect() {
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

var _ ServerStreamSender = &testServerStream{}

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

func (s *testServerStream) getEventsByStreamID(streamID int64) []*kvpb.MuxRangeFeedEvent {
	s.Lock()
	defer s.Unlock()
	return s.streamEvents[streamID]
}

func (s *testServerStream) iterateEvents(f func(id int64, events []*kvpb.MuxRangeFeedEvent) bool) {
	s.Lock()
	defer s.Unlock()
	for id, v := range s.streamEvents {
		if !f(id, v) {
			return
		}
	}
}

func (s *testServerStream) totalEventsFilterBy(f func(e *kvpb.MuxRangeFeedEvent) bool) int {
	s.Lock()
	defer s.Unlock()
	count := 0
	for _, v := range s.streamEvents {
		for _, streamEvent := range v {
			if f(streamEvent) {
				count++
			}
		}
	}
	return count
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
	s.Lock()
	defer s.Unlock()
	var str strings.Builder
	fmt.Fprintf(&str, "Total Events Sent: %d\n", len(s.streamEvents))
	for streamID, eventList := range s.streamEvents {
		fmt.Fprintf(&str, "\tStreamID:%d, Len:%d", streamID, len(eventList))
		for _, ev := range eventList {
			switch {
			case ev.Val != nil:
				fmt.Fprintf(&str, "\t\tvalue")
			case ev.Checkpoint != nil:
				fmt.Fprintf(&str, "\t\tcheckpoint")
			case ev.SST != nil:
				fmt.Fprintf(&str, "\t\tsst")
			case ev.DeleteRange != nil:
				fmt.Fprintf(&str, "\t\tdelete")
			case ev.Metadata != nil:
				fmt.Fprintf(&str, "\t\tmetadata")
			case ev.Error != nil:
				fmt.Fprintf(&str, "\t\terror")
			default:
				panic("unknown event type")
			}
		}
		fmt.Fprintf(&str, "\n")
	}
	return str.String()
}

func (s *testServerStream) SendIsThreadSafe() {}

// Send mocks grpc.ServerStream Send method. It only counts events and stores
// events by streamID in streamEvents.
func (s *testServerStream) SendUnbuffered(e *kvpb.MuxRangeFeedEvent) error {
	s.Lock()
	defer s.Unlock()
	s.eventsSent++
	s.streamEvents[e.StreamID] = append(s.streamEvents[e.StreamID], e)
	return nil
}

// BlockSend blocks any subsequent Send methods until the unblock callback is
// called.
func (s *testServerStream) BlockSend() (unblock func()) {
	s.Lock()
	var once sync.Once
	return func() {
		once.Do(s.Unlock) //nolint:deferunlockcheck
	}
}

func NewTestPerRangeEventSink(
	ctx context.Context,
	streamID int64,
	rangeID int64,
	streamMuxer *StreamMuxer,
	regType registrationType,
) Stream {
	r := NewPerRangeEventSink(ctx, roachpb.RangeID(rangeID), streamID, streamMuxer)
	switch regType {
	case unbuffered:
		return &BufferedPerRangeEventSink{
			PerRangeEventSink: r,
		}
	case buffered:
		return r
	default:
		panic("unknown registration type")
	}
}

func NewStreamMuxerWithOpts(
	sender ServerStreamSender, metrics RangefeedMetricsRecorder, regType registrationType,
) *StreamMuxer {
	switch regType {
	case unbuffered:
		return NewStreamMuxer(&BufferedStreamSender{sender}, metrics)
	case buffered:
		return NewStreamMuxer(sender, metrics)
	default:
		panic("unknown registration type")
	}
}
