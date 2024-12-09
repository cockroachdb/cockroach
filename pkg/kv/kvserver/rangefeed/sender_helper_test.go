// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
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
	c.UpdateMetricsOnRangefeedDisconnectBy(1)
}

func (c *testRangefeedCounter) UpdateMetricsOnRangefeedDisconnectBy(num int64) {
	c.count.Add(int32(-num))
}

func (c *testRangefeedCounter) get() int {
	return int(c.count.Load())
}

func (c *testRangefeedCounter) waitForRangefeedCount(t *testing.T, count int) {
	testutils.SucceedsSoon(t, func() error {
		if c.get() == count {
			return nil
		}
		return errors.Newf("expected %d rangefeeds, found %d", count, c.get())
	})
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

func (s *testServerStream) getEventsByStreamID(streamID int64) (res []*kvpb.RangeFeedEvent) {
	s.Lock()
	defer s.Unlock()
	for _, ev := range s.streamEvents[streamID] {
		res = append(res, &ev.RangeFeedEvent)
	}
	return res
}

func (s *testServerStream) waitForEvent(t *testing.T, ev *kvpb.MuxRangeFeedEvent) {
	testutils.SucceedsSoon(t, func() error {
		if s.hasEvent(ev) {
			return nil
		}
		return errors.Newf("expected error %v not found in %s", *ev, s.String())
	})
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
	fmt.Fprintf(&str, "Total Streams Sent: %d\n", len(s.streamEvents))
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
func (s *testServerStream) Send(e *kvpb.MuxRangeFeedEvent) error {
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

// reset clears the state of the testServerStream.
func (s *testServerStream) reset() {
	s.Lock()
	defer s.Unlock()
	s.eventsSent = 0
	s.streamEvents = make(map[int64][]*kvpb.MuxRangeFeedEvent)
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

func (s *testServerStream) waitForEventCount(t *testing.T, count int) {
	testutils.SucceedsSoon(t, func() error {
		if s.totalEventsSent() == count {
			return nil
		}
		return errors.Newf("expected %d events, found %d", count, s.totalEventsSent())
	})
}

func (s *testServerStream) iterateEventsByStreamID(
	f func(id int64, events []*kvpb.MuxRangeFeedEvent),
) {
	s.Lock()
	defer s.Unlock()
	for id, v := range s.streamEvents {
		f(id, v)
	}
}

type cancelCtxDisconnector struct {
	mu struct {
		syncutil.Mutex
		disconnected bool
	}
	cancel func()
}

func (c *cancelCtxDisconnector) Disconnect(_ *kvpb.Error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.disconnected {
		return
	}
	c.mu.disconnected = true
	c.cancel()
}

func (c *cancelCtxDisconnector) IsDisconnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.disconnected
}
