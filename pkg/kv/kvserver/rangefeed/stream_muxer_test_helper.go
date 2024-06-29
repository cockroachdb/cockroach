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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type testServerStream struct {
	syncutil.Mutex
	eventsSent   int
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

func (s *testServerStream) String() string {
	str := strings.Builder{}
	for streamID, eventList := range s.streamEvents {
		str.WriteString(
			fmt.Sprintf("StreamID:%d, Len:%d\n", streamID, len(eventList)))
	}
	return str.String()
}

func (s *testServerStream) Send(e *kvpb.MuxRangeFeedEvent) error {
	s.Lock()
	defer s.Unlock()
	s.eventsSent++
	s.streamEvents[e.StreamID] = append(s.streamEvents[e.StreamID], e)
	return nil
}

// NewTestStreamMuxer is a helper function to create a StreamMuxer for testing.
// It uses the actual muxer. Example usage:
//
// serverStream := newTestServerStream()
// streamMuxer, cleanUp := NewTestStreamMuxer(t, ctx, stopper, serverStream)
// defer cleanUp()
// defer stopper.Stop(ctx) // or defer cancel() - important to stop the muxer before cleanUp()
func NewTestStreamMuxer(
	t *testing.T, ctx context.Context, stopper *stop.Stopper, sender severStreamSender,
) (muxer *StreamMuxer, cleanUp func()) {
	muxer = NewStreamMuxer(sender)
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
