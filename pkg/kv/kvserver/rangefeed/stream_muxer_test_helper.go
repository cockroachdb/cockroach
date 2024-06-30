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
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

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

type testServerStream struct {
	syncutil.Mutex
	eventsSent   int
	streamEvents map[int64][]*kvpb.MuxRangeFeedEvent
	streamsDone  map[int64]chan error
}

func newTestServerStream() *testServerStream {
	return &testServerStream{
		streamEvents: make(map[int64][]*kvpb.MuxRangeFeedEvent),
		streamsDone:  make(map[int64]chan error),
	}
}

func (s *testServerStream) totalEventsSent() int {
	s.Lock()
	defer s.Unlock()
	return s.eventsSent
}

func (s *testServerStream) nonErrorEventsSentById(streamID int64) []*kvpb.RangeFeedEvent {
	return s.filterEventsSentById(streamID, func(e *kvpb.MuxRangeFeedEvent) bool {
		return e.Error == nil
	})
}

func (s *testServerStream) filterEventsSentById(
	streamID int64, condition func(*kvpb.MuxRangeFeedEvent) bool,
) (rangefeedEvents []*kvpb.RangeFeedEvent) {
	s.Lock()
	defer s.Unlock()
	sent := s.streamEvents[streamID]
	s.streamEvents[streamID] = nil
	for _, e := range sent {
		if condition(e) {
			rangefeedEvents = append(rangefeedEvents, &e.RangeFeedEvent)
		}
	}
	return
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
	if e.Error != nil {
		if doneCh, ok := s.streamsDone[e.StreamID]; ok {
			var t *kvpb.RangeFeedRetryError
			if errors.As(e.Error.Error.GoError(), &t) && t.Reason == kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED {
				// StreamMuxer converts nil errors to RANGEFEED_CLOSED when sending to
				// the client. We revert it back to nil here to maintain expected
				// behavior. Ideally, a testing knob should be added to disable this
				// transformation, but we'll keep it simple for now.
				doneCh <- nil
			} else {
				doneCh <- e.Error.Error.GoError()
			}
		}
	}
	return nil
}

func (s *testServerStream) registerDoneCh(streamID int64, doneCh chan error) {
	s.Lock()
	defer s.Unlock()
	s.streamsDone[streamID] = doneCh
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
	return newTestStreamMuxerWithRangefeedCounter(t, ctx, stopper, sender, newTestRangefeedCounter())
}

func newTestStreamMuxerWithRangefeedCounter(
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

// TestCleanUpOnlyStreamMuxer is a helper struct to mock StreamMuxer for
// testing. It only handles rangefeed cleanup callback.
type TestCleanUpOnlyStreamMuxer struct {
	rangefeedCleanUps sync.Map
	notifyCleanUp     chan int64
}

func NewTestCleanUpOnlyStreamMuxer(
	stopper *stop.Stopper,
) (muxer *TestCleanUpOnlyStreamMuxer, cleanUp func()) {
	muxer = &TestCleanUpOnlyStreamMuxer{
		notifyCleanUp: make(chan int64, 20),
	}
	var wg sync.WaitGroup
	wg.Add(1)
	// Make sure to shut down the muxer before wg.Wait().
	if err := stopper.RunAsyncTask(context.Background(), "mux-term-forwarder", func(ctx context.Context) {
		defer wg.Done()
		muxer.Run(ctx, stopper)
	}); err != nil {
		wg.Done()
	}
	return muxer, wg.Wait
}

func (m *TestCleanUpOnlyStreamMuxer) RegisterRangefeedCleanUp(streamID int64, cleanUp func()) {
	m.rangefeedCleanUps.Store(streamID, cleanUp)
}

func (m *TestCleanUpOnlyStreamMuxer) DisconnectRangefeedWithError(streamID int64) {
	m.notifyCleanUp <- streamID
}

func (m *TestCleanUpOnlyStreamMuxer) Run(ctx context.Context, stopper *stop.Stopper) {
	for {
		select {
		case streamID := <-m.notifyCleanUp:
			if cleanUp, ok := m.rangefeedCleanUps.LoadAndDelete(streamID); ok {
				if f, ok := cleanUp.(func()); ok {
					f()
				}
			}
		case <-ctx.Done():
			return
		case <-stopper.ShouldQuiesce():
			return
		}
	}
}

type testSingleFeedStream struct {
	ctx      context.Context
	streamID int64
	rangeID  roachpb.RangeID
	// Signal rangefeed completion.
	doneCh  chan error
	wrapped *StreamMuxer

	// For blocking send only.
	mu struct {
		syncutil.Mutex
		sendErr error
	}
}

// For simplicity, test streams usually do not utilize the actual muxer but only
// implemented the interface. This is a special one that uses the actual muxer
// so tha we have more test coverage on the muxer. This struct mocks
// setRangeIDEventSink.
func newTestSingleFeedStream(
	streamID int64, muxer *StreamMuxer, serverStream *testServerStream,
) *testSingleFeedStream {
	ctx, done := context.WithCancel(context.Background())
	muxer.AddStream(streamID, 1, done)
	doneCh := make(chan error, 1)
	serverStream.registerDoneCh(streamID, doneCh)
	return &testSingleFeedStream{
		ctx:      ctx,
		streamID: streamID,
		rangeID:  1,
		wrapped:  muxer,
		doneCh:   doneCh,
	}
}

func (s *testSingleFeedStream) Context() context.Context {
	return s.ctx
}

func (s *testSingleFeedStream) Send(event *kvpb.RangeFeedEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.sendErr != nil {
		return s.mu.sendErr
	}
	response := &kvpb.MuxRangeFeedEvent{
		RangeFeedEvent: *event,
		RangeID:        s.rangeID,
		StreamID:       s.streamID,
	}
	return s.wrapped.Send(response)
}

func (s *testSingleFeedStream) SetSendErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.sendErr = err
}

func (s *testSingleFeedStream) Disconnect(err *kvpb.Error) {
	s.wrapped.DisconnectRangefeedWithError(s.streamID, s.rangeID, err)
}

func (s *testSingleFeedStream) RegisterRangefeedCleanUp(rangefeedCleanUp func()) {
	s.wrapped.RegisterRangefeedCleanUp(s.streamID, rangefeedCleanUp)
}

func (s *testSingleFeedStream) BlockSend() func() {
	s.mu.Lock()
	var once sync.Once
	return func() {
		// safe to call multiple times, e.g. defer and explicit
		once.Do(s.mu.Unlock) //nolint:deferunlockcheck
	}
}

func (s *testSingleFeedStream) WaitForErr(t *testing.T) error {
	select {
	case err := <-s.doneCh:
		return err
	case <-time.After(30 * time.Second):
		t.Fatalf("time out waiting for rangefeed completion")
		return nil
	}
}

func (s *testSingleFeedStream) GetErrIfDone() error {
	select {
	case err := <-s.doneCh:
		return err
	default:
		return nil
	}
}
