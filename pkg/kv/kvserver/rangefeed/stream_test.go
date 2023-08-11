// Copyright 2023 The Cockroach Authors.
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
	"math"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func muxFeedEvent(id int64, key string) *kvpb.MuxRangeFeedEvent {
	e := &kvpb.MuxRangeFeedEvent{RangeID: roachpb.RangeID(id), StreamID: id}
	e.SetValue(&kvpb.RangeFeedValue{Key: roachpb.Key(key)})
	return e
}

func rfErr(id int64, err *kvpb.Error) *kvpb.MuxRangeFeedEvent {
	e := &kvpb.MuxRangeFeedEvent{RangeID: roachpb.RangeID(id), StreamID: id}
	rfe := &kvpb.RangeFeedError{Error: *err}
	e.SetValue(rfe)
	return e
}

type testMuxSink struct {
	ctx     context.Context
	cancel  func()
	pauseC  chan interface{}
	paused  chan interface{}
	resumeC chan interface{}

	mu struct {
		syncutil.Mutex
		sinkFailure error
		data        []*kvpb.MuxRangeFeedEvent
	}
}

var _ kvpb.MuxRangeFeedEventSink = (*testMuxSink)(nil)

func newTestMuxSink(ctx context.Context) *testMuxSink {
	ctx, cancel := context.WithCancel(ctx)
	return &testMuxSink{
		ctx:     ctx,
		cancel:  cancel,
		pauseC:  make(chan interface{}, 1),
		paused:  make(chan interface{}, 1),
		resumeC: make(chan interface{}),
	}
}

func (s *testMuxSink) Context() context.Context {
	return s.ctx
}

func (s *testMuxSink) Send(ev *kvpb.MuxRangeFeedEvent) error {
	select {
	case <-s.pauseC:
		select {
		case s.paused <- struct{}{}:
		default:
		}
		select {
		case <-s.resumeC:
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.data = append(s.mu.data, ev)
	return s.mu.sinkFailure
}

func (s *testMuxSink) pause() {
	select {
	case s.pauseC <- struct{}{}:
	default:
	}
}

func (s *testMuxSink) waitPaused() {
	select {
	case <-s.paused:
	case <-s.ctx.Done():
	}
}

func (s *testMuxSink) resume(t *testing.T) {
	t.Helper()
	select {
	case s.resumeC <- struct{}{}:
	case <-s.ctx.Done():
		t.Fatal("sink context was cancelled while trying to resume")
	}
}

func (s *testMuxSink) expectSoon(t *testing.T, evs ...*kvpb.MuxRangeFeedEvent) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if exp, actual := len(evs), len(s.mu.data); exp != actual {
			return errors.Newf("incorrect number of keys: expected %d, found %d", exp, actual)
		}
		j := 0
		for i, event := range s.mu.data {
			if event.Error != nil {
				continue
			}
			if j >= len(evs) {
				return errors.Newf("unexpected event %s in excess of expected %d", event, j)
			}
			require.Equal(t, evs[j], event, "unexpected value %s at index %d", i, event)
			j++
		}
		return nil
	})
}

func (s *testMuxSink) expectHas(t *testing.T, evs ...*kvpb.MuxRangeFeedEvent) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		j := 0
		for _, event := range s.mu.data {
			if event.Error != nil {
				continue
			}
			if assert.ObjectsAreEqual(evs[j], event) {
				j++
				if j >= len(evs) {
					return nil
				}
			}
		}
		return errors.Newf("only found events up to index %d", j)
	})
}

func (s *testMuxSink) expectHasNo(t *testing.T, ev *kvpb.MuxRangeFeedEvent) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, event := range s.mu.data {
			if event.Error != nil {
				continue
			}
			if assert.ObjectsAreEqual(ev, event) {
				return errors.Newf("expect event %s to not be present but found at %d", ev, i)
			}
		}
		return nil
	})
}

func (s *testMuxSink) expectErrorSoon(t *testing.T, id int64, err *kvpb.Error) {
	t.Helper()
	errEvent := rfErr(id, err)
	testutils.SucceedsSoon(t, func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, event := range s.mu.data {
			if assert.ObjectsAreEqual(errEvent, event) {
				break
			}
		}
		return nil
	})
}

func newEvent(key string) *kvpb.RangeFeedEvent {
	return &kvpb.RangeFeedEvent{Val: &kvpb.RangeFeedValue{Key: roachpb.Key(key)}}
}

func TestSingleUnbufferedFeed(t *testing.T) {
	ctx := context.Background()
	isDone := make(chan *kvpb.Error, 1)
	ts := newTestStream()
	ss := NewSingleFeedStream(ctx, isDone, ts.unbuffered())
	require.NoError(t, ss.Send(newEvent("a")), "failed to send")
	require.NoError(t, ss.Send(newEvent("b")), "failed to send")
	ss.Error(kvpb.NewError(
		kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED),
	))
	// Ensure we don't block on second stop attempt.
	doneC := make(chan interface{})
	go func() {
		ss.Error(kvpb.NewError(
			kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED),
		))
		close(doneC)
	}()
	select {
	case <-doneC:
	case <-time.After(30 * time.Second):
		t.Fatalf("failed to write second stream error within timeout.")
	}
}

type testSink struct {
	ctx    context.Context
	cancel func()
	block  chan interface{}
	resume chan interface{}

	mu struct {
		syncutil.Mutex
		sinkFailure error
		data        []*kvpb.RangeFeedEvent
	}
}

func newTestSink(ctx context.Context) *testSink {
	ctx, cancel := context.WithCancel(ctx)
	return &testSink{
		ctx:    ctx,
		cancel: cancel,
		block:  make(chan interface{}, 1),
		resume: make(chan interface{}),
	}
}

func (s *testSink) Context() context.Context {
	return s.ctx
}

func (s *testSink) Send(ev *kvpb.RangeFeedEvent) error {
	select {
	case <-s.block:
		select {
		case <-s.resume:
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.data = append(s.mu.data, ev)
	return s.mu.sinkFailure
}

func (s *testSink) Pause() {
	select {
	case s.block <- struct{}{}:
	default:
	}
}

func (s *testSink) Resume(t *testing.T) {
	t.Helper()
	select {
	case s.resume <- struct{}{}:
	case <-s.ctx.Done():
		t.Fatal("sink context was cancelled while trying to resume")
	}
}

func (s *testSink) SetError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.sinkFailure = err
}

func (s *testSink) ExpectSoon(t *testing.T, events ...*kvpb.RangeFeedEvent) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if exp, actual := len(events), len(s.mu.data); exp != actual {
			return errors.Newf("incorrect number of keys: expected %d, found %d", exp, actual)
		}
		require.EqualValues(t, events, s.mu.data, "incorrect events received")
		return nil
	})
}

// ExpectSoonAtMost checks that stream contains prefix of data.
func (s *testSink) ExpectSoonAtMost(t *testing.T, events ...*kvpb.RangeFeedEvent) {
	t.Helper()
	checkPrefix := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if exp, actual := len(events), len(s.mu.data); exp < actual {
			return errors.Newf("incorrect number of keys: expected at most %d, found %d", exp, actual)
		}
		for i, data := range s.mu.data {
			require.Equal(t, events[i], data, "wrong value at index %d", i)
		}
		return nil
	}
	testutils.SucceedsSoon(t, checkPrefix)
	<-time.After(time.Second)
	if err := checkPrefix(); err != nil {
		t.Fatalf("failed to verify update history: %s", err)
	}
}

func testFeedBudget(t *testing.T) (*FeedBudget, func(size int64) *SharedBudgetAllocation, func()) {
	budget := newTestBudget(math.MaxInt64)
	var allocs []*SharedBudgetAllocation
	return budget, func(size int64) *SharedBudgetAllocation {
			if size == 0 {
				return nil
			}
			alloc, err := budget.TryGet(context.Background(), size)
			require.NoError(t, err, "test has insufficient allocation budget set in `testFeedBudget`")
			allocs = append(allocs, alloc)
			return alloc
		}, func() {
			for _, alloc := range allocs {
				alloc.Release(context.Background())
			}
			allocs = nil
		}
}

func TestBufferedStreamSend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	sink := newTestSink(ctx)
	defer sink.cancel()
	budget, alloc, procRelease := testFeedBudget(t)
	defer budget.Close(ctx)

	drainDone := make(chan interface{})
	feedDone := make(chan *kvpb.Error, 1)
	s := NewSingleBufferedStream(sink.ctx, sink, 3000, false, func() {
		close(drainDone)
	})
	// Run feed worker loop in separate goroutine.
	go func() {
		feedDone <- s.Done()
	}()
	s.Send(feedEvent("a"), alloc(10))
	s.Send(feedEvent("b"), alloc(10))
	s.Send(feedEvent("c"), alloc(10))
	procRelease()

	sink.ExpectSoon(t, feedEvent("a"), feedEvent("b"), feedEvent("c"))
}

func TestBufferedStreamBackpressure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	sink := newTestSink(ctx)
	defer sink.cancel()
	budget, alloc, procRelease := testFeedBudget(t)
	defer budget.Close(ctx)
	defer procRelease()

	drainDone := make(chan interface{})
	feedDone := make(chan *kvpb.Error, 1)
	s := NewSingleBufferedStream(sink.ctx, sink, 1, true, func() {
		close(drainDone)
	})
	// Run feed worker loop in separate goroutine.
	go func() {
		feedDone <- s.Done()
	}()
	sink.Pause()
	wroteEvent := make(chan struct{})
	go func() {
		s.Send(feedEvent("a"), alloc(10))
		s.Send(feedEvent("b"), alloc(10))
		s.Send(feedEvent("c"), alloc(10))
		close(wroteEvent)
	}()
	once := sync.Once{}
	resume := func() {
		once.Do(func() {
			sink.Resume(t)
		})
	}
	// Need to resume if test fatals to release pending goroutine above.
	defer resume()
	select {
	case <-wroteEvent:
		t.Fatal("even was written or discarded despite backpressure")
	case <-time.After(3 * time.Second):
		// 3 sec wait for paused sink to not let event through.
	}
	resume()
	sink.ExpectSoon(t, feedEvent("a"), feedEvent("b"), feedEvent("c"))
}

func feedEvent(key string) *kvpb.RangeFeedEvent {
	return &kvpb.RangeFeedEvent{Val: &kvpb.RangeFeedValue{Key: roachpb.Key(key)}}
}

func TestBufferedStreamOverflow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	sink := newTestSink(ctx)
	defer sink.cancel()
	budget, alloc, procRelease := testFeedBudget(t)
	defer budget.Close(ctx)

	drainDone := make(chan interface{})
	feedDone := make(chan *kvpb.Error, 1)
	s := NewSingleBufferedStream(sink.ctx, sink, 3000, false, func() {
		close(drainDone)
	})
	// Run feed worker loop in separate goroutine.
	go func() {
		feedDone <- s.Done()
	}()
	sink.Pause()
	for i := 0; i < 4000; i++ {
		s.Send(feedEvent(fmt.Sprintf("%05d", i)), alloc(10))
	}
	procRelease()
	sink.Resume(t)
	waitStreamFailed(t, feedDone, newErrBufferCapacityExceeded())
	waitStreamNotified(t, drainDone)
	require.Zero(t, budget.mu.memBudget.Used(), "memory budget wasn't released")
}

func waitStreamFailed(t *testing.T, done chan *kvpb.Error, exp *kvpb.Error) {
	t.Helper()
	select {
	case err := <-done:
		require.Equal(t, exp, err, "wrong error")
	case <-time.After(30 * time.Second):
		t.Fatal("stream failed to terminate in 30 sec of overflow")
	}
}

func waitStreamNotified(t *testing.T, done chan interface{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("stream failed to get drain notification in 30 sec after cancel")
	}
}

func TestBufferedStreamCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	sink := newTestSink(ctx)
	defer sink.cancel()
	budget, alloc, procRelease := testFeedBudget(t)
	defer budget.Close(ctx)

	drainDone := make(chan interface{})
	feedDone := make(chan *kvpb.Error, 1)
	s := NewSingleBufferedStream(sink.ctx, sink, 3000, false, func() {
		close(drainDone)
	})
	// Run feed worker loop in separate goroutine.
	go func() {
		feedDone <- s.Done()
	}()
	s.Send(feedEvent("a"), alloc(10))
	s.Send(feedEvent("b"), alloc(10))
	s.Send(feedEvent("c"), alloc(10))
	procRelease()
	sink.cancel()
	waitStreamFailed(t, feedDone, kvpb.NewError(context.Canceled))
	waitStreamNotified(t, drainDone)
	require.Zero(t, budget.mu.memBudget.Used(), "memory budget wasn't released")
}

func TestBufferedStreamError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	sink := newTestSink(ctx)
	defer sink.cancel()
	budget, alloc, procRelease := testFeedBudget(t)
	defer budget.Close(ctx)

	drainDone := make(chan interface{})
	feedDone := make(chan *kvpb.Error, 1)
	s := NewSingleBufferedStream(sink.ctx, sink, 3000, false, func() {
		close(drainDone)
	})
	// Run feed worker loop in separate goroutine.
	go func() {
		feedDone <- s.Done()
	}()
	s.Send(feedEvent("a"), alloc(10))
	s.Send(feedEvent("b"), alloc(10))
	s.SendError(kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_REPLICA_REMOVED)))
	s.Send(feedEvent("c"), alloc(10))
	procRelease()
	waitStreamFailed(t, feedDone,
		kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_REPLICA_REMOVED)))
	sink.ExpectSoonAtMost(t, feedEvent("a"), feedEvent("b"), feedEvent("c"))
	waitStreamNotified(t, drainDone)
	require.Zero(t, budget.mu.memBudget.Used(), "memory budget wasn't released")
}

func TestBufferedMuxedStreamSend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	sink := newTestMuxSink(ctx)
	defer sink.cancel()
	muxOutput := NewStreamMuxer(sink, 1000)
	go func() {
		muxOutput.OutputLoop(ctx)
		t.Log("mux output loop finished")
	}()
	budget, alloc, procRelease := testFeedBudget(t)
	defer budget.Close(ctx)

	drainDone := make(chan interface{})
	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	s := NewMuxBufferedStream(reqCtx, 3, 3, func() {
		close(drainDone)
	}, muxOutput)
	s.Send(feedEvent("a"), alloc(10))
	s.Send(feedEvent("b"), alloc(10))
	s.Send(feedEvent("c"), alloc(10))
	procRelease()

	sink.expectSoon(
		t, muxFeedEvent(3, "a"), muxFeedEvent(3, "b"), muxFeedEvent(3, "c"))
}

func TestBufferedMuxedStreamOverflow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	sink := newTestMuxSink(ctx)
	defer sink.cancel()
	muxer := NewStreamMuxer(sink, 1000)
	go func() {
		muxer.OutputLoop(ctx)
	}()
	budget, alloc, procRelease := testFeedBudget(t)
	defer budget.Close(ctx)

	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	drain1Done := make(chan interface{})
	s1 := NewMuxBufferedStream(reqCtx, 3, 3, func() {
		close(drain1Done)
	}, muxer)
	drain2Done := make(chan interface{})
	s2 := NewMuxBufferedStream(reqCtx, 4, 4, func() {
		close(drain2Done)
	}, muxer)
	sink.pause()
	s2.Send(feedEvent("key"), alloc(10))
	for i := 0; i < 4000; i++ {
		s1.Send(feedEvent(fmt.Sprintf("%05d", i)), alloc(10))
	}
	procRelease()
	sink.resume(t)
	sink.expectErrorSoon(t, s1.streamID, newErrBufferCapacityExceeded())
	sink.expectErrorSoon(t, s2.streamID, newErrBufferCapacityExceeded())
	waitStreamNotified(t, drain1Done)
	waitStreamNotified(t, drain2Done)
	require.Zero(t, budget.mu.memBudget.Used(), "memory budget wasn't released")
}

func TestBufferedMuxedStreamCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	sink := newTestMuxSink(ctx)
	defer sink.cancel()
	muxer := NewStreamMuxer(sink, 1000)
	go func() {
		muxer.OutputLoop(ctx)
	}()
	budget, alloc, procRelease := testFeedBudget(t)
	defer budget.Close(ctx)

	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	drain1Done := make(chan interface{})
	s1 := NewMuxBufferedStream(reqCtx, 3, 3, func() {
		close(drain1Done)
	}, muxer)
	drain2Done := make(chan interface{})
	s2 := NewMuxBufferedStream(reqCtx, 4, 4, func() {
		close(drain2Done)
	}, muxer)

	// Test pauses sink, writes some data and then waits for sink to actually
	// stop with first send. It then cancels sink which would resume sink with
	// error and triggers feed termination.
	// Test then expects errors to be sent for every stream and that drain
	// notifications are sent.
	sink.pause()
	s1.Send(feedEvent("a"), alloc(10))
	s2.Send(feedEvent("b"), alloc(10))
	s1.Send(feedEvent("a"), alloc(10))
	s2.Send(feedEvent("b"), alloc(10))
	procRelease()
	sink.waitPaused()
	sink.cancel()
	sink.expectErrorSoon(t, s1.streamID, kvpb.NewError(context.Canceled))
	sink.expectErrorSoon(t, s2.streamID, kvpb.NewError(context.Canceled))
	waitStreamNotified(t, drain1Done)
	waitStreamNotified(t, drain2Done)
	require.Zero(t, budget.mu.memBudget.Used(), "memory budget wasn't released")
}

func TestBufferedMuxedStreamError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	sink := newTestMuxSink(ctx)
	defer sink.cancel()
	muxer := NewStreamMuxer(sink, 1000)
	go func() {
		muxer.OutputLoop(ctx)
	}()
	budget, alloc, procRelease := testFeedBudget(t)
	defer budget.Close(ctx)

	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	drain1Done := make(chan interface{})
	s1 := NewMuxBufferedStream(reqCtx, 3, 3, func() {
		close(drain1Done)
	}, muxer)
	drain2Done := make(chan interface{})
	s2 := NewMuxBufferedStream(reqCtx, 4, 4, func() {
		close(drain2Done)
	}, muxer)

	s1.Send(feedEvent("a"), alloc(10))
	s2.Send(feedEvent("b"), alloc(10))
	s1.SendError(kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_REPLICA_REMOVED)))
	s2.Send(feedEvent("c"), alloc(10))
	s1.Send(feedEvent("d"), alloc(10))
	s2.Send(feedEvent("e"), alloc(10))
	procRelease()
	sink.expectErrorSoon(t, s1.streamID,
		kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_REPLICA_REMOVED)))
	waitStreamNotified(t, drain1Done)
	sink.expectHas(
		t,
		muxFeedEvent(s2.streamID, "b"),
		muxFeedEvent(s2.streamID, "c"),
		muxFeedEvent(s2.streamID, "e"))
	sink.expectHasNo(t, muxFeedEvent(s1.streamID, "d"))
	require.Zero(t, budget.mu.memBudget.Used(), "memory budget wasn't released")
}
