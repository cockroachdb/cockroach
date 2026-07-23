// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstream

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const (
	noMaxStaleness = time.Duration(0)
	noTriggerLen   = 0
	noMaxLen       = 0
)

var (
	type1 = log.EventType("type1")
	type2 = log.EventType("type2")
)

func makeTypedEvent(eventType log.EventType, event any) *typedEvent {
	return &typedEvent{
		eventType: eventType,
		event:     event,
	}
}

func TestAsyncProcessorBuffer_register(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	buffer := getMockAsyncProcessorRouter(ctx, t, stopper, noMaxStaleness, 1 /* triggerLen */, noMaxLen)

	ctrl := gomock.NewController(t)
	mock1A := NewMockProcessor(ctrl)
	mock1B := NewMockProcessor(ctrl)
	mock2A := NewMockProcessor(ctrl)
	mock2B := NewMockProcessor(ctrl)

	buffer.register(type1, mock1A)
	buffer.register(type1, mock1B)
	func() {
		buffer.rwmu.Lock()
		defer buffer.rwmu.Unlock()
		type1Processors := buffer.rwmu.routes[type1]
		require.Equal(t, []Processor{mock1A, mock2A}, type1Processors)
	}()

	buffer.register(type2, mock2A)
	buffer.register(type2, mock2B)
	func() {
		buffer.rwmu.Lock()
		defer buffer.rwmu.Unlock()
		type2Processors := buffer.rwmu.routes[type2]
		require.Equal(t, []Processor{mock1B, mock2B}, type2Processors)
	}()
}

func TestAsyncProcessorRouter_RoutesOnEventType(t *testing.T) {
	t.Run("routes based on event type to the correct processor(s)", func(t *testing.T) {
		defer leaktest.AfterTest(t)()
		defer log.Scope(t).Close(t)
		ctx := context.Background()
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		buffer := getMockAsyncProcessorRouter(ctx, t, stopper, noMaxStaleness, 1 /* triggerLen */, noMaxLen)
		mock1A := registerMockRoute(t, buffer, type1)
		mock1B := registerMockRoute(t, buffer, type1)
		mock2A := registerMockRoute(t, buffer, type2)
		mock2B := registerMockRoute(t, buffer, type2)

		event1 := makeTypedEvent(type1, "eventA")
		event2 := makeTypedEvent(type2, "eventB")

		mock1A.EXPECT().Process(gomock.Any(), event1.event)
		mock1B.EXPECT().Process(gomock.Any(), event1.event)
		mock2A.EXPECT().Process(gomock.Any(), event2.event)
		mock2B.EXPECT().Process(gomock.Any(), event2.event)

		require.NoError(t, buffer.Process(ctx, event1))
		require.NoError(t, buffer.Process(ctx, event2))
	})

	t.Run("gracefully handles events for which there's no registered processor", func(t *testing.T) {
		defer leaktest.AfterTest(t)()
		defer log.Scope(t).Close(t)
		ctx := context.Background()
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		buffer := getMockAsyncProcessorRouter(ctx, t, stopper, noMaxStaleness, 1 /* triggerLen */, noMaxLen)
		unregisteredEventType := log.EventType("unregistered")
		event := &typedEvent{
			eventType: unregisteredEventType,
			event:     "event",
		}
		require.NoError(t, buffer.Process(ctx, event))
	})
}

func TestTriggerLen(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	buffer := getMockAsyncProcessorRouter(ctx, t, stopper, noMaxStaleness, 1 /* triggerLen */, noMaxLen)
	mock := registerMockRoute(t, buffer, type1)

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	element := makeTypedEvent(type1, "test_element")
	mock.EXPECT().Process(gomock.Any(), element.event).Do(addArgs(wg.Done))
	require.NoError(t, buffer.Process(ctx, element))
}

func TestMaxStaleness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	buffer := getMockAsyncProcessorRouter(ctx, t, stopper, time.Second, noTriggerLen, noMaxLen)
	mock := registerMockRoute(t, buffer, type1)

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	element := makeTypedEvent(type1, "test_element")
	mock.EXPECT().Process(gomock.Any(), element.event).Do(addArgs(wg.Done))
	require.NoError(t, buffer.Process(ctx, element))
}

func TestMaxLen(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	buffer := getMockAsyncProcessorRouter(ctx, t, stopper, noMaxStaleness, noTriggerLen, 5)
	mock := registerMockRoute(t, buffer, type1)

	elementTmpl := "element%d"

	// When the stopper is triggered at the end of the test, we expect the
	// final 5 elements (not including the one dropped) to be flushed.
	flushC := make(chan struct{})
	gomock.InOrder(
		mock.EXPECT().Process(gomock.Any(), "element2"),
		mock.EXPECT().Process(gomock.Any(), "element3"),
		mock.EXPECT().Process(gomock.Any(), "element4"),
		mock.EXPECT().Process(gomock.Any(), "element5"),
		mock.EXPECT().Process(gomock.Any(), "element6").Do(addArgs(func() { close(flushC) })))

	for i := 1; i <= 5; i++ {
		require.NoError(t, buffer.Process(ctx, makeTypedEvent(type1, fmt.Sprintf(elementTmpl, i))))
	}

	func() {
		buffer.mu.Lock()
		defer buffer.mu.Unlock()
		require.Equal(t, 5, buffer.mu.buf.len())
	}()

	// The 6th element should exceed the maximum, causing the buffer to drop
	// the oldest element ("element1")
	require.NoError(t, buffer.Process(ctx, makeTypedEvent(type1, "element6")))
	func() {
		buffer.mu.Lock()
		defer buffer.mu.Unlock()
		require.Equal(t, 5, buffer.mu.buf.len())
		require.Equal(t, "element2", buffer.mu.buf.events.Front().Value.(*typedEvent).event)
	}()

	// Now, trigger the stopper and wait for the remaining elements to drain.
	stopper.Stop(ctx)
	select {
	case <-flushC:
	case <-time.After(10 * time.Second):
		t.Fatalf("context cancellation did not trigger flush, or the flush timed out")
	}
}

func TestMultipleFlushes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	buffer := getMockAsyncProcessorRouter(ctx, t, stopper, noMaxStaleness, 2 /*triggerLen*/, noMaxLen)
	mock := registerMockRoute(t, buffer, type1)

	flush1C := make(chan struct{})
	flush2C := make(chan struct{})
	waitWithTimeout := func(waitCh chan struct{}) {
		select {
		case <-waitCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("timeout, flush did not occur")
		}
	}

	e1 := makeTypedEvent(type1, "element1")
	e2 := makeTypedEvent(type1, "element2")
	e3 := makeTypedEvent(type1, "element3")
	e4 := makeTypedEvent(type1, "element4")

	gomock.InOrder(
		mock.EXPECT().
			Process(gomock.Any(), gomock.Eq(e1.event)),
		mock.EXPECT().
			Process(gomock.Any(), gomock.Eq(e2.event)).Do(addArgs(func() { close(flush1C) })),
		mock.EXPECT().
			Process(gomock.Any(), gomock.Eq(e3.event)),
		mock.EXPECT().
			Process(gomock.Any(), gomock.Eq(e4.event)).Do(addArgs(func() { close(flush2C) })))

	require.NoError(t, buffer.Process(ctx, e1))
	require.NoError(t, buffer.Process(ctx, e2))
	waitWithTimeout(flush1C)

	require.NoError(t, buffer.Process(ctx, e3))
	require.NoError(t, buffer.Process(ctx, e4))
	waitWithTimeout(flush2C)
}

func TestCtxDoneFlushesBeforeExit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	buffer := getMockAsyncProcessorRouter(ctx, t, stopper, noMaxStaleness, noTriggerLen, noMaxLen)
	mock := registerMockRoute(t, buffer, type1)

	e1 := makeTypedEvent(type1, "element1")
	e2 := makeTypedEvent(type1, "element2")
	e3 := makeTypedEvent(type1, "element3")

	flushC := make(chan struct{})

	gomock.InOrder(
		mock.EXPECT().
			Process(gomock.Any(), gomock.Eq(e1.event)),
		mock.EXPECT().
			Process(gomock.Any(), gomock.Eq(e2.event)),
		mock.EXPECT().
			Process(gomock.Any(), gomock.Eq(e3.event)).Do(addArgs(func() { close(flushC) })))

	require.NoError(t, buffer.Process(ctx, e1))
	require.NoError(t, buffer.Process(ctx, e2))
	require.NoError(t, buffer.Process(ctx, e3))

	stopper.Stop(ctx)
	select {
	case <-flushC:
	case <-time.After(10 * time.Second):
		t.Fatalf("context cancellation did not trigger flush, or the flush timed out")
	}
}

// Test that we still accept & buffer new events while a flush is in-flight, and the flush channel buffer is full.
func TestBlockedFlush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	buffer := getMockAsyncProcessorRouter(ctx, t, stopper, noMaxStaleness, 1, noMaxLen)
	mock := registerMockRoute(t, buffer, type1)

	// We start by buffering a single event, which will immediately trigger a flush due to triggerLen.
	// We'll use flush1C to cause the underlying mock processor to hang the first flush, which enables us to
	// queue up additional flushes & verify that .Process() is a non-blocking operation for clients.
	flush1C := make(chan struct{})
	firstEvent := makeTypedEvent(type1, "element1")
	mock.EXPECT().Process(gomock.Any(), firstEvent.event).Do(addArgs(func() {
		// Indicate that the flush has been triggered.
		flush1C <- struct{}{}
		// Nobody is listening on flush1C after the 1st signal, so this will hang (for now).
		select {
		case <-flush1C:
		case <-stopper.ShouldQuiesce():
			t.Log("first flush's Processor quiescing")
		}
	}))
	require.NoError(t, buffer.Process(ctx, firstEvent))
	// Ensure that the first flush has been triggered (which should now be hanging).
	select {
	case <-flush1C:
	case <-time.After(10 * time.Second):
		t.Fatalf("first flush timed out")
	}

	// Now, we buffer a 2nd flush. The 2nd flush cannot begin until the 1st flush has completed.
	secondEvent := makeTypedEvent(type1, "element2")
	flush2C := make(chan any)
	mock.EXPECT().Process(gomock.Any(), secondEvent.event).Do(func(ctx context.Context, e any) {
		select {
		case flush2C <- e:
		case <-stopper.ShouldQuiesce():
			t.Logf("second flush, first event's Processor quiescing")
		}
	})
	require.NoError(t, buffer.Process(ctx, secondEvent))
	select {
	case <-flush2C:
		t.Fatalf("Unexpected 2nd flush while first flush is in-flight")
	case <-time.After(10 * time.Millisecond):
		// 2nd flush didn't happen, which is good.
	}

	// By this point, we have:
	// 		- The 1st flush hanging.
	//		- The 2nd flush buffered on the flushC, waiting.
	// Now, let's make sure that a full flushC on the asyncProcessorRouter doesn't block on .Process().
	// Instead, we expect it to be buffered and included in the second flush.
	thirdEvent := makeTypedEvent(type1, "element3")
	processReturnedC := make(chan struct{})
	mock.EXPECT().Process(gomock.Any(), thirdEvent.event).Do(func(ctx context.Context, e any) {
		select {
		case flush2C <- e:
		case <-stopper.ShouldQuiesce():
			t.Logf("second flush, second event's Processor quiescing")
		}
	})
	go func() {
		require.NoError(t, buffer.Process(ctx, thirdEvent))
		select {
		case processReturnedC <- struct{}{}:
		case <-stopper.ShouldQuiesce():
			t.Logf("second Process call's goroutine quiescing")
		}
	}()
	select {
	case <-processReturnedC:
	case <-time.After(10 * time.Second):
		t.Fatalf("queued flushes appear to be blocking calls to .Process()")
	}

	// Now, we can unwind the entire stack of queued flushes.
	flush1C <- struct{}{}

	// We expect the second & third events to be given to us on flush2C.
	for i := 0; i < 2; i++ {
		select {
		case <-time.After(10 * time.Second):
			t.Fatalf("Timed out waiting for elements from the 2nd flush.")
		case out := <-flush2C:
			require.Equal(t, fmt.Sprintf("element%d", i+2), out)
		}
	}
}

func getMockAsyncProcessorRouter(
	ctx context.Context,
	t *testing.T,
	stop *stop.Stopper,
	maxStaleness time.Duration,
	triggerLen int,
	maxLen int,
) *asyncProcessorRouter {
	buffer := newAsyncProcessorRouter(maxStaleness, triggerLen, maxLen)
	require.NoError(t, buffer.Start(ctx, stop))
	return buffer
}

func registerMockRoute(
	t *testing.T, router *asyncProcessorRouter, eventType log.EventType,
) *MockProcessor {
	ctrl := gomock.NewController(t)
	mock := NewMockProcessor(ctrl)
	router.register(eventType, mock)
	return mock
}

// addArgs adapts a zero-arg call for usage in gomock.Call.Do
func addArgs(f func()) func(context.Context, any) {
	return func(context.Context, any) {
		f()
	}
}
