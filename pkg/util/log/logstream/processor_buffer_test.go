// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logstream

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const (
	noMaxStaleness = time.Duration(0)
	noTriggerLen   = 0
	noMaxLen       = 0
)

func TestTriggerLen(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buffer, mock := getMockProcessorBuffer(ctx, t, noMaxStaleness, 1 /* triggerLen */, noMaxLen)

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	element := "test_element"
	mock.EXPECT().Process(gomock.Any(), element).Do(addArgs(wg.Done))
	require.NoError(t, buffer.Process(ctx, element))
}

func TestMaxStaleness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buffer, mock := getMockProcessorBuffer(ctx, t, time.Second, noTriggerLen, noMaxLen)

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	element := "test_element"
	mock.EXPECT().Process(gomock.Any(), element).Do(addArgs(wg.Done))
	require.NoError(t, buffer.Process(ctx, element))
}

func TestMaxLen(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx, cancel := context.WithCancel(context.Background())
	buffer, mock := getMockProcessorBuffer(ctx, t, noMaxStaleness, noTriggerLen, 5)

	elementTmpl := "element%d"
	// When the ctx is cancelled at the end of the test, we expect the
	// final 5 elements (not including the one dropped) to be flushed.
	flushC := make(chan struct{})
	gomock.InOrder(
		mock.EXPECT().Process(ctx, "element2"),
		mock.EXPECT().Process(ctx, "element3"),
		mock.EXPECT().Process(ctx, "element4"),
		mock.EXPECT().Process(ctx, "element5"),
		mock.EXPECT().Process(ctx, "element6").Do(addArgs(func() { close(flushC) })))

	for i := 1; i <= 5; i++ {
		require.NoError(t, buffer.Process(ctx, fmt.Sprintf(elementTmpl, i)))
	}

	func() {
		buffer.mu.Lock()
		defer buffer.mu.Unlock()
		require.Equal(t, 5, buffer.mu.buf.len())
	}()

	// The 6th element should exceed the maximum, causing the buffer to drop
	// the oldest element ("element1")
	require.NoError(t, buffer.Process(ctx, "element6"))
	func() {
		buffer.mu.Lock()
		defer buffer.mu.Unlock()
		require.Equal(t, 5, buffer.mu.buf.len())
		require.Equal(t, "element2", buffer.mu.buf.events.Front().Value)
	}()

	// Now, cancel the context and wait for the remaining elements to drain.
	cancel()
	select {
	case <-flushC:
	case <-time.After(10 * time.Second):
		t.Fatalf("context cancellation did not trigger flush, or the flush timed out")
	}
}

func TestMultipleFlushes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buffer, mock := getMockProcessorBuffer(ctx, t, noMaxStaleness, 2 /*triggerLen*/, noMaxLen)

	flush1C := make(chan struct{})
	flush2C := make(chan struct{})
	waitWithTimeout := func(waitCh chan struct{}) {
		select {
		case <-waitCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("timeout, flush did not occur")
		}
	}

	e1 := "element1"
	e2 := "element2"
	e3 := "element3"
	e4 := "element4"

	gomock.InOrder(
		mock.EXPECT().
			Process(gomock.Any(), gomock.Eq(e1)),
		mock.EXPECT().
			Process(gomock.Any(), gomock.Eq(e2)).Do(addArgs(func() { close(flush1C) })),
		mock.EXPECT().
			Process(gomock.Any(), gomock.Eq(e3)),
		mock.EXPECT().
			Process(gomock.Any(), gomock.Eq(e4)).Do(addArgs(func() { close(flush2C) })))

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
	ctx, cancel := context.WithCancel(context.Background())
	buffer, mock := getMockProcessorBuffer(ctx, t, noMaxStaleness, noTriggerLen, noMaxLen)

	e1 := "element1"
	e2 := "element2"
	e3 := "element3"

	flushC := make(chan struct{})

	gomock.InOrder(
		mock.EXPECT().
			Process(gomock.Any(), gomock.Eq(e1)),
		mock.EXPECT().
			Process(gomock.Any(), gomock.Eq(e2)),
		mock.EXPECT().
			Process(gomock.Any(), gomock.Eq(e3)).Do(addArgs(func() { close(flushC) })))

	require.NoError(t, buffer.Process(ctx, e1))
	require.NoError(t, buffer.Process(ctx, e2))
	require.NoError(t, buffer.Process(ctx, e3))

	cancel()
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buffer, mock := getMockProcessorBuffer(ctx, t, noMaxStaleness, 1, noMaxLen)

	// We start by buffering a single event, which will immediately trigger a flush due to triggerLen.
	// We'll use flush1C to cause the underlying mock Processor to hang the first flush, which enables us to
	// queue up additional flushes & verify that .Process() is a non-blocking operation for clients.
	flush1C := make(chan struct{})
	firstEvent := "element1"
	mock.EXPECT().Process(ctx, firstEvent).Do(addArgs(func() {
		// Indicate that the flush has been triggered.
		flush1C <- struct{}{}
		// Nobody is listening on flush1C after the 1st signal, so this will hang (for now).
		<-flush1C
	}))
	require.NoError(t, buffer.Process(ctx, firstEvent))
	// Ensure that the first flush has been triggered (which should now be hanging).
	select {
	case <-flush1C:
	case <-time.After(10 * time.Second):
		t.Fatalf("first flush timed out")
	}

	// Now, we buffer a 2nd flush. The 2nd flush cannot begin until the 1st flush has completed.
	secondEvent := "element2"
	flush2C := make(chan any)
	mock.EXPECT().Process(gomock.Any(), secondEvent).Do(func(ctx context.Context, e any) {
		flush2C <- e
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
	// Now, let's make sure that a full flushC on the processorBuffer doesn't block on .Process().
	// Instead, we expect it to be buffered and included in the second flush.
	thirdEvent := "element3"
	processReturnedC := make(chan struct{})
	mock.EXPECT().Process(gomock.Any(), thirdEvent).Do(func(ctx context.Context, e any) {
		flush2C <- e
	})
	go func() {
		require.NoError(t, buffer.Process(ctx, thirdEvent))
		processReturnedC <- struct{}{}
	}()
	select {
	case <-processReturnedC:
	case <-time.After(100 * time.Millisecond):
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

func getMockProcessorBuffer(
	ctx context.Context, t *testing.T, maxStaleness time.Duration, triggerLen int, maxLen int,
) (*processorBuffer, *MockProcessor) {
	ctrl := gomock.NewController(t)
	mock := NewMockProcessor(ctrl)
	buffer := newProcessorBuffer(mock, maxStaleness, triggerLen, maxLen)
	buffer.Start(ctx)
	return buffer, mock
}

// addArgs adapts a zero-arg call for usage in gomock.Call.Do
func addArgs(f func()) func(context.Context, any) {
	return func(context.Context, any) {
		f()
	}
}
