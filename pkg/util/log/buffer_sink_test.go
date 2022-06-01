// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func getMockBufferSync(
	t *testing.T, maxStaleness time.Duration, sizeTrigger int, errCallback func(error),
) (sink *bufferSink, mock *MockLogSink, cleanup func()) {
	ctx, cancel := context.WithCancel(context.Background())
	ctrl := gomock.NewController(t)
	mock = NewMockLogSink(ctrl)
	sink = newBufferSink(ctx, mock, maxStaleness, sizeTrigger, 2 /* maxInFlight */, errCallback)
	cleanup = func() {
		cancel()
		ctrl.Finish()
	}
	return sink, mock, cleanup
}

// addArgs adapts a zero-arg call to take the args expected by logSink.output,
// for usage in gomock.Call.Do
func addArgs(f func()) func([]byte, sinkOutputOptions) {
	return func([]byte, sinkOutputOptions) {
		f()
	}
}

func TestBufferOneLine(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sink, mock, cleanup := getMockBufferSync(t, 0 /* maxStaleness*/, 0 /* sizeTrigger */, nil /* errCallback*/)
	defer cleanup()

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	message := []byte("test")
	mock.EXPECT().
		output(gomock.Eq(message), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)}).
		Do(addArgs(wg.Done))

	require.NoError(t, sink.output(message, sinkOutputOptions{extraFlush: true}))
}

func TestBufferManyLinesOneFlush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sink, mock, cleanup := getMockBufferSync(t, 0 /* maxStaleness*/, 0 /* sizeTrigger */, nil /* errCallback*/)
	defer cleanup()

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	message := []byte("test")
	mock.EXPECT().
		output(gomock.Eq([]byte("test\ntest")), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)}).
		Do(addArgs(wg.Done))

	require.NoError(t, sink.output(message, sinkOutputOptions{}))
	require.NoError(t, sink.output(message, sinkOutputOptions{extraFlush: true}))
}

func TestBufferMaxStaleness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sink, mock, cleanup := getMockBufferSync(t, time.Second /* maxStaleness*/, 0 /* sizeTrigger */, nil /* errCallback*/)
	defer cleanup()

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	message := []byte("test")
	mock.EXPECT().
		output(gomock.Eq(message), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)}).
		Do(addArgs(wg.Done))

	require.NoError(t, sink.output(message, sinkOutputOptions{}))
}

func TestBufferSizeTrigger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sink, mock, cleanup := getMockBufferSync(t, 0 /* maxStaleness*/, 2 /* sizeTrigger */, nil /* errCallback*/)
	defer cleanup()

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	message := []byte("test")
	mock.EXPECT().
		output(gomock.Eq(message), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)}).
		Do(addArgs(wg.Done))

	require.NoError(t, sink.output(message, sinkOutputOptions{}))
}

func TestBufferSizeTriggerMultipleFlush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sink, mock, cleanup := getMockBufferSync(t, 0 /* maxStaleness*/, 8 /* sizeTrigger */, nil /* errCallback*/)
	defer cleanup()

	var wg sync.WaitGroup
	wg.Add(2)
	defer wg.Wait()

	gomock.InOrder(
		mock.EXPECT().
			output(gomock.Eq([]byte("test1\ntest2")), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)}).
			Do(addArgs(wg.Done)),
		mock.EXPECT().
			output(gomock.Eq([]byte("test3")), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)}).
			Do(addArgs(wg.Done)),
	)

	require.NoError(t, sink.output([]byte("test1"), sinkOutputOptions{}))
	require.NoError(t, sink.output([]byte("test2"), sinkOutputOptions{}))
	require.NoError(t, sink.output([]byte("test3"), sinkOutputOptions{extraFlush: true}))
}

type testError struct{}

func (testError) Error() string {
	return "Test Error"
}

func TestBufferErrCallback(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCh := make(chan struct{})
	errCallback := func(error) {
		close(testCh)
	}

	sink, mock, cleanup := getMockBufferSync(t, 0 /* maxStaleness*/, 0 /* sizeTrigger */, errCallback)
	defer cleanup()

	message := []byte("test")
	err := testError{}
	mock.EXPECT().
		output(gomock.Eq(message), gomock.Any()).Return(err)
	require.NoError(t, sink.output(message, sinkOutputOptions{extraFlush: true}))

	<-testCh
}

func TestBufferForceSync(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sink, mock, cleanup := getMockBufferSync(t, 0 /* maxStaleness*/, 0 /* sizeTrigger */, nil /* errCallback*/)
	defer cleanup()

	ch := make(chan struct{})
	message := []byte("test")
	// Make the child sink block until ch is closed.
	mock.EXPECT().
		output(gomock.Eq(message), sinkOutputOptionsMatcher{forceSync: gomock.Eq(true)}).
		Do(addArgs(func() { <-ch }))

	var marker int32
	go func() {
		// Wait a second, verify that the call to output() is still blocked,
		// then close the channel to unblock it.
		<-time.After(time.Second)
		if atomic.LoadInt32(&marker) != 0 {
			t.Error("sink.output returned while child sync should be blocking")
		}
		close(ch)
	}()
	require.NoError(t, sink.output(message, sinkOutputOptions{forceSync: true}))
	// Set marker to be non-zero.
	// This should happen quickly after the above call unblocks.
	atomic.StoreInt32(&marker, 1)
}

// Test that a forceSync message that is dropped by the bufferSink does not lead
// to the respective output() call deadlocking.
func TestForceSyncDropNoDeadlock(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockLogSink(ctrl)
	sink := newBufferSink(ctx, mock, 0 /*maxStaleness*/, 1 /*triggerSize*/, 1 /* maxInFlight */, nil /*errCallback*/)

	// firstFlushC will be signaled when the bufferSink flushes for the first
	// time. That flush will be blocked until the channel is written to again.
	firstFlushSem := make(chan struct{})

	// We'll write a message. This message will trigger a flush based on the byte
	// limit. We'll block that flush and write a second message.
	m1 := []byte("some message")
	mock.EXPECT().
		output(gomock.Any(), gomock.Any()).
		Do(func([]byte, sinkOutputOptions) {
			firstFlushSem <- struct{}{}
			<-firstFlushSem
		})
	require.NoError(t, sink.output(m1, sinkOutputOptions{}))
	select {
	case <-firstFlushSem:
	case <-time.After(10 * time.Second):
		t.Fatal("expected flush didn't happen")
	}
	// Unblock the flush when the test is done.
	defer func() {
		firstFlushSem <- struct{}{}
	}()

	// With the flush blocked, we now send a second message with the forceSync
	// option. This message is expected to be dropped because of the maxInFlight
	// limit. We install an onCompact hook to intercept the message drop and
	// unblock the flush.
	compactCh := make(chan struct{}, 1)
	sink.onMsgDrop = func() {
		select {
		case compactCh <- struct{}{}:
		default:
		}
	}
	m2 := []byte("a sync message")
	logCh := make(chan error)
	go func() {
		logCh <- sink.output(m2, sinkOutputOptions{forceSync: true})
	}()
	// Wait to be notified that the message was dropped.
	<-compactCh

	select {
	case err := <-logCh:
		require.ErrorIs(t, err, errSyncMsgDropped)
	case <-time.After(10 * time.Second):
		t.Fatal("sink.Output call never returned. Deadlock?")
	}
}

func TestBufferCtxDoneFlushesRemainingMsgs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx, cancel := context.WithCancel(context.Background())
	ctrl := gomock.NewController(t)
	mock := NewMockLogSink(ctrl)
	sink := newBufferSink(ctx, mock, 0 /*maxStaleness*/, 0 /*sizeTrigger*/, 2 /* maxInFlight */, nil /*errCallback*/)
	defer ctrl.Finish()

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	// With no sizeTrigger, all 3 of the buffered calls to `sink.output()` after
	// this will be concatenated and flushed as a single string to the underlying
	// mock sink. This single call to `mock.output()` occurs when we signal on the
	// `ctx.Done()` channel with the `cancel()` function.
	//
	// Expect this call, and signal the wait group once it happens. We use the
	// wait group because flushing to the mock sink happens asynchronously.
	mock.EXPECT().
		output(gomock.Eq([]byte("test1\ntest2\ntest3")), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)}).
		Do(addArgs(wg.Done))

	require.NoError(t, sink.output([]byte("test1"), sinkOutputOptions{}))
	require.NoError(t, sink.output([]byte("test2"), sinkOutputOptions{}))
	require.NoError(t, sink.output([]byte("test3"), sinkOutputOptions{}))
	cancel()
}

type sinkOutputOptionsMatcher struct {
	extraFlush   gomock.Matcher
	ignoreErrors gomock.Matcher
	forceSync    gomock.Matcher
}

func (m sinkOutputOptionsMatcher) Matches(x interface{}) bool {
	opts, ok := x.(sinkOutputOptions)
	if !ok {
		return false
	}
	if m.extraFlush != nil && !m.extraFlush.Matches(opts.extraFlush) ||
		m.ignoreErrors != nil && !m.ignoreErrors.Matches(opts.ignoreErrors) ||
		m.forceSync != nil && !m.forceSync.Matches(opts.forceSync) {
		return false
	}
	return true
}

func (m sinkOutputOptionsMatcher) String() string {
	var acc []string
	if m.extraFlush != nil {
		acc = append(acc, fmt.Sprintf("extraFlush %v", m.extraFlush.String()))
	}
	if m.ignoreErrors != nil {
		acc = append(acc, fmt.Sprintf("ignoreErrors %v", m.ignoreErrors.String()))
	}
	if m.forceSync != nil {
		acc = append(acc, fmt.Sprintf("forceSync %v", m.forceSync.String()))
	}
	if len(acc) == 0 {
		return "is anything"
	}
	return strings.Join(acc, ", ")
}
