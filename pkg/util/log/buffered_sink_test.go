// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const noMaxStaleness = time.Duration(0)
const noSizeTrigger = 0
const noMaxBufferSize = 0

func getMockBufferedSync(
	t *testing.T,
	maxStaleness time.Duration,
	sizeTrigger uint64,
	maxBufferSize uint64,
	fmtType *logconfig.BufferFormat,
) (sink *bufferedSink, mock *MockLogSink, cleanup func()) {
	ctrl := gomock.NewController(t)
	mock = NewMockLogSink(ctrl)
	sink = newBufferedSink(mock, maxStaleness, sizeTrigger, maxBufferSize, false /* crashOnAsyncFlushErr */, fmtType)
	closer := newBufferedSinkCloser()
	sink.Start(closer)
	cleanup = func() {
		require.NoError(t, closer.Close(defaultCloserTimeout))
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
	defer Scope(t).Close(t)
	sink, mock, cleanup := getMockBufferedSync(t, noMaxStaleness, noSizeTrigger, noMaxBufferSize, nil)
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

func TestBufferSinkBuffers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer Scope(t).Close(t)
	sink, mock, cleanup := getMockBufferedSync(t, noMaxStaleness, noSizeTrigger, noMaxBufferSize, nil)
	defer cleanup()

	flushC := make(chan struct{})

	message := []byte("test")
	mock.EXPECT().
		output(gomock.Eq([]byte("test\ntest")), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)}).
		Do(addArgs(func() { close(flushC) }))

	// Send one message; it should be buffered.
	require.NoError(t, sink.output(message, sinkOutputOptions{}))
	// Sleep a little bit to convince ourselves that no flush is happening. The
	// mock would yell if it did happen.
	time.Sleep(50 * time.Millisecond)
	// Send another message and ask for a flush.
	require.NoError(t, sink.output(message, sinkOutputOptions{extraFlush: true}))
	select {
	case <-flushC:
	// Good, we got our flush.
	case <-time.After(10 * time.Second):
		t.Fatalf("expected flush didn't happen")
	}
}

func TestBufferMaxStaleness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer Scope(t).Close(t)
	sink, mock, cleanup := getMockBufferedSync(t, time.Second /* maxStaleness*/, noSizeTrigger, noMaxBufferSize, nil)
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
	defer Scope(t).Close(t)
	sink, mock, cleanup := getMockBufferedSync(t, noMaxStaleness, 2 /* sizeTrigger */, noMaxBufferSize, nil)
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
	defer Scope(t).Close(t)
	sink, mock, cleanup := getMockBufferedSync(t, noMaxStaleness, 8 /* sizeTrigger */, noMaxBufferSize, nil)
	defer cleanup()

	flush1C := make(chan struct{})
	flush2C := make(chan struct{})

	gomock.InOrder(
		mock.EXPECT().
			output(gomock.Eq([]byte("test1\ntest2")), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)}).
			Do(addArgs(func() { close(flush1C) })),
		mock.EXPECT().
			output(gomock.Eq([]byte("test3")), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)}).
			Do(addArgs(func() { close(flush2C) })),
	)

	require.NoError(t, sink.output([]byte("test1"), sinkOutputOptions{}))
	require.NoError(t, sink.output([]byte("test2"), sinkOutputOptions{}))
	select {
	case <-flush1C:
	case <-time.After(10 * time.Second):
		t.Fatal("first flush didn't happen")
	}
	require.NoError(t, sink.output([]byte("test3"), sinkOutputOptions{extraFlush: true}))
	select {
	case <-flush2C:
	case <-time.After(10 * time.Second):
		t.Fatal("first flush didn't happen")
	}
}

func TestBufferedSinkCrashOnAsyncFlushErr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer Scope(t).Close(t)
	closer := newBufferedSinkCloser()
	defer func() { require.NoError(t, closer.Close(defaultCloserTimeout)) }()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockLogSink(ctrl)
	bufferMaxSize := uint64(20)
	triggerSize := uint64(10)
	// Configure a sink to crash on flush errors.
	sink := newBufferedSink(mock, noMaxStaleness, triggerSize, bufferMaxSize, true /* crashOnAsyncFlushErr */, nil)
	sink.Start(closer)

	crashC := make(chan struct{})
	SetExitFunc(false /* hideStack */, func(code exit.Code) {
		close(crashC)
	})
	defer ResetExitFunc()

	// Inject an error in flushes.
	mock.EXPECT().output(gomock.Any(), gomock.Any()).Return(errors.New("boom"))
	mock.EXPECT().exitCode().Return(exit.LoggingNetCollectorUnavailable())
	// Force a flush.
	require.NoError(t, sink.output([]byte("test"), sinkOutputOptions{extraFlush: true}))
	// Check that we crashed.
	select {
	case <-crashC:
		// Good; we would have crashed in production.
	case <-time.After(10 * time.Second):
		t.Fatalf("expected crash didn't happen")
	}
}

// Test that a call to output() with the tryForceSync option doesn't return until
// the flush is done, in the case where a tryForceSync isn't already scheduled.
func TestBufferedSinkTryForceSync(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer Scope(t).Close(t)
	sink, mock, cleanup := getMockBufferedSync(t, noMaxStaleness, noSizeTrigger, noMaxBufferSize, nil)
	defer cleanup()

	ch := make(chan struct{})
	message := []byte("test")
	// Make the child sink block until ch is closed.
	mock.EXPECT().
		output(gomock.Eq(message), sinkOutputOptionsMatcher{tryForceSync: gomock.Eq(true)}).
		Do(addArgs(func() { <-ch }))

	var marker int32
	go func() {
		// Wait a second, verify that the call to output() is still blocked,
		// then close the channel to unblock it.
		<-time.After(50 * time.Millisecond)
		if atomic.LoadInt32(&marker) != 0 {
			t.Error("sink.output returned while child sync should be blocking")
		}
		close(ch)
	}()
	require.NoError(t, sink.output(message, sinkOutputOptions{tryForceSync: true}))
	// Set marker to be non-zero.
	// This should happen quickly after the above call unblocks.
	atomic.StoreInt32(&marker, 1)
}

// Test that a call to output() with the tryForceSync option doesn't block if a
// tryForceSync is already scheduled, and that the message is included as part
// of the already-scheduled flush.
func TestBufferedSinkTryForceSync_SyncFlushAlreadyScheduled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer Scope(t).Close(t)
	closer := newBufferedSinkCloser()
	defer func() { require.NoError(t, closer.Close(defaultCloserTimeout)) }()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	child := newTestWaitGroupSink(ctrl)
	sink := newBufferedSink(child, noMaxStaleness, noSizeTrigger, noMaxBufferSize, false /* crashOnAsyncFlushErr */, nil)
	sink.Start(closer)

	// We're scheduling 3 tryForceSync flushes:
	// 		A: The first flush, which will hang until we signal the child sink WaitGroup.
	//			 This will cause the next flush to wait in the queue to simulate "stacking"
	//			 scheduled tryForceSync flushes.
	//		B: A second flush, which will be scheduled in flushC but not execute until the
	//			 first flush completes (which it can't until we signal the child sink WaitGroup).
	//		C: A third flush, where tryForceSync can no longer be supported as a blocking
	//			 operation. This is because the second flush is already scheduled & consuming
	// 			 the 1 extra space in flushC. The expectation is the message is buffered and we
	//			 return instead. The message is expected to be included in the second flush,
	//			 which is already scheduled.
	//
	// We use channels and the child sink WaitGroup to coordinate these sequentially across
	// 3 separate goroutines, simulating 3 separate writers to the bufferedSink.
	child.mock.EXPECT().output(gomock.Eq([]byte("a")), sinkOutputOptionsMatcher{tryForceSync: gomock.Eq(true)})
	child.mock.EXPECT().output(gomock.Eq([]byte("b\nc")), sinkOutputOptionsMatcher{tryForceSync: gomock.Eq(true)})

	child.wg.Add(1)
	firstFlushTriggered := make(chan struct{})
	secondFlushTriggered := make(chan struct{})
	secondFlushDone := make(chan struct{})
	// First flush, which blocks until the child sink WaitGroup is signaled later on.
	go func() {
		time.AfterFunc(50*time.Millisecond, func() {
			firstFlushTriggered <- struct{}{}
		})
		require.NoError(t, sink.output([]byte("a"), sinkOutputOptions{tryForceSync: true}))
	}()
	// Second flush, which gets scheduled & blocks until the first flush completes, which
	// won't do so until the child sink WaitGroup is signaled.
	go func() {
		<-firstFlushTriggered
		time.AfterFunc(50*time.Millisecond, func() {
			secondFlushTriggered <- struct{}{}
		})
		require.NoError(t, sink.output([]byte("b"), sinkOutputOptions{tryForceSync: true}))
		secondFlushDone <- struct{}{}
	}()
	// Third flush, which is unable to block because we are already at the max number of
	// tryForceSync flushes scheduled. It doesn't block, but instead has its message buffered
	// to be included in the second flush (already scheduled).
	//
	// This is where we signal the child sink's WaitGroup to finally unblock the first flush
	// from completing, which will unblock the second flush and allow it to execute.
	go func() {
		<-secondFlushTriggered
		require.NoError(t, sink.output([]byte("c"), sinkOutputOptions{tryForceSync: true}))
		child.wg.Done()
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for bufferedSink flushes to complete")
	case <-secondFlushDone:
	}
}

// Test that messages are buffered while a flush is in-flight.
func TestBufferedSinkBlockedFlush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer Scope(t).Close(t)
	closer := newBufferedSinkCloser()
	defer func() { require.NoError(t, closer.Close(defaultCloserTimeout)) }()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockLogSink(ctrl)
	bufferMaxSize := uint64(20)
	triggerSize := uint64(10)
	sink := newBufferedSink(mock, noMaxStaleness, triggerSize, bufferMaxSize, false /* crashOnAsyncFlushErr */, nil)
	sink.Start(closer)

	// firstFlushSem will be signaled when the bufferedSink flushes for the first
	// time. That flush will be blocked until the channel is written to again.
	firstFlushSem := make(chan struct{})

	// We'll write a large message which will trigger a flush based on the
	// triggerSize limit. We'll block that flush and then write more messages.
	largeMsg := bytes.Repeat([]byte("a"), int(triggerSize))
	mock.EXPECT().
		output(gomock.Any(), gomock.Any()).
		Do(func([]byte, sinkOutputOptions) {
			firstFlushSem <- struct{}{}
			<-firstFlushSem
		})
	require.NoError(t, sink.output(largeMsg, sinkOutputOptions{}))
	select {
	case <-firstFlushSem:
	case <-time.After(10 * time.Second):
		t.Fatal("expected flush didn't happen")
	}

	// With the flush blocked, we now send more messages. These messages will run
	// into the bufferMaxSize limit, and so the oldest will be dropped.

	// First, we arm a channel for a second flush. We don't expect this to fire
	// yet.
	secondFlush := make(chan []byte)
	mock.EXPECT().
		output(gomock.Any(), gomock.Any()).
		Do(func(logs []byte, _ sinkOutputOptions) {
			secondFlush <- logs
		})

	// We're going to send a sequence of messages. They'll overflow the buffer,
	// and we'll expect only the last few to be eventually flushed.
	for i := 0; i < 10; i++ {
		s := fmt.Sprintf("a%d", i)
		require.NoError(t, sink.output([]byte(s), sinkOutputOptions{}))
	}
	for i := 0; i < 10; i++ {
		s := fmt.Sprintf("b%d", i)
		require.NoError(t, sink.output([]byte(s), sinkOutputOptions{}))
	}

	select {
	case <-secondFlush:
		t.Fatalf("unexpected second flush while first flush is in-flight")
	case <-time.After(10 * time.Millisecond):
		// Good; it appears that a second flush does not happen while the first is in-flight.
	}

	// Now unblock the original flush, which in turn will allow a second flush to happen.
	firstFlushSem <- struct{}{}

	// Check that the second flush happens, and delivers the tail of the messages.
	select {
	case <-time.After(10 * time.Second):
		t.Fatal("expected 2nd flush didn't happen")
	case out := <-secondFlush:
		require.Equal(t, []byte(`b4
b5
b6
b7
b8
b9`), out)
	}
}

// Test that multiple messages with the tryForceSync option work.
func TestBufferedSinkSyncFlush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer Scope(t).Close(t)
	closer := newBufferedSinkCloser()
	defer func() { require.NoError(t, closer.Close(defaultCloserTimeout)) }()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := NewMockLogSink(ctrl)
	sink := newBufferedSink(mock, noMaxStaleness, noSizeTrigger, noMaxBufferSize, false /* crashOnAsyncFlushErr */, nil)
	sink.Start(closer)

	mock.EXPECT().output(gomock.Eq([]byte("a")), gomock.Any())
	mock.EXPECT().output(gomock.Eq([]byte("b")), gomock.Any())
	require.NoError(t, sink.output([]byte("a"), sinkOutputOptions{tryForceSync: true}))
	require.NoError(t, sink.output([]byte("b"), sinkOutputOptions{tryForceSync: true}))
}

func TestBufferCtxDoneFlushesRemainingMsgs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer Scope(t).Close(t)
	closer := newBufferedSinkCloser()
	ctrl := gomock.NewController(t)
	mock := NewMockLogSink(ctrl)
	sink := newBufferedSink(mock, noMaxStaleness, noSizeTrigger, noMaxBufferSize, false /* crashOnAsyncFlushErr */, nil)
	sink.Start(closer)
	defer ctrl.Finish()

	// With no sizeTrigger, all 3 of the buffered calls to `sink.output()` after
	// this will be concatenated and flushed as a single string to the underlying
	// mock sink. This single call to `mock.output()` occurs when we call `closer.Close()`
	//
	// Expect this call, and signal the wait group once it happens. We use the
	// wait group because flushing to the mock sink happens asynchronously.
	mock.EXPECT().
		output(gomock.Eq([]byte("test1\ntest2\ntest3")), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)})

	require.NoError(t, sink.output([]byte("test1"), sinkOutputOptions{}))
	require.NoError(t, sink.output([]byte("test2"), sinkOutputOptions{}))
	require.NoError(t, sink.output([]byte("test3"), sinkOutputOptions{}))
	require.NoError(t, closer.Close(defaultCloserTimeout))
}

func TestBufferFormatJsonArray(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer Scope(t).Close(t)
	fmtType := logconfig.BufferFormat("json-array")
	sink, mock, cleanup := getMockBufferedSync(t, noMaxStaleness, 8 /* sizeTrigger */, noMaxBufferSize, &fmtType)
	defer cleanup()

	// Test that the following occurs:
	// json-array format should prepend '[' and append ']' to the buffer output.
	// Entries are separated with ",".
	// It does not modify log entries themselves.

	flush1C := make(chan struct{})
	flush2C := make(chan struct{})

	gomock.InOrder(
		mock.EXPECT().
			output(gomock.Eq([]byte("[test1,test2]")), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)}).
			Do(addArgs(func() { close(flush1C) })),
		mock.EXPECT().
			output(gomock.Eq([]byte("[test3]")), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)}).
			Do(addArgs(func() { close(flush2C) })),
	)

	require.NoError(t, sink.output([]byte("test1"), sinkOutputOptions{}))
	require.NoError(t, sink.output([]byte("test2"), sinkOutputOptions{}))
	select {
	case <-flush1C:
	case <-time.After(10 * time.Second):
		t.Fatal("first flush didn't happen")
	}
	require.NoError(t, sink.output([]byte("test3"), sinkOutputOptions{extraFlush: true}))
	select {
	case <-flush2C:
	case <-time.After(10 * time.Second):
		t.Fatal("second flush didn't happen")
	}
}

func TestBufferFormatNone(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer Scope(t).Close(t)
	fmtType := logconfig.BufferFmtNone
	sink, mock, cleanup := getMockBufferedSync(t, noMaxStaleness, 8 /* sizeTrigger */, noMaxBufferSize, &fmtType)
	defer cleanup()

	// Test that the bufferedSink doesn't modify/format the output messages.
	flush1C := make(chan struct{})
	flush2C := make(chan struct{})

	gomock.InOrder(
		mock.EXPECT().
			output(gomock.Eq([]byte("test1test2")), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)}).
			Do(addArgs(func() { close(flush1C) })),
		mock.EXPECT().
			output(gomock.Eq([]byte("test3")), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)}).
			Do(addArgs(func() { close(flush2C) })),
	)

	require.NoError(t, sink.output([]byte("test1"), sinkOutputOptions{}))
	require.NoError(t, sink.output([]byte("test2"), sinkOutputOptions{}))
	select {
	case <-flush1C:
	case <-time.After(10 * time.Second):
		t.Fatal("first flush didn't happen")
	}
	require.NoError(t, sink.output([]byte("test3"), sinkOutputOptions{extraFlush: true}))
	select {
	case <-flush2C:
	case <-time.After(10 * time.Second):
		t.Fatal("second flush didn't happen")
	}
}

func TestMsgBufFlushFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer Scope(t).Close(t)
	testCases := []struct {
		bufferContents []string
		prefix         string
		suffix         string
		delimiter      string
		expected       string
	}{
		{
			bufferContents: []string{"hello", "world"},
			prefix:         "",
			suffix:         "",
			delimiter:      "\n",
			expected:       "hello\nworld",
		},
		{
			bufferContents: []string{"a", "b", "cde", "efg", "hijklmn", "o", "pqrst"},
			prefix:         "MyPrefix ",
			suffix:         " MySuffix",
			delimiter:      " , ",
			expected:       "MyPrefix a , b , cde , efg , hijklmn , o , pqrst MySuffix",
		},
		{
			bufferContents: []string{"onetwo", "three", "fourfive", "sixseven", "eightnineten"},
			prefix:         "\n",
			suffix:         "\n",
			delimiter:      "",
			expected:       "\nonetwothreefourfivesixseveneightnineten\n",
		},
		{
			bufferContents: []string{"onetwo", "three", "fourfive", "sixseven", "eightnineten"},
			prefix:         "",
			suffix:         "",
			delimiter:      "",
			expected:       "onetwothreefourfivesixseveneightnineten",
		},
	}

	for _, tc := range testCases {
		// Set up the contents of the msgBuf.
		buf := msgBuf{}
		for _, strMsg := range tc.bufferContents {
			msg := getBuffer()
			msg.WriteString(strMsg)
			require.NoError(t, buf.appendMsg(msg))
		}

		// Flush.
		resBuf, _ := buf.flush(tc.prefix, tc.suffix, tc.delimiter)

		// Check output.
		require.Equal(t, tc.expected, resBuf.String())
	}
}

type sinkOutputOptionsMatcher struct {
	extraFlush   gomock.Matcher
	ignoreErrors gomock.Matcher
	tryForceSync gomock.Matcher
}

func (m sinkOutputOptionsMatcher) Matches(x interface{}) bool {
	opts, ok := x.(sinkOutputOptions)
	if !ok {
		return false
	}
	if m.extraFlush != nil && !m.extraFlush.Matches(opts.extraFlush) ||
		m.ignoreErrors != nil && !m.ignoreErrors.Matches(opts.ignoreErrors) ||
		m.tryForceSync != nil && !m.tryForceSync.Matches(opts.tryForceSync) {
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
	if m.tryForceSync != nil {
		acc = append(acc, fmt.Sprintf("tryForceSync %v", m.tryForceSync.String()))
	}
	if len(acc) == 0 {
		return "is anything"
	}
	return strings.Join(acc, ", ")
}

// testWaitGroupSink is a mock child sink that will wait on its sync.WaitGroup (wg)
// during calls to output(), before passing the call along to the underlying
// MockLogSink. This enables us to coordinate tests using concurrent writers by
// preventing the flusher goroutine in the bufferedSink from completing until
// we signal the sync.WaitGroup.
type testWaitGroupSink struct {
	wg   *sync.WaitGroup
	mock *MockLogSink
}

func newTestWaitGroupSink(ctrl *gomock.Controller) *testWaitGroupSink {
	return &testWaitGroupSink{
		wg:   &sync.WaitGroup{},
		mock: NewMockLogSink(ctrl),
	}
}

func (t *testWaitGroupSink) active() bool {
	return true
}

func (t *testWaitGroupSink) attachHints(i []byte) []byte {
	return nil
}

func (t *testWaitGroupSink) output(b []byte, opts sinkOutputOptions) error {
	t.wg.Wait()
	return t.mock.output(b, opts)
}

func (t *testWaitGroupSink) exitCode() exit.Code {
	return exit.Success()
}

var _ logSink = (*testWaitGroupSink)(nil)
