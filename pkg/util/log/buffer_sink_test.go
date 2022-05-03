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
	lc := NewCloser()
	mock = NewMockLogSink(ctrl)
	sink = newBufferSink(ctx, mock, maxStaleness, sizeTrigger, 2 /* maxInFlight */, errCallback, lc)
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

func TestBufferCtxDoneFlushesRemainingMsgs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx, cancel := context.WithCancel(context.Background())
	ctrl := gomock.NewController(t)
	mock := NewMockLogSink(ctrl)
	lc := NewCloser()
	lc.SetShutdownFn(func() {
		cancel()
	})
	sink := newBufferSink(ctx, mock, 0 /*maxStaleness*/, 0 /*sizeTrigger*/, 2 /* maxInFlight */, nil /*errCallback*/, lc)
	defer ctrl.Finish()

	// With no sizeTrigger, all 3 of the buffered calls to `sink.output()` after
	// this will be concatenated and flushed as a single string to the underlying
	// mock sink. This single call to `mock.output()` occurs when we signal on the
	// `ctx.Done()` channel within the shutdown function set on the `log.Closer`
	// provided to the bufferSink.
	mock.EXPECT().
		output(gomock.Eq([]byte("test1\ntest2\ntest3")), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)})

	require.NoError(t, sink.output([]byte("test1"), sinkOutputOptions{}))
	require.NoError(t, sink.output([]byte("test2"), sinkOutputOptions{}))
	require.NoError(t, sink.output([]byte("test3"), sinkOutputOptions{}))
	lc.Close()
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
