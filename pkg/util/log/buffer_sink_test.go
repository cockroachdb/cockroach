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
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func getMockBufferSync(
	t *testing.T, maxStaleness time.Duration, sizeTrigger int, errCallback func(error),
) (sink *bufferSink, mock *MockLogSink, cleanup func()) {
	ctx, cancel := context.WithCancel(context.Background())
	ctrl := gomock.NewController(t)
	mock = NewMockLogSink(ctrl)
	sink = newBufferSink(ctx, mock, maxStaleness, sizeTrigger, 2, errCallback)
	cleanup = func() {
		ctrl.Finish()
		cancel()
	}
	return sink, mock, cleanup
}

func TestBufferOneLine(t *testing.T) {
	sink, mock, cleanup := getMockBufferSync(t, 0 /* maxStaleness*/, 0 /* sizeTrigger */, nil /* errCallback*/)
	defer cleanup()

	message := []byte("test")
	mock.EXPECT().output(gomock.Eq(message), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)})

	sink.output(message, sinkOutputOptions{extraFlush: true})
	<-time.After(time.Second)
}

func TestBufferManyLinesOneFlush(t *testing.T) {
	sink, mock, cleanup := getMockBufferSync(t, 0 /* maxStaleness*/, 0 /* sizeTrigger */, nil /* errCallback*/)
	defer cleanup()

	message := []byte("test")
	mock.EXPECT().output(gomock.Eq([]byte("test\ntest")), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)})

	sink.output(message, sinkOutputOptions{})
	sink.output(message, sinkOutputOptions{extraFlush: true})
	<-time.After(time.Second)
}

func TestBufferMaxStaleness(t *testing.T) {
	sink, mock, cleanup := getMockBufferSync(t, time.Second /* maxStaleness*/, 0 /* sizeTrigger */, nil /* errCallback*/)
	defer cleanup()

	message := []byte("test")
	mock.EXPECT().output(gomock.Eq(message), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)})

	sink.output(message, sinkOutputOptions{})
	<-time.After(2 * time.Second)
}

func TestBufferSizeTrigger(t *testing.T) {
	sink, mock, cleanup := getMockBufferSync(t, 0 /* maxStaleness*/, 2 /* sizeTrigger */, nil /* errCallback*/)
	defer cleanup()

	message := []byte("test")
	mock.EXPECT().output(gomock.Eq(message), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)})

	sink.output(message, sinkOutputOptions{})
	<-time.After(time.Second)
}

func TestBufferSizeTriggerMultipleFlush(t *testing.T) {
	sink, mock, cleanup := getMockBufferSync(t, 0 /* maxStaleness*/, 8 /* sizeTrigger */, nil /* errCallback*/)
	defer cleanup()

	gomock.InOrder(
		mock.EXPECT().output(gomock.Eq([]byte("test1\ntest2")), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)}),
		mock.EXPECT().output(gomock.Eq([]byte("test3")), sinkOutputOptionsMatcher{extraFlush: gomock.Eq(true)}),
	)

	sink.output([]byte("test1"), sinkOutputOptions{})
	sink.output([]byte("test2"), sinkOutputOptions{})
	sink.output([]byte("test3"), sinkOutputOptions{extraFlush: true})
	<-time.After(time.Second)
}

type testError struct{}

func (testError) Error() string {
	return "Test Error"
}

func TestBufferErrCallback(t *testing.T) {
	errCallbackCalled := false
	errCallback := func(error) {
		errCallbackCalled = true
	}

	sink, mock, cleanup := getMockBufferSync(t, 0 /* maxStaleness*/, 0 /* sizeTrigger */, errCallback)
	defer cleanup()

	message := []byte("test")
	err := testError{}
	mock.EXPECT().
		output(gomock.Eq(message), gomock.Any()).Return(err)
	sink.output(message, sinkOutputOptions{extraFlush: true})

	<-time.After(time.Second)
	require.True(t, errCallbackCalled)
}

func TestBufferForceSync(t *testing.T) {
	sink, mock, cleanup := getMockBufferSync(t, 0 /* maxStaleness*/, 0 /* sizeTrigger */, nil /* errCallback*/)
	defer cleanup()

	ch := make(chan struct{})
	message := []byte("test")
	// Make the child sink block until ch is closed.
	mock.EXPECT().
		output(gomock.Eq(message), sinkOutputOptionsMatcher{forceSync: gomock.Eq(true)}).
		Do(func(_, _ interface{}) { <-ch })

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
	sink.output(message, sinkOutputOptions{forceSync: true})
	// Set marker to be non-zero.
	// This should happen quickly after the above call unblocks.
	atomic.StoreInt32(&marker, 1)
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
