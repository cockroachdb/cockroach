// Copyright 2022 The Cockroach Authors.
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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClose(t *testing.T) {
	t.Run("returns after all registered sinks exit", func(t *testing.T) {
		fakeSinkRoutine := func(ctx context.Context, bs *bufferedSink, closer *BufferedSinkCloser) {
			defer closer.BufferSinkDone(bs)
			for {
				select {
				case <-ctx.Done():
					return
				default:
					continue
				}
			}
		}

		closer := NewBufferedSinkCloser()
		for i := 0; i < 3; i++ {
			fakeBufferSink := &bufferedSink{}
			ctx := closer.RegisterBufferSink(fakeBufferSink)
			go fakeSinkRoutine(ctx, fakeBufferSink, closer)
		}

		doneChan := make(chan struct{})
		go func() {
			closer.Close()
			doneChan <- struct{}{}
		}()

		timeout := time.After(10 * time.Second)
		select {
		case <-doneChan:
			return
		case <-timeout:
			t.Error("Close hanging")
			return
		}
	})

	t.Run("times out if leaked bufferedSink doesn't signal BufferSinkDone()", func(t *testing.T) {
		lc := NewBufferedSinkCloser()
		lc.timeout = 500 * time.Millisecond
		_ = lc.RegisterBufferSink(&bufferedSink{})

		doneChan := make(chan struct{})
		go func() {
			lc.Close()
			doneChan <- struct{}{}
		}()

		timeout := time.After(2 * time.Second)
		select {
		case <-doneChan:
		case <-timeout:
			t.Error("(*BufferedSinkCloser).Close() failed to timeout")
		}
	})
}

func TestRegisterBufferSink(t *testing.T) {
	t.Run("registers bufferedSink as expected", func(t *testing.T) {
		lc := NewBufferedSinkCloser()
		bs := &bufferedSink{}
		lc.RegisterBufferSink(bs)
		lc.mu.Lock()
		defer lc.mu.Unlock()
		_, ok := lc.mu.sinkRegistry[bs]
		assert.True(t, ok)
	})

	t.Run("panics if same bufferedSink registered twice", func(t *testing.T) {
		lc := NewBufferedSinkCloser()
		bs := &bufferedSink{}
		lc.RegisterBufferSink(bs)
		assert.Panics(t,
			func() { lc.RegisterBufferSink(bs) },
			"expected RegisterBufferSink() to panic when same sink registered twice.")
	})
}

func TestBufferSinkDone(t *testing.T) {
	t.Run("signals waitgroup and removes bufferSink from registry", func(t *testing.T) {
		lc := NewBufferedSinkCloser()
		bs := &bufferedSink{}

		lc.RegisterBufferSink(bs)
		lc.BufferSinkDone(bs)

		lc.mu.Lock()
		assert.Len(t, lc.mu.sinkRegistry, 0, "expected sink registry to be empty")
		lc.mu.Unlock()

		timeout := time.After(500 * time.Millisecond)
		doneChan := make(chan struct{})
		go func() {
			lc.Close()
			doneChan <- struct{}{}
		}()
		select {
		case <-doneChan:
		case <-timeout:
			t.Error("(*BufferedSinkCloser).Close() timed out")
		}
	})

	t.Run("ignores unregistered bufferSinks", func(t *testing.T) {
		lc := NewBufferedSinkCloser()
		bs1 := &bufferedSink{}
		bs2 := &bufferedSink{}

		lc.RegisterBufferSink(bs1)

		lc.mu.Lock()
		_, ok := lc.mu.sinkRegistry[bs1]
		assert.Len(t, lc.mu.sinkRegistry, 1, "length of bufferSink registry larger than expected")
		assert.True(t, ok, "expected bufferSink to be in registry")
		lc.mu.Unlock()

		lc.BufferSinkDone(bs2)

		lc.mu.Lock()
		_, ok = lc.mu.sinkRegistry[bs1]
		assert.Len(t, lc.mu.sinkRegistry, 1, "length of bufferSink registry larger than expected")
		assert.True(t, ok, "expected bufferSink to still be in registry")
		lc.mu.Unlock()

		// Now verify that `(*BufferedSinkCloser).Close()` hangs, indicating that the call
		// to `BufferSinkDone` with an unregistered sink did not signal the WaitGroup.
		doneChan := make(chan struct{})
		go func() {
			lc.Close()
			doneChan <- struct{}{}
		}()
		timeout := time.After(50 * time.Millisecond)
		select {
		case <-doneChan:
			t.Error("expected (*BufferedSinkCloser).Close() to hang, but operation finished")
		case <-timeout:
			return
		}
	})
}
