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
	t.Run("no NPE error if shutdown function not explicitly set", func(t *testing.T) {
		lc := NewCloser()
		assert.NotPanics(t, lc.Close)
	})

	t.Run("returns after all registered sinks exit", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		lc := NewCloser()
		lc.SetShutdownFn(func() {
			cancel()
		})

		fakeSinkRoutine := func(ctx context.Context, ls *Closer) {
			fakeBufferSink := &bufferSink{}
			ls.RegisterBufferSink(fakeBufferSink)
			defer ls.BufferSinkDone(fakeBufferSink)
			for {
				select {
				case <-ctx.Done():
					return
				default:
					continue
				}
			}
		}

		for i := 0; i < 3; i++ {
			go fakeSinkRoutine(ctx, lc)
		}

		doneChan := make(chan struct{})
		go func() {
			lc.Close()
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

	t.Run("times out if leaked bufferSink doesn't signal BufferSinkDone()", func(t *testing.T) {
		lc := NewCloser()
		lc.timeout = 500 * time.Millisecond
		lc.RegisterBufferSink(&bufferSink{})

		doneChan := make(chan struct{})
		go func() {
			lc.Close()
			doneChan <- struct{}{}
		}()

		timeout := time.After(2 * time.Second)
		select {
		case <-doneChan:
		case <-timeout:
			t.Error("(*Closer).Close() failed to timeout")
		}
	})
}

func TestRegisterBufferSink(t *testing.T) {
	t.Run("registers bufferSink as expected", func(t *testing.T) {
		lc := NewCloser()
		bs := &bufferSink{}
		lc.RegisterBufferSink(bs)
		lc.registryMu.Lock()
		defer lc.registryMu.Unlock()
		_, ok := lc.registryMu.sinkRegistry[bs]
		assert.True(t, ok)
	})

	t.Run("panics if same bufferSink registered twice", func(t *testing.T) {
		lc := NewCloser()
		bs := &bufferSink{}
		lc.RegisterBufferSink(bs)
		assert.Panics(t,
			func() { lc.RegisterBufferSink(bs) },
			"expected RegisterBufferSink() to panic when same sink registered twice.")
	})
}

func TestBufferSinkDone(t *testing.T) {
	t.Run("signals waitgroup and removes bufferSink from registry", func(t *testing.T) {
		lc := NewCloser()
		bs := &bufferSink{}

		lc.RegisterBufferSink(bs)
		lc.BufferSinkDone(bs)

		lc.registryMu.Lock()
		assert.Len(t, lc.registryMu.sinkRegistry, 0, "expected sink registry to be empty")
		lc.registryMu.Unlock()

		timeout := time.After(500 * time.Millisecond)
		doneChan := make(chan struct{})
		go func() {
			lc.Close()
			doneChan <- struct{}{}
		}()
		select {
		case <-doneChan:
		case <-timeout:
			t.Error("(*Closer).Close() timed out")
		}
	})

	t.Run("ignores unregistered bufferSinks", func(t *testing.T) {
		lc := NewCloser()
		bs1 := &bufferSink{}
		bs2 := &bufferSink{}

		lc.RegisterBufferSink(bs1)

		lc.registryMu.Lock()
		_, ok := lc.registryMu.sinkRegistry[bs1]
		assert.Len(t, lc.registryMu.sinkRegistry, 1, "length of bufferSink registry larger than expected")
		assert.True(t, ok, "expected bufferSink to be in registry")
		lc.registryMu.Unlock()

		lc.BufferSinkDone(bs2)

		lc.registryMu.Lock()
		_, ok = lc.registryMu.sinkRegistry[bs1]
		assert.Len(t, lc.registryMu.sinkRegistry, 1, "length of bufferSink registry larger than expected")
		assert.True(t, ok, "expected bufferSink to still be in registry")
		lc.registryMu.Unlock()

		// Now verify that `(*Closer).Close()` hangs, indicating that the call
		// to `BufferSinkDone` with an unregistered sink did not signal the WaitGroup.
		doneChan := make(chan struct{})
		go func() {
			lc.Close()
			doneChan <- struct{}{}
		}()
		timeout := time.After(50 * time.Millisecond)
		select {
		case <-doneChan:
			t.Error("expected (*Closer).Close() to hang, but operation finished")
		case <-timeout:
			return
		}
	})
}
