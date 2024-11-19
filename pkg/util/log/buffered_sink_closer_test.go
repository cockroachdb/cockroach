// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClose(t *testing.T) {
	t.Run("returns after all registered sinks exit", func(t *testing.T) {
		fakeSinkRoutine := func(stopC <-chan struct{}, bs *bufferedSink, cleanup func()) {
			defer cleanup()
			<-stopC
		}

		closer := newBufferedSinkCloser()
		for i := 0; i < 3; i++ {
			fakeBufferSink := &bufferedSink{}
			stopC, cleanup := closer.RegisterBufferedSink(fakeBufferSink)
			go fakeSinkRoutine(stopC, fakeBufferSink, cleanup)
		}

		require.NoError(t, closer.Close(1000*time.Hour) /* timeout - verify that it doesn't expire */)
	})

	t.Run("times out if leaked bufferedSink doesn't shut down", func(t *testing.T) {
		closer := newBufferedSinkCloser()
		_, _ = closer.RegisterBufferedSink(&bufferedSink{})
		require.Error(t, closer.Close(time.Nanosecond /* timeout */))
	})
}

func TestRegisterBufferSink(t *testing.T) {
	t.Run("registers bufferedSink as expected", func(t *testing.T) {
		lc := newBufferedSinkCloser()
		bs := &bufferedSink{}
		lc.RegisterBufferedSink(bs)
		lc.mu.Lock()
		defer lc.mu.Unlock()
		_, ok := lc.mu.sinkRegistry[bs]
		assert.True(t, ok)
	})

	t.Run("panics if same bufferedSink registered twice", func(t *testing.T) {
		lc := newBufferedSinkCloser()
		bs := &bufferedSink{}
		lc.RegisterBufferedSink(bs)
		assert.Panics(t,
			func() { lc.RegisterBufferedSink(bs) },
			"expected RegisterBufferedSink() to panic when same sink registered twice.")
	})
}

func TestBufferSinkDone(t *testing.T) {
	t.Run("signals waitgroup and removes bufferSink from registry", func(t *testing.T) {
		closer := newBufferedSinkCloser()
		bs := &bufferedSink{}

		closer.RegisterBufferedSink(bs)

		closer.mu.Lock()
		assert.Len(t, closer.mu.sinkRegistry, 1, "expected sink registry to include registered bufferedSink")
		closer.mu.Unlock()

		closer.bufferedSinkDone(bs)

		closer.mu.Lock()
		assert.Empty(t, closer.mu.sinkRegistry, "expected sink registry to be empty")
		closer.mu.Unlock()

		require.NoError(t, closer.Close(time.Second /* timeout */), "bufferedSinkCloser timed out unexpectedly")
	})

	t.Run("panics if called on unregistered bufferSink", func(t *testing.T) {
		closer := newBufferedSinkCloser()
		bs1 := &bufferedSink{}
		bs2 := &bufferedSink{}

		closer.RegisterBufferedSink(bs1)

		closer.mu.Lock()
		_, ok := closer.mu.sinkRegistry[bs1]
		assert.Len(t, closer.mu.sinkRegistry, 1, "length of bufferSink registry larger than expected")
		assert.True(t, ok, "expected bufferSink to be in registry")
		closer.mu.Unlock()

		require.Panics(t, func() {
			closer.bufferedSinkDone(bs2)
		})
	})
}
