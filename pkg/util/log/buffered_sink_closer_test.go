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
	"github.com/stretchr/testify/require"
)

func TestClose(t *testing.T) {
	t.Run("returns after all registered sinks exit", func(t *testing.T) {
		fakeSinkRoutine := func(ctx context.Context, bs *bufferedSink, closer *BufferedSinkCloser) {
			defer closer.BufferedSinkDone(bs)
			<-ctx.Done()
		}

		closer := NewBufferedSinkCloser()
		for i := 0; i < 3; i++ {
			fakeBufferSink := &bufferedSink{}
			ctx := closer.RegisterBufferedSink(fakeBufferSink)
			go fakeSinkRoutine(ctx, fakeBufferSink, closer)
		}

		closer.timeout = 5 * time.Millisecond
		require.NoError(t, closer.Close())
	})

	t.Run("times out if leaked bufferedSink doesn't signal BufferedSinkDone()", func(t *testing.T) {
		closer := NewBufferedSinkCloser()
		closer.timeout = 1 * time.Millisecond
		_ = closer.RegisterBufferedSink(&bufferedSink{})
		require.Error(t, closer.Close())
	})
}

func TestRegisterBufferSink(t *testing.T) {
	t.Run("registers bufferedSink as expected", func(t *testing.T) {
		lc := NewBufferedSinkCloser()
		bs := &bufferedSink{}
		lc.RegisterBufferedSink(bs)
		lc.mu.Lock()
		defer lc.mu.Unlock()
		_, ok := lc.mu.sinkRegistry[bs]
		assert.True(t, ok)
	})

	t.Run("panics if same bufferedSink registered twice", func(t *testing.T) {
		lc := NewBufferedSinkCloser()
		bs := &bufferedSink{}
		lc.RegisterBufferedSink(bs)
		assert.Panics(t,
			func() { lc.RegisterBufferedSink(bs) },
			"expected RegisterBufferedSink() to panic when same sink registered twice.")
	})
}

func TestBufferSinkDone(t *testing.T) {
	t.Run("signals waitgroup and removes bufferSink from registry", func(t *testing.T) {
		closer := NewBufferedSinkCloser()
		bs := &bufferedSink{}

		closer.RegisterBufferedSink(bs)

		closer.mu.Lock()
		assert.Len(t, closer.mu.sinkRegistry, 1, "expected sink registry to include registered bufferedSink")
		closer.mu.Unlock()

		closer.BufferedSinkDone(bs)

		closer.mu.Lock()
		assert.Empty(t, closer.mu.sinkRegistry, "expected sink registry to be empty")
		closer.mu.Unlock()

		closer.timeout = 10 * time.Millisecond
		require.NoError(t, closer.Close(), "BufferedSinkCloser timed out unexpectedly")
	})

	t.Run("panics if called on unregistered bufferSink", func(t *testing.T) {
		closer := NewBufferedSinkCloser()
		bs1 := &bufferedSink{}
		bs2 := &bufferedSink{}

		closer.RegisterBufferedSink(bs1)

		closer.mu.Lock()
		_, ok := closer.mu.sinkRegistry[bs1]
		assert.Len(t, closer.mu.sinkRegistry, 1, "length of bufferSink registry larger than expected")
		assert.True(t, ok, "expected bufferSink to be in registry")
		closer.mu.Unlock()

		require.Panics(t, func() {
			closer.BufferedSinkDone(bs2)
		})
	})
}
