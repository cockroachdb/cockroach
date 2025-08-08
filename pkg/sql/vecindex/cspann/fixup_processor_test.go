// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestDelayInsertOrDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// Set up the fixup processor with a minimum delay and allowed ops/sec = 1.
	var fp FixupProcessor
	fp.minDelay = time.Hour
	var index Index
	fp.Init(ctx, stopper, &index, 42 /* seed */)
	fp.mu.pacer.Init(1, 0, fp.mu.pacer.monoNow)

	// Cancel the context and verify that DelayInsertOrDelete immediately
	// returns with an error rather than waiting out the delay.
	cancel()
	require.ErrorIs(t, fp.DelayInsertOrDelete(ctx), context.Canceled)

	// Validate that the pacer token was returned to the bucket, since operation
	// didn't actually happen.
	require.GreaterOrEqual(t, fp.mu.pacer.currentTokens, 0.0)
}

func TestProcess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("canceling initialization context should abort Process", func(t *testing.T) {
		initCtx, cancel := context.WithCancel(context.Background())
		stopper := stop.NewStopper()
		defer stopper.Stop(initCtx)

		var fp FixupProcessor
		var index Index
		fp.Init(initCtx, stopper, &index, 42 /* seed */)

		// Process should immediately return when there are no fixups in the queue.
		ctx := context.Background()
		require.NoError(t, fp.Process(ctx))

		// Add fixup and ensure that canceling the initialization context will
		// abort fixup processing.
		fp.AddSplit(ctx, nil /* treeKey */, 1, 2, false /* singleStep */)
		go func() {
			cancel()
		}()

		require.NoError(t, fp.Process(ctx))

		// Reset the initialization context.
		fp.initCtx, cancel = context.WithCancel(context.Background())

		// First call to nextFixup should return the queued fixup.
		next, ok := fp.nextFixup(ctx)
		require.Equal(t, splitFixup, next.Type)
		require.True(t, ok)

		// Next call should block until the initialization context is canceled.
		go func() {
			cancel()
		}()
		next, ok = fp.nextFixup(ctx)
		require.Equal(t, fixup{}, next)
		require.False(t, ok)
	})

	t.Run("canceling context should abort processing", func(t *testing.T) {
		initCtx := context.Background()
		stopper := stop.NewStopper()
		defer stopper.Stop(initCtx)

		var fp FixupProcessor
		var index Index
		fp.Init(initCtx, stopper, &index, 42 /* seed */)

		// Add fixup and ensure that canceling the context will abort fixup
		// processing.
		ctx, cancel := context.WithCancel(context.Background())
		fp.AddSplit(ctx, nil /* treeKey */, 1, 2, false /* singleStep */)
		go func() {
			cancel()
		}()
		require.NoError(t, fp.Process(ctx))

		// First call to nextFixup should return the queued fixup.
		ctx, cancel = context.WithCancel(context.Background())
		next, ok := fp.nextFixup(ctx)
		require.Equal(t, splitFixup, next.Type)
		require.True(t, ok)

		// Next call should block until the context is canceled.
		go func() {
			cancel()
		}()
		next, ok = fp.nextFixup(ctx)
		require.Equal(t, fixup{}, next)
		require.False(t, ok)
	})

	t.Run("multiple goroutines wait for fixups to be processed", func(t *testing.T) {
		ctx := context.Background()
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)

		var fp FixupProcessor
		var index Index
		fp.Init(ctx, stopper, &index, 42 /* seed */)

		// Add fixup and ensure that multiple goroutines wait for it to be
		// processed.
		var counter atomic.Int32
		fp.AddSplit(ctx, nil /* treeKey */, 1, 2, false /* singleStep */)
		go func() {
			require.NoError(t, fp.Process(ctx))
			// counter should already have been incremented by the foreground
			// goroutine.
			require.Greater(t, counter.Add(1), int32(1))
		}()
		go func() {
			require.NoError(t, fp.Process(ctx))
			// counter should already have been incremented by the foreground
			// goroutine.
			require.Greater(t, counter.Add(1), int32(1))
		}()

		// Small delay to allow background goroutines to run.
		time.Sleep(10 * time.Millisecond)

		// Process the enqueued fixup.
		next, ok := fp.nextFixup(ctx)
		require.True(t, ok)
		require.Equal(t, 1, fp.mu.runningWorkers)
		counter.Add(1)
		fp.removeFixup(ctx, next)
		require.Equal(t, 0, fp.mu.runningWorkers)

		require.Eventually(t, func() bool { return counter.Load() == 3 },
			1*time.Second, 10*time.Millisecond)
	})
}
