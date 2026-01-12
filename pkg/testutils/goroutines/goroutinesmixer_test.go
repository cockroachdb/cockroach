// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goroutines

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestGoroutineMixer_ZipfDepthSamplerBoundsAndNonDegenerate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s := stop.NewStopper()
	_, seed := randutil.NewTestRand()

	h, err := Start(ctx, s, Options{
		Initial:         0,
		Max:             1,
		SpawnBatch:      1000,
		StackDepthMin:   0,
		StackDepthMax:   16,
		StackDepthZipfS: 1.3,
		StackDepthZipfV: 2,
		StackDepthSeed:  seed,
		Name:            "zipf-depth-test",
	})
	require.NoError(t, err)

	sampler := h.newDepthSampler(999)

	seen := make(map[int]struct{})
	maxSeen := -1
	for i := 0; i < 50_000; i++ {
		d := sampler()
		require.GreaterOrEqual(t, d, 0)
		require.LessOrEqual(t, d, 32)
		seen[d] = struct{}{}
		if d > maxSeen {
			maxSeen = d
		}
	}

	// Not degenerate.
	require.Greater(t, len(seen), 5, "expected a variety of depths")

	// Some tail (with these params and sample size, this should be stable).
	require.GreaterOrEqual(t, maxSeen, 12, "expected some deeper stacks in the tail")

	s.Stop(ctx)
	require.Equal(t, 0, h.Running())
}

func TestGoroutineMixer_DoubleAndMax(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s := stop.NewStopper()
	defer s.Stop(ctx)

	h, err := Start(ctx, s, Options{
		Initial:    50,
		Max:        400,
		SpawnBatch: 1000,
		Name:       "double-test",
	})
	require.NoError(t, err)

	testutils.SucceedsSoon(t, func() error {
		if h.Running() != 50 {
			return errors.Errorf("expected 50 running goroutines, got %d", h.Running())
		}
		return nil
	})

	h.Double()
	testutils.SucceedsSoon(t, func() error {
		if h.Running() != 100 {
			return errors.Errorf("expected 100 running goroutines, got %d", h.Running())
		}
		return nil
	})

	h.Double()
	testutils.SucceedsSoon(t, func() error {
		if h.Running() != 200 {
			return errors.Errorf("expected 200 running goroutines, got %d", h.Running())
		}
		return nil
	})

	h.Double()
	testutils.SucceedsSoon(t, func() error {
		if h.Running() != 400 {
			return errors.Errorf("expected 400 running goroutines, got %d", h.Running())
		}
		return nil
	})

	// Further doubles should clamp to Max.
	h.Double()
	require.Equal(t, 400, h.Running())
}

func TestGoroutineMixer_ShutdownAndRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	// Start & stop once.
	{
		s := stop.NewStopper()
		h, err := Start(ctx, s, Options{
			Initial:    200,
			Max:        1000,
			SpawnBatch: 1000,
			Name:       "restart-1",
		})
		require.NoError(t, err)

		testutils.SucceedsSoon(t, func() error {
			if h.Running() != 200 {
				return errors.Errorf("expected 200 running goroutines, got %d", h.Running())
			}
			return nil
		})

		s.Stop(ctx)
		// After Stop returns, closers ran and ctxgroup waited: running should drain to 0.
		require.Equal(t, 0, h.Running())
	}

	// Restart with a new stopper.
	{
		s := stop.NewStopper()

		h, err := Start(ctx, s, Options{
			Initial: 123,
			Name:    "restart-2",
		})
		require.NoError(t, err)

		testutils.SucceedsSoon(t, func() error {
			if h.Running() != 123 {
				return errors.Errorf("expected 123 running goroutines, got %d", h.Running())
			}
			return nil
		})

		s.Stop(ctx)
		// After Stop returns, closers ran and ctxgroup waited: running should drain to 0.
		require.Equal(t, 0, h.Running())
	}
}

func TestGoroutineMixer_GrowByIsLosslessAndCoalesced(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s := stop.NewStopper()
	defer s.Stop(ctx)

	mix := DefaultMix
	if skip.Duress() {
		// Stick to a lightweight workload mix under duress to avoid test timeouts.
		mix = Mix{Select: 1}
	}

	h, err := Start(ctx, s, Options{
		Initial:    0,
		Max:        1000,
		SpawnBatch: 10000,
		Mix:        mix,
		Name:       "growby-coalesce",
	})
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		h.GrowBy(2) // total 200
	}
	testutils.SucceedsSoon(t, func() error {
		if h.Running() != 200 {
			return errors.Errorf("expected 200 running goroutines, got %d", h.Running())
		}
		return nil
	})

	// Another big chunk.
	for i := 0; i < 1000; i++ {
		h.GrowBy(5) // +5000, but Max clamps at 1000
	}
	testutils.SucceedsSoon(t, func() error {
		if h.Running() != 1000 {
			return errors.Errorf("expected 1000 running goroutines, got %d", h.Running())
		}
		return nil
	})
}

func TestGoroutineMixer_IOCountDrainsOnShutdown(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s := stop.NewStopper()

	h, err := Start(ctx, s, Options{
		Initial:        200,
		Max:            200,
		SpawnBatch:     1000,
		IOWaitMaxConns: 64, // force some IO goroutines to fall back
		TimerPeriod:    50 * time.Millisecond,
		CPUBurst:       50 * time.Microsecond,
		CPUSleep:       1 * time.Millisecond,
		StackDepth:     2,
		Mix:            Mix{IO: 3, Chan: 1},
		Name:           "io-drain",
	})
	require.NoError(t, err)

	testutils.SucceedsSoon(t, func() error {
		if h.Running() != 200 {
			return errors.Errorf("expected 200 running goroutines, got %d", h.Running())
		}
		return nil
	})

	// Stopper should cause IO goroutines to return and decrement ioCount.
	s.Stop(ctx)
	require.Equal(t, 0, h.Running())
	require.LessOrEqual(t, int(h.ioCount.Load()), 0)
}
