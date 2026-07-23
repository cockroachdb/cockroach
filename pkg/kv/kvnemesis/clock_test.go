// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvnemesis

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestTestClock(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, _ := randutil.NewTestRand()
	maxOffset := 10 * time.Millisecond
	period := 10 * time.Second
	tolerance := 50 * time.Microsecond

	t.Run("clock nears amplitude at period", func(t *testing.T) {
		c := NewTestClock(maxOffset, period, rng)
		clock := timeutil.NewManualTime(timeutil.Unix(0, 0))
		c.clock = clock
		clock.Advance(period)
		now := c.Now()
		require.Less(t, tolerance, abs(c.amplitude.Nanoseconds()-abs(period.Nanoseconds()-now.UnixNano())))
	})
	t.Run("5000 advances", func(t *testing.T) {
		clock := timeutil.NewManualTime(timeutil.Unix(0, 0))

		c1 := NewTestClock(maxOffset, period, rng)
		c2 := NewTestClock(maxOffset, period, rng)
		c1.clock = clock
		c2.clock = clock

		for range 5000 {
			adv := rng.Int31n(100)
			clock.Advance(time.Duration(adv) * time.Millisecond)
			now1 := c1.Now()
			now2 := c2.Now()
			actual := clock.Now()

			// The two clocks should never be further apart than maxOffset
			require.LessOrEqual(t, time.Duration(abs(int64(now1.Sub(now2)))), maxOffset)

			// Also just check each clock on its own.
			clockInsideBounds := func(t *testing.T, computed time.Time, actual time.Time) {
				maxErr := maxOffset/2 + tolerance
				max := actual.Add(maxErr)
				min := actual.Add(-maxErr)
				require.Less(t, computed, max)
				require.Greater(t, computed, min)
			}
			clockInsideBounds(t, now1, actual)
			clockInsideBounds(t, now2, actual)
		}
	})
}

func abs(i int64) int64 {
	if i < 0 {
		return -1 * i
	}
	return i
}
