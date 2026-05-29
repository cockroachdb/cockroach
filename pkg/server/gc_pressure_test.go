// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestGCPressureChecker(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const threshold = 0.25
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tick := 10 * time.Second

	t.Run("no trigger below threshold", func(t *testing.T) {
		var c gcPressureChecker
		now := baseTime
		for i := 0; i < 10; i++ {
			now = now.Add(tick)
			require.False(t, c.check(0.10, threshold, now))
		}
	})

	t.Run("no trigger on single sample above threshold", func(t *testing.T) {
		var c gcPressureChecker
		require.False(t, c.check(0.30, threshold, baseTime))
	})

	t.Run("trigger after sustained samples above threshold", func(t *testing.T) {
		var c gcPressureChecker
		require.False(t, c.check(0.30, threshold, baseTime))
		require.False(t, c.check(0.40, threshold, baseTime.Add(tick)))
		require.True(t, c.check(0.50, threshold, baseTime.Add(2*tick)))
	})

	t.Run("trigger at exact threshold boundary", func(t *testing.T) {
		var c gcPressureChecker
		require.False(t, c.check(threshold, threshold, baseTime))
		require.False(t, c.check(threshold, threshold, baseTime.Add(tick)))
		require.True(t, c.check(threshold, threshold, baseTime.Add(2*tick)))
	})

	t.Run("reset when sample drops below threshold", func(t *testing.T) {
		var c gcPressureChecker
		require.False(t, c.check(0.30, threshold, baseTime))
		require.False(t, c.check(0.40, threshold, baseTime.Add(tick)))
		require.False(t, c.check(0.10, threshold, baseTime.Add(2*tick)))
		require.False(t, c.check(0.30, threshold, baseTime.Add(3*tick)))
		require.False(t, c.check(0.40, threshold, baseTime.Add(4*tick)))
		require.True(t, c.check(0.50, threshold, baseTime.Add(5*tick)))
	})

	t.Run("cooldown prevents rapid warnings", func(t *testing.T) {
		var c gcPressureChecker
		c.check(0.30, threshold, baseTime)
		c.check(0.40, threshold, baseTime.Add(tick))
		require.True(t, c.check(0.50, threshold, baseTime.Add(2*tick)))

		afterTrigger := baseTime.Add(3 * tick)
		c.check(0.30, threshold, afterTrigger)
		c.check(0.40, threshold, afterTrigger.Add(tick))
		require.False(t, c.check(0.50, threshold, afterTrigger.Add(2*tick)))
	})

	t.Run("cooldown boundary still blocks", func(t *testing.T) {
		var c gcPressureChecker
		c.check(0.30, threshold, baseTime)
		c.check(0.40, threshold, baseTime.Add(tick))
		require.True(t, c.check(0.50, threshold, baseTime.Add(2*tick)))

		atCooldownEdge := baseTime.Add(gcPressureCooldown - time.Nanosecond)
		c.check(0.30, threshold, atCooldownEdge)
		c.check(0.40, threshold, atCooldownEdge.Add(tick))
		require.False(t, c.check(0.50, threshold, atCooldownEdge.Add(2*tick)))
	})

	t.Run("warning after cooldown expires", func(t *testing.T) {
		var c gcPressureChecker
		c.check(0.30, threshold, baseTime)
		c.check(0.40, threshold, baseTime.Add(tick))
		require.True(t, c.check(0.50, threshold, baseTime.Add(2*tick)))

		pastCooldown := baseTime.Add(2*tick + gcPressureCooldown + time.Second)
		c.check(0.30, threshold, pastCooldown)
		c.check(0.40, threshold, pastCooldown.Add(tick))
		require.True(t, c.check(0.50, threshold, pastCooldown.Add(2*tick)))
	})

	t.Run("threshold zero always triggers if called", func(t *testing.T) {
		// The call site guards with threshold > 0, but verify the
		// checker's behavior: any non-negative ratio >= 0 counts.
		var c gcPressureChecker
		require.False(t, c.check(0.0, 0.0, baseTime))
		require.False(t, c.check(0.0, 0.0, baseTime.Add(tick)))
		require.True(t, c.check(0.0, 0.0, baseTime.Add(2*tick)))
	})
}
