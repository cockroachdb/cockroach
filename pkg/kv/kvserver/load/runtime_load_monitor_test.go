// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package load

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRuntimeLoadMonitor tests the basic functionality of the
// RuntimeLoadMonitor.
func TestRuntimeLoadMonitor(t *testing.T) {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	t.Run("get cpu stats", func(t *testing.T) {
		rlm := newRuntimeLoadMonitor(stopper, 1*time.Millisecond, 1*time.Millisecond)

		stats := rlm.GetCPUStats()
		require.NotNil(t, stats)
		assert.IsType(t, &RunTimeLoadStats{}, stats)

		assert.GreaterOrEqual(t, stats.CPUUsageNanoPerSec, int64(0))
		assert.GreaterOrEqual(t, stats.CPUCapacityNanoPerSec, int64(0))
		require.GreaterOrEqual(t, stats.CPUCapacityNanoPerSec, stats.CPUUsageNanoPerSec)
	})
	t.Run("start, stop, and record", func(t *testing.T) {
		rlm := newRuntimeLoadMonitor(stopper, 1*time.Millisecond, 1*time.Millisecond)
		rlm.Run(context.Background())

		testutils.SucceedsSoon(t, func() error {
			stats := rlm.GetCPUStats()
			require.NotNil(t, stats)
			if stats.CPUUsageNanoPerSec == 0 || stats.CPUCapacityNanoPerSec == 0 {
				return errors.New("CPU usage or capacity is 0")
			}
			require.GreaterOrEqual(t, stats.CPUCapacityNanoPerSec, stats.CPUUsageNanoPerSec)
			return nil
		})

		ctx, cancel := context.WithCancel(context.Background())
		rlm2 := newRuntimeLoadMonitor(stopper, 1*time.Millisecond, 1*time.Millisecond)
		rlm2.Run(ctx)

		cancel()

		// Make sure the monitor does not return nil after cancellation.
		assert.NotNil(t, rlm2.GetCPUStats())
	})
}
