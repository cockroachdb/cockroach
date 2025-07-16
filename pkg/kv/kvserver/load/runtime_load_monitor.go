// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package load

import (
	"context"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// defaultMovingAverageAge defines the effective time window size. With a value
// of 20 and 1 second sampling intervals, newer CPU usage measurements are
// weighted more heavily, and data from the past 20 seconds contributes
// to the average.
const defaultMovingAverageAge = 20

// RunTimeLoadStats is the stats of the runtime load.
type RunTimeLoadStats struct {
	// CPUUsageNanoPerSec is the CPU usage in nano-seconds per second.
	CPUUsageNanoPerSec int64
	// CPUCapacityNanoPerSec is the CPU capacity in logicalCPU nano-seconds per
	// second.
	CPUCapacityNanoPerSec int64
}

// RuntimeLoadMonitor is the monitor for the runtime load. It measures the cpu
// usage and capacity of the node directly using the status package.
type RuntimeLoadMonitor struct {
	usageRefreshInterval    time.Duration
	capacityRefreshInterval time.Duration
	stopper                 *stop.Stopper
	mu                      struct {
		syncutil.Mutex
		// Last cumulative CPU usage in nano-sec using
		// status.GetProcCPUTime.
		lastTotalUsageNanos float64
		// Moving average of CPU usage in nano-sec measured using
		// status.GetProcCPUTime. To get the usage per second, we divide the
		// usage by the usage refresh interval in GetCPUStats.
		usageEWMA ewma.MovingAverage
		// logicalCPU-sec per sec capacity of the node measured using
		// status.GetCPUCapacity. To get the capacity per second, we convert the
		// unit to nanoseconds in GetCPUStats.
		logicalCPUsPerSec int64
	}
}

func newRuntimeLoadMonitor(
	stopper *stop.Stopper, usageRefreshInterval time.Duration, capacityRefreshInterval time.Duration,
) *RuntimeLoadMonitor {
	if stopper == nil {
		panic("programming error: stopper cannot be nil")
	}
	monitor := &RuntimeLoadMonitor{}
	monitor.stopper = stopper
	monitor.mu.usageEWMA = ewma.NewMovingAverage(defaultMovingAverageAge)
	monitor.usageRefreshInterval = usageRefreshInterval
	monitor.capacityRefreshInterval = capacityRefreshInterval
	return monitor
}

// GetCPUStats returns the current CPU usage and capacity stats for the node.
func (m *RuntimeLoadMonitor) GetCPUStats() *RunTimeLoadStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &RunTimeLoadStats{
		CPUUsageNanoPerSec:    int64(m.mu.usageEWMA.Value() / m.usageRefreshInterval.Seconds()),
		CPUCapacityNanoPerSec: m.mu.logicalCPUsPerSec * (time.Second.Nanoseconds()),
	}
}

// recordCPUUsage records the CPU usage of the node.
func (m *RuntimeLoadMonitor) recordCPUUsage(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	userTime, sysTime, err := status.GetProcCPUTime(ctx)
	if buildutil.CrdbTestBuild && err != nil {
		panic(err)
	}
	// Convert milliseconds to nanoseconds.
	totalUsageNanos := float64(userTime*1e6 + sysTime*1e6)
	if buildutil.CrdbTestBuild && m.mu.lastTotalUsageNanos > totalUsageNanos {
		panic("programming error: last cpu usage is larger than current")
	}
	m.mu.usageEWMA.Add(totalUsageNanos - m.mu.lastTotalUsageNanos)
	m.mu.lastTotalUsageNanos = totalUsageNanos
}

// recordCPUCapacity records the CPU capacity of the node.
func (m *RuntimeLoadMonitor) recordCPUCapacity() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.logicalCPUsPerSec = int64(status.GetCPUCapacity())
}

// run is the main loop of the RuntimeLoadMonitor. It records the CPU usage and
// capacity periodically. It continues to run until the context is done or the
// stopper is quiesced.
func (m *RuntimeLoadMonitor) run(ctx context.Context) {
	usageTimer := time.NewTicker(m.usageRefreshInterval)
	defer usageTimer.Stop()
	capacityTimer := time.NewTicker(m.capacityRefreshInterval)
	defer capacityTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopper.ShouldQuiesce():
			return
		case <-usageTimer.C:
			usageTimer.Reset(m.usageRefreshInterval)
			m.recordCPUUsage(ctx)
		case <-capacityTimer.C:
			capacityTimer.Reset(m.capacityRefreshInterval)
			m.recordCPUCapacity()
		}
	}
}

func (m *RuntimeLoadMonitor) Run(ctx context.Context) {
	_ = m.stopper.RunAsyncTask(ctx, "runtime-load-monitor", func(ctx context.Context) {
		m.run(ctx)
	})
}
