// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package load

import (
	"context"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// StoresStatsAggregator provides aggregated cpu usage stats across all stores.
type StoresStatsAggregator interface {
	// GetAggregatedStoreStats returns the total cpu usage across all stores and
	// the count of stores. If useCached is true, it uses the cached store
	// descriptor instead of computing new ones. Implemented by Stores.
	GetAggregatedStoreStats(useCached bool) (aggregatedCPUUsage int64, totalStoreCount int32)
}

// NodeCapacityProvider reports node-level cpu usage and capacity by sampling
// runtime stats and aggregating store-level cpu capacity across all stores. It
// is used by Store to populate the NodeCapacity field in the StoreDescriptor.
type NodeCapacityProvider struct {
	stores             StoresStatsAggregator
	runtimeLoadMonitor *runtimeLoadMonitor
}

// NodeCapacityProviderConfig holds the configuration for creating a
// NodeCapacityProvider.
type NodeCapacityProviderConfig struct {
	// CPUUsageRefreshInterval controls how often cpu usage measurements are
	// sampled.
	CPUUsageRefreshInterval time.Duration
	// CPUCapacityRefreshInterval controls how often the total CPU capacity is
	// polled.
	CPUCapacityRefreshInterval time.Duration
	// CPUUsageMovingAverageAge defines the effective time window size for the
	// moving average when sampling cpu usage.
	CPUUsageMovingAverageAge float64
}

// NewNodeCapacityProvider creates a new NodeCapacityProvider that monitors CPU
// metrics using the provided stores aggregator and configuration.
func NewNodeCapacityProvider(
	stopper *stop.Stopper, stores StoresStatsAggregator, config NodeCapacityProviderConfig,
) *NodeCapacityProvider {
	if stopper == nil || stores == nil {
		panic("programming error: stopper or stores aggregator cannot be nil")
	}

	monitor := &runtimeLoadMonitor{
		stopper:                 stopper,
		usageRefreshInterval:    config.CPUUsageRefreshInterval,
		capacityRefreshInterval: config.CPUCapacityRefreshInterval,
	}
	monitor.mu.usageEWMA = ewma.NewMovingAverage(config.CPUUsageMovingAverageAge)
	monitor.recordCPUCapacity(context.Background())
	return &NodeCapacityProvider{
		stores:             stores,
		runtimeLoadMonitor: monitor,
	}
}

// Run starts the background monitoring of cpu metrics.
func (n *NodeCapacityProvider) Run(ctx context.Context) {
	_ = n.runtimeLoadMonitor.stopper.RunAsyncTask(ctx, "runtime-load-monitor", func(ctx context.Context) {
		n.runtimeLoadMonitor.run(ctx)
	})
}

// GetNodeCapacity returns the NodeCapacity which node-level cpu usage and
// capacity and aggregated store-level cpu usage. If useCached is true, it will
// use cached store descriptors to aggregate the sum of store-level cpu
// capacity.
func (n *NodeCapacityProvider) GetNodeCapacity(useCached bool) roachpb.NodeCapacity {
	storesCPURate, numStores := n.stores.GetAggregatedStoreStats(useCached)
	// TODO(wenyihu6): may be unexpected to caller that useCached only applies to
	// the stores stats but not runtime load monitor. We can change
	// runtimeLoadMonitor to also fetch updated stats.
	// TODO(wenyihu6): NodeCPURateCapacity <= NodeCPURateUsage fails on CI and
	// requires more investigation.
	cpuUsageNanoPerSec, cpuCapacityNanoPerSec := n.runtimeLoadMonitor.GetCPUStats()
	return roachpb.NodeCapacity{
		StoresCPURate:       storesCPURate,
		NumStores:           numStores,
		NodeCPURateCapacity: cpuCapacityNanoPerSec,
		NodeCPURateUsage:    cpuUsageNanoPerSec,
	}
}

// runtimeLoadMonitor polls cpu usage and capacity stats of the node
// periodically and maintaining a moving average.
type runtimeLoadMonitor struct {
	usageRefreshInterval    time.Duration
	capacityRefreshInterval time.Duration
	stopper                 *stop.Stopper

	mu struct {
		syncutil.Mutex
		// lastTotalUsageNanos tracks cumulative cpu usage in nanoseconds using
		// status.GetProcCPUTime.
		lastTotalUsageNanos float64
		// usageEWMA maintains a moving average of delta cpu usage between two
		// subsequent polls in nanoseconds. The cpu usage is obtained by polling
		// stats from status.GetProcCPUTime which is cumulative.
		usageEWMA ewma.MovingAverage
		// logicalCPUsPerSec represents the node's cpu capacity in logical
		// CPU-seconds per second, obtained from status.GetCPUCapacity.
		logicalCPUsPerSec int64
	}
}

// GetCPUStats returns the current cpu usage and capacity stats for the node.
func (m *runtimeLoadMonitor) GetCPUStats() (cpuUsageNanoPerSec int64, cpuCapacityNanoPerSec int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// usageEWMA is usage in nanoseconds. Divide by refresh interval to get the
	// per-second nano-sec rate.
	cpuUsageNanoPerSec = int64(m.mu.usageEWMA.Value() / m.usageRefreshInterval.Seconds())
	// logicalCPUsPerSec is in logical cpu-seconds per second. Convert the unit
	// from cpu-seconds to cpu-nanoseconds.
	cpuCapacityNanoPerSec = m.mu.logicalCPUsPerSec * time.Second.Nanoseconds()
	return
}

// recordCPUUsage samples and records the current cpu usage of the node.
func (m *runtimeLoadMonitor) recordCPUUsage(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	userTimeMillis, sysTimeMillis, err := status.GetProcCPUTime(ctx)
	if err != nil {
		if buildutil.CrdbTestBuild {
			panic(err)
		}
		// TODO(wenyihu6): we should revisit error handling here for production.
		log.Warningf(ctx, "failed to get cpu usage: %v", err)
	}
	// Convert milliseconds to nanoseconds.
	totalUsageNanos := float64(userTimeMillis*1e6 + sysTimeMillis*1e6)
	if buildutil.CrdbTestBuild && m.mu.lastTotalUsageNanos > totalUsageNanos {
		panic(errors.Newf("programming error: last cpu usage is larger than current: %v > %v",
			m.mu.lastTotalUsageNanos, totalUsageNanos))
	}
	m.mu.usageEWMA.Add(totalUsageNanos - m.mu.lastTotalUsageNanos)
	m.mu.lastTotalUsageNanos = totalUsageNanos
}

// recordCPUCapacity samples and records the current cpu capacity of the node.
func (m *runtimeLoadMonitor) recordCPUCapacity(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.logicalCPUsPerSec = int64(status.GetCPUCapacity())
	if m.mu.logicalCPUsPerSec == 0 {
		if buildutil.CrdbTestBuild {
			panic("programming error: cpu capacity is 0")
		}
		// TODO(wenyihu6): we should pass in an actual context here.
		log.Warningf(ctx, "failed to get cpu capacity")
	}
}

// run is the main loop of the RuntimeLoadMonitor and periodically polls the cpu
// usage and capacity. It continues to run until the context is done or the
// stopper is quiesced.
func (m *runtimeLoadMonitor) run(ctx context.Context) {
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
			m.recordCPUCapacity(ctx)
		}
	}
}
