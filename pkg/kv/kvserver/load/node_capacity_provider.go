// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package load

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// StoresStatsAggregator provides aggregated CPU usage stats across all stores.
type StoresStatsAggregator interface {
	// GetAggregatedStoreStats returns the total CPU usage across all stores and
	// the count of stores. If useCached is true, it uses the cached store
	// descriptor instead of computing new ones.
	GetAggregatedStoreStats(useCached bool) (aggregatedCPUUsage int64, totalStoreCount int32)
}

// NodeCapacityProvider monitors and reports node-level CPU capacity and usage
// by aggregating stores and runtime stats.
type NodeCapacityProvider struct {
	stores             StoresStatsAggregator
	runtimeLoadMonitor *RuntimeLoadMonitor
}

// refreshIntervals define how frequently CPU metrics are updated.
const (
	// defaultCPUUsageRefreshInterval controls how often CPU usage measurements
	// are taken.
	defaultCPUUsageRefreshInterval = time.Second

	// defaultCPUCapacityRefreshInterval controls how often the total CPU
	// capacity of the node is re-calculated. This is less frequent than usage
	// since capacity changes happen less often.
	defaultCPUCapacityRefreshInterval = 10 * time.Second
)

// NewNodeCapacityProvider creates a new NodeCapacityProvider that monitors CPU
// metrics using the provided stores aggregator. The optional knobs parameter
// allows customizing refresh intervals for testing.
func NewNodeCapacityProvider(
	stopper *stop.Stopper, stores StoresStatsAggregator, knobs *NodeCapacityProviderTestingKnobs,
) *NodeCapacityProvider {
	usageInterval := defaultCPUUsageRefreshInterval
	capacityInterval := defaultCPUCapacityRefreshInterval
	if knobs != nil {
		usageInterval = knobs.CpuUsageRefreshInterval
		capacityInterval = knobs.CpuCapacityRefreshInterval
	}
	return &NodeCapacityProvider{
		stores:             stores,
		runtimeLoadMonitor: newRuntimeLoadMonitor(stopper, usageInterval, capacityInterval),
	}
}

// Run starts the background monitoring of CPU metrics.
func (n *NodeCapacityProvider) Run(ctx context.Context) {
	n.runtimeLoadMonitor.Run(ctx)
}

// GetNodeCapacity returns the current CPU capacity and usage metrics for the
// node.
func (n *NodeCapacityProvider) GetNodeCapacity(useCached bool) roachpb.NodeCapacity {
	storesCPURate, numStores := n.stores.GetAggregatedStoreStats(useCached)
	stats := n.runtimeLoadMonitor.GetCPUStats()
	return roachpb.NodeCapacity{
		StoresCPURate:       storesCPURate,
		NumStores:           numStores,
		NodeCPURateCapacity: stats.CPUCapacityNanoPerSec,
		NodeCPURateUsage:    stats.CPUUsageNanoPerSec,
	}
}
