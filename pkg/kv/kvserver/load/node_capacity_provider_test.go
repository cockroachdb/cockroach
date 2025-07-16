// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package load

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// mockStoresStatsAggregator implements StoresStatsAggregator for testing.
type mockStoresStatsAggregator struct {
	cpuUsage   int64
	storeCount int32
}

func (m *mockStoresStatsAggregator) GetAggregatedStoreStats(
	_ bool,
) (totalCPUUsage int64, totalStoreCount int32) {
	return m.cpuUsage, m.storeCount
}

// TestNodeCapacityProvider tests the basic functionality of the
// NodeCapacityProvider.
func TestNodeCapacityProvider(t *testing.T) {
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	mockStores := &mockStoresStatsAggregator{
		cpuUsage:   1000,
		storeCount: 3,
	}

	activeProvider := NewNodeCapacityProvider(stopper, mockStores, &NodeCapacityProviderTestingKnobs{
		CpuUsageRefreshInterval:    1 * time.Millisecond,
		CpuCapacityRefreshInterval: 1 * time.Millisecond,
	})

	activeProvider.Run(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	canceledProvider := NewNodeCapacityProvider(stopper, mockStores, &NodeCapacityProviderTestingKnobs{
		CpuUsageRefreshInterval:    1 * time.Millisecond,
		CpuCapacityRefreshInterval: 1 * time.Millisecond,
	})
	canceledProvider.Run(ctx)
	cancel()

	// Active provider should have valid stats.
	testutils.SucceedsSoon(t, func() error {
		nc := activeProvider.GetNodeCapacity(false)
		require.NotNil(t, nc)
		if nc.NodeCPURateUsage == 0 || nc.NodeCPURateCapacity == 0 || nc.StoresCPURate == 0 {
			return errors.Newf(
				"CPU usage or capacity is 0: node cpu rate usage %v, node cpu rate capacity %v, stores cpu rate %v",
				nc.NodeCPURateUsage, nc.NodeCPURateCapacity, nc.StoresCPURate)
		}
		require.GreaterOrEqual(t, nc.NodeCPURateCapacity, nc.NodeCPURateUsage)
		return nil
	})

	// Make sure the canceled provider does not crash after cancellation.
	// GetNodeCapacity should still return valid stats.
	nc := canceledProvider.GetNodeCapacity(false)
	usage := nc.NodeCPURateUsage
	require.Greater(t, nc.NodeCPURateCapacity, int64(0))
	require.Never(t, func() bool {
		stats := canceledProvider.GetNodeCapacity(false)
		return stats.NodeCPURateUsage != usage
	}, 10*time.Millisecond, 1*time.Millisecond)
}
