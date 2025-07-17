// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package load_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/load"
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

	provider := load.NewNodeCapacityProvider(stopper, mockStores, load.NodeCapacityProviderConfig{
		CPUUsageRefreshInterval:    1 * time.Millisecond,
		CPUCapacityRefreshInterval: 1 * time.Millisecond,
		CPUUsageMovingAverageAge:   20,
	})

	ctx, cancel := context.WithCancel(context.Background())
	provider.Run(ctx)

	// Provider should have valid stats.
	testutils.SucceedsSoon(t, func() error {
		nc := provider.GetNodeCapacity(false)
		if nc.NodeCPURateUsage == 0 || nc.NodeCPURateCapacity == 0 || nc.StoresCPURate == 0 {
			return errors.Newf(
				"CPU usage or capacity is 0: node cpu rate usage %v, node cpu rate capacity %v, stores cpu rate %v",
				nc.NodeCPURateUsage, nc.NodeCPURateCapacity, nc.StoresCPURate)
		}
		return nil
	})

	cancel()
	// GetNodeCapacity should still return valid stats after cancellation.
	nc := provider.GetNodeCapacity(false)
	require.Greater(t, nc.NodeCPURateCapacity, int64(0))
	require.Greater(t, nc.NodeCPURateUsage, int64(0))
}
