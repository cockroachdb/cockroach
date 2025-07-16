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
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
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

func TestNewNodeCapacityProviderBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	mockStores := &mockStoresStatsAggregator{
		cpuUsage:   1000,
		storeCount: 3,
	}

	provider := NewNodeCapacityProvider(stopper, mockStores, &NodeCapacityProviderTestingKnobs{
		CpuUsageRefreshInterval:    1 * time.Millisecond,
		CpuCapacityRefreshInterval: 1 * time.Millisecond,
	})
	require.NotNil(t, provider)
	assert.Equal(t, mockStores, provider.stores)
	assert.NotNil(t, provider.runtimeLoadMonitor)

	ctx := context.Background()
	provider.Run(ctx)
	testutils.SucceedsSoon(t, func() error {
		stats := provider.GetNodeCapacity(false)
		require.NotNil(t, stats)
		if stats.NodeCPURateUsage == 0 || stats.NodeCPURateCapacity == 0 || stats.StoresCPURate == 0 {
			return errors.New("CPU usage or capacity is 0")
		}
		require.GreaterOrEqual(t, stats.NodeCPURateCapacity, stats.NodeCPURateUsage)
		return nil
	})
}
