// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// testingNotifierProvider implements the gossipStoreDescriptorProvider
// interface and gossipStoreCapacityChangeNotifier interface.
type testingProviderNotifier struct {
	descMap  map[roachpb.StoreID]roachpb.StoreDescriptor
	onChange []func(roachpb.StoreID, roachpb.StoreCapacity, roachpb.StoreCapacity)
}

// GetStores returns information on all the stores with descriptor in the pool.
func (tpn *testingProviderNotifier) GetStores() map[roachpb.StoreID]roachpb.StoreDescriptor {
	return tpn.descMap
}

func (tpn *testingProviderNotifier) set(desc roachpb.StoreDescriptor) {
	old := tpn.descMap[desc.StoreID].Capacity
	cur := desc.Capacity
	tpn.descMap[desc.StoreID] = desc

	for _, fn := range tpn.onChange {
		fn(desc.StoreID, old, cur)
	}
}

// SetOnCapacityChange installs a callback to be called when any store capacity
// changes.
func (tpn *testingProviderNotifier) SetOnCapacityChange(fn storepool.CapacityChangeFn) {
	tpn.onChange = append(tpn.onChange, fn)
}

func testMakeStoreDescWithCPU(storeID roachpb.StoreID, cpu float64) roachpb.StoreDescriptor {
	return roachpb.StoreDescriptor{
		Node:     roachpb.NodeDescriptor{NodeID: roachpb.NodeID(storeID)},
		StoreID:  storeID,
		Capacity: roachpb.StoreCapacity{CPUPerSecond: cpu},
	}
}

// testMakeProviderNotifier creates a mock implementation of the
// gossipStoreDescriptorProvider interface suitable for testing.
func testMakeProviderNotifier(
	gossipStoreCPUValues map[roachpb.StoreID]float64,
) *testingProviderNotifier {
	descMap := map[roachpb.StoreID]roachpb.StoreDescriptor{}
	for storeID, cpu := range gossipStoreCPUValues {
		descMap[storeID] = testMakeStoreDescWithCPU(storeID, cpu)
	}
	return &testingProviderNotifier{
		descMap,
		[]func(roachpb.StoreID, roachpb.StoreCapacity, roachpb.StoreCapacity){},
	}
}

var allPositiveCPUMap = map[roachpb.StoreID]float64{1: 1, 2: 2, 3: 3}
var oneNegativeCPUMap = map[roachpb.StoreID]float64{1: 1, 2: 2, 3: -1}

func TestLoadBasedRebalancingObjective(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// On ARM MacOS and other architectures, grunning isn't supported so
	// changing the objective to CPU should never work. If this test is  run on
	// one of these unsupported aarch, test this behavior only.
	if !grunning.Supported() {
		st := cluster.MakeTestingClusterSettings()

		gossipStoreDescProvider := testMakeProviderNotifier(allPositiveCPUMap)
		LoadBasedRebalancingObjective.Override(ctx, &st.SV, LBRebalancingQueries)
		require.Equal(t,
			LBRebalancingQueries,
			ResolveLBRebalancingObjective(ctx, st, gossipStoreDescProvider.GetStores()),
		)

		// Despite setting to CPU, only QPS should be returned since this aarch
		// doesn't support grunning.
		LoadBasedRebalancingObjective.Override(ctx, &st.SV, LBRebalancingCPU)
		require.Equal(t,
			LBRebalancingQueries,
			ResolveLBRebalancingObjective(ctx, st, gossipStoreDescProvider.GetStores()),
		)
		return
	}

	t.Run("latest version supports all rebalance objectives", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		gossipStoreDescProvider := testMakeProviderNotifier(allPositiveCPUMap)
		LoadBasedRebalancingObjective.Override(ctx, &st.SV, LBRebalancingQueries)
		require.Equal(t,
			LBRebalancingQueries,
			ResolveLBRebalancingObjective(ctx, st, gossipStoreDescProvider.GetStores()),
		)

		LoadBasedRebalancingObjective.Override(ctx, &st.SV, LBRebalancingCPU)
		require.Equal(t,
			LBRebalancingCPU,
			ResolveLBRebalancingObjective(ctx, st, gossipStoreDescProvider.GetStores()),
		)

		LoadBasedRebalancingObjective.Override(ctx, &st.SV, LBRebalancingQueries)
		require.Equal(t,
			LBRebalancingQueries,
			ResolveLBRebalancingObjective(ctx, st, gossipStoreDescProvider.GetStores()),
		)
	})

	t.Run("remote node set cpu to -1, signalling no support", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		// The store with StoreID 3 has a -1 CPUPerSecond value, this indicates
		// no support for grunning and the objective should revert to
		// LBRebalancingQueries if the cluster setting is set to
		// LBRebalancingCPU.
		gossipStoreDescProvider := testMakeProviderNotifier(oneNegativeCPUMap)
		LoadBasedRebalancingObjective.Override(ctx, &st.SV, LBRebalancingQueries)
		require.Equal(t,
			LBRebalancingQueries,
			ResolveLBRebalancingObjective(ctx, st, gossipStoreDescProvider.GetStores()),
		)

		LoadBasedRebalancingObjective.Override(ctx, &st.SV, LBRebalancingCPU)
		require.Equal(t,
			LBRebalancingQueries,
			ResolveLBRebalancingObjective(ctx, st, gossipStoreDescProvider.GetStores()),
		)

		LoadBasedRebalancingObjective.Override(ctx, &st.SV, LBRebalancingQueries)
		require.Equal(t,
			LBRebalancingQueries,
			ResolveLBRebalancingObjective(ctx, st, gossipStoreDescProvider.GetStores()),
		)
	})
}

func TestRebalanceObjectiveManager(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	makeTestManager := func(
		st *cluster.Settings,
		providerNotifier *testingProviderNotifier,
	) (*RebalanceObjectiveManager, *[]LBRebalancingObjective) {
		callbacks := make([]LBRebalancingObjective, 0, 1)
		cb := func(ctx context.Context, obj LBRebalancingObjective) {
			callbacks = append(callbacks, obj)
		}
		return newRebalanceObjectiveManager(
			ctx, log.MakeTestingAmbientCtxWithNewTracer(),
			st, cb,
			providerNotifier, providerNotifier,
		), &callbacks
	}

	// On ARM MacOS and other architectures, grunning isn't supported so
	// changing the objective to CPU should never work. If this test is  run on
	// one of these unsupported aarch, test this behavior only.
	if !grunning.Supported() {
		st := cluster.MakeTestingClusterSettings()
		LoadBasedRebalancingObjective.Override(ctx, &st.SV, LBRebalancingQueries)
		providerNotifier := testMakeProviderNotifier(allPositiveCPUMap)
		manager, callbacks := makeTestManager(st, providerNotifier)

		require.Equal(t, LBRebalancingQueries, manager.Objective())

		// Changing the objective to CPU should not work since it isn't
		// supported on this aarch.
		LoadBasedRebalancingObjective.Override(ctx, &st.SV, LBRebalancingCPU)
		require.Equal(t, LBRebalancingQueries, manager.Objective())
		require.Len(t, *callbacks, 0)

		return
	}

	t.Run("latest version", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LoadBasedRebalancingObjective.Override(ctx, &st.SV, LBRebalancingCPU)
		providerNotifier := testMakeProviderNotifier(allPositiveCPUMap)
		manager, callbacks := makeTestManager(st, providerNotifier)

		// Initially expect the manager to have CPU set as the cluster
		// setting as it is overriden above.
		require.Equal(t, LBRebalancingCPU, manager.Objective())
		require.Len(t, *callbacks, 0)

		// Override the setting to be QPS, which will trigger a callback.
		LoadBasedRebalancingObjective.Override(ctx, &st.SV, LBRebalancingQueries)
		require.Equal(t, LBRebalancingQueries, manager.Objective())
		require.Len(t, *callbacks, 1)
		require.Equal(t, LBRebalancingQueries, (*callbacks)[0])

		// Override the setting again back to CPU, which will trigger a
		// callback.
		LoadBasedRebalancingObjective.Override(ctx, &st.SV, LBRebalancingCPU)
		require.Equal(t, LBRebalancingCPU, manager.Objective())
		require.Len(t, *callbacks, 2)
		require.Equal(t, LBRebalancingCPU, (*callbacks)[1])
	})

	t.Run("latest version, remote node no cpu support", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LoadBasedRebalancingObjective.Override(ctx, &st.SV, LBRebalancingCPU)
		providerNotifier := testMakeProviderNotifier(allPositiveCPUMap)
		manager, callbacks := makeTestManager(st, providerNotifier)

		// Initially expect the manager to have CPU set as the cluster
		// setting as it is overriden above.
		require.Equal(t, LBRebalancingCPU, manager.Objective())
		require.Len(t, *callbacks, 0)

		// Add a new store to the map which has a CPU value set that indicates
		// no support. This should trigger a callback as the objective reverts
		// to LBRebalancingQueries which is supported.
		providerNotifier.set(testMakeStoreDescWithCPU(4, -1))
		require.Equal(t, LBRebalancingQueries, manager.Objective())
		require.Len(t, *callbacks, 1)
		require.Equal(t, LBRebalancingQueries, (*callbacks)[0])

		// Update the store added above to have a non-negative cpu value
		// indicating that it now supports cpu (not likely to happen in real
		// life but possible). This should trigger another callback as the
		// objective switches to LBRebalancingCPU.
		providerNotifier.set(testMakeStoreDescWithCPU(4, 1))
		require.Equal(t, LBRebalancingCPU, manager.Objective())
		require.Len(t, *callbacks, 2)
		require.Equal(t, LBRebalancingCPU, (*callbacks)[1])
	})
}
