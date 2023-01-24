// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestLoadBasedRebalancingObjective(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	t.Run("latest version supports all rebalance objectives", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LoadBasedRebalancingObjective.Override(ctx, &st.SV, int64(LBRebalancingQueries))
		require.Equal(t, LBRebalancingQueries, ResolveLBRebalancingObjective(ctx, st))

		LoadBasedRebalancingObjective.Override(ctx, &st.SV, int64(LBRebalancingCPU))
		require.Equal(t, LBRebalancingCPU, ResolveLBRebalancingObjective(ctx, st))

		LoadBasedRebalancingObjective.Override(ctx, &st.SV, int64(LBRebalancingQueries))
		require.Equal(t, LBRebalancingQueries, ResolveLBRebalancingObjective(ctx, st))
	})

	t.Run("older version only supports QPS", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettingsWithVersions(
			clusterversion.ByKey(clusterversion.V22_2),
			clusterversion.ByKey(clusterversion.V22_2), true)
		LoadBasedRebalancingObjective.Override(ctx, &st.SV, int64(LBRebalancingQueries))
		require.Equal(t, LBRebalancingQueries, ResolveLBRebalancingObjective(ctx, st))

		// Despite setting to CPU, only QPS should be returned.
		LoadBasedRebalancingObjective.Override(ctx, &st.SV, int64(LBRebalancingCPU))
		require.Equal(t, LBRebalancingQueries, ResolveLBRebalancingObjective(ctx, st))
	})
}

func TestRebalanceObjectiveManager(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	makeTestManager := func(st *cluster.Settings) (*RebalanceObjectiveManager, *[]LBRebalancingObjective) {
		callbacks := make([]LBRebalancingObjective, 0, 1)
		cb := func(ctx context.Context, obj LBRebalancingObjective) {
			callbacks = append(callbacks, obj)
		}
		return newRebalanceObjectiveManager(ctx, st, cb), &callbacks
	}

	t.Run("latest version", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		LoadBasedRebalancingObjective.Override(ctx, &st.SV, int64(LBRebalancingCPU))
		manager, callbacks := makeTestManager(st)

		// Initially expect the manager to have CPU set as the cluster
		// setting as it is overriden above.
		require.Equal(t, LBRebalancingCPU, manager.Objective())
		require.Len(t, *callbacks, 0)

		// Override the setting to be QPS, which will trigger a callback.
		LoadBasedRebalancingObjective.Override(ctx, &st.SV, int64(LBRebalancingQueries))
		require.Equal(t, LBRebalancingQueries, manager.Objective())
		require.Len(t, *callbacks, 1)
		require.Equal(t, LBRebalancingQueries, (*callbacks)[0])

		// Override the setting again back to CPU, which will trigger a
		// callback.
		LoadBasedRebalancingObjective.Override(ctx, &st.SV, int64(LBRebalancingCPU))
		require.Equal(t, LBRebalancingCPU, manager.Objective())
		require.Len(t, *callbacks, 2)
		require.Equal(t, LBRebalancingCPU, (*callbacks)[1])
	})

	// After updating the active cluster version to an earlier version, the
	// objective should change from CPU to QPS as CPU is not
	// supported on this version.
	t.Run("store cpu unsupported version", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettingsWithVersions(
			clusterversion.TestingBinaryVersion,
			clusterversion.ByKey(clusterversion.V22_2), false)
		require.NoError(t, st.Version.SetActiveVersion(ctx, clusterversion.ClusterVersion{
			Version: clusterversion.ByKey(clusterversion.V22_2),
		}))
		LoadBasedRebalancingObjective.Override(ctx, &st.SV, int64(LBRebalancingCPU))

		manager, callbacks := makeTestManager(st)

		// Initially expect the manager to have QPS set, despite the cluster
		// setting being overriden to CPU.
		require.Equal(t, LBRebalancingQueries, manager.Objective())
		require.Len(t, *callbacks, 0)

		// Override the setting to be CPU, which shouldn't trigger a
		// callback as it isn't supported in this version.
		LoadBasedRebalancingObjective.Override(ctx, &st.SV, int64(LBRebalancingCPU))
		require.Equal(t, LBRebalancingQueries, manager.Objective())
		require.Len(t, *callbacks, 0)

		// Update the active version to match the minimum version required for
		// allowing CPU objective. This should trigger a callback and also
		// update the objective to CPU.
		require.NoError(t, st.Version.SetActiveVersion(ctx, clusterversion.ClusterVersion{
			Version: clusterversion.ByKey(clusterversion.V23_1AllocatorCPUBalancing),
		}))
		require.Equal(t, LBRebalancingCPU, manager.Objective())
		require.Len(t, *callbacks, 1)
		require.Equal(t, LBRebalancingCPU, (*callbacks)[0])

		// Override the setting to be QPS, which will trigger a callback after
		// switching from CPU.
		LoadBasedRebalancingObjective.Override(ctx, &st.SV, int64(LBRebalancingQueries))
		require.Equal(t, LBRebalancingQueries, manager.Objective())
		require.Len(t, *callbacks, 2)
		require.Equal(t, LBRebalancingQueries, (*callbacks)[1])
	})
}
