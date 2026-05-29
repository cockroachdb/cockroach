// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserverbase

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/stretchr/testify/require"
)

// TestGetLoadBasedRebalancingMode covers the three cross-cutting concerns of
// GetLoadBasedRebalancingMode: explicit modes pass through unchanged, the
// LBRebalancingAuto value resolves against the v26.3 cluster version gate,
// and the COCKROACH_DISABLE_MMA kill-switch overrides any resolved MMA mode.
func TestGetLoadBasedRebalancingMode(t *testing.T) {
	ctx := context.Background()

	// settingsAtVersion returns a *cluster.Settings whose active version is
	// pinned to `v`. We use this to simulate clusters that have or have not
	// finalized through the v26.3 gate.
	settingsAtVersion := func(v clusterversion.Key) *cluster.Settings {
		return cluster.MakeTestingClusterSettingsWithVersions(
			v.Version(), v.Version(), true, /* initializeVersion */
		)
	}

	tests := []struct {
		name         string
		mode         LBRebalancingMode
		version      clusterversion.Key
		disableMMA   bool
		expectedMode LBRebalancingMode
	}{
		{
			name:         "auto pre-gate resolves to legacy",
			mode:         LBRebalancingAuto,
			version:      clusterversion.MinSupported,
			expectedMode: LBRebalancingLeasesAndReplicas,
		},
		{
			name:         "auto post-gate resolves to MMA-and-count",
			mode:         LBRebalancingAuto,
			version:      clusterversion.V26_3,
			expectedMode: LBRebalancingMultiMetricAndCount,
		},
		{
			name:         "auto post-gate respects kill-switch",
			mode:         LBRebalancingAuto,
			version:      clusterversion.V26_3,
			disableMMA:   true,
			expectedMode: LBRebalancingLeasesAndReplicas,
		},
		{
			name:         "explicit MMA-and-count post-gate passes through",
			mode:         LBRebalancingMultiMetricAndCount,
			version:      clusterversion.V26_3,
			expectedMode: LBRebalancingMultiMetricAndCount,
		},
		{
			name:         "explicit MMA-only post-gate passes through",
			mode:         LBRebalancingMultiMetricOnly,
			version:      clusterversion.V26_3,
			expectedMode: LBRebalancingMultiMetricOnly,
		},
		{
			name:         "explicit MMA pre-gate passes through",
			mode:         LBRebalancingMultiMetricOnly,
			version:      clusterversion.MinSupported,
			expectedMode: LBRebalancingMultiMetricOnly,
		},
		{
			name:         "explicit legacy post-gate passes through",
			mode:         LBRebalancingLeasesAndReplicas,
			version:      clusterversion.V26_3,
			expectedMode: LBRebalancingLeasesAndReplicas,
		},
		{
			name:         "explicit MMA + kill-switch falls back",
			mode:         LBRebalancingMultiMetricAndCount,
			version:      clusterversion.V26_3,
			disableMMA:   true,
			expectedMode: LBRebalancingLeasesAndReplicas,
		},
		{
			name:         "off pre-gate passes through",
			mode:         LBRebalancingOff,
			version:      clusterversion.MinSupported,
			expectedMode: LBRebalancingOff,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := settingsAtVersion(tc.version)
			OverrideLoadBasedRebalancingMode(ctx, &st.SV, tc.mode)

			// Swap the package-level kill-switch for the duration of this
			// case. The variable is read by GetLoadBasedRebalancingMode.
			prev := disableMMA
			disableMMA = tc.disableMMA
			defer func() { disableMMA = prev }()

			require.Equal(t, tc.expectedMode, GetLoadBasedRebalancingMode(ctx, st))
		})
	}
}

// TestGetLoadBasedRebalancingModeUninitializedVersion confirms that
// resolving auto against a *cluster.Settings whose version handle was never
// initialized (as in the asim simulator's settings) returns the conservative
// legacy mode instead of fataling.
func TestGetLoadBasedRebalancingModeUninitializedVersion(t *testing.T) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.Latest.Version(),
		clusterversion.MinSupported.Version(),
		false, /* initializeVersion */
	)
	OverrideLoadBasedRebalancingMode(ctx, &st.SV, LBRebalancingAuto)

	require.Equal(t, LBRebalancingLeasesAndReplicas, GetLoadBasedRebalancingMode(ctx, st))
}

// TestLoadBasedRebalancingDefaultIsAuto pins down the cluster setting's
// default value: it must be LBRebalancingAuto so that fresh clusters get the
// version-gated behavior rather than the explicit legacy value. A regression
// here would silently disable the v26.3 MMA rollout.
func TestLoadBasedRebalancingDefaultIsAuto(t *testing.T) {
	ctx := context.Background()

	// Pre-finalization cluster: default auto resolves to legacy.
	stPre := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.MinSupported.Version(),
		clusterversion.MinSupported.Version(),
		true, /* initializeVersion */
	)
	require.Equal(t, LBRebalancingAuto, loadBasedRebalancingMode.Get(&stPre.SV))
	require.Equal(t, LBRebalancingLeasesAndReplicas, GetLoadBasedRebalancingMode(ctx, stPre))

	// Post-finalization cluster: default auto resolves to MMA-and-count.
	stPost := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.V26_3.Version(),
		clusterversion.V26_3.Version(),
		true, /* initializeVersion */
	)
	require.Equal(t, LBRebalancingAuto, loadBasedRebalancingMode.Get(&stPost.SV))
	require.Equal(t, LBRebalancingMultiMetricAndCount, GetLoadBasedRebalancingMode(ctx, stPost))
}
