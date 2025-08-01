// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestRaftFortificationEnabledForRangeID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Ensure fracEnabled=0.0 never returns true.
	t.Run("fracEnabled=0.0", func(t *testing.T) {
		for i := range 10_000 {
			require.False(t, kvserver.RaftFortificationEnabledForRangeID(0.0, roachpb.RangeID(i)))
		}
	})

	// Ensure fracEnabled=1.0 always returns true.
	t.Run("fracEnabled=1.0", func(t *testing.T) {
		for i := range 10_000 {
			require.True(t, kvserver.RaftFortificationEnabledForRangeID(1.0, roachpb.RangeID(i)))
		}
	})

	// Test some specific cases with partially enabled fortification.
	testCases := []struct {
		fracEnabled float64
		rangeID     roachpb.RangeID
		exp         bool
	}{
		// 25% enabled.
		{0.25, 1, false},
		{0.25, 2, true},
		{0.25, 3, false},
		{0.25, 4, true},
		{0.25, 5, false},
		{0.25, 6, false},
		{0.25, 7, false},
		{0.25, 8, false},
		// 50% enabled.
		{0.50, 1, false},
		{0.50, 2, true},
		{0.50, 3, false},
		{0.50, 4, true},
		{0.50, 5, false},
		{0.50, 6, false},
		{0.50, 7, true},
		{0.50, 8, false},
		// 75% enabled.
		{0.75, 1, false},
		{0.75, 2, true},
		{0.75, 3, true},
		{0.75, 4, true},
		{0.75, 5, true},
		{0.75, 6, false},
		{0.75, 7, true},
		{0.75, 8, false},
	}
	for _, tc := range testCases {
		name := fmt.Sprintf("fracEnabled=%f/rangeID=%d", tc.fracEnabled, tc.rangeID)
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.exp, kvserver.RaftFortificationEnabledForRangeID(tc.fracEnabled, tc.rangeID))
		})
	}

	// Test with different fracEnabled precisions.
	for _, fracEnabled := range []float64{0.1, 0.01, 0.001} {
		t.Run(fmt.Sprintf("fracEnabled=%f", fracEnabled), func(t *testing.T) {
			enabled := 0
			const total = 100_000
			for i := range total {
				if kvserver.RaftFortificationEnabledForRangeID(fracEnabled, roachpb.RangeID(i)) {
					enabled++
				}
			}
			exp := int(fracEnabled * float64(total))
			require.InEpsilonf(t, exp, enabled, 0.1, "expected %d, got %d", exp, enabled)
		})
	}

	// Test panic on invalid fracEnabled.
	t.Run("fracEnabled=invalid", func(t *testing.T) {
		require.Panics(t, func() {
			kvserver.RaftFortificationEnabledForRangeID(-0.1, 1)
		})
		require.Panics(t, func() {
			kvserver.RaftFortificationEnabledForRangeID(1.1, 1)
		})
	})
}

// TestFortificationDisabledForExpirationBasedLeases ensures that raft
// fortification is disabled for ranges that want to use expiration based
// leases. This is always the case for meta ranges and the node liveness range,
// and can be the case for other ranges if the cluster setting to always use
// expiration based leases is turned on.
func TestRaftFortificationDisabledForExpirationBasedLeases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "useExpirationBasedLeases", func(t *testing.T, useExpirationBasedLeases bool) {
		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Settings: st,
			},
		})
		defer tc.Stopper().Stop(ctx)

		if useExpirationBasedLeases {
			kvserver.OverrideDefaultLeaseType(ctx, &st.SV, roachpb.LeaseExpiration)
		} else {
			kvserver.OverrideDefaultLeaseType(ctx, &st.SV, roachpb.LeaseLeader)
		}

		testCases := []struct {
			key roachpb.Key
			exp bool
		}{
			{
				key: keys.NodeLivenessPrefix,
				exp: false,
			},
			{
				key: keys.Meta1Prefix,
				exp: false,
			},
			{
				key: keys.Meta2Prefix,
				exp: false,
			},
			{
				key: keys.NamespaceTableMin,
				exp: true,
			},
			{
				key: keys.TableDataMin,
				exp: true,
			},
		}

		for _, test := range testCases {
			repl := tc.GetFirstStoreFromServer(t, 0).LookupReplica(roachpb.RKey(test.key))
			if useExpirationBasedLeases {
				require.False(t, repl.SupportFromEnabled())
			} else {
				require.Equal(t, test.exp, repl.SupportFromEnabled())
			}
		}
	})
}

func BenchmarkRaftFortificationEnabledForRangeID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = kvserver.RaftFortificationEnabledForRangeID(0.5, roachpb.RangeID(i))
	}
}
