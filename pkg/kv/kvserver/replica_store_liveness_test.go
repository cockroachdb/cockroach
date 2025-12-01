// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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
				require.False(t, repl.SupportFromEnabled(repl.Desc()))
			} else {
				require.Equal(t, test.exp, repl.SupportFromEnabled(repl.Desc()))
			}
		}
	})
}

func BenchmarkRaftFortificationEnabledForRangeID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = kvserver.RaftFortificationEnabledForRangeID(0.5, roachpb.RangeID(i))
	}
}

// TestLeaseTransferAfterStoreLivenessSupportWithdrawn is a regression test for
// https://github.com/cockroachdb/cockroach/issues/158513. It demonstrates the
// hazard where a lease can be transferred back to a store that has recently had
// its support withdrawn.
func TestLeaseTransferAfterStoreLivenessSupportWithdrawn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	// Ensure we're using leader leases which rely on store liveness.
	kvserver.OverrideDefaultLeaseType(ctx, &st.SV, roachpb.LeaseLeader)

	const numNodes = 3
	// Set up zone config with lease preference on n1 (rack=1).
	zcfg := zonepb.DefaultZoneConfig()
	zcfg.LeasePreferences = []zonepb.LeasePreference{
		{
			Constraints: []zonepb.Constraint{
				{
					Type:  zonepb.Constraint_REQUIRED,
					Key:   "rack",
					Value: "1",
				},
			},
		},
	}

	// Create server args with localities for each node.
	serverArgs := make(map[int]base.TestServerArgs, numNodes)
	for i := 0; i < numNodes; i++ {
		serverArgs[i] = base.TestServerArgs{
			Settings: st,
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{{Key: "rack", Value: fmt.Sprintf("%d", i+1)}},
			},
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ConfigureScratchRange: true,
				},
				Server: &server.TestingKnobs{
					DefaultZoneConfigOverride: &zcfg,
				},
			},
			RaftConfig: base.RaftConfig{
				// Speed up the test by using faster raft ticks.
				RaftTickInterval:           100 * time.Millisecond,
				RaftElectionTimeoutTicks:   5,
				RaftHeartbeatIntervalTicks: 1,
			},
		}
	}

	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode:   base.ReplicationManual,
		ServerArgsPerNode: serverArgs,
	})
	defer tc.Stopper().Stop(ctx)

	// Setup the test by creating a scratch range with voters on n1, n2, and n3.
	// Ensure the lease is on n1 to begin with.
	scratchKey := tc.ScratchRange(t)
	desc := tc.AddVotersOrFatal(t, scratchKey, tc.Targets(1, 2)...)
	tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(0))
	var repl1 *kvserver.Replica
	testutils.SucceedsSoon(t, func() error {
		repl1 = tc.GetFirstStoreFromServer(t, 0).LookupReplica(roachpb.RKey(scratchKey))
		if repl1 == nil {
			return errors.New("replica not found")
		}
		leaseStatus := repl1.CurrentLeaseStatus(ctx)
		if !leaseStatus.IsValid() {
			return errors.New("lease not valid")
		}
		if leaseStatus.Lease.Type() != roachpb.LeaseLeader {
			return errors.Errorf("lease type is %s, expected leader lease", leaseStatus.Lease.Type())
		}
		if leaseStatus.Lease.Replica.StoreID != tc.Target(0).StoreID {
			return errors.Errorf("lease holder is %d, expected %d",
				leaseStatus.Lease.Replica.StoreID, tc.Target(0).StoreID)
		}
		return nil
	})

	// Set up store liveness heartbeat dropping from n1 to n2 and n3, which then
	// causes n2 and n3 to withdraw support from n1.
	var dropMessages atomic.Bool
	dropMessages.Store(true)
	dropStoreLivenessHeartbeatsFrom(t, tc.Server(1), desc, []roachpb.ReplicaID{1}, &dropMessages)
	dropStoreLivenessHeartbeatsFrom(t, tc.Server(2), desc, []roachpb.ReplicaID{1}, &dropMessages)
	t.Logf("blocking store liveness heartbeats from n1 to n2 and n3")

	// Wait for n2 and n3 to withdraw support from n1.
	store2 := tc.GetFirstStoreFromServer(t, 1)
	store3 := tc.GetFirstStoreFromServer(t, 2)
	n1StoreIdent := slpb.StoreIdent{NodeID: tc.Target(0).NodeID, StoreID: tc.Target(0).StoreID}

	testutils.SucceedsSoon(t, func() error {
		_, supported2 := store2.TestingStoreLivenessSupportManager().SupportFor(n1StoreIdent)
		_, supported3 := store3.TestingStoreLivenessSupportManager().SupportFor(n1StoreIdent)
		if supported2 || supported3 {
			return errors.Errorf("support not yet withdrawn from n1: n2=%t, n3=%t", supported2, supported3)
		}
		return nil
	})
	t.Logf("n2 and n3 have withdrawn support from n1")

	// We expect n1 to step down as the raft leader because of CheckQuorum and
	// leadership to move to either n2 or n3.
	var newLeaderIdx int
	testutils.SucceedsSoon(t, func() error {
		for i := 0; i < numNodes; i++ {
			repl := tc.GetFirstStoreFromServer(t, i).LookupReplica(roachpb.RKey(scratchKey))
			if repl == nil {
				continue
			}
			rs := repl.RaftStatus()
			if rs != nil && rs.RaftState == raftpb.StateLeader {
				switch i {
				case 0:
					return errors.New("n1 is still the raft leader")
				default:
					newLeaderIdx = i
					return nil
				}
			}
		}
		return errors.New("no new leader elected yet")
	})
	t.Logf("n1 stepped down; new raft leader is n%d", newLeaderIdx+1)

	// Send a Get request to the new leader's store to trigger lease acquisition.
	newLeaderStore := tc.GetFirstStoreFromServer(t, newLeaderIdx)
	_, err := newLeaderStore.DB().Get(ctx, scratchKey)
	require.NoError(t, err)
	t.Logf("sent Get request to n%d to trigger lease acquisition", newLeaderIdx+1)

	// Re-enable store liveness heartbeats from n1 so it regains support.
	dropMessages.Store(false)
	t.Logf("re-enabled store liveness heartbeats from n1")

	// Wait for n1 to regain support from n2 and n3.
	testutils.SucceedsSoon(t, func() error {
		_, supported2 := store2.TestingStoreLivenessSupportManager().SupportFor(n1StoreIdent)
		_, supported3 := store3.TestingStoreLivenessSupportManager().SupportFor(n1StoreIdent)
		if !supported2 || !supported3 {
			return errors.Errorf("n1 has not yet regained support: n2=%t, n3=%t", supported2, supported3)
		}
		return nil
	})
	t.Logf("n1 has regained support from n2 and n3")

	// Now enqueue the range in the lease queue on the current leaseholder.
	// The lease queue should attempt to transfer the lease back to n1, since
	// n1 is the preferred leaseholder, but it should only do so if it doesn't
	// treat n1 as suspect. Currently, n1 is not treated as suspect if support
	// has been recently withdrawn from it, which demonstrates the issue.
	repl := newLeaderStore.LookupReplica(roachpb.RKey(scratchKey))
	require.NotNil(t, repl)

	_, err = newLeaderStore.Enqueue(ctx, "lease", repl, true /* skipShouldQueue */, false /* async */)
	require.NoError(t, err)

	// TODO(arul): This should be a require.Never followed by a require.Eventually
	// once the suspect duration has passed.
	require.Eventually(t, func() bool {
		repl1 := tc.GetFirstStoreFromServer(t, 0).LookupReplica(roachpb.RKey(scratchKey))
		leaseStatus := repl1.CurrentLeaseStatus(ctx)
		if !leaseStatus.IsValid() {
			return false
		}
		return leaseStatus.OwnedBy(tc.Target(0).StoreID)
	}, 20*time.Second, 100*time.Millisecond)
}
