// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/leases"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestLeaseRenewer tests that the store lease renewer or the Raft scheduler
// correctly tracks and extends expiration-based leases. The responsibility
// is given by kv.expiration_leases_only.enabled.
func TestLeaseRenewer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// race and deadlock make the test too slow, resulting in an inability
	// to maintain leases and Raft leadership.
	skip.UnderRace(t)
	skip.UnderDeadlock(t)

	// We test with kv.expiration_leases_only.enabled both enabled and disabled,
	// to ensure meta/liveness ranges are still eagerly extended, while regular
	// ranges are upgraded to epoch or leader leases.
	leases.RunEachLeaseType(t, func(t *testing.T, leaseType roachpb.LeaseType) {
		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		OverrideDefaultLeaseType(ctx, &st.SV, leaseType)
		tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Settings: st,
				// Speed up lease extensions to speed up the test, but adjust tick-based
				// timeouts to retain their default wall-time values.
				RaftConfig: base.RaftConfig{
					RangeLeaseRenewalFraction:  0.95,
					RaftTickInterval:           100 * time.Millisecond,
					RaftElectionTimeoutTicks:   20,
					RaftReproposalTimeoutTicks: 30,
				},
			},
		})
		defer tc.Stopper().Stop(ctx)

		require.NoError(t, tc.WaitForFullReplication())

		lookupNode := func(nodeID roachpb.NodeID) int {
			for idx, id := range tc.NodeIDs() {
				if id == nodeID {
					return idx
				}
			}
			t.Fatalf("couldn't look up node %d", nodeID)
			return 0
		}

		getNodeStore := func(nodeID roachpb.NodeID) *Store {
			srv := tc.Server(lookupNode(nodeID))
			s, err := srv.GetStores().(*Stores).GetStore(srv.GetFirstStoreID())
			require.NoError(t, err)
			return s
		}

		getNodeReplica := func(nodeID roachpb.NodeID, rangeID roachpb.RangeID) *Replica {
			repl, err := getNodeStore(nodeID).GetReplica(rangeID)
			require.NoError(t, err)
			return repl
		}

		// assertLeaseExtension asserts that the given range has an expiration-based
		// lease that is eagerly extended.
		assertLeaseExtension := func(rangeID roachpb.RangeID) {
			repl := getNodeReplica(1, rangeID)
			lease, _ := repl.GetLease()
			require.Equal(t, roachpb.LeaseExpiration, lease.Type())

			var extensions int
			require.Eventually(t, func() bool {
				newLease, _ := repl.GetLease()
				require.Equal(t, roachpb.LeaseExpiration, newLease.Type())
				if *newLease.Expiration != *lease.Expiration {
					extensions++
					lease = newLease
					t.Logf("r%d lease extended: %v", rangeID, lease)
				}
				return extensions >= 3
			}, 20*time.Second, 100*time.Millisecond)
		}

		// assertLeaseUpgrade asserts that the range is eventually upgraded
		// to an epoch or leader lease.
		assertLeaseUpgrade := func(rangeID roachpb.RangeID, leaseType roachpb.LeaseType) {
			repl := getNodeReplica(1, rangeID)
			require.Eventually(t, func() bool {
				lease, _ := repl.GetLease()
				return lease.Type() == leaseType
			}, 20*time.Second, 100*time.Millisecond)
		}

		// The meta range should always be eagerly renewed.
		firstRangeID := tc.LookupRangeOrFatal(t, keys.MinKey).RangeID
		assertLeaseExtension(firstRangeID)

		// Split off an expiration-based range, and assert that the lease is extended.
		desc := tc.LookupRangeOrFatal(t, tc.ScratchRangeWithExpirationLease(t))
		// Wait for the split to apply on all nodes first.
		require.NoError(t, tc.WaitForFullReplication())
		// Assert that the lease is extended.
		assertLeaseExtension(desc.RangeID)

		// Transfer the lease to a different leaseholder, and assert that the lease is
		// still extended.
		lease, _ := getNodeReplica(1, desc.RangeID).GetLease()
		target := tc.Target(lookupNode(lease.Replica.NodeID%3 + 1))
		tc.TransferRangeLeaseOrFatal(t, desc, target)
		assertLeaseExtension(desc.RangeID)

		// Split off a regular non-system range. This should only be eagerly
		// extended if kv.expiration_leases_only.enabled is true.
		desc = tc.LookupRangeOrFatal(t, tc.ScratchRange(t))
		if leaseType == roachpb.LeaseExpiration {
			assertLeaseExtension(desc.RangeID)
		} else {
			assertLeaseUpgrade(desc.RangeID, leaseType)
		}
	})
}
