// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/plan"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestLeaseQueueLeasePreferencePurgatoryError tests that not finding a
// lease transfer target whilst violating lease preferences, will put the
// replica in the lease queue purgatory.
func TestLeaseQueueLeasePreferencePurgatoryError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	skip.UnderRace(t) // too slow under stressrace
	skip.UnderDeadlock(t)
	skip.UnderShort(t)

	const initialPreferredNode = 1
	const nextPreferredNode = 2
	const numRanges = 40
	const numNodes = 3

	var blockTransferTarget atomic.Bool

	blockTransferTargetFn := func(_ roachpb.RangeID) bool {
		block := blockTransferTarget.Load()
		return block
	}

	knobs := base.TestingKnobs{
		Store: &kvserver.StoreTestingKnobs{
			AllocatorKnobs: &allocator.TestingKnobs{
				BlockTransferTarget: blockTransferTargetFn,
			},
		},
	}

	serverArgs := make(map[int]base.TestServerArgs, numNodes)
	for i := 0; i < numNodes; i++ {
		serverArgs[i] = base.TestServerArgs{
			Knobs: knobs,
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{{Key: "rack", Value: fmt.Sprintf("%d", i+1)}},
			},
		}
	}

	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ServerArgsPerNode: serverArgs,
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.Conns[0]
	setLeasePreferences := func(node int) {
		_, err := db.Exec(fmt.Sprintf(`ALTER TABLE t CONFIGURE ZONE USING
      num_replicas=3, num_voters=3, voter_constraints='[]', lease_preferences='[[+rack=%d]]'`,
			node))
		require.NoError(t, err)
	}

	leaseCount := func(node int) int {
		var count int
		err := db.QueryRow(fmt.Sprintf(
			"SELECT count(*) FROM [SHOW RANGES FROM TABLE t WITH DETAILS] WHERE lease_holder = %d", node),
		).Scan(&count)
		require.NoError(t, err)
		return count
	}

	checkLeaseCount := func(node, expectedLeaseCount int) error {
		if count := leaseCount(node); count != expectedLeaseCount {
			return errors.Errorf("expected %d leases on node %d, found %d",
				expectedLeaseCount, node, count)
		}
		return nil
	}

	// Create a test table with numRanges-1 splits, to end up with numRanges
	// ranges. We will use the test table ranges to assert on the purgatory lease
	// preference behavior.
	_, err := db.Exec("CREATE TABLE t (i int);")
	require.NoError(t, err)
	_, err = db.Exec(
		fmt.Sprintf("INSERT INTO t(i) select generate_series(1,%d)", numRanges-1))
	require.NoError(t, err)
	_, err = db.Exec("ALTER TABLE t SPLIT AT SELECT i FROM t;")
	require.NoError(t, err)
	require.NoError(t, tc.WaitForFullReplication())

	// Set a preference on the initial node, then wait until all the leases for
	// the test table are on that node.
	setLeasePreferences(initialPreferredNode)
	testutils.SucceedsSoon(t, func() error {
		for serverIdx := 0; serverIdx < numNodes; serverIdx++ {
			require.NoError(t, tc.GetFirstStoreFromServer(t, serverIdx).
				ForceLeaseQueueProcess())
		}
		return checkLeaseCount(initialPreferredNode, numRanges)
	})

	// Block returning transfer targets from the allocator, then update the
	// preferred node. We expect that every range for the test table will end up
	// in purgatory on the initially preferred node.
	store := tc.GetFirstStoreFromServer(t, 0)
	blockTransferTarget.Store(true)
	setLeasePreferences(nextPreferredNode)
	testutils.SucceedsSoon(t, func() error {
		require.NoError(t, store.ForceLeaseQueueProcess())
		if purgLen := store.LeaseQueuePurgatoryLength(); purgLen != numRanges {
			return errors.Errorf("expected %d in purgatory but got %v", numRanges, purgLen)
		}
		return nil
	})

	// Lastly, unblock returning transfer targets. Expect that the leases from
	// the test table all move to the new preference. Note we don't force a
	// lease queue scan, as the purgatory retry should handle the
	// transfers.
	blockTransferTarget.Store(false)
	testutils.SucceedsSoon(t, func() error {
		return checkLeaseCount(nextPreferredNode, numRanges)
	})
}

// TestLeaseQueueExpirationLeasesOnly tests that changing
// kv.expiration_leases_only.enabled switches all leases to the correct kind.
func TestLeaseQueueExpirationLeasesOnly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t) // too slow under stressrace
	skip.UnderDeadlock(t)
	skip.UnderShort(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	kvserver.ExpirationLeasesOnly.Override(ctx, &st.SV, false) // override metamorphism

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// Speed up the lease queue, which switches the lease type.
			Settings:        st,
			ScanMinIdleTime: time.Millisecond,
			ScanMaxIdleTime: time.Millisecond,
		},
	})
	defer tc.Stopper().Stop(ctx)

	require.NoError(t, tc.WaitForFullReplication())

	db := tc.Server(0).DB()
	sqlDB := tc.ServerConn(0)

	// Split off a few ranges so we have something to work with.
	scratchKey := tc.ScratchRange(t)
	for i := 0; i <= 255; i++ {
		splitKey := append(scratchKey.Clone(), byte(i))
		require.NoError(t, db.AdminSplit(ctx, splitKey, hlc.MaxTimestamp))
	}

	countLeases := func() (epoch int64, expiration int64) {
		for i := 0; i < tc.NumServers(); i++ {
			require.NoError(t, tc.Server(i).GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
				require.NoError(t, s.ComputeMetrics(ctx))
				expiration += s.Metrics().LeaseExpirationCount.Value()
				epoch += s.Metrics().LeaseEpochCount.Value()
				return nil
			}))
		}
		return
	}

	// We expect to have both expiration and epoch leases at the start, since the
	// meta and liveness ranges require expiration leases. However, it's possible
	// that there are a few other stray expiration leases too, since lease
	// transfers use expiration leases as well.
	epochLeases, expLeases := countLeases()
	require.NotZero(t, epochLeases)
	require.NotZero(t, expLeases)
	initialExpLeases := expLeases
	t.Logf("initial: epochLeases=%d expLeases=%d", epochLeases, expLeases)

	// Switch to expiration leases and wait for them to change.
	_, err := sqlDB.ExecContext(ctx, `SET CLUSTER SETTING kv.expiration_leases_only.enabled = true`)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		epochLeases, expLeases = countLeases()
		t.Logf("enabling: epochLeases=%d expLeases=%d", epochLeases, expLeases)
		return epochLeases == 0 && expLeases > 0
	}, 30*time.Second, 500*time.Millisecond) // accomodate stress/deadlock builds

	// Run a scan across the ranges, just to make sure they work.
	scanCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err = db.Scan(scanCtx, scratchKey, scratchKey.PrefixEnd(), 1)
	require.NoError(t, err)

	// Switch back to epoch leases and wait for them to change. We still expect to
	// have some required expiration leases, but they should be at or below the
	// number of expiration leases we had at the start (primarily the meta and
	// liveness ranges, but possibly a few more since lease transfers also use
	// expiration leases).
	_, err = sqlDB.ExecContext(ctx, `SET CLUSTER SETTING kv.expiration_leases_only.enabled = false`)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		epochLeases, expLeases = countLeases()
		t.Logf("disabling: epochLeases=%d expLeases=%d", epochLeases, expLeases)
		return epochLeases > 0 && expLeases > 0 && expLeases <= initialExpLeases
	}, 30*time.Second, 500*time.Millisecond)
}

// TestLeaseQueueRaceReplicateQueue asserts that the replicate/lease queue will
// not process a replica unless it can obtain the allocator token for the
// replica, i.e. changes are processed serially per range leaseholder.
func TestLeaseQueueRaceReplicateQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var blockRangeID int32
	var block syncutil.Mutex
	blocked := make(chan struct{})
	blockTransferTargetFn := func(rangeID roachpb.RangeID) bool {
		if atomic.LoadInt32(&blockRangeID) == int32(rangeID) {
			blocked <- struct{}{}
			block.Lock()
			defer block.Unlock()
		}
		return false
	}

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					AllocatorKnobs: &allocator.TestingKnobs{
						BlockTransferTarget: blockTransferTargetFn,
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	scratchKey := tc.ScratchRange(t)

	repl := tc.GetRaftLeader(t, roachpb.RKey(scratchKey))
	atomic.StoreInt32(&blockRangeID, int32(repl.Desc().RangeID))
	block.Lock()
	defer block.Unlock()

	// Process the replica in the lease queue async, then attempt to process the
	// replica in the replicate queue synchronously. The lease queue processing
	// should block on the block mutex above, causing the replicate queue to
	// return a AllocatorTokenErr trying to process the replica.
	_, _, _ = repl.Store().Enqueue(ctx, "lease", repl, true /* skipShouldQueue */, true /* async */)
	<-blocked
	_, processErr, _ := repl.Store().Enqueue(ctx, "replicate", repl, true /* skipShouldQueue */, false /* async */)
	require.ErrorIs(t, processErr, plan.NewErrAllocatorToken("lease"))
}

// TestLeaseQueueProactiveEnqueueOnPreferences asserts that a lease quickly
// transfers back to a store which satisfies the first applied lease
// preference.
func TestLeaseQueueProactiveEnqueueOnPreferences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	const numNodes = 3
	const preferredNode = 1
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
		{
			Constraints: []zonepb.Constraint{
				{
					Type:  zonepb.Constraint_REQUIRED,
					Key:   "rack",
					Value: "2",
				},
			},
		},
	}

	knobs := base.TestingKnobs{
		SpanConfig: &spanconfig.TestingKnobs{
			ConfigureScratchRange: false,
		},
		Server: &server.TestingKnobs{
			DefaultZoneConfigOverride: &zcfg,
		},
	}
	serverArgs := make(map[int]base.TestServerArgs, numNodes)
	for i := 0; i < numNodes; i++ {
		serverArgs[i] = base.TestServerArgs{
			Knobs: knobs,
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{{Key: "rack", Value: fmt.Sprintf("%d", i+1)}},
			},
		}
	}
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgsPerNode: serverArgs,
	})
	defer tc.Stopper().Stop(ctx)

	scratchKey := tc.ScratchRange(t)
	require.NoError(t, tc.WaitForFullReplication())
	desc := tc.GetRaftLeader(t, roachpb.RKey(scratchKey)).Desc()

	t.Run("violating", func(t *testing.T) {
		tc.TransferRangeLeaseOrFatal(t, *desc, roachpb.ReplicationTarget{NodeID: 3, StoreID: 3})
		testutils.SucceedsSoon(t, func() error {
			target, err := tc.FindRangeLeaseHolder(*desc, nil)
			if err != nil {
				return err
			}
			if target.StoreID != preferredNode {
				return errors.Errorf("lease not on preferred node %v, on %v",
					preferredNode, target)
			}
			return nil
		})
	})

	t.Run("less-preferred", func(t *testing.T) {
		tc.TransferRangeLeaseOrFatal(t, *desc, roachpb.ReplicationTarget{NodeID: 2, StoreID: 2})
		testutils.SucceedsSoon(t, func() error {
			target, err := tc.FindRangeLeaseHolder(*desc, nil)
			if err != nil {
				return err
			}
			if target.StoreID != preferredNode {
				return errors.Errorf("lease not on preferred node %v, on %v",
					preferredNode, target)
			}
			return nil
		})
	})
}
