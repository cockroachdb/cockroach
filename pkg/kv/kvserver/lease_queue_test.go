// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/plan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/listenerutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
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
	tdb := sqlutils.MakeSQLRunner(tc.Conns[0])

	setLeasePreferences := func(node int) {
		tdb.Exec(t, fmt.Sprintf(`ALTER TABLE t CONFIGURE ZONE USING
      num_replicas=3, num_voters=3, voter_constraints='[]', lease_preferences='[[+rack=%d]]'`,
			node))
	}

	checkSplits := func() error {
		var count int
		var startSplit bool
		tdb.QueryRow(t,
			"SELECT count(*), bool_or(start_key ~ 'TableMin') FROM [SHOW RANGES FROM TABLE t WITH DETAILS];",
		).Scan(&count, &startSplit)
		if count != numRanges {
			return errors.Errorf("expected %d ranges in table, found %d", numRanges, count)
		}
		if !startSplit {
			return errors.New("expected table to be split at /TableMin")
		}
		return nil
	}

	leaseCount := func(node int) int {
		var count int
		tdb.QueryRow(t, fmt.Sprintf(
			"SELECT count(*) FROM [SHOW RANGES FROM TABLE t WITH DETAILS] WHERE lease_holder = %d", node),
		).Scan(&count)
		return count
	}

	checkLeaseCount := func(node, expectedLeaseCount int) error {
		if count := leaseCount(node); count != expectedLeaseCount {
			return errors.Errorf("expected %d leases on node %d, found %d",
				expectedLeaseCount, node, count)
		}
		return nil
	}

	// Shorten the closed timestamp target duration and span config reconciliation
	// interval so that span configs propagate more rapidly.
	tdb.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)
	tdb.Exec(t, `SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '100ms'`)
	tdb.Exec(t, `SET CLUSTER SETTING spanconfig.reconciliation_job.checkpoint_interval = '100ms'`)

	// Create a test table with numRanges-1 splits, to end up with numRanges
	// ranges. We will use the test table ranges to assert on the purgatory lease
	// preference behavior.
	tdb.Exec(t, "CREATE TABLE t (i int);")
	tdb.Exec(t, fmt.Sprintf("INSERT INTO t(i) select generate_series(1,%d)", numRanges-1))
	tdb.Exec(t, "ALTER TABLE t SPLIT AT SELECT i FROM t;")
	require.NoError(t, tc.WaitForFullReplication())

	// Set a preference on the initial node, then wait until:
	// (1) the first span in the test table (i.e. [/Table/<id>, /Table/<id>/1/1))
	//     has been split off into its own range.
	// (2) all the leases for the test table are on the initial node.
	setLeasePreferences(initialPreferredNode)
	testutils.SucceedsSoon(t, func() error {
		for serverIdx := 0; serverIdx < numNodes; serverIdx++ {
			store := tc.GetFirstStoreFromServer(t, serverIdx)
			require.NoError(t, store.ForceSplitScanAndProcess())
			require.NoError(t, store.ForceLeaseQueueProcess())
		}
		if err := checkSplits(); err != nil {
			return err
		}
		return checkLeaseCount(initialPreferredNode, numRanges)
	})

	// Block returning transfer targets from the allocator, then update the
	// preferred node. We expect that every range for the test table will end up
	// in purgatory on the initially preferred node.
	store := tc.GetFirstStoreFromServer(t, 0)
	blockTransferTarget.Store(true)
	setLeasePreferences(nextPreferredNode)
	rangeIDs := func() map[roachpb.RangeID]struct{} {
		rows := tdb.Query(t, `SELECT range_id FROM [ SHOW RANGES FROM TABLE t ]`)
		var rangeID roachpb.RangeID
		m := map[roachpb.RangeID]struct{}{}
		for rows.Next() {
			require.NoError(t, rows.Scan(&rangeID))
			m[rangeID] = struct{}{}
		}
		require.NoError(t, rows.Err())
		return m
	}()
	require.Len(t, rangeIDs, numRanges)
	testutils.SucceedsSoon(t, func() error {
		require.NoError(t, store.ForceLeaseQueueProcess())
		purg := store.LeaseQueuePurgatory()
		var missing []roachpb.RangeID
		for k := range rangeIDs {
			if _, ok := purg[k]; !ok {
				missing = append(missing, k)
			}
		}
		sort.Slice(missing, func(i, j int) bool {
			return missing[i] < missing[j]
		})
		if len(missing) > 0 {
			return errors.Errorf("replicas missing from purgatory: %v", missing)
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

	testutils.RunValues(t, "lease-type", roachpb.EpochAndLeaderLeaseType(), func(t *testing.T, leaseType roachpb.LeaseType) {
		kvserver.OverrideDefaultLeaseType(ctx, &st.SV, leaseType)

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

		countLeases := func() (epoch, leader, expiration int64) {
			for i := 0; i < tc.NumServers(); i++ {
				require.NoError(t, tc.Server(i).GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
					require.NoError(t, s.ComputeMetrics(ctx))
					epoch += s.Metrics().LeaseEpochCount.Value()
					leader += s.Metrics().LeaseLeaderCount.Value()
					expiration += s.Metrics().LeaseExpirationCount.Value()
					return nil
				}))
			}
			return
		}

		// We expect to have both expiration and leader/epoch leases (based on the
		// version of the test) at the start, since the meta and liveness ranges
		// require expiration leases. However, it's possible that there are a few
		// other stray expiration leases too, since lease transfers use expiration
		// leases as well.
		epochLeases, leaderLeases, expLeases := countLeases()
		switch leaseType {
		case roachpb.LeaseLeader:
			require.Zero(t, epochLeases)
			require.NotZero(t, leaderLeases)
		case roachpb.LeaseEpoch:
			require.NotZero(t, epochLeases)
			require.Zero(t, leaderLeases)
		default:
			panic("unexpected")
		}
		require.NotZero(t, expLeases)
		initialExpLeases := expLeases
		t.Logf("initial: epochLeases=%d leaderLeases=%d expLeases=%d", epochLeases, leaderLeases, expLeases)

		// Switch to expiration leases and wait for them to change.
		_, err := sqlDB.ExecContext(ctx, `SET CLUSTER SETTING kv.expiration_leases_only.enabled = true`)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			epochLeases, leaderLeases, expLeases = countLeases()
			t.Logf("enabling: epochLeases=%d leaderLeases=%d expLeases=%d", epochLeases, leaderLeases, expLeases)
			return epochLeases == 0 && leaderLeases == 0 && expLeases > 0
		}, 30*time.Second, 500*time.Millisecond) // accommodate stress/deadlock builds

		// Run a scan across the ranges, just to make sure they work.
		scanCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		_, err = db.Scan(scanCtx, scratchKey, scratchKey.PrefixEnd(), 1)
		require.NoError(t, err)

		// Switch back to epoch or leader leases (based on the test variant) and
		// wait for them to change. We still expect to have some required expiration
		// leases, but they should be at or below the number of expiration leases we
		// had at the start (primarily the meta and liveness ranges, but possibly a
		// few more since lease transfers also use expiration leases).
		_, err = sqlDB.ExecContext(ctx, `SET CLUSTER SETTING kv.expiration_leases_only.enabled = false`)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			epochLeases, leaderLeases, expLeases = countLeases()
			t.Logf("disabling: epochLeases=%d leaderLeases=%d expLeases=%d", epochLeases, leaderLeases, expLeases)

			switch leaseType {
			case roachpb.LeaseLeader:
				return epochLeases == 0 && leaderLeases > 0 && expLeases > 0 && expLeases <= initialExpLeases
			case roachpb.LeaseEpoch:
				return epochLeases > 0 && leaderLeases == 0 && expLeases > 0 && expLeases <= initialExpLeases
			default:
				panic("unexpected")
			}
		}, 30*time.Second, 500*time.Millisecond)
	})
}

// TestLeaseQueueSwitchesLeaseType tests that changing the value of the
// kv.raft.leader_fortification.fraction_enabled cluster setting switches leases
// on non-system ranges between epoch-based leases and leader leases.
func TestLeaseQueueSwitchesLeaseType(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t) // too slow under stressrace
	skip.UnderDeadlock(t)
	skip.UnderShort(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	kvserver.ExpirationLeasesOnly.Override(ctx, &st.SV, false)                 // override metamorphism
	kvserver.RaftLeaderFortificationFractionEnabled.Override(ctx, &st.SV, 0.0) // override metamorphism

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

	countLeases := func() (epoch, leader, expiration int64) {
		for i := 0; i < tc.NumServers(); i++ {
			require.NoError(t, tc.Server(i).GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
				require.NoError(t, s.ComputeMetrics(ctx))
				epoch += s.Metrics().LeaseEpochCount.Value()
				leader += s.Metrics().LeaseLeaderCount.Value()
				expiration += s.Metrics().LeaseExpirationCount.Value()
				return nil
			}))
		}
		return
	}

	// We expect to have both expiration and epoch leases at the start, since the
	// meta and liveness ranges require expiration leases. However, we should have
	// no leader leases.
	epochLeases, leaderLeases, expLeases := countLeases()
	require.NotZero(t, epochLeases)
	require.Zero(t, leaderLeases)
	require.NotZero(t, expLeases)
	t.Logf("initial: epochLeases=%d leaderLeases=%d expLeases=%d", epochLeases, leaderLeases, expLeases)

	waitForLeasesToSwitch := func(name string, someEpoch, someLeader bool) {
		require.Eventually(t, func() bool {
			epochLeases, leaderLeases, expLeases = countLeases()
			t.Logf("%s: epochLeases=%d leaderLeases=%d expLeases=%d", name, epochLeases, leaderLeases, expLeases)
			return ((epochLeases > 0) == someEpoch) && ((leaderLeases > 0) == someLeader)
		}, 30*time.Second, 500*time.Millisecond) // accommodate stress/deadlock builds

		// Run a scan across the ranges, just to make sure they work.
		scanCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		_, err := db.Scan(scanCtx, scratchKey, scratchKey.PrefixEnd(), 1)
		require.NoError(t, err)
	}

	// Switch 100% of ranges to use leader fortification, which should allow them
	// to switch to leader leases, except for the system ranges, which still
	// require expiration-based leases.
	_, err := sqlDB.ExecContext(ctx, `SET CLUSTER SETTING kv.raft.leader_fortification.fraction_enabled = 1.00`)
	require.NoError(t, err)
	waitForLeasesToSwitch("100%", false /* someEpoch */, true /* someLeader */)

	// Switch to 50% of ranges using leader fortification, and by extension, leader
	// leases. The other 50% should switch back to epoch-based leases.
	_, err = sqlDB.ExecContext(ctx, `SET CLUSTER SETTING kv.raft.leader_fortification.fraction_enabled = 0.50`)
	require.NoError(t, err)
	waitForLeasesToSwitch("50%", true /* someEpoch */, true /* someLeader */)

	// Disable leader leases, even on ranges using raft fortification. All leader
	// leases should switch back to epoch-based leases.
	_, err = sqlDB.ExecContext(ctx, `SET CLUSTER SETTING kv.leases.leader_leases.enabled = false`)
	require.NoError(t, err)
	waitForLeasesToSwitch("disabled", true /* someEpoch */, false /* someLeader */)
}

// TestUpdateLastUpdateTimesUsingStoreLiveness tests that `lastUpdateTimes` is
// updated when the leader is supported by a follower in store liveness even if
// it's not updating the map upon receiving followers' messages.
func TestUpdateLastUpdateTimesUsingStoreLiveness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t) // too slow under stressrace
	skip.UnderDeadlock(t)
	skip.UnderShort(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	// This test is only relevant for leader leases.
	kvserver.OverrideDefaultLeaseType(ctx, &st.SV, roachpb.LeaseLeader)

	manualClock := hlc.NewHybridManualClock()
	knobs := base.TestingKnobs{
		Server: &server.TestingKnobs{
			WallClock: manualClock,
		},
		Store: &kvserver.StoreTestingKnobs{
			// Disable updating the `lastUpdateTimes` map when the leader receives
			// messages from followers. This is to simulate the leader not
			// sending/receiving any messages because it doesn't have any updates. We
			// only want to do this for ranges that use leader leases (read: we want
			// to omit the meta ranges/NodeLiveness range which always use expiration
			// based leases and don't partake in fortification).
			//
			// Also see updateLastUpdateTimesUsingStoreLivenessRLocked which is the
			// method being tested here.
			DisableUpdateLastUpdateTimesMapOnRaftGroupStep: func(r *kvserver.Replica) bool {
				return r.SupportFromEnabled()
			},
		},
	}

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// Speed up the lease queue, which switches the lease type.
			Settings:        st,
			ScanMinIdleTime: time.Millisecond,
			ScanMaxIdleTime: time.Millisecond,
			Knobs:           knobs,
		},
	})
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())

	db := tc.Server(0).DB()
	// Split off a few ranges so we have something to work with.
	scratchKey := tc.ScratchRange(t)
	for i := 0; i <= 16; i++ {
		splitKey := append(scratchKey.Clone(), byte(i))
		require.NoError(t, db.AdminSplit(ctx, splitKey, hlc.MaxTimestamp))
	}

	// Increment the manual clock to ensure that all followers are initially
	// considered inactive.
	manualClock.Increment(time.Second.Nanoseconds() * 3)

	// Make sure that the replicas are considered active.
	require.Eventually(t, func() bool {
		allActive := true
		for i := 0; i < tc.NumServers(); i++ {
			store, err := tc.Server(i).GetStores().(*kvserver.Stores).
				GetStore(tc.Server(i).GetFirstStoreID())
			require.NoError(t, err)

			store.VisitReplicas(func(r *kvserver.Replica) (wantMore bool) {
				leader := tc.GetRaftLeader(t, r.Desc().StartKey)

				// Any replica that is not active will cause this check to repeat.
				if !leader.IsFollowerActiveSince(r.ReplicaID(), leader.Clock().PhysicalTime(),
					time.Second) {
					allActive = false
				}

				return true
			})
		}

		return allActive
	}, 45*time.Second, 1*time.Second) // accommodate stress
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
	_, _ = repl.Store().Enqueue(ctx, "lease", repl, true /* skipShouldQueue */, true /* async */)
	<-blocked
	processErr, _ := repl.Store().Enqueue(ctx, "replicate", repl, true /* skipShouldQueue */, false /* async */)
	require.ErrorIs(t, processErr, plan.NewErrAllocatorToken("lease"))
}

// TestLeaseQueueShedsOnIOOverload asserts that leases are shed if the store
// becomes IO overloaded.
func TestLeaseQueueShedsOnIOOverload(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	s1 := tc.GetFirstStoreFromServer(t, 0)
	capacityBefore, err := s1.Capacity(ctx, false /* useCached */)
	require.NoError(t, err)
	// The test wouldn't be interesting if there aren't already leases on s1.
	require.Greater(t, capacityBefore.LeaseCount, int32(20))

	ioThreshold := allocatorimpl.TestingIOThresholdWithScore(1)
	s1.UpdateIOThreshold(&ioThreshold)
	testutils.SucceedsSoon(t, func() error {
		capacityAfter, err := s1.Capacity(ctx, false /* useCached */)
		if err != nil {
			return err
		}
		if capacityAfter.LeaseCount > 0 {
			return errors.Errorf("expected 0 leases on store 1, found %d",
				capacityAfter.LeaseCount)
		}
		return nil
	})
}

// TestLeaseQueueProactiveEnqueueOnPreferences asserts that a lease quickly
// transfers back to a store which satisfies the first applied lease
// preference.
func TestLeaseQueueProactiveEnqueueOnPreferences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	const preferredNode = 1
	zcfg := zonepb.DefaultZoneConfig()
	zcfg.LeasePreferences = []zonepb.LeasePreference{
		{
			Constraints: []zonepb.Constraint{
				{
					Type:  zonepb.Constraint_REQUIRED,
					Key:   "dc",
					Value: "dc1",
				},
			},
		},
		{
			Constraints: []zonepb.Constraint{
				{
					Type:  zonepb.Constraint_REQUIRED,
					Key:   "dc",
					Value: "dc2",
				},
			},
		},
	}

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ConfigureScratchRange: true,
				},
				Server: &server.TestingKnobs{
					DefaultZoneConfigOverride: &zcfg,
				},
			},
		},
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

func toggleLeaseQueues(tc *testcluster.TestCluster, active bool) {
	for _, s := range tc.Servers {
		_ = s.GetStores().(*kvserver.Stores).VisitStores(func(store *kvserver.Store) error {
			store.TestingSetLeaseQueueActive(active)
			return nil
		})
	}
}

// TestLeaseQueueAcquiresInvalidLeases asserts that following a restart, leases
// are invalidated and that the lease queue acquires invalid leases when
// enabled.
func TestLeaseQueueAcquiresInvalidLeases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	kvserver.ExpirationLeasesOnly.Override(ctx, &st.SV, false) // override metamorphism

	lisReg := listenerutil.NewListenerRegistry()
	defer lisReg.Close()

	zcfg := zonepb.DefaultZoneConfig()
	zcfg.NumReplicas = proto.Int32(1)
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			// Disable the replication queue initially, to assert on the lease
			// statuses pre and post enabling the replicate queue.
			ReplicationMode:     base.ReplicationManual,
			ReusableListenerReg: lisReg,
			ServerArgs: base.TestServerArgs{
				Settings:          st,
				DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
				ScanMinIdleTime:   time.Millisecond,
				ScanMaxIdleTime:   time.Millisecond,
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						StickyVFSRegistry:         fs.NewStickyRegistry(),
						DefaultZoneConfigOverride: &zcfg,
					},
				},
			},
		},
	)
	defer tc.Stopper().Stop(ctx)
	db := tc.Conns[0]
	// Disable consistency checker and sql stats collection that may acquire a
	// lease by querying a range.
	_, err := db.Exec("set cluster setting server.consistency_check.interval = '0s'")
	require.NoError(t, err)
	_, err = db.Exec("set cluster setting sql.stats.automatic_collection.enabled = false")
	require.NoError(t, err)

	// Create ranges to assert on their lease status post restart and after
	// replicate queue processing.
	ranges := 30
	scratchRangeKeys := make([]roachpb.Key, ranges)
	splitKey := tc.ScratchRange(t)
	for i := range scratchRangeKeys {
		_, _ = tc.SplitRangeOrFatal(t, splitKey)
		scratchRangeKeys[i] = splitKey
		splitKey = splitKey.Next()
	}

	invalidLeases := func() []kvserverpb.LeaseStatus {
		invalid := []kvserverpb.LeaseStatus{}
		for _, key := range scratchRangeKeys {
			// Assert that the lease is invalid after restart.
			repl := tc.GetRaftLeader(t, roachpb.RKey(key))
			if leaseStatus := repl.CurrentLeaseStatus(ctx); !leaseStatus.IsValid() {
				invalid = append(invalid, leaseStatus)
			}
		}
		return invalid
	}

	// Assert that the leases are valid initially.
	require.Len(t, invalidLeases(), 0)

	// Restart the servers to invalidate the leases.
	for i := range tc.Servers {
		tc.StopServer(i)
		err = tc.RestartServerWithInspect(i, nil)
		require.NoError(t, err)
	}

	forceProcess := func() {
		// Speed up the queue processing.
		for _, s := range tc.Servers {
			err := s.GetStores().(*kvserver.Stores).VisitStores(func(store *kvserver.Store) error {
				return store.ForceLeaseQueueProcess()
			})
			require.NoError(t, err)
		}
	}

	// NB: The cosnsistency checker and sql stats collector both will attempt a
	// lease acquisition when processing a range, if it the lease is currently
	// invalid. They are disabled in this test. We do not assert on the number
	// of invalid leases prior to enabling the lease queue here to avoid
	// test flakiness if this changes in the future or for some other reason.
	// Instead, we are only concerned that no invalid leases remain.
	toggleLeaseQueues(tc, true /* active */)
	testutils.SucceedsSoon(t, func() error {
		forceProcess()
		// Assert that there are now no invalid leases.
		invalid := invalidLeases()
		if len(invalid) > 0 {
			return errors.Newf("The number of invalid leases are greater than 0, %+v", invalid)
		}
		return nil
	})
}
