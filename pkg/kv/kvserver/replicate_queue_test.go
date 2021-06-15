// Copyright 2016 The Cockroach Authors.
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
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/tracker"
)

func TestReplicateQueueRebalance(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test was seen taking north of 20m under race.
	skip.UnderRace(t)
	skip.UnderShort(t)

	const numNodes = 5

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs: base.TestServerArgs{
				ScanMinIdleTime: time.Millisecond,
				ScanMaxIdleTime: time.Millisecond,
			},
		},
	)
	defer tc.Stopper().Stop(context.Background())

	for _, server := range tc.Servers {
		st := server.ClusterSettings()
		st.Manual.Store(true)
		kvserver.LoadBasedRebalancingMode.Override(ctx, &st.SV, int64(kvserver.LBRebalancingOff))
	}

	const newRanges = 10
	trackedRanges := map[roachpb.RangeID]struct{}{}
	for i := 0; i < newRanges; i++ {
		tableID := keys.MinUserDescID + i
		splitKey := keys.SystemSQLCodec.TablePrefix(uint32(tableID))
		// Retry the splits on descriptor errors which are likely as the replicate
		// queue is already hard at work.
		testutils.SucceedsSoon(t, func() error {
			desc := tc.LookupRangeOrFatal(t, splitKey)
			if i > 0 && len(desc.Replicas().VoterDescriptors()) > 3 {
				// Some system ranges have five replicas but user ranges only three,
				// so we'll see downreplications early in the startup process which
				// we want to ignore. Delay the splits so that we don't create
				// more over-replicated ranges.
				// We don't do this for i=0 since that range stays at five replicas.
				return errors.Errorf("still downreplicating: %s", &desc)
			}
			_, rightDesc, err := tc.SplitRange(splitKey)
			if err != nil {
				return err
			}
			t.Logf("split off %s", &rightDesc)
			if i > 0 {
				trackedRanges[rightDesc.RangeID] = struct{}{}
			}
			return nil
		})
	}

	countReplicas := func() []int {
		counts := make([]int, len(tc.Servers))
		for _, s := range tc.Servers {
			err := s.Stores().VisitStores(func(s *kvserver.Store) error {
				counts[s.StoreID()-1] += s.ReplicaCount()
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		return counts
	}

	initialRanges, err := server.ExpectedInitialRangeCount(tc.Servers[0].DB(), zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef())
	if err != nil {
		t.Fatal(err)
	}
	numRanges := newRanges + initialRanges
	numReplicas := numRanges * 3
	const minThreshold = 0.9
	minReplicas := int(math.Floor(minThreshold * (float64(numReplicas) / numNodes)))

	testutils.SucceedsSoon(t, func() error {
		counts := countReplicas()
		for _, c := range counts {
			if c < minReplicas {
				err := errors.Errorf(
					"not balanced (want at least %d replicas on all stores): %d", minReplicas, counts)
				log.Infof(ctx, "%v", err)
				return err
			}
		}
		return nil
	})

	// Query the range log to see if anything unexpected happened. Concretely,
	// we'll make sure that our tracked ranges never had >3 replicas.
	infos, err := queryRangeLog(tc.Conns[0], `SELECT info FROM system.rangelog ORDER BY timestamp DESC`)
	require.NoError(t, err)
	for _, info := range infos {
		if _, ok := trackedRanges[info.UpdatedDesc.RangeID]; !ok || len(info.UpdatedDesc.Replicas().VoterDescriptors()) <= 3 {
			continue
		}
		// If we have atomic changes enabled, we expect to never see four replicas
		// on our tracked ranges. If we don't have atomic changes, we can't avoid
		// it.
		t.Error(info)
	}
}

// TestReplicateQueueUpReplicateOddVoters tests that up-replication only
// proceeds if there are a good number of candidates to up-replicate to.
// Specifically, we won't up-replicate to an even number of replicas unless
// there is an additional candidate that will allow a subsequent up-replication
// to an odd number.
func TestReplicateQueueUpReplicateOddVoters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderRaceWithIssue(t, 57144, "flaky under race")
	defer log.Scope(t).Close(t)
	const replicaCount = 3

	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{ReplicationMode: base.ReplicationAuto},
	)
	defer tc.Stopper().Stop(context.Background())

	testKey := keys.MetaMin
	desc, err := tc.LookupRange(testKey)
	if err != nil {
		t.Fatal(err)
	}

	if len(desc.InternalReplicas) != 1 {
		t.Fatalf("replica count, want 1, current %d", len(desc.InternalReplicas))
	}

	tc.AddAndStartServer(t, base.TestServerArgs{})

	if err := tc.Servers[0].Stores().VisitStores(func(s *kvserver.Store) error {
		return s.ForceReplicationScanAndProcess()
	}); err != nil {
		t.Fatal(err)
	}
	// After the initial splits have been performed, all of the resulting ranges
	// should be present in replicate queue purgatory (because we only have a
	// single store in the test and thus replication cannot succeed).
	expected, err := tc.Servers[0].ExpectedInitialRangeCount()
	if err != nil {
		t.Fatal(err)
	}

	var store *kvserver.Store
	_ = tc.Servers[0].Stores().VisitStores(func(s *kvserver.Store) error {
		store = s
		return nil
	})

	if n := store.ReplicateQueuePurgatoryLength(); expected != n {
		t.Fatalf("expected %d replicas in purgatory, but found %d", expected, n)
	}

	tc.AddAndStartServer(t, base.TestServerArgs{})

	// Now wait until the replicas have been up-replicated to the
	// desired number.
	testutils.SucceedsSoon(t, func() error {
		descriptor, err := tc.LookupRange(testKey)
		if err != nil {
			t.Fatal(err)
		}
		if len(descriptor.InternalReplicas) != replicaCount {
			return errors.Errorf("replica count, want %d, current %d", replicaCount, len(desc.InternalReplicas))
		}
		return nil
	})

	infos, err := filterRangeLog(
		tc.Conns[0], desc.RangeID, kvserverpb.RangeLogEventType_add_voter, kvserverpb.ReasonRangeUnderReplicated,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) < 1 {
		t.Fatalf("found no upreplication due to underreplication in the range logs")
	}
}

// TestReplicateQueueDownReplicate verifies that the replication queue will
// notice over-replicated ranges and remove replicas from them.
func TestReplicateQueueDownReplicate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "takes >1min under race")

	ctx := context.Background()
	const replicaCount = 3

	// The goal of this test is to ensure that down replication occurs correctly
	// using the replicate queue, and to ensure that's the case, the test
	// cluster needs to be kept in auto replication mode.
	tc := testcluster.StartTestCluster(t, replicaCount+2,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs: base.TestServerArgs{
				ScanMinIdleTime: 10 * time.Millisecond,
				ScanMaxIdleTime: 10 * time.Millisecond,
			},
		},
	)
	defer tc.Stopper().Stop(ctx)

	// Disable the replication queues so that the range we're about to create
	// doesn't get down-replicated too soon.
	tc.ToggleReplicateQueues(false)

	testKey := tc.ScratchRange(t)
	desc := tc.LookupRangeOrFatal(t, testKey)
	// At the end of StartTestCluster(), all ranges have 5 replicas since they're
	// all "system ranges". When the ScratchRange() splits its range, it also
	// starts up with 5 replicas. Since it's not a system range, its default zone
	// config asks for 3x replication, and the replication queue will
	// down-replicate it.
	require.Len(t, desc.Replicas().Descriptors(), 5)
	// Re-enable the replication queue.
	tc.ToggleReplicateQueues(true)

	// Now wait until the replicas have been down-replicated back to the
	// desired number.
	testutils.SucceedsSoon(t, func() error {
		descriptor, err := tc.LookupRange(testKey)
		if err != nil {
			t.Fatal(err)
		}
		if len(descriptor.InternalReplicas) != replicaCount {
			return errors.Errorf("replica count, want %d, current %d", replicaCount, len(desc.InternalReplicas))
		}
		return nil
	})

	infos, err := filterRangeLog(
		tc.Conns[0], desc.RangeID, kvserverpb.RangeLogEventType_remove_voter, kvserverpb.ReasonRangeOverReplicated,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) < 1 {
		t.Fatalf("found no downreplication due to over-replication in the range logs")
	}
}

func scanAndGetNumNonVoters(
	ctx context.Context,
	t *testing.T,
	tc *testcluster.TestCluster,
	store *kvserver.Store,
	scratchKey roachpb.Key,
) (numNonVoters int) {
	// Nudge the replicateQueue to up/down-replicate our scratch range.
	if err := store.ForceReplicationScanAndProcess(); err != nil {
		t.Fatal(err)
	}
	scratchRange := tc.LookupRangeOrFatal(t, scratchKey)
	row := tc.ServerConn(0).QueryRow(
		`SELECT array_length(non_voting_replicas, 1) FROM crdb_internal.ranges_no_leases WHERE range_id=$1`,
		scratchRange.GetRangeID())
	err := row.Scan(&numNonVoters)
	log.Warningf(ctx, "error while retrieving the number of non-voters: %s", err)

	return numNonVoters
}

// TestReplicateQueueUpAndDownReplicateNonVoters is an end-to-end test ensuring
// that the replicateQueue will add or remove non-voter(s) to a range based on
// updates to its zone configuration.
func TestReplicateQueueUpAndDownReplicateNonVoters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderRace(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{ReplicationMode: base.ReplicationAuto},
	)
	defer tc.Stopper().Stop(context.Background())

	scratchKey := tc.ScratchRange(t)
	scratchRange := tc.LookupRangeOrFatal(t, scratchKey)

	// Since we started the TestCluster with 1 node, that first node should have
	// 1 voting replica.
	require.Len(t, scratchRange.Replicas().VoterDescriptors(), 1)
	// Set up the default zone configs such that every range should have 1 voting
	// replica and 2 non-voting replicas.
	_, err := tc.ServerConn(0).Exec(
		`ALTER RANGE DEFAULT CONFIGURE ZONE USING num_replicas = 3, num_voters = 1`,
	)
	require.NoError(t, err)

	// Add two new servers and expect that 2 non-voters are added to the range.
	tc.AddAndStartServer(t, base.TestServerArgs{})
	tc.AddAndStartServer(t, base.TestServerArgs{})
	store, err := tc.Server(0).GetStores().(*kvserver.Stores).GetStore(
		tc.Server(0).GetFirstStoreID())
	require.NoError(t, err)

	var expectedNonVoterCount = 2
	testutils.SucceedsSoon(t, func() error {
		if found := scanAndGetNumNonVoters(ctx, t, tc, store, scratchKey); found != expectedNonVoterCount {
			return errors.Errorf("expected upreplication to %d non-voters; found %d",
				expectedNonVoterCount, found)
		}
		return nil
	})

	// Now remove all non-voting replicas and expect that the range will
	// down-replicate to having just 1 voting replica.
	_, err = tc.ServerConn(0).Exec(`ALTER RANGE DEFAULT CONFIGURE ZONE USING num_replicas = 1`)
	require.NoError(t, err)
	expectedNonVoterCount = 0
	testutils.SucceedsSoon(t, func() error {
		if found := scanAndGetNumNonVoters(ctx, t, tc, store, scratchKey); found != expectedNonVoterCount {
			return errors.Errorf("expected downreplication to %d non-voters; found %d",
				expectedNonVoterCount, found)
		}
		return nil
	})
}

func checkReplicaCount(
	ctx context.Context,
	tc *testcluster.TestCluster,
	rangeDesc roachpb.RangeDescriptor,
	voterCount, nonVoterCount int,
) (bool, error) {
	err := forceScanOnAllReplicationQueues(tc)
	if err != nil {
		log.Infof(ctx, "store.ForceReplicationScanAndProcess() failed with: %s", err)
		return false, err
	}
	rangeDesc, err = tc.LookupRange(rangeDesc.StartKey.AsRawKey())
	if err != nil {
		return false, err
	}
	if len(rangeDesc.Replicas().VoterDescriptors()) != voterCount {
		return false, nil
	}
	if len(rangeDesc.Replicas().NonVoterDescriptors()) != nonVoterCount {
		return false, nil
	}
	return true, nil
}

// TestReplicateQueueDecommissioningNonVoters is an end-to-end test ensuring
// that the replicateQueue will replace or remove non-voter(s) on
// decommissioning nodes.
func TestReplicateQueueDecommissioningNonVoters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "takes a long time or times out under race")

	ctx := context.Background()

	// Setup a scratch range on a test cluster with 2 non-voters and 1 voter.
	setupFn := func(t *testing.T) (*testcluster.TestCluster, roachpb.RangeDescriptor) {
		tc := testcluster.StartTestCluster(t, 5,
			base.TestClusterArgs{ReplicationMode: base.ReplicationAuto},
		)

		scratchKey := tc.ScratchRange(t)
		scratchRange := tc.LookupRangeOrFatal(t, scratchKey)
		_, err := tc.ServerConn(0).Exec(
			`ALTER RANGE DEFAULT CONFIGURE ZONE USING num_replicas = 3, num_voters = 1`,
		)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			ok, err := checkReplicaCount(ctx, tc, scratchRange, 1 /* voterCount */, 2 /* nonVoterCount */)
			if err != nil {
				log.Errorf(ctx, "error checking replica count: %s", err)
				return false
			}
			return ok
		}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond)
		return tc, scratchRange
	}

	// Check that non-voters on decommissioning nodes are replaced by
	// upreplicating elsewhere. This test is supposed to tickle the
	// `AllocatorReplaceDecommissioningNonVoter` code path.
	t.Run("replace", func(t *testing.T) {
		tc, scratchRange := setupFn(t)
		defer tc.Stopper().Stop(ctx)
		// Do a fresh look up on the range descriptor.
		scratchRange = tc.LookupRangeOrFatal(t, scratchRange.StartKey.AsRawKey())
		beforeNodeIDs := getNonVoterNodeIDs(scratchRange)
		// Decommission each of the two nodes that have the non-voters and make sure
		// that those non-voters are upreplicated elsewhere.
		require.NoError(t,
			tc.Server(0).Decommission(ctx, livenesspb.MembershipStatus_DECOMMISSIONING, beforeNodeIDs))

		require.Eventually(t, func() bool {
			ok, err := checkReplicaCount(ctx, tc, scratchRange, 1 /* voterCount */, 2 /* nonVoterCount */)
			if err != nil {
				log.Errorf(ctx, "error checking replica count: %s", err)
				return false
			}
			if !ok {
				return false
			}
			// Ensure that the non-voters have actually been removed from the dead
			// nodes and moved to others.
			scratchRange = tc.LookupRangeOrFatal(t, scratchRange.StartKey.AsRawKey())
			afterNodeIDs := getNonVoterNodeIDs(scratchRange)
			for _, before := range beforeNodeIDs {
				for _, after := range afterNodeIDs {
					if after == before {
						return false
					}
				}
			}
			return true
		}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond)
	})

	// Check that when we have more non-voters than needed and some of those
	// non-voters are on decommissioning nodes, that we simply remove those
	// non-voters. This test is supposed to tickle the
	// `AllocatorRemoveDecommissioningNonVoter` code path.
	t.Run("remove", func(t *testing.T) {
		tc, scratchRange := setupFn(t)
		defer tc.Stopper().Stop(ctx)

		// Turn off the replicateQueue and update the zone configs to remove all
		// non-voters. At the same time, also mark all the nodes that have
		// non-voters as decommissioning.
		tc.ToggleReplicateQueues(false)
		_, err := tc.ServerConn(0).Exec(
			`ALTER RANGE DEFAULT CONFIGURE ZONE USING num_replicas = 1`,
		)
		require.NoError(t, err)

		// Do a fresh look up on the range descriptor.
		scratchRange = tc.LookupRangeOrFatal(t, scratchRange.StartKey.AsRawKey())
		var nonVoterNodeIDs []roachpb.NodeID
		for _, repl := range scratchRange.Replicas().NonVoterDescriptors() {
			nonVoterNodeIDs = append(nonVoterNodeIDs, repl.NodeID)
		}
		require.NoError(t,
			tc.Server(0).Decommission(ctx, livenesspb.MembershipStatus_DECOMMISSIONING, nonVoterNodeIDs))

		// At this point, we know that we have an over-replicated range with
		// non-voters on nodes that are marked as decommissioning. So turn the
		// replicateQueue on and ensure that these redundant non-voters are removed.
		tc.ToggleReplicateQueues(true)
		require.Eventually(t, func() bool {
			ok, err := checkReplicaCount(ctx, tc, scratchRange, 1 /* voterCount */, 0 /* nonVoterCount */)
			if err != nil {
				log.Errorf(ctx, "error checking replica count: %s", err)
				return false
			}
			return ok
		}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond)
	})
}

// TestReplicateQueueDeadNonVoters is an end to end test ensuring that
// non-voting replicas on dead nodes are replaced or removed.
func TestReplicateQueueDeadNonVoters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "takes a long time or times out under race")

	ctx := context.Background()

	var livenessTrap atomic.Value
	setupFn := func(t *testing.T) (*testcluster.TestCluster, roachpb.RangeDescriptor) {
		tc := testcluster.StartTestCluster(t, 5,
			base.TestClusterArgs{
				ReplicationMode: base.ReplicationAuto,
				ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{
						NodeLiveness: kvserver.NodeLivenessTestingKnobs{
							StorePoolNodeLivenessFn: func(
								id roachpb.NodeID, now time.Time, duration time.Duration,
							) livenesspb.NodeLivenessStatus {
								val := livenessTrap.Load()
								if val == nil {
									return livenesspb.NodeLivenessStatus_LIVE
								}
								return val.(func(nodeID roachpb.NodeID) livenesspb.NodeLivenessStatus)(id)
							},
						},
					},
				},
			},
		)
		// Setup a scratch range on a test cluster with 2 non-voters and 1 voter.
		scratchKey := tc.ScratchRange(t)
		scratchRange := tc.LookupRangeOrFatal(t, scratchKey)
		_, err := tc.ServerConn(0).Exec(
			`ALTER RANGE DEFAULT CONFIGURE ZONE USING num_replicas = 3, num_voters = 1`,
		)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			ok, err := checkReplicaCount(ctx, tc, scratchRange, 1 /* voterCount */, 2 /* nonVoterCount */)
			if err != nil {
				log.Errorf(ctx, "error checking replica count: %s", err)
				return false
			}
			return ok
		}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond)
		return tc, scratchRange
	}

	markDead := func(nodeIDs []roachpb.NodeID) {
		livenessTrap.Store(func(id roachpb.NodeID) livenesspb.NodeLivenessStatus {
			for _, dead := range nodeIDs {
				if dead == id {
					return livenesspb.NodeLivenessStatus_DEAD
				}
			}
			return livenesspb.NodeLivenessStatus_LIVE
		})
	}

	// This subtest checks that non-voters on dead nodes are replaced by
	// upreplicating elsewhere. This test is supposed to tickle the
	// `AllocatorReplaceDeadNonVoter` code path. It does the following:
	//
	// 1. On a 5 node cluster, instantiate a range with 1 voter and 2 non-voters.
	// 2. Kill the 2 nodes that have the non-voters.
	// 3. Check that those non-voters are replaced.
	t.Run("replace", func(t *testing.T) {
		tc, scratchRange := setupFn(t)
		defer tc.Stopper().Stop(ctx)

		beforeNodeIDs := getNonVoterNodeIDs(scratchRange)
		markDead(beforeNodeIDs)
		require.Eventually(t, func() bool {
			ok, err := checkReplicaCount(ctx, tc, scratchRange, 1 /* voterCount */, 2 /* nonVoterCount */)
			if err != nil {
				log.Errorf(ctx, "error checking replica count: %s", err)
				return false
			}
			if !ok {
				return false
			}
			// Ensure that the non-voters have actually been removed from the dead
			// nodes and moved to others.
			scratchRange = tc.LookupRangeOrFatal(t, scratchRange.StartKey.AsRawKey())
			afterNodeIDs := getNonVoterNodeIDs(scratchRange)
			for _, before := range beforeNodeIDs {
				for _, after := range afterNodeIDs {
					if after == before {
						return false
					}
				}
			}
			return true
		}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond)
	})

	// This subtest checks that when we have more non-voters than needed and some
	// existing non-voters are on dead nodes, we will simply remove these
	// non-voters. This test is supposed to tickle the
	// AllocatorRemoveDeadNonVoter` code path. The test does the following:
	//
	// 1. Instantiate a range with 1 voter and 2 non-voters on a 5-node cluster.
	// 2. Turn off the replicateQueue
	// 3. Change the zone configs such that there should be no non-voters --
	// the two existing non-voters should now be considered "over-replicated"
	// by the system.
	// 4. Kill the nodes that have non-voters.
	// 5. Turn on the replicateQueue
	// 6. Make sure that the non-voters are downreplicated from the dead nodes.
	t.Run("remove", func(t *testing.T) {
		tc, scratchRange := setupFn(t)
		defer tc.Stopper().Stop(ctx)

		toggleReplicationQueues(tc, false)
		_, err := tc.ServerConn(0).Exec(
			// Remove all non-voters.
			"ALTER RANGE default CONFIGURE ZONE USING num_replicas = 1",
		)
		require.NoError(t, err)
		beforeNodeIDs := getNonVoterNodeIDs(scratchRange)
		markDead(beforeNodeIDs)

		toggleReplicationQueues(tc, true)
		require.Eventually(t, func() bool {
			ok, err := checkReplicaCount(ctx, tc, scratchRange, 1 /* voterCount */, 0 /* nonVoterCount */)
			if err != nil {
				log.Errorf(ctx, "error checking replica count: %s", err)
				return false
			}
			return ok
		}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond)
	})
}

func getNonVoterNodeIDs(rangeDesc roachpb.RangeDescriptor) (result []roachpb.NodeID) {
	for _, repl := range rangeDesc.Replicas().NonVoterDescriptors() {
		result = append(result, repl.NodeID)
	}
	return result
}

// TestReplicateQueueSwapVoterWithNonVoters tests that voting replicas can
// rebalance to stores that already have a non-voter by "swapping" with them.
// "Swapping" in this context means simply changing the `ReplicaType` on the
// receiving store from non-voter to voter and changing it on the other side
// from voter to non-voter.
func TestReplicateQueueSwapVotersWithNonVoters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "takes a long time or times out under race")

	ctx := context.Background()
	serverArgs := make(map[int]base.TestServerArgs)
	// Assign each store a rack number so we can constrain individual voting and
	// non-voting replicas to them.
	for i := 1; i <= 5; i++ {
		serverArgs[i-1] = base.TestServerArgs{
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{
						Key: "rack", Value: strconv.Itoa(i),
					},
				},
			},
		}
	}
	clusterArgs := base.TestClusterArgs{
		ReplicationMode:   base.ReplicationAuto,
		ServerArgsPerNode: serverArgs,
	}

	synthesizeRandomConstraints := func() (
		constraints string, voterStores, nonVoterStores []roachpb.StoreID,
	) {
		storeList := []roachpb.StoreID{1, 2, 3, 4, 5}
		// Shuffle the list of stores and designate the first 3 as voters and the
		// rest as non-voters.
		rand.Shuffle(5, func(i, j int) {
			storeList[i], storeList[j] = storeList[j], storeList[i]
		})
		voterStores = storeList[:3]
		nonVoterStores = storeList[3:5]

		var overallConstraints, voterConstraints []string
		for _, store := range nonVoterStores {
			overallConstraints = append(overallConstraints, fmt.Sprintf(`"+rack=%d": 1`, store))
		}
		for _, store := range voterStores {
			voterConstraints = append(voterConstraints, fmt.Sprintf(`"+rack=%d": 1`, store))
		}
		return fmt.Sprintf(
			"ALTER RANGE default CONFIGURE ZONE USING num_replicas = 5, num_voters = 3,"+
				" constraints = '{%s}', voter_constraints = '{%s}'",
			strings.Join(overallConstraints, ","), strings.Join(voterConstraints, ","),
		), voterStores, nonVoterStores
	}

	tc := testcluster.StartTestCluster(t, 5, clusterArgs)
	defer tc.Stopper().Stop(context.Background())

	scratchKey := tc.ScratchRange(t)
	// Start with 1 voter and 4 non-voters. This ensures that we also exercise the
	// swapping behavior during voting replica allocation when we upreplicate to 3
	// voters after calling `synthesizeRandomConstraints` below. See comment
	// inside `allocateTargetFromList`.
	_, err := tc.ServerConn(0).Exec("ALTER RANGE default CONFIGURE ZONE USING" +
		" num_replicas=5, num_voters=1")
	require.NoError(t, err)
	testutils.SucceedsSoon(t, func() error {
		if err := forceScanOnAllReplicationQueues(tc); err != nil {
			return err
		}
		scratchRange := tc.LookupRangeOrFatal(t, scratchKey)
		if voters := scratchRange.Replicas().VoterDescriptors(); len(voters) != 1 {
			return errors.Newf("expected 1 voter; got %v", voters)
		}
		if nonVoters := scratchRange.Replicas().NonVoterDescriptors(); len(nonVoters) != 4 {
			return errors.Newf("expected 4 non-voters; got %v", nonVoters)
		}
		return nil
	})

	checkRelocated := func(t *testing.T, voterStores, nonVoterStores []roachpb.StoreID) {
		testutils.SucceedsSoon(t, func() error {
			if err := forceScanOnAllReplicationQueues(tc); err != nil {
				return err
			}
			scratchRange := tc.LookupRangeOrFatal(t, scratchKey)
			if n := len(scratchRange.Replicas().VoterDescriptors()); n != 3 {
				return errors.Newf("number of voters %d does not match expectation", n)
			}
			if n := len(scratchRange.Replicas().NonVoterDescriptors()); n != 2 {
				return errors.Newf("number of non-voters %d does not match expectation", n)
			}

			// Check that each replica set is present on the stores designated by
			// synthesizeRandomConstraints.
			for _, store := range voterStores {
				replDesc, ok := scratchRange.GetReplicaDescriptor(store)
				if !ok {
					return errors.Newf("no replica found on store %d", store)
				}
				if typ := replDesc.GetType(); typ != roachpb.VOTER_FULL {
					return errors.Newf("replica on store %d does not match expectation;"+
						" expected VOTER_FULL, got %s", typ)
				}
			}
			for _, store := range nonVoterStores {
				replDesc, ok := scratchRange.GetReplicaDescriptor(store)
				if !ok {
					return errors.Newf("no replica found on store %d", store)
				}
				if typ := replDesc.GetType(); typ != roachpb.NON_VOTER {
					return errors.Newf("replica on store %d does not match expectation;"+
						" expected NON_VOTER, got %s", typ)
				}
			}
			return nil
		})
	}

	var numIterations = 10
	if util.RaceEnabled {
		numIterations = 1
	}
	for i := 0; i < numIterations; i++ {
		// Generate random (but valid) constraints for the 3 voters and 2 non_voters
		// and check that the replicate queue achieves conformance.
		//
		// NB: `synthesizeRandomConstraints` sets up the default zone configs such
		// that every range should have 3 voting replica and 2 non-voting replicas.
		// The crucial thing to note here is that we have 5 stores and 5 replicas,
		// and since we never allow a single store to have >1 replica for a range at
		// any given point, any change in the configuration of these 5 replicas
		// _must_ go through atomic non-voter promotions and voter demotions.
		alterStatement, voterStores, nonVoterStores := synthesizeRandomConstraints()
		log.Infof(ctx, "applying: %s", alterStatement)
		_, err := tc.ServerConn(0).Exec(alterStatement)
		require.NoError(t, err)
		checkRelocated(t, voterStores, nonVoterStores)
	}
}

// TestReplicateQueueShouldQueueNonVoter tests that, in situations where the
// voting replicas don't need to be rebalanced but the non-voting replicas do,
// that the replicate queue correctly accepts the replica into the queue.
func TestReplicateQueueShouldQueueNonVoter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	serverArgs := make(map[int]base.TestServerArgs)
	// Assign each store a rack number so we can constrain individual voting and
	// non-voting replicas to them.
	for i := 1; i <= 3; i++ {
		serverArgs[i-1] = base.TestServerArgs{
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{
						Key: "rack", Value: strconv.Itoa(i),
					},
				},
			},
		}
	}

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode:   base.ReplicationAuto,
		ServerArgsPerNode: serverArgs,
	})
	defer tc.Stopper().Stop(ctx)

	scratchStartKey := tc.ScratchRange(t)
	_, err := tc.ServerConn(0).Exec("ALTER RANGE default CONFIGURE ZONE USING" +
		" num_replicas = 2, num_voters = 1," +
		" constraints='{\"+rack=2\": 1}', voter_constraints='{\"+rack=1\": 1}'")
	require.NoError(t, err)

	// Make sure that the range has conformed to the constraints we just set
	// above.
	require.Eventually(t, func() bool {
		if err := forceScanOnAllReplicationQueues(tc); err != nil {
			log.Warningf(ctx, "received error while forcing a replicateQueue scan: %s", err)
			return false
		}
		scratchRange := tc.LookupRangeOrFatal(t, scratchStartKey)
		if len(scratchRange.Replicas().VoterDescriptors()) != 1 {
			return false
		}
		if len(scratchRange.Replicas().NonVoterDescriptors()) != 1 {
			return false
		}
		// Ensure that the voter is on rack 1 and the non-voter is on rack 2.
		if scratchRange.Replicas().VoterDescriptors()[0].NodeID != tc.Server(0).NodeID() {
			return false
		}
		if scratchRange.Replicas().NonVoterDescriptors()[0].NodeID != tc.Server(1).NodeID() {
			return false
		}
		return true
	}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond)

	// Turn off the replicateQueues to prevent them from taking action on
	// `scratchRange`. We will manually enqueue the leaseholder for `scratchRange`
	// below.
	toggleReplicationQueues(tc, false)
	// We change the default zone configuration to dictate that the existing
	// voter doesn't need to be rebalanced but non-voter should be rebalanced to
	// rack 3 instead.
	_, err = tc.ServerConn(0).Exec("ALTER RANGE default CONFIGURE ZONE USING" +
		" constraints='{\"+rack=3\": 1}', voter_constraints='{\"+rack=1\": 1}'")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		// NB: Manually enqueuing the replica on server 0 (i.e. rack 1) is copacetic
		// because we know that it is the leaseholder (since it is the only voting
		// replica).
		store, repl := getFirstStoreReplica(t, tc.Server(0), scratchStartKey)
		recording, processErr, err := store.ManuallyEnqueue(
			ctx,
			"replicate",
			repl,
			false, /* skipShouldQueue */
		)
		if err != nil {
			log.Errorf(ctx, "err: %s", err.Error())
			return false
		}
		if processErr != nil {
			log.Errorf(ctx, "processErr: %s", processErr.Error())
			return false
		}
		if matched, err := regexp.Match("rebalance target found for non-voter, enqueuing",
			[]byte(recording.String())); !matched {
			require.NoError(t, err)
			return false
		}
		return true
	}, testutils.DefaultSucceedsSoonDuration, 100*time.Millisecond)
}

// queryRangeLog queries the range log. The query must be of type:
// `SELECT info from system.rangelog ...`.
func queryRangeLog(
	conn *gosql.DB, query string, args ...interface{},
) ([]kvserverpb.RangeLogEvent_Info, error) {

	// The range log can get large and sees unpredictable writes, so run this in a
	// proper txn to avoid spurious retries.
	var events []kvserverpb.RangeLogEvent_Info
	err := crdb.ExecuteTx(context.Background(), conn, nil, func(conn *gosql.Tx) error {
		events = nil // reset in case of a retry

		rows, err := conn.Query(query, args...)
		if err != nil {
			return err
		}

		defer rows.Close()
		var numEntries int
		for rows.Next() {
			numEntries++
			var infoStr string
			if err := rows.Scan(&infoStr); err != nil {
				return err
			}
			var info kvserverpb.RangeLogEvent_Info
			if err := json.Unmarshal([]byte(infoStr), &info); err != nil {
				return errors.Errorf("error unmarshaling info string %q: %s", infoStr, err)
			}
			events = append(events, info)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return nil
	})
	return events, err

}

func filterRangeLog(
	conn *gosql.DB,
	rangeID roachpb.RangeID,
	eventType kvserverpb.RangeLogEventType,
	reason kvserverpb.RangeLogEventReason,
) ([]kvserverpb.RangeLogEvent_Info, error) {
	return queryRangeLog(conn, `SELECT info FROM system.rangelog WHERE "rangeID" = $1 AND "eventType" = $2 AND info LIKE concat('%', $3, '%');`, rangeID, eventType.String(), reason)
}

func toggleReplicationQueues(tc *testcluster.TestCluster, active bool) {
	for _, s := range tc.Servers {
		_ = s.Stores().VisitStores(func(store *kvserver.Store) error {
			store.SetReplicateQueueActive(active)
			return nil
		})
	}
}

func forceScanOnAllReplicationQueues(tc *testcluster.TestCluster) (err error) {
	for _, s := range tc.Servers {
		err = s.Stores().VisitStores(func(store *kvserver.Store) error {
			return store.ForceReplicationScanAndProcess()
		})
	}
	return err
}

func toggleSplitQueues(tc *testcluster.TestCluster, active bool) {
	for _, s := range tc.Servers {
		_ = s.Stores().VisitStores(func(store *kvserver.Store) error {
			store.SetSplitQueueActive(active)
			return nil
		})
	}
}

// Test that ranges larger than range_max_bytes that can't be split can still be
// processed by the replication queue (in particular, up-replicated).
func TestLargeUnsplittableRangeReplicate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStress(t, 38565)
	skip.UnderRaceWithIssue(t, 38565)
	skip.UnderShort(t, 38565)
	ctx := context.Background()

	// Create a cluster with really small ranges.
	const rangeMaxSize = base.MinRangeMaxBytes
	zcfg := zonepb.DefaultZoneConfig()
	zcfg.RangeMinBytes = proto.Int64(rangeMaxSize / 2)
	zcfg.RangeMaxBytes = proto.Int64(rangeMaxSize)
	tc := testcluster.StartTestCluster(t, 5,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs: base.TestServerArgs{
				ScanMinIdleTime: time.Millisecond,
				ScanMaxIdleTime: time.Millisecond,
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						DefaultZoneConfigOverride: &zcfg,
					},
				},
			},
		},
	)
	defer tc.Stopper().Stop(ctx)

	// We're going to create a table with a big row and a small row. We'll split
	// the table in between the rows, to produce a large range and a small one.
	// Then we'll increase the replication factor to 5 and check that both ranges
	// behave the same - i.e. they both get up-replicated. For the purposes of
	// this test we're only worried about the large one up-replicating, but we
	// test the small one as a control so that we don't fool ourselves.

	// Disable the queues so they don't mess with our manual relocation. We'll
	// re-enable them later.
	toggleReplicationQueues(tc, false /* active */)
	toggleSplitQueues(tc, false /* active */)

	db := tc.Conns[0]
	_, err := db.Exec("create table t (i int primary key, s string)")
	require.NoError(t, err)

	_, err = db.Exec(`ALTER TABLE t EXPERIMENTAL_RELOCATE VALUES (ARRAY[1,2,3], 1)`)
	require.NoError(t, err)
	_, err = db.Exec(`ALTER TABLE t SPLIT AT VALUES (2)`)
	require.NoError(t, err)

	toggleReplicationQueues(tc, true /* active */)
	toggleSplitQueues(tc, true /* active */)

	// We're going to create a row that's larger than range_max_bytes, but not
	// large enough that write back-pressuring kicks in and refuses it.
	var sb strings.Builder
	for i := 0; i < 1.5*rangeMaxSize; i++ {
		sb.WriteRune('a')
	}
	_, err = db.Exec("insert into t(i,s) values (1, $1)", sb.String())
	require.NoError(t, err)
	_, err = db.Exec("insert into t(i,s) values (2, 'b')")
	require.NoError(t, err)

	// Now ask everybody to up-replicate.
	_, err = db.Exec("alter table t configure zone using num_replicas = 5")
	require.NoError(t, err)

	forceProcess := func() {
		// Speed up the queue processing.
		for _, s := range tc.Servers {
			err := s.Stores().VisitStores(func(store *kvserver.Store) error {
				return store.ForceReplicationScanAndProcess()
			})
			require.NoError(t, err)
		}
	}

	// Wait until the smaller range (the 2nd) has up-replicated.
	testutils.SucceedsSoon(t, func() error {
		forceProcess()
		r := db.QueryRow(
			"select replicas from [show ranges from table t] where start_key='/2'")
		var repl string
		if err := r.Scan(&repl); err != nil {
			return err
		}
		if repl != "{1,2,3,4,5}" {
			return fmt.Errorf("not up-replicated yet. replicas: %s", repl)
		}
		return nil
	})

	// Now check that the large range also gets up-replicated.
	testutils.SucceedsSoon(t, func() error {
		forceProcess()
		r := db.QueryRow(
			"select replicas from [show ranges from table t] where start_key is null")
		var repl string
		if err := r.Scan(&repl); err != nil {
			return err
		}
		if repl != "{1,2,3,4,5}" {
			return fmt.Errorf("not up-replicated yet")
		}
		return nil
	})
}

type delayingRaftMessageHandler struct {
	kvserver.RaftMessageHandler
	leaseHolderNodeID uint64
	rangeID           roachpb.RangeID
}

const (
	queryInterval = 10 * time.Millisecond
	raftDelay     = 175 * time.Millisecond
)

func (h delayingRaftMessageHandler) HandleRaftRequest(
	ctx context.Context,
	req *kvserver.RaftMessageRequest,
	respStream kvserver.RaftMessageResponseStream,
) *roachpb.Error {
	if h.rangeID != req.RangeID {
		return h.RaftMessageHandler.HandleRaftRequest(ctx, req, respStream)
	}
	go func() {
		time.Sleep(raftDelay)
		err := h.RaftMessageHandler.HandleRaftRequest(ctx, req, respStream)
		if err != nil {
			log.Infof(ctx, "HandleRaftRequest returned err %s", err)
		}
	}()

	return nil
}

func TestTransferLeaseToLaggingNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "takes >1min under race")

	ctx := context.Background()
	clusterArgs := base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {
				ScanMaxIdleTime: time.Millisecond,
				StoreSpecs: []base.StoreSpec{{
					InMemory: true, Attributes: roachpb.Attributes{Attrs: []string{"n1"}},
				}},
			},
			1: {
				ScanMaxIdleTime: time.Millisecond,
				StoreSpecs: []base.StoreSpec{{
					InMemory: true, Attributes: roachpb.Attributes{Attrs: []string{"n2"}},
				}},
			},
			2: {
				ScanMaxIdleTime: time.Millisecond,
				StoreSpecs: []base.StoreSpec{{
					InMemory: true, Attributes: roachpb.Attributes{Attrs: []string{"n3"}},
				}},
			},
		},
	}

	tc := testcluster.StartTestCluster(t,
		len(clusterArgs.ServerArgsPerNode), clusterArgs)
	defer tc.Stopper().Stop(ctx)

	if err := tc.WaitForFullReplication(); err != nil {
		t.Fatal(err)
	}

	// Get the system.comments' range and lease holder
	var rangeID roachpb.RangeID
	var leaseHolderNodeID uint64
	s := sqlutils.MakeSQLRunner(tc.Conns[0])
	s.Exec(t, "insert into system.comments values(0,0,0,'abc')")
	s.QueryRow(t,
		"select range_id, lease_holder from "+
			"[show ranges from table system.comments] limit 1",
	).Scan(&rangeID, &leaseHolderNodeID)
	remoteNodeID := uint64(1)
	if leaseHolderNodeID == 1 {
		remoteNodeID = 2
	}
	log.Infof(ctx, "RangeID %d, RemoteNodeID %d, LeaseHolderNodeID %d",
		rangeID, remoteNodeID, leaseHolderNodeID)
	leaseHolderSrv := tc.Servers[leaseHolderNodeID-1]
	leaseHolderStoreID := leaseHolderSrv.GetFirstStoreID()
	leaseHolderStore, err := leaseHolderSrv.Stores().GetStore(leaseHolderStoreID)
	if err != nil {
		t.Fatal(err)
	}

	// Start delaying Raft messages to the remote node
	remoteSrv := tc.Servers[remoteNodeID-1]
	remoteStoreID := remoteSrv.GetFirstStoreID()
	remoteStore, err := remoteSrv.Stores().GetStore(remoteStoreID)
	if err != nil {
		t.Fatal(err)
	}
	remoteStore.Transport().Listen(
		remoteStoreID,
		delayingRaftMessageHandler{remoteStore, leaseHolderNodeID, rangeID},
	)

	workerReady := make(chan bool)
	// Create persistent range load.
	require.NoError(t, tc.Stopper().RunAsyncTask(ctx, "load", func(ctx context.Context) {
		s = sqlutils.MakeSQLRunner(tc.Conns[remoteNodeID-1])
		workerReady <- true
		for {
			s.Exec(t, fmt.Sprintf("update system.comments set comment='abc' "+
				"where type=0 and object_id=0 and sub_id=0"))

			select {
			case <-ctx.Done():
				return
			case <-tc.Stopper().ShouldQuiesce():
				return
			case <-time.After(queryInterval):
			}
		}
	}))
	<-workerReady
	// Wait until we see remote making progress
	leaseHolderRepl, err := leaseHolderStore.GetReplica(rangeID)
	if err != nil {
		t.Fatal(err)
	}

	var remoteRepl *kvserver.Replica
	testutils.SucceedsSoon(t, func() error {
		remoteRepl, err = remoteStore.GetReplica(rangeID)
		return err
	})
	testutils.SucceedsSoon(t, func() error {
		status := leaseHolderRepl.RaftStatus()
		progress := status.Progress[uint64(remoteRepl.ReplicaID())]
		if progress.Match > 0 {
			return nil
		}
		return errors.Errorf(
			"remote is not making progress: %+v", progress.Match,
		)
	})

	// Wait until we see the remote replica lagging behind
	for {
		// Ensure that the replica on the remote node is lagging.
		status := leaseHolderRepl.RaftStatus()
		progress := status.Progress[uint64(remoteRepl.ReplicaID())]
		if progress.State == tracker.StateReplicate &&
			(status.Commit-progress.Match) > 0 {
			break
		}
		time.Sleep(13 * time.Millisecond)
	}

	// Set the zone preference for the replica to show that it has to be moved
	// to the remote node.
	desc, zone := leaseHolderRepl.DescAndZone()
	newZone := *zone
	newZone.LeasePreferences = []zonepb.LeasePreference{
		{
			Constraints: []zonepb.Constraint{
				{
					Type:  zonepb.Constraint_REQUIRED,
					Value: fmt.Sprintf("n%d", remoteNodeID),
				},
			},
		},
	}

	// By now the lease holder may have changed.
	testutils.SucceedsSoon(t, func() error {
		leaseBefore, _ := leaseHolderRepl.GetLease()
		log.Infof(ctx, "Lease before transfer %+v\n", leaseBefore)

		if uint64(leaseBefore.Replica.NodeID) == remoteNodeID {
			log.Infof(
				ctx,
				"Lease successfully transferred to desired node %d\n",
				remoteNodeID,
			)
			return nil
		}
		currentSrv := tc.Servers[leaseBefore.Replica.NodeID-1]
		leaseStore, err := currentSrv.Stores().GetStore(currentSrv.GetFirstStoreID())
		if err != nil {
			return err
		}
		leaseRepl, err := leaseStore.GetReplica(rangeID)
		if err != nil {
			return err
		}
		transferred, err := leaseStore.FindTargetAndTransferLease(
			ctx, leaseRepl, desc, &newZone)
		if err != nil {
			return err
		}
		if !transferred {
			return errors.Errorf("unable to transfer")
		}
		return errors.Errorf("Repeat check for correct leaseholder")
	})
}
