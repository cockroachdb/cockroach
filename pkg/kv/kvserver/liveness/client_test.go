// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package liveness_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/plan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNodeLivenessAppearsAtStart tests that liveness records are written right
// when nodes are added to the cluster (during bootstrap, and when connecting to
// a bootstrapped node). The test verifies that the liveness records found are
// what we expect them to be.
func TestNodeLivenessAppearsAtStart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	// At this point StartTestCluster has waited for all nodes to become live.

	// Verify liveness records exist for all nodes.
	for i := 0; i < tc.NumServers(); i++ {
		nodeID := tc.Server(i).NodeID()
		nl := tc.Server(i).NodeLiveness().(*liveness.NodeLiveness)

		if !nl.GetNodeVitalityFromCache(nodeID).IsLive(livenesspb.IsAliveNotification) {
			t.Fatalf("node %d not live", nodeID)
		}

		livenessRec, ok := nl.GetLiveness(nodeID)
		assert.True(t, ok)
		if livenessRec.NodeID != nodeID {
			t.Fatalf("expected node ID %d, got %d", nodeID, livenessRec.NodeID)
		}
		// We expect epoch=1 as nodes first create a liveness record at epoch=0,
		// and then increment it during their first heartbeat.
		if livenessRec.Epoch != 1 {
			t.Fatalf("expected epoch=1, got epoch=%d", livenessRec.Epoch)
		}
		if !livenessRec.Membership.Active() {
			t.Fatalf("expected membership=active, got membership=%s", livenessRec.Membership)
		}
	}
}

// TestScanNodeVitalityFromKV verifies that fetching liveness records from KV
// directly retrieves all the records we expect.
func TestScanNodeVitalityFromKV(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	// At this point StartTestCluster has waited for all nodes to become live.

	// Verify that each servers sees the same set of liveness records in KV.
	for i := 0; i < tc.NumServers(); i++ {
		nodeID := tc.Server(i).NodeID()
		nl := tc.Server(i).NodeLiveness().(*liveness.NodeLiveness)
		require.True(t, nl.GetNodeVitalityFromCache(nodeID).IsLive(livenesspb.IsAliveNotification))

		livenesses, err := nl.ScanNodeVitalityFromKV(ctx)
		assert.Nil(t, err)
		assert.Equal(t, len(livenesses), tc.NumServers())

		var nodeIDs []roachpb.NodeID
		for nodeID, liveness := range livenesses {
			nodeIDs = append(nodeIDs, nodeID)

			// We expect epoch=1 as nodes first create a liveness record at epoch=0,
			// and then increment it during their first heartbeat.
			require.Equal(t, int64(1), liveness.GetInternalLiveness().Epoch)
			require.Equal(t, livenesspb.MembershipStatus_ACTIVE, liveness.MembershipStatus())
			// The scan will also update the cache, verify the epoch is updated there also.
			require.Equal(t, int64(1), nl.GetNodeVitalityFromCache(nodeID).GenLiveness().Epoch)
		}

		sort.Slice(nodeIDs, func(i, j int) bool {
			return nodeIDs[i] < nodeIDs[j]
		})
		for i := range nodeIDs {
			// Node IDs are 1-indexed.
			require.Equal(t, roachpb.NodeID(i+1), nodeIDs[i])
		}
	}

}

func TestNodeLivenessStatusMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderShort(t)

	serverArgs := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				// Disable replica rebalancing to ensure that the liveness range
				// does not get out of the first node (we'll be shutting down nodes).
				ReplicaPlannerKnobs: plan.ReplicaPlannerTestingKnobs{
					DisableReplicaRebalancing: true,
				},
				// Disable LBS because when the scan is happening at the rate it's happening
				// below, it's possible that one of the system ranges trigger a split.
				DisableLoadBasedSplitting: true,
			},
		},
		RaftConfig: base.RaftConfig{
			// Make everything tick faster to ensure dead nodes are
			// recognized dead faster.
			RaftTickInterval: 100 * time.Millisecond,
		},
		// Scan like a bat out of hell to ensure replication and replica GC
		// happen in a timely manner.
		ScanInterval: 50 * time.Millisecond,
	}
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: serverArgs,
		// Disable full replication otherwise StartTestCluster with just 1
		// node will wait forever.
		ReplicationMode: base.ReplicationManual,
	})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	ctx = logtags.AddTag(ctx, "in test", nil)

	log.Infof(ctx, "setting zone config to disable replication")
	if _, err := tc.Conns[0].Exec(`ALTER RANGE meta CONFIGURE ZONE using num_replicas = 1`); err != nil {
		t.Fatal(err)
	}

	log.Infof(ctx, "starting 3 more nodes")
	tc.AddAndStartServer(t, serverArgs)
	tc.AddAndStartServer(t, serverArgs)
	tc.AddAndStartServer(t, serverArgs)

	log.Infof(ctx, "waiting for node statuses")
	tc.WaitForNodeStatuses(t)
	tc.WaitForNodeLiveness(t)
	log.Infof(ctx, "waiting done")

	firstServer := tc.Server(0)

	liveNodeID := firstServer.NodeID()

	deadNodeID := tc.Server(1).NodeID()
	log.Infof(ctx, "shutting down node %d", deadNodeID)
	tc.StopServer(1)
	log.Infof(ctx, "done shutting down node %d", deadNodeID)

	decommissioningNodeID := tc.Server(2).NodeID()
	log.Infof(ctx, "marking node %d as decommissioning", decommissioningNodeID)
	if err := firstServer.Decommission(ctx, livenesspb.MembershipStatus_DECOMMISSIONING, []roachpb.NodeID{decommissioningNodeID}); err != nil {
		t.Fatal(err)
	}
	log.Infof(ctx, "marked node %d as decommissioning", decommissioningNodeID)

	removedNodeID := tc.Server(3).NodeID()
	log.Infof(ctx, "marking node %d as decommissioning and shutting it down", removedNodeID)
	if err := firstServer.Decommission(ctx, livenesspb.MembershipStatus_DECOMMISSIONING, []roachpb.NodeID{removedNodeID}); err != nil {
		t.Fatal(err)
	}
	tc.StopServer(3)
	log.Infof(ctx, "done removing node %d", removedNodeID)

	log.Infof(ctx, "checking status map")

	// See what comes up in the status.
	admin := tc.GetAdminClient(t, 0)

	type testCase struct {
		nodeID         roachpb.NodeID
		expectedStatus livenesspb.NodeLivenessStatus
	}

	// Below we're going to check that all statuses converge and stabilize
	// to a known situation.
	testData := []testCase{
		{liveNodeID, livenesspb.NodeLivenessStatus_LIVE},
		{deadNodeID, livenesspb.NodeLivenessStatus_DEAD},
		{decommissioningNodeID, livenesspb.NodeLivenessStatus_DECOMMISSIONING},
		{removedNodeID, livenesspb.NodeLivenessStatus_DECOMMISSIONED},
	}

	for _, test := range testData {
		t.Run(fmt.Sprintf("n%d->%s", test.nodeID, test.expectedStatus), func(t *testing.T) {
			nodeID, expectedStatus := test.nodeID, test.expectedStatus

			testutils.SucceedsSoon(t, func() error {
				// Ensure that dead nodes are quickly recognized as dead by
				// gossip. Overriding cluster settings is generally a really bad
				// idea as they are also populated via Gossip and so our update
				// is possibly going to be wiped out. But going through SQL
				// doesn't allow durations below 1m15s, which is much too long
				// for a test.
				// We do this in every SucceedsSoon attempt, so we'll be good.
				liveness.TimeUntilNodeDead.Override(ctx, &firstServer.ClusterSettings().SV, liveness.TestTimeUntilNodeDead)

				log.Infof(ctx, "checking expected status (%s) for node %d", expectedStatus, nodeID)
				resp, err := admin.Liveness(ctx, &serverpb.LivenessRequest{})
				require.NoError(t, err)
				nodeStatuses := resp.Statuses

				st, ok := nodeStatuses[nodeID]
				if !ok {
					return errors.Errorf("node %d: not in statuses\n", nodeID)
				}
				if st != expectedStatus {
					return errors.Errorf("node %d: unexpected status: got %s, expected %s\n",
						nodeID, st, expectedStatus,
					)
				}
				return nil
			})
		})
	}
}

func TestNodeLivenessDecommissionedCallback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	var cb struct {
		syncutil.Mutex
		m map[roachpb.NodeID]bool // id -> decommissioned
	}

	tArgs := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				OnDecommissionedCallback: func(id roachpb.NodeID) {
					cb.Lock()
					if cb.m == nil {
						cb.m = map[roachpb.NodeID]bool{}
					}
					cb.m[id] = true
					cb.Unlock()
				},
			},
		},
	}
	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual, // for speed
		ServerArgs:      tArgs,
	}
	tc := testcluster.NewTestCluster(t, 3, args)
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)

	nl1 := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness)

	// Make sure the callback doesn't fire willy-nilly...
	func() {
		chg, err := nl1.SetMembershipStatus(ctx, 2, livenesspb.MembershipStatus_DECOMMISSIONING)
		require.NoError(t, err)
		require.True(t, chg)
		cb.Lock()
		defer cb.Unlock()
		require.Zero(t, cb.m)
	}()

	// ... but only when a node actually gets decommissioned.
	{
		chg, err := nl1.SetMembershipStatus(ctx, 2, livenesspb.MembershipStatus_DECOMMISSIONED)
		require.NoError(t, err)
		require.True(t, chg)
		testutils.SucceedsSoon(t, func() error {
			cb.Lock()
			sl := pretty.Diff(map[roachpb.NodeID]bool{2: true}, cb.m)
			cb.Unlock()
			if len(sl) > 0 {
				return errors.Errorf("diff(exp,act) = %s", strings.Join(sl, "\n"))
			}
			return nil
		})
	}
}

func getActiveNodes(nl *liveness.NodeLiveness) []roachpb.NodeID {
	var nodes []roachpb.NodeID
	for id, nv := range nl.ScanNodeVitalityFromCache() {
		if !nv.IsDecommissioning() && !nv.IsDecommissioned() {
			nodes = append(nodes, id)
		}
	}

	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	return nodes
}

// TestGetActiveNodes tests ScanNodeVitalityFromCache() and is similar to the
// code used within the store_pool for computing the number of active node.
func TestGetActiveNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// This test starts a 5 node cluster and is prone to overload remote execution
	// during race and deadlock builds.
	skip.UnderRace(t)
	skip.UnderDeadlock(t)

	numNodes := 5
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	// At this point StartTestCluster has waited for all nodes to become live.
	nl1 := tc.Servers[0].NodeLiveness().(*liveness.NodeLiveness)
	require.Equal(t, []roachpb.NodeID{1, 2, 3, 4, 5}, getActiveNodes(nl1))

	// Mark n5 as decommissioning, which should reduce node count.
	_, err := nl1.SetMembershipStatus(ctx, 5, livenesspb.MembershipStatus_DECOMMISSIONING)
	require.NoError(t, err)
	// Since we are already checking the expected membership status below, there
	// is no benefit to additionally checking the returned statusChanged flag, as
	// it can be inaccurate if the write experiences an AmbiguousResultError.
	// Checking for nil error and the expected status is sufficient.
	testutils.SucceedsSoon(t, func() error {
		l, ok := nl1.GetLiveness(5)
		if !ok || !l.Membership.Decommissioning() {
			return errors.Errorf("expected n5 to be decommissioning")
		}
		numNodes -= 1
		return nil
	})
	require.Equal(t, []roachpb.NodeID{1, 2, 3, 4}, getActiveNodes(nl1))

	// Mark n5 as decommissioning -> decommissioned, which should not change node count.
	_, err = nl1.SetMembershipStatus(ctx, 5, livenesspb.MembershipStatus_DECOMMISSIONED)
	require.NoError(t, err)
	testutils.SucceedsSoon(t, func() error {
		l, ok := nl1.GetLiveness(5)
		if !ok || !l.Membership.Decommissioned() {
			return errors.Errorf("expected n5 to be decommissioned")
		}
		return nil
	})
	require.Equal(t, []roachpb.NodeID{1, 2, 3, 4}, getActiveNodes(nl1))
}

// TestLivenessRangeGetsPeriodicallyCompacted tests that the liveness range
// gets compacted when we set the liveness range compaction interval.
func TestLivenessRangeGetsPeriodicallyCompacted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	// Enable the liveness range compaction and set the interval to 1s to speed
	// up the test.
	c := tc.Server(0).SystemLayer().SQLConn(t)
	_, err := c.ExecContext(ctx, "set cluster setting kv.liveness_range_compact.interval='1s'")
	require.NoError(t, err)

	// Get the original file number of the sstable for the liveness range. We
	// expect to see this file number change as the liveness range gets compacted.
	livenessFileNumberQuery := "WITH replicas(n) AS (SELECT unnest(replicas) FROM " +
		"crdb_internal.ranges_no_leases WHERE range_id = 2), sstables AS (SELECT " +
		"(crdb_internal.sstable_metrics(n, n, start_key, end_key)).* " +
		"FROM crdb_internal.ranges_no_leases, replicas WHERE range_id = 2) " +
		"SELECT file_num FROM sstables"

	sqlDB := tc.ApplicationLayer(0).SQLConn(t)
	var original_file_num string
	testutils.SucceedsSoon(t, func() error {
		rows := sqlDB.QueryRow(livenessFileNumberQuery)
		if err := rows.Scan(&original_file_num); err != nil {
			return err
		}
		return nil
	})

	// Expect that the liveness file number changes.
	testutils.SucceedsSoon(t, func() error {
		var current_file_num string
		rows := sqlDB.QueryRow(livenessFileNumberQuery)
		if err := rows.Scan(&current_file_num); err != nil {
			return err
		}
		if current_file_num == original_file_num {
			return errors.Errorf("Liveness compaction hasn't happened yet")
		}
		return nil
	})
}
