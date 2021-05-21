// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package liveness_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
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
	"github.com/gogo/protobuf/proto"
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

		if live, err := nl.IsLive(nodeID); err != nil {
			t.Fatal(err)
		} else if !live {
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

// TestGetLivenessesFromKV verifies that fetching liveness records from KV
// directly retrieves all the records we expect.
func TestGetLivenessesFromKV(t *testing.T) {
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

		if live, err := nl.IsLive(nodeID); err != nil {
			t.Fatal(err)
		} else if !live {
			t.Fatalf("node %d not live", nodeID)
		}

		livenesses, err := nl.GetLivenessesFromKV(ctx)
		assert.Nil(t, err)
		assert.Equal(t, len(livenesses), tc.NumServers())

		var nodeIDs []roachpb.NodeID
		for _, liveness := range livenesses {
			nodeIDs = append(nodeIDs, liveness.NodeID)

			// We expect epoch=1 as nodes first create a liveness record at epoch=0,
			// and then increment it during their first heartbeat.
			if liveness.Epoch != 1 {
				t.Fatalf("expected epoch=1, got epoch=%d", liveness.Epoch)
			}
			if !liveness.Membership.Active() {
				t.Fatalf("expected membership=active, got membership=%s", liveness.Membership)
			}
		}

		sort.Slice(nodeIDs, func(i, j int) bool {
			return nodeIDs[i] < nodeIDs[j]
		})
		for i := range nodeIDs {
			expNodeID := roachpb.NodeID(i + 1) // Node IDs are 1-indexed.
			if nodeIDs[i] != expNodeID {
				t.Fatalf("expected nodeID=%d, got %d", expNodeID, nodeIDs[i])
			}
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
				DisableReplicaRebalancing: true,
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
	// Allow for inserting zone configs without having to go through (or
	// duplicate the logic from) the CLI.
	config.TestingSetupZoneConfigHook(tc.Stopper())
	zoneConfig := zonepb.DefaultZoneConfig()
	// Force just one replica per range to ensure that we can shut down
	// nodes without endangering the liveness range.
	zoneConfig.NumReplicas = proto.Int32(1)
	config.TestingSetZoneConfig(keys.MetaRangesID, zoneConfig)

	log.Infof(ctx, "starting 3 more nodes")
	tc.AddAndStartServer(t, serverArgs)
	tc.AddAndStartServer(t, serverArgs)
	tc.AddAndStartServer(t, serverArgs)

	log.Infof(ctx, "waiting for node statuses")
	tc.WaitForNodeStatuses(t)
	tc.WaitForNodeLiveness(t)
	log.Infof(ctx, "waiting done")

	firstServer := tc.Server(0).(*server.TestServer)

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
	admin, err := tc.GetAdminClient(ctx, t, 0)
	if err != nil {
		t.Fatal(err)
	}

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
				kvserver.TimeUntilStoreDead.Override(ctx, &firstServer.ClusterSettings().SV, kvserver.TestTimeUntilStoreDead)

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
				OnDecommissionedCallback: func(rec livenesspb.Liveness) {
					cb.Lock()
					if cb.m == nil {
						cb.m = map[roachpb.NodeID]bool{}
					}
					cb.m[rec.NodeID] = rec.Membership == livenesspb.MembershipStatus_DECOMMISSIONED
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
