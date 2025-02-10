// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestPartialPartitionProxyThreeOK(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testPartialPartition(t, true, 3)
}
func TestPartialPartitionProxyFiveOk(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testPartialPartition(t, true, 5)
}

func TestPartialPartitionDirectThreeFail(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testPartialPartition(t, false, 3)
}
func TestPartialPartitionDirectFiveFail(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testPartialPartition(t, false, 5)
}

// TestPartialPartition verifies various complex success/failure scenarios.
// The leaseholder is always on n2(idx 1) and the client is on n1(idx 0).
// Additionally validate that a rangefeed sees the update.
func testPartialPartition(t *testing.T, useProxy bool, numServers int) {
	skip.UnderDuress(t, "test does heavy lifting")
	partition := [][2]roachpb.NodeID{{1, 2}}
	ctx := context.Background()

	t.Run(fmt.Sprintf("%t-%d", useProxy, numServers), func(t *testing.T) {
		testutils.RunValues(t, "lease-type", roachpb.TestingAllLeaseTypes(), func(t *testing.T, leaseType roachpb.LeaseType) {
			if leaseType == roachpb.LeaseEpoch {
				// With epoch leases this test doesn't work reliably. It passes
				// in cases where it should fail and fails in cases where it
				// should pass.
				// TODO(baptist): Attempt to pin the liveness leaseholder to
				// node 3 to make epoch leases reliable.
				skip.IgnoreLint(t, "flaky with epoch leases")
			}

			st := cluster.MakeTestingClusterSettings()
			kvcoord.ProxyBatchRequest.Override(ctx, &st.SV, useProxy)
			kvserver.OverrideDefaultLeaseType(ctx, &st.SV, leaseType)
			kvserver.RangefeedEnabled.Override(ctx, &st.SV, true)
			kvserver.RangeFeedRefreshInterval.Override(ctx, &st.SV, 10*time.Millisecond)
			closedts.TargetDuration.Override(ctx, &st.SV, 10*time.Millisecond)
			closedts.SideTransportCloseInterval.Override(ctx, &st.SV, 10*time.Millisecond)
			// Configure the number of replicas and voters to have a replica on every node.
			zoneConfig := zonepb.DefaultZoneConfig()
			numNodes := int32(numServers)
			zoneConfig.NumReplicas = &numNodes
			zoneConfig.NumVoters = &numNodes

			var p rpc.Partitioner
			tc := testcluster.StartTestCluster(t, numServers, base.TestClusterArgs{
				ServerArgsPerNode: func() map[int]base.TestServerArgs {
					perNode := make(map[int]base.TestServerArgs)
					for i := 0; i < numServers; i++ {
						ctk := rpc.ContextTestingKnobs{}
						p.RegisterTestingKnobs(roachpb.NodeID(i+1), partition, &ctk)
						perNode[i] = base.TestServerArgs{
							Settings:         st,
							DisableSQLServer: true,
							Knobs: base.TestingKnobs{
								Server: &server.TestingKnobs{
									DefaultZoneConfigOverride: &zoneConfig,
									ContextTestingKnobs:       ctk,
								},
							},
						}
					}
					return perNode
				}(),
			})

			// Set up the mapping after the nodes have started and we have their
			// addresses.
			for i := 0; i < numServers; i++ {
				g := tc.Servers[i].StorageLayer().GossipI().(*gossip.Gossip)
				addr := g.GetNodeAddr().String()
				nodeID := g.NodeID.Get()
				p.RegisterNodeAddr(addr, nodeID)
			}

			scratchKey := tc.ScratchRange(t)
			// We want all ranges to have full replication to be available through partitions.
			require.NoError(t, tc.WaitForFullReplication())

			desc := tc.LookupRangeOrFatal(t, scratchKey)
			tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(1))

			// TODO(baptist): This test should work without this block.
			// After the lease is transferred, the lease might still be on
			// the n1. Eventually n2 will fail to become leader, and the
			// lease will expire and n1 will reacquire the lease. DistSender
			// doesn't correctly handle this case today, but will in the
			// future. Today, if we partition in a split leader/leaseholder
			// split, a request will sit waiting for the proposal buffer on
			// n1 and and never return to DistSender without success or
			// failure. Without a timeout or other circuit breaker in
			// DistSender we will never succeed once we partition. Remove
			// this block once #118943 is fixed.
			testutils.SucceedsSoon(t, func() error {
				sl := tc.StorageLayer(1)
				store, err := sl.GetStores().(*kvserver.Stores).GetStore(sl.GetFirstStoreID())
				require.NoError(t, err)
				status := store.LookupReplica(roachpb.RKey(scratchKey)).RaftStatus()
				if status == nil || status.RaftState != raftpb.StateLeader {
					return errors.Newf("Leader leaseholder split %v", status)
				}
				return nil
			})

			p.EnablePartition(true)

			txn := tc.ApplicationLayer(0).DB().NewTxn(ctx, "test")
			// DistSender will retry forever. For the failure cases we want
			// to fail faster.
			cancelCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			err := txn.Put(cancelCtx, scratchKey, "abc")
			if useProxy {
				require.NoError(t, err)
				require.NoError(t, txn.Commit(cancelCtx))
			} else {
				require.Error(t, err)
				require.NoError(t, txn.Rollback(cancelCtx))
			}

			// Stop all the clients first to avoid getting stuck on failing tests.
			for i := 0; i < numServers; i++ {
				tc.ApplicationLayer(i).AppStopper().Stop(ctx)
			}

			tc.Stopper().Stop(ctx)
		})
	})
}
