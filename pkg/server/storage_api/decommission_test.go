// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage_api_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/decommissioning"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestDecommissionPreCheckInvalid tests decommission pre check expected errors.
func TestDecommissionPreCheckInvalid(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Set up test cluster.
	ctx := context.Background()
	tc := serverutils.StartCluster(t, 4, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
		ReplicationMode: base.ReplicationManual,
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: decommissionTsArgs("a", "n1"),
			1: decommissionTsArgs("b", "n2"),
			2: decommissionTsArgs("c", "n3"),
			3: decommissionTsArgs("a", "n4"),
		},
	})
	defer tc.Stopper().Stop(ctx)

	firstSvr := tc.Server(0)

	// Create database and tables.
	ac := firstSvr.AmbientCtx()
	ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
	defer span.Finish()

	// Attempt to decommission check with unlimited traces.
	decommissioningNodeIDs := []roachpb.NodeID{tc.Server(3).NodeID()}
	result, err := firstSvr.DecommissionPreCheck(ctx, decommissioningNodeIDs,
		true /* strictReadiness */, true /* collectTraces */, 0, /* maxErrors */
	)
	require.Error(t, err)
	status, ok := status.FromError(err)
	require.True(t, ok, "expected grpc status error")
	require.Equal(t, codes.InvalidArgument, status.Code())
	require.Equal(t, decommissioning.PreCheckResult{}, result)
}

// TestDecommissionPreCheckEvaluation tests evaluation of decommission readiness
// of several nodes in a cluster given the replicas that exist on those nodes.
func TestDecommissionPreCheckEvaluation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t) // can't handle 7-node clusters

	tsArgs := func(attrs ...string) base.TestServerArgs {
		return decommissionTsArgs("a", attrs...)
	}

	// Set up test cluster.
	ctx := context.Background()
	tc := serverutils.StartCluster(t, 7, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: tsArgs("ns1", "origin"),
			1: tsArgs("ns2", "west"),
			2: tsArgs("ns3", "central"),
			3: tsArgs("ns4", "central"),
			4: tsArgs("ns5", "east"),
			5: tsArgs("ns6", "east"),
			6: tsArgs("ns7", "east"),
		},
	})
	defer tc.Stopper().Stop(ctx)

	firstSvr := tc.Server(0)
	db := tc.ServerConn(0)
	runQueries := func(queries ...string) {
		for _, q := range queries {
			if _, err := db.Exec(q); err != nil {
				t.Fatalf("error executing '%s': %s", q, err)
			}
		}
	}

	// Create database and tables.
	ac := firstSvr.AmbientCtx()
	ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
	defer span.Finish()
	setupQueries := []string{
		"CREATE DATABASE test",
		"CREATE TABLE test.tblA (val STRING)",
		"CREATE TABLE test.tblB (val STRING)",
		"INSERT INTO test.tblA VALUES ('testvalA')",
		"INSERT INTO test.tblB VALUES ('testvalB')",
	}
	runQueries(setupQueries...)
	alterQueries := []string{
		"ALTER TABLE test.tblA CONFIGURE ZONE USING num_replicas = 3, constraints = '{+west: 1, +central: 1, +east: 1}', " +
			"range_max_bytes = 500000000, range_min_bytes = 100",
		"ALTER TABLE test.tblB CONFIGURE ZONE USING num_replicas = 3, constraints = '{+east}', " +
			"range_max_bytes = 500000000, range_min_bytes = 100",
	}
	runQueries(alterQueries...)
	tblAID, err := firstSvr.QueryTableID(ctx, username.RootUserName(), "test", "tblA")
	require.NoError(t, err)
	tblBID, err := firstSvr.QueryTableID(ctx, username.RootUserName(), "test", "tblB")
	require.NoError(t, err)
	startKeyTblA := firstSvr.Codec().TablePrefix(uint32(tblAID))
	startKeyTblB := firstSvr.Codec().TablePrefix(uint32(tblBID))

	// Split off ranges for tblA and tblB.
	_, rDescA, err := firstSvr.SplitRange(startKeyTblA)
	require.NoError(t, err)
	_, rDescB, err := firstSvr.SplitRange(startKeyTblB)
	require.NoError(t, err)

	// Ensure all nodes have the correct span configs for tblA and tblB.
	waitForSpanConfig(t, tc, rDescA.StartKey, 500000000)
	waitForSpanConfig(t, tc, rDescB.StartKey, 500000000)

	// Transfer tblA to [west, central, east] and tblB to [east].
	tc.AddVotersOrFatal(t, startKeyTblA, tc.Target(1), tc.Target(2), tc.Target(4))
	tc.TransferRangeLeaseOrFatal(t, rDescA, tc.Target(1))
	tc.RemoveVotersOrFatal(t, startKeyTblA, tc.Target(0))
	tc.AddVotersOrFatal(t, startKeyTblB, tc.Target(4), tc.Target(5), tc.Target(6))
	tc.TransferRangeLeaseOrFatal(t, rDescB, tc.Target(4))
	tc.RemoveVotersOrFatal(t, startKeyTblB, tc.Target(0))

	// Validate range distribution.
	rDescA = tc.LookupRangeOrFatal(t, startKeyTblA)
	rDescB = tc.LookupRangeOrFatal(t, startKeyTblB)
	for _, desc := range []roachpb.RangeDescriptor{rDescA, rDescB} {
		require.Lenf(t, desc.Replicas().VoterAndNonVoterDescriptors(), 3, "expected 3 replicas, have %v", desc)
	}

	require.True(t, hasReplicaOnServers(tc, &rDescA, 1, 2, 4))
	require.True(t, hasReplicaOnServers(tc, &rDescB, 4, 5, 6))

	// Evaluate n5 decommission check.
	decommissioningNodeIDs := []roachpb.NodeID{tc.Server(4).NodeID()}
	result, err := firstSvr.DecommissionPreCheck(ctx, decommissioningNodeIDs,
		true /* strictReadiness */, true /* collectTraces */, 10000, /* maxErrors */
	)
	require.NoError(t, err)
	require.Equal(t, 2, result.RangesChecked, "unexpected number of ranges checked")
	require.Equalf(t, 2, result.ActionCounts[allocatorimpl.AllocatorReplaceDecommissioningVoter.String()],
		"unexpected allocator actions, got %v", result.ActionCounts)
	require.Lenf(t, result.RangesNotReady, 1, "unexpected number of unready ranges")

	// Validate error on tblB's range as it requires 3 replicas in "east".
	unreadyResult := result.RangesNotReady[0]
	require.Equalf(t, rDescB.StartKey, unreadyResult.Desc.StartKey,
		"expected tblB's range to be unready, got %s", unreadyResult.Desc,
	)
	require.Errorf(t, unreadyResult.Err, "expected error on %s", unreadyResult.Desc)
	require.NotEmptyf(t, unreadyResult.TracingSpans, "expected tracing spans on %s", unreadyResult.Desc)
	var allocatorError allocator.AllocationError
	require.ErrorAsf(t, unreadyResult.Err, &allocatorError, "expected allocator error on %s", unreadyResult.Desc)

	// Evaluate n3 decommission check (not required to satisfy constraints).
	decommissioningNodeIDs = []roachpb.NodeID{tc.Server(2).NodeID()}
	result, err = firstSvr.DecommissionPreCheck(ctx, decommissioningNodeIDs,
		true /* strictReadiness */, false /* collectTraces */, 0, /* maxErrors */
	)
	require.NoError(t, err)
	require.Equal(t, 1, result.RangesChecked, "unexpected number of ranges checked")
	require.Equalf(t, 1, result.ActionCounts[allocatorimpl.AllocatorReplaceDecommissioningVoter.String()],
		"unexpected allocator actions, got %v", result.ActionCounts)
	require.Lenf(t, result.RangesNotReady, 0, "unexpected number of unready ranges")
}

// TestDecommissionPreCheckOddToEven tests evaluation of decommission readiness
// when moving from 5 nodes to 3, in which case ranges with RF of 5 should have
// an effective RF of 3.
func TestDecommissionPreCheckOddToEven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Set up test cluster.
	ctx := context.Background()
	tc := serverutils.StartCluster(t, 5, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	})
	defer tc.Stopper().Stop(ctx)

	firstSvr := tc.Server(0)
	db := tc.ServerConn(0)
	runQueries := func(queries ...string) {
		for _, q := range queries {
			if _, err := db.Exec(q); err != nil {
				t.Fatalf("error executing '%s': %s", q, err)
			}
		}
	}

	// Create database and tables.
	ac := firstSvr.AmbientCtx()
	ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
	defer span.Finish()
	setupQueries := []string{
		"CREATE DATABASE test",
		"CREATE TABLE test.tblA (val STRING)",
		"INSERT INTO test.tblA VALUES ('testvalA')",
	}
	runQueries(setupQueries...)
	alterQueries := []string{
		"ALTER TABLE test.tblA CONFIGURE ZONE USING num_replicas = 5, " +
			"range_max_bytes = 500000000, range_min_bytes = 100",
	}
	runQueries(alterQueries...)
	tblAID, err := firstSvr.QueryTableID(ctx, username.RootUserName(), "test", "tblA")
	require.NoError(t, err)
	startKeyTblA := firstSvr.Codec().TablePrefix(uint32(tblAID))

	// Split off range for tblA.
	_, rDescA, err := firstSvr.SplitRange(startKeyTblA)
	require.NoError(t, err)

	// Ensure all nodes have the correct span configs for tblA.
	waitForSpanConfig(t, tc, rDescA.StartKey, 500000000)

	// Transfer tblA to all nodes.
	tc.AddVotersOrFatal(t, startKeyTblA, tc.Target(1), tc.Target(2), tc.Target(3), tc.Target(4))
	tc.TransferRangeLeaseOrFatal(t, rDescA, tc.Target(1))

	// Validate range distribution.
	rDescA = tc.LookupRangeOrFatal(t, startKeyTblA)
	require.Lenf(t, rDescA.Replicas().VoterAndNonVoterDescriptors(), 5, "expected 5 replicas, have %v", rDescA)

	require.True(t, hasReplicaOnServers(tc, &rDescA, 0, 1, 2, 3, 4))

	// Evaluate n5 decommission check.
	decommissioningNodeIDs := []roachpb.NodeID{tc.Server(4).NodeID()}
	result, err := firstSvr.DecommissionPreCheck(ctx, decommissioningNodeIDs,
		true /* strictReadiness */, true /* collectTraces */, 10000, /* maxErrors */
	)
	require.NoError(t, err)
	require.Equal(t, 1, result.RangesChecked, "unexpected number of ranges checked")
	require.Equalf(t, 1, result.ActionCounts[allocatorimpl.AllocatorRemoveDecommissioningVoter.String()],
		"unexpected allocator actions, got %v", result.ActionCounts)
	require.Lenf(t, result.RangesNotReady, 0, "unexpected number of unready ranges")
}

// decommissionTsArgs returns a base.TestServerArgs for creating a test cluster
// with per-store attributes using a single, in-memory store for each node.
func decommissionTsArgs(region string, attrs ...string) base.TestServerArgs {
	return base.TestServerArgs{
		Locality: roachpb.Locality{
			Tiers: []roachpb.Tier{
				{
					Key:   "region",
					Value: region,
				},
			},
		},
		StoreSpecs: []base.StoreSpec{
			{InMemory: true, Attributes: roachpb.Attributes{Attrs: attrs}},
		},
	}
}

// hasReplicaOnServers returns true if the range has replicas on given servers.
func hasReplicaOnServers(
	tc serverutils.TestClusterInterface, desc *roachpb.RangeDescriptor, serverIdxs ...int,
) bool {
	for _, idx := range serverIdxs {
		if !desc.Replicas().HasReplicaOnNode(tc.Server(idx).NodeID()) {
			return false
		}
	}
	return true
}

// waitForSpanConfig waits until all servers in the test cluster have a span
// config for the key with the expected number of max bytes for the range.
func waitForSpanConfig(
	t *testing.T, tc serverutils.TestClusterInterface, key roachpb.RKey, exp int64,
) {
	testutils.SucceedsSoon(t, func() error {
		for i := 0; i < tc.NumServers(); i++ {
			s := tc.Server(i)
			store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
			if err != nil {
				return errors.Wrapf(err, "missing store on server %d", i)
			}
			conf, _, err := store.GetStoreConfig().SpanConfigSubscriber.GetSpanConfigForKey(context.Background(), key)
			if err != nil {
				return errors.Wrapf(err, "missing span config for %s on server %d", key, i)
			}
			if conf.RangeMaxBytes != exp {
				return errors.Errorf("expected %d max bytes, got %d", exp, conf.RangeMaxBytes)
			}
		}
		return nil
	})
}

// TestDecommissionPreCheckBasicReadiness tests the basic functionality of the
// DecommissionPreCheck endpoint.
func TestDecommissionPreCheckBasicReadiness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t) // can't handle 7-node clusters

	ctx := context.Background()
	tc := serverutils.StartCluster(t, 7, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
		ReplicationMode: base.ReplicationManual, // saves time
	})
	defer tc.Stopper().Stop(ctx)

	adminSrv := tc.Server(4)
	adminClient := adminSrv.GetAdminClient(t)

	resp, err := adminClient.DecommissionPreCheck(ctx, &serverpb.DecommissionPreCheckRequest{
		NodeIDs: []roachpb.NodeID{tc.Server(5).NodeID()},
	})
	require.NoError(t, err)
	require.Len(t, resp.CheckedNodes, 1)
	checkNodeCheckResultReady(t, tc.Server(5).NodeID(), 0, resp.CheckedNodes[0])
}

// TestDecommissionPreCheckUnready tests the functionality of the
// DecommissionPreCheck endpoint with some nodes not ready.
func TestDecommissionPreCheckUnready(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t) // can't handle 7-node clusters

	ctx := context.Background()
	tc := serverutils.StartCluster(t, 7, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
		ReplicationMode: base.ReplicationManual, // saves time
	})
	defer tc.Stopper().Stop(ctx)

	// Add replicas to a node we will check.
	// Scratch range should have RF=3, liveness range should have RF=5.
	adminSrvIdx := 3
	decommissioningSrvIdx := 5
	scratchKey := tc.ScratchRange(t)
	scratchDesc := tc.AddVotersOrFatal(t, scratchKey, tc.Target(decommissioningSrvIdx))
	livenessDesc := tc.LookupRangeOrFatal(t, keys.NodeLivenessPrefix)
	livenessDesc = tc.AddVotersOrFatal(t, livenessDesc.StartKey.AsRawKey(), tc.Target(decommissioningSrvIdx))

	adminSrv := tc.Server(adminSrvIdx)
	decommissioningSrv := tc.Server(decommissioningSrvIdx)
	adminClient := adminSrv.GetAdminClient(t)

	checkNodeReady := func(nID roachpb.NodeID, replicaCount int64, strict bool) {
		resp, err := adminClient.DecommissionPreCheck(ctx, &serverpb.DecommissionPreCheckRequest{
			NodeIDs:         []roachpb.NodeID{nID},
			StrictReadiness: strict,
		})
		require.NoError(t, err)
		require.Len(t, resp.CheckedNodes, 1)
		checkNodeCheckResultReady(t, nID, replicaCount, resp.CheckedNodes[0])
	}

	awaitDecommissioned := func(nID roachpb.NodeID) {
		testutils.SucceedsSoon(t, func() error {
			livenesses, err := adminSrv.NodeLiveness().(*liveness.NodeLiveness).ScanNodeVitalityFromKV(ctx)
			if err != nil {
				return err
			}
			for nodeID, nodeLiveness := range livenesses {
				if nodeID == nID {
					if nodeLiveness.IsDecommissioned() {
						return nil
					} else {
						return errors.Errorf("n%d has membership: %s", nID, nodeLiveness.MembershipStatus())
					}
				}
			}
			return errors.Errorf("n%d liveness not found", nID)
		})
	}

	checkAndDecommission := func(srvIdx int, replicaCount int64, strict bool) {
		nID := tc.Server(srvIdx).NodeID()
		checkNodeReady(nID, replicaCount, strict)
		require.NoError(t, adminSrv.Decommission(
			ctx, livenesspb.MembershipStatus_DECOMMISSIONING, []roachpb.NodeID{nID}))
		require.NoError(t, adminSrv.Decommission(
			ctx, livenesspb.MembershipStatus_DECOMMISSIONED, []roachpb.NodeID{nID}))
		awaitDecommissioned(nID)
	}

	// In non-strict mode, this decommission appears "ready". This is because the
	// ranges with replicas on decommissioningSrv have priority action "AddVoter",
	// and they have valid targets.
	checkNodeReady(decommissioningSrv.NodeID(), 2, false)

	// In strict mode, we would expect the readiness check to fail.
	resp, err := adminClient.DecommissionPreCheck(ctx, &serverpb.DecommissionPreCheckRequest{
		NodeIDs:          []roachpb.NodeID{decommissioningSrv.NodeID()},
		NumReplicaReport: 50,
		StrictReadiness:  true,
		CollectTraces:    true,
	})
	require.NoError(t, err)
	nodeCheckResult := resp.CheckedNodes[0]
	require.Equalf(t, serverpb.DecommissionPreCheckResponse_ALLOCATION_ERRORS, nodeCheckResult.DecommissionReadiness,
		"expected n%d to have allocation errors, got %s", nodeCheckResult.NodeID, nodeCheckResult.DecommissionReadiness)
	require.Len(t, nodeCheckResult.CheckedRanges, 2)
	checkRangeCheckResult(t, livenessDesc, nodeCheckResult.CheckedRanges[0],
		"add voter", "needs repair beyond replacing/removing", true,
	)
	checkRangeCheckResult(t, scratchDesc, nodeCheckResult.CheckedRanges[1],
		"add voter", "needs repair beyond replacing/removing", true,
	)

	// Add replicas to ensure we have the correct number of replicas for each range.
	scratchDesc = tc.AddVotersOrFatal(t, scratchKey, tc.Target(adminSrvIdx))
	livenessDesc = tc.AddVotersOrFatal(t, livenessDesc.StartKey.AsRawKey(),
		tc.Target(adminSrvIdx), tc.Target(4), tc.Target(6),
	)
	require.True(t, hasReplicaOnServers(tc, &scratchDesc, 0, adminSrvIdx, decommissioningSrvIdx))
	require.True(t, hasReplicaOnServers(tc, &livenessDesc, 0, adminSrvIdx, decommissioningSrvIdx, 4, 6))
	require.Len(t, scratchDesc.InternalReplicas, 3)
	require.Len(t, livenessDesc.InternalReplicas, 5)

	// Decommissioning pre-check should pass on decommissioningSrv in both strict
	// and non-strict modes, as each range can find valid upreplication targets.
	checkNodeReady(decommissioningSrv.NodeID(), 2, true)

	// Check and decommission empty nodes, decreasing to a 5-node cluster.
	checkAndDecommission(1, 0, true)
	checkAndDecommission(2, 0, true)

	// Check that we can still decommission.
	// Below 5 nodes, system ranges will have an effective RF=3.
	checkNodeReady(decommissioningSrv.NodeID(), 2, true)

	// Check that we can decommission the nodes with liveness replicas only.
	checkAndDecommission(4, 1, true)
	checkAndDecommission(6, 1, true)

	// Check range descriptors are as expected.
	scratchDesc = tc.LookupRangeOrFatal(t, scratchDesc.StartKey.AsRawKey())
	livenessDesc = tc.LookupRangeOrFatal(t, livenessDesc.StartKey.AsRawKey())
	require.True(t, hasReplicaOnServers(tc, &scratchDesc, 0, adminSrvIdx, decommissioningSrvIdx))
	require.True(t, hasReplicaOnServers(tc, &livenessDesc, 0, adminSrvIdx, decommissioningSrvIdx, 4, 6))
	require.Len(t, scratchDesc.InternalReplicas, 3)
	require.Len(t, livenessDesc.InternalReplicas, 5)

	// Cleanup orphaned liveness replicas and check.
	livenessDesc = tc.RemoveVotersOrFatal(t, livenessDesc.StartKey.AsRawKey(), tc.Target(4), tc.Target(6))
	require.True(t, hasReplicaOnServers(tc, &livenessDesc, 0, adminSrvIdx, decommissioningSrvIdx))
	require.Len(t, livenessDesc.InternalReplicas, 3)

	// Validate that the node is not ready to decommission.
	resp, err = adminClient.DecommissionPreCheck(ctx, &serverpb.DecommissionPreCheckRequest{
		NodeIDs:          []roachpb.NodeID{decommissioningSrv.NodeID()},
		NumReplicaReport: 1, // Test that we limit errors.
		StrictReadiness:  true,
	})
	require.NoError(t, err)
	nodeCheckResult = resp.CheckedNodes[0]
	require.Equalf(t, serverpb.DecommissionPreCheckResponse_ALLOCATION_ERRORS, nodeCheckResult.DecommissionReadiness,
		"expected n%d to have allocation errors, got %s", nodeCheckResult.NodeID, nodeCheckResult.DecommissionReadiness)
	require.Equal(t, int64(2), nodeCheckResult.ReplicaCount)
	require.Len(t, nodeCheckResult.CheckedRanges, 1)
	checkRangeCheckResult(t, livenessDesc, nodeCheckResult.CheckedRanges[0],
		"replace decommissioning voter",
		"0 of 2 live stores are able to take a new replica for the range "+
			"(2 already have a voter, 0 already have a non-voter); "+
			"likely not enough nodes in cluster",
		false,
	)
}

// TestDecommissionPreCheckMultiple tests the functionality of the
// DecommissionPreCheck endpoint with multiple nodes.
func TestDecommissionPreCheckMultiple(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := serverutils.StartCluster(t, 5, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
		ReplicationMode: base.ReplicationManual, // saves time
	})
	defer tc.Stopper().Stop(ctx)

	// TODO(sarkesian): Once #95909 is merged, test checks on a 3-node decommission.
	// e.g. Test both server idxs 3,4 and 2,3,4 (which should not pass checks).
	adminSrvIdx := 1
	decommissioningSrvIdxs := []int{3, 4}
	decommissioningSrvNodeIDs := make([]roachpb.NodeID, len(decommissioningSrvIdxs))
	for i, srvIdx := range decommissioningSrvIdxs {
		decommissioningSrvNodeIDs[i] = tc.Server(srvIdx).NodeID()
	}

	// Add replicas to nodes we will check.
	// Scratch range should have RF=3, liveness range should have RF=5.
	rangeDescs := []roachpb.RangeDescriptor{
		tc.LookupRangeOrFatal(t, keys.NodeLivenessPrefix),
		tc.LookupRangeOrFatal(t, tc.ScratchRange(t)),
	}
	rangeDescSrvIdxs := [][]int{
		{0, 1, 2, 3, 4},
		{0, 3, 4},
	}
	rangeDescSrvTargets := make([][]roachpb.ReplicationTarget, len(rangeDescs))
	for i, srvIdxs := range rangeDescSrvIdxs {
		for _, srvIdx := range srvIdxs {
			if srvIdx != 0 {
				rangeDescSrvTargets[i] = append(rangeDescSrvTargets[i], tc.Target(srvIdx))
			}
		}
	}

	for i, rangeDesc := range rangeDescs {
		rangeDescs[i] = tc.AddVotersOrFatal(t, rangeDesc.StartKey.AsRawKey(), rangeDescSrvTargets[i]...)
	}

	for i, rangeDesc := range rangeDescs {
		require.True(t, hasReplicaOnServers(tc, &rangeDesc, rangeDescSrvIdxs[i]...))
		require.Len(t, rangeDesc.InternalReplicas, len(rangeDescSrvIdxs[i]))
	}

	adminSrv := tc.Server(adminSrvIdx)
	adminClient := adminSrv.GetAdminClient(t)

	// We expect to be able to decommission the targeted nodes simultaneously.
	resp, err := adminClient.DecommissionPreCheck(ctx, &serverpb.DecommissionPreCheckRequest{
		NodeIDs:          decommissioningSrvNodeIDs,
		NumReplicaReport: 50,
		StrictReadiness:  true,
		CollectTraces:    true,
	})
	require.NoError(t, err)
	require.Len(t, resp.CheckedNodes, len(decommissioningSrvIdxs))
	for i, nID := range decommissioningSrvNodeIDs {
		checkNodeCheckResultReady(t, nID, int64(len(rangeDescs)), resp.CheckedNodes[i])
	}
}

// TestDecommissionPreCheckInvalidNode tests the functionality of the
// DecommissionPreCheck endpoint where some nodes are invalid.
func TestDecommissionPreCheckInvalidNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := serverutils.StartCluster(t, 5, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
		ReplicationMode: base.ReplicationManual, // saves time
	})
	defer tc.Stopper().Stop(ctx)

	adminSrvIdx := 1
	validDecommissioningNodeID := roachpb.NodeID(5)
	invalidDecommissioningNodeID := roachpb.NodeID(34)
	decommissioningNodeIDs := []roachpb.NodeID{validDecommissioningNodeID, invalidDecommissioningNodeID}

	// Add replicas to nodes we will check.
	// Scratch range should have RF=3, liveness range should have RF=5.
	rangeDescs := []roachpb.RangeDescriptor{
		tc.LookupRangeOrFatal(t, keys.NodeLivenessPrefix),
		tc.LookupRangeOrFatal(t, tc.ScratchRange(t)),
	}
	rangeDescSrvIdxs := [][]int{
		{0, 1, 2, 3, 4},
		{0, 3, 4},
	}
	rangeDescSrvTargets := make([][]roachpb.ReplicationTarget, len(rangeDescs))
	for i, srvIdxs := range rangeDescSrvIdxs {
		for _, srvIdx := range srvIdxs {
			if srvIdx != 0 {
				rangeDescSrvTargets[i] = append(rangeDescSrvTargets[i], tc.Target(srvIdx))
			}
		}
	}

	for i, rangeDesc := range rangeDescs {
		rangeDescs[i] = tc.AddVotersOrFatal(t, rangeDesc.StartKey.AsRawKey(), rangeDescSrvTargets[i]...)
	}

	for i, rangeDesc := range rangeDescs {
		require.True(t, hasReplicaOnServers(tc, &rangeDesc, rangeDescSrvIdxs[i]...))
		require.Len(t, rangeDesc.InternalReplicas, len(rangeDescSrvIdxs[i]))
	}

	adminSrv := tc.Server(adminSrvIdx)
	adminClient := adminSrv.GetAdminClient(t)

	// We expect the pre-check to fail as some node IDs are invalid.
	resp, err := adminClient.DecommissionPreCheck(ctx, &serverpb.DecommissionPreCheckRequest{
		NodeIDs:          decommissioningNodeIDs,
		NumReplicaReport: 50,
		StrictReadiness:  true,
		CollectTraces:    true,
	})
	require.NoError(t, err)
	require.Len(t, resp.CheckedNodes, len(decommissioningNodeIDs))
	checkNodeCheckResultReady(t, validDecommissioningNodeID, int64(len(rangeDescs)), resp.CheckedNodes[0])
	require.Equal(t, serverpb.DecommissionPreCheckResponse_NodeCheckResult{
		NodeID:                invalidDecommissioningNodeID,
		DecommissionReadiness: serverpb.DecommissionPreCheckResponse_UNKNOWN,
		ReplicaCount:          0,
		CheckedRanges:         nil,
	}, resp.CheckedNodes[1])
}

func TestDecommissionSelf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t) // can't handle 7-node clusters
	skip.UnderDeadlockWithIssue(t, 112918)

	// Set up test cluster.
	ctx := context.Background()
	tc := serverutils.StartCluster(t, 7, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
		ReplicationMode: base.ReplicationManual, // saves time
	})
	defer tc.Stopper().Stop(ctx)

	// Decommission several nodes, including the node we're submitting the
	// decommission request to. We use the admin client in order to test the
	// admin server's logic, which involves a subsequent DecommissionStatus
	// call which could fail if used from a node that's just decommissioned.
	adminSrv := tc.Server(4)
	adminClient := adminSrv.GetAdminClient(t)
	decomNodeIDs := []roachpb.NodeID{
		tc.Server(4).NodeID(),
		tc.Server(5).NodeID(),
		tc.Server(6).NodeID(),
	}

	// The DECOMMISSIONING call should return a full status response.
	resp, err := adminClient.Decommission(ctx, &serverpb.DecommissionRequest{
		NodeIDs:          decomNodeIDs,
		TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONING,
	})
	require.NoError(t, err)
	require.Len(t, resp.Status, len(decomNodeIDs))
	for i, nodeID := range decomNodeIDs {
		status := resp.Status[i]
		require.Equal(t, nodeID, status.NodeID)
		// Liveness entries may not have been updated yet.
		require.Contains(t, []livenesspb.MembershipStatus{
			livenesspb.MembershipStatus_ACTIVE,
			livenesspb.MembershipStatus_DECOMMISSIONING,
		}, status.Membership, "unexpected membership status %v for node %v", status, nodeID)
	}

	// The DECOMMISSIONED call should return an empty response, to avoid
	// erroring due to loss of cluster RPC access when decommissioning self.
	resp, err = adminClient.Decommission(ctx, &serverpb.DecommissionRequest{
		NodeIDs:          decomNodeIDs,
		TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONED,
	})
	require.NoError(t, err)
	require.Empty(t, resp.Status)

	// The nodes should now have been (or soon become) decommissioned.
	for i := 0; i < tc.NumServers(); i++ {
		srv := tc.Server(i)
		expect := livenesspb.MembershipStatus_ACTIVE
		for _, nodeID := range decomNodeIDs {
			if srv.NodeID() == nodeID {
				expect = livenesspb.MembershipStatus_DECOMMISSIONED
				break
			}
		}
		require.Eventually(t, func() bool {
			liveness, ok := srv.NodeLiveness().(*liveness.NodeLiveness).GetLiveness(srv.NodeID())
			return ok && liveness.Membership == expect
		}, 5*time.Second, 100*time.Millisecond, "timed out waiting for node %v status %v", i, expect)
	}
}

// TestDecommissionEnqueueReplicas tests that a decommissioning node's replicas
// are proactively enqueued into their replicateQueues by the other nodes in the
// system.
func TestDecommissionEnqueueReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t) // can't handle 7-node clusters

	ctx := context.Background()
	enqueuedRangeIDs := make(chan roachpb.RangeID)
	tc := serverutils.StartCluster(t, 7, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					EnqueueReplicaInterceptor: func(
						queueName string, repl *kvserver.Replica,
					) {
						require.Equal(t, queueName, "replicate")
						enqueuedRangeIDs <- repl.RangeID
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	decommissionAndCheck := func(decommissioningSrvIdx int) {
		t.Logf("decommissioning n%d", tc.Target(decommissioningSrvIdx).NodeID)
		// Add a scratch range's replica to a node we will decommission.
		scratchKey := tc.ScratchRange(t)
		decommissioningSrv := tc.Server(decommissioningSrvIdx)
		tc.AddVotersOrFatal(t, scratchKey, tc.Target(decommissioningSrvIdx))

		adminClient := decommissioningSrv.GetAdminClient(t)
		decomNodeIDs := []roachpb.NodeID{tc.Server(decommissioningSrvIdx).NodeID()}
		_, err := adminClient.Decommission(
			ctx,
			&serverpb.DecommissionRequest{
				NodeIDs:          decomNodeIDs,
				TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONING,
			},
		)
		require.NoError(t, err)

		// Ensure that the scratch range's replica was proactively enqueued.
		require.Equal(t, <-enqueuedRangeIDs, tc.LookupRangeOrFatal(t, scratchKey).RangeID)

		// Check that the node was marked as decommissioning in each of the nodes'
		// decommissioningNodeMap. This needs to be wrapped in a SucceedsSoon to
		// deal with gossip propagation delays.
		testutils.SucceedsSoon(t, func() error {
			for i := 0; i < tc.NumServers(); i++ {
				srv := tc.Server(i)
				if _, exists := srv.DecommissioningNodeMap()[decommissioningSrv.NodeID()]; !exists {
					return errors.Newf("node %d not detected to be decommissioning", decommissioningSrv.NodeID())
				}
			}
			return nil
		})
	}

	decommissionAndCheck(2 /* decommissioningSrvIdx */)
	decommissionAndCheck(3 /* decommissioningSrvIdx */)
	decommissionAndCheck(5 /* decommissioningSrvIdx */)
}

func TestAdminDecommissionedOperations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "test uses timeouts, and race builds cause the timeouts to be exceeded")

	ctx := context.Background()
	tc := serverutils.StartCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual, // saves time
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(81590),
		},
	})
	defer tc.Stopper().Stop(ctx)

	// Configure drain to immediately cancel SQL queries and jobs to speed up the
	// test and avoid timeouts.
	serverutils.SetClusterSetting(t, tc, string(server.QueryShutdownTimeout.Name()), 0)
	serverutils.SetClusterSetting(t, tc, string(server.JobShutdownTimeout.Name()), 0)

	scratchKey := tc.ScratchRange(t)
	scratchRange := tc.LookupRangeOrFatal(t, scratchKey)
	require.Len(t, scratchRange.InternalReplicas, 1)
	require.Equal(t, tc.Server(0).NodeID(), scratchRange.InternalReplicas[0].NodeID)

	// Decommission server 1 and wait for it to lose cluster access.
	srv := tc.Server(0)
	decomSrv := tc.Server(1)
	for _, status := range []livenesspb.MembershipStatus{
		livenesspb.MembershipStatus_DECOMMISSIONING, livenesspb.MembershipStatus_DECOMMISSIONED,
	} {
		require.NoError(t, srv.Decommission(ctx, status, []roachpb.NodeID{decomSrv.NodeID()}))
	}

	testutils.SucceedsSoon(t, func() error {
		timeoutCtx, cancel := context.WithTimeout(ctx, testutils.DefaultSucceedsSoonDuration)
		defer cancel()
		_, err := decomSrv.DB().Scan(timeoutCtx, keys.LocalMax, keys.MaxKey, 0)
		if err == nil {
			return errors.New("expected error")
		}
		s, ok := status.FromError(errors.UnwrapAll(err))
		if ok && s.Code() == codes.PermissionDenied {
			return nil
		}
		return err
	})

	// Set up an admin client.
	adminClient := decomSrv.GetAdminClient(t)

	// Run some operations on the decommissioned node. The ones that require
	// access to the cluster should fail, other should succeed. We're mostly
	// concerned with making sure they return rather than hang due to internal
	// retries.
	testcases := []struct {
		name       string
		expectCode codes.Code
		op         func(context.Context, serverpb.AdminClient) error
	}{
		{"Cluster", codes.OK, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.Cluster(ctx, &serverpb.ClusterRequest{})
			return err
		}},
		{"Databases", codes.PermissionDenied, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.Databases(ctx, &serverpb.DatabasesRequest{})
			return err
		}},
		{"DatabaseDetails", codes.PermissionDenied, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.DatabaseDetails(ctx, &serverpb.DatabaseDetailsRequest{Database: "foo"})
			return err
		}},
		{"DataDistribution", codes.PermissionDenied, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.DataDistribution(ctx, &serverpb.DataDistributionRequest{})
			return err
		}},
		{"Decommission", codes.Internal, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.Decommission(ctx, &serverpb.DecommissionRequest{
				NodeIDs:          []roachpb.NodeID{srv.NodeID(), decomSrv.NodeID()},
				TargetMembership: livenesspb.MembershipStatus_DECOMMISSIONED,
			})
			return err
		}},
		{"DecommissionStatus", codes.PermissionDenied, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.DecommissionStatus(ctx, &serverpb.DecommissionStatusRequest{
				NodeIDs: []roachpb.NodeID{srv.NodeID(), decomSrv.NodeID()},
			})
			return err
		}},
		{"EnqueueRange", codes.PermissionDenied, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.EnqueueRange(ctx, &serverpb.EnqueueRangeRequest{
				RangeID: scratchRange.RangeID,
				Queue:   "replicaGC",
			})
			return err
		}},
		{"Events", codes.PermissionDenied, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.Events(ctx, &serverpb.EventsRequest{})
			return err
		}},
		{"Health", codes.OK, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.Health(ctx, &serverpb.HealthRequest{})
			return err
		}},
		{"Jobs", codes.PermissionDenied, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.Jobs(ctx, &serverpb.JobsRequest{})
			return err
		}},
		{"Liveness", codes.PermissionDenied, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.Liveness(ctx, &serverpb.LivenessRequest{})
			return err
		}},
		{"Locations", codes.PermissionDenied, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.Locations(ctx, &serverpb.LocationsRequest{})
			return err
		}},
		{"NonTableStats", codes.PermissionDenied, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.NonTableStats(ctx, &serverpb.NonTableStatsRequest{})
			return err
		}},
		{"QueryPlan", codes.OK, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.QueryPlan(ctx, &serverpb.QueryPlanRequest{Query: "SELECT 1"})
			return err
		}},
		{"RangeLog", codes.PermissionDenied, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.RangeLog(ctx, &serverpb.RangeLogRequest{})
			return err
		}},
		{"Settings", codes.OK, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.Settings(ctx, &serverpb.SettingsRequest{})
			return err
		}},
		{"TableStats", codes.PermissionDenied, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.TableStats(ctx, &serverpb.TableStatsRequest{Database: "foo", Table: "bar"})
			return err
		}},
		{"TableDetails", codes.PermissionDenied, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.TableDetails(ctx, &serverpb.TableDetailsRequest{Database: "foo", Table: "bar"})
			return err
		}},
		{"Users", codes.PermissionDenied, func(ctx context.Context, c serverpb.AdminClient) error {
			_, err := c.Users(ctx, &serverpb.UsersRequest{})
			return err
		}},
		// We drain at the end, since it may evict us.
		{"Drain", codes.PermissionDenied, func(ctx context.Context, c serverpb.AdminClient) error {
			stream, err := c.Drain(ctx, &serverpb.DrainRequest{DoDrain: true})
			if err != nil {
				return err
			}
			_, err = stream.Recv()
			return err
		}},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			testutils.SucceedsSoon(t, func() error {
				timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				err := tc.op(timeoutCtx, adminClient)
				if tc.expectCode == codes.OK {
					require.NoError(t, err)
					return nil
				}
				if err == nil {
					// This will cause SuccessWithin to retry.
					return errors.New("expected error, got no error")
				}
				s, ok := status.FromError(errors.UnwrapAll(err))
				if !ok {
					// Not a gRPC error.
					// This will cause SuccessWithin to retry.
					return err
				}
				if tc.expectCode == s.Code() {
					return nil
				}
				return err
			})
		})
	}
}

// checkNodeCheckResultReady is a helper function for validating that the
// results of a decommission pre-check on a single node show it is ready.
func checkNodeCheckResultReady(
	t *testing.T,
	nID roachpb.NodeID,
	replicaCount int64,
	checkResult serverpb.DecommissionPreCheckResponse_NodeCheckResult,
) {
	require.Equal(t, serverpb.DecommissionPreCheckResponse_NodeCheckResult{
		NodeID:                nID,
		DecommissionReadiness: serverpb.DecommissionPreCheckResponse_READY,
		ReplicaCount:          replicaCount,
		CheckedRanges:         nil,
	}, checkResult)
}

// checkRangeCheckResult is a helper function for validating a range error
// returned as part of a decommission pre-check.
func checkRangeCheckResult(
	t *testing.T,
	desc roachpb.RangeDescriptor,
	checkResult serverpb.DecommissionPreCheckResponse_RangeCheckResult,
	expectedAction string,
	expectedErrSubstr string,
	expectTraces bool,
) {
	passed := false
	defer func() {
		if !passed {
			t.Logf("failed checking %s", desc)
			if expectTraces {
				var traceBuilder strings.Builder
				for _, event := range checkResult.Events {
					fmt.Fprintf(&traceBuilder, "\n(%s) %s", event.Time, event.Message)
				}
				t.Logf("trace events: %s", traceBuilder.String())
			}
		}
	}()
	require.Equalf(t, desc.RangeID, checkResult.RangeID, "expected r%d, got r%d with error: \"%s\"",
		desc.RangeID, checkResult.RangeID, checkResult.Error)
	require.Equalf(t, expectedAction, checkResult.Action, "r%d expected action %s, got action %s with error: \"%s\"",
		desc.RangeID, expectedAction, checkResult.Action, checkResult.Error)
	require.NotEmptyf(t, checkResult.Error, "r%d expected non-empty error", checkResult.RangeID)
	if len(expectedErrSubstr) > 0 {
		require.Containsf(t, checkResult.Error, expectedErrSubstr, "r%d expected error with \"%s\", got error: \"%s\"",
			desc.RangeID, expectedErrSubstr, checkResult.Error)
	}
	if expectTraces {
		require.NotEmptyf(t, checkResult.Events, "r%d expected traces, got none with error: \"%s\"",
			checkResult.RangeID, checkResult.Error)
	} else {
		require.Emptyf(t, checkResult.Events, "r%d expected no traces with error: \"%s\"",
			checkResult.RangeID, checkResult.Error)
	}
	passed = true
}
