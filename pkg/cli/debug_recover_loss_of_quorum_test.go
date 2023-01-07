// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestCollectInfoFromMultipleStores performs basic sanity checks on replica info collection.
// This is done by running three node cluster with disk backed storage,
// stopping it and verifying content of collected replica info file.
// This check verifies that:
//
//	we successfully iterate requested stores,
//	data is written in expected location,
//	data contains info only about stores requested.
func TestCollectInfoFromMultipleStores(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	c := NewCLITest(TestCLIParams{
		NoServer: true,
	})
	defer c.Cleanup()

	tc := testcluster.NewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {StoreSpecs: []base.StoreSpec{{Path: dir + "/store-1"}}},
			1: {StoreSpecs: []base.StoreSpec{{Path: dir + "/store-2"}}},
			2: {StoreSpecs: []base.StoreSpec{{Path: dir + "/store-3"}}},
		},
	})
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)
	// Wait up-replication.
	require.NoError(t, tc.WaitForFullReplication())
	// Shutdown.
	tc.Stopper().Stop(ctx)

	replicaInfoFileName := dir + "/node-1.json"

	c.RunWithArgs([]string{"debug", "recover", "collect-info", "--store=" + dir + "/store-1",
		"--store=" + dir + "/store-2", replicaInfoFileName})

	replicas, err := readReplicaInfoData([]string{replicaInfoFileName})
	require.NoError(t, err, "failed to read generated replica info")
	stores := map[roachpb.StoreID]interface{}{}
	for _, r := range replicas.LocalInfo[0].Replicas {
		stores[r.StoreID] = struct{}{}
	}
	require.Equal(t, 2, len(stores), "collected replicas from stores")
}

// TestCollectInfoFromOnlineCluster verifies that given a test cluster with
// one stopped node, we can collect replica info and metadata from remaining
// nodes using an admin recovery call.
func TestCollectInfoFromOnlineCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	c := NewCLITest(TestCLIParams{
		NoServer: true,
	})
	defer c.Cleanup()

	tc := testcluster.NewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{{InMemory: true}},
			Insecure:   true,
		},
	})
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())
	tc.ToggleReplicateQueues(false)

	r := tc.ServerConn(0).QueryRow("select count(*) from crdb_internal.ranges_no_leases")
	var totalRanges int
	require.NoError(t, r.Scan(&totalRanges), "failed to query range count")

	tc.StopServer(0)
	replicaInfoFileName := dir + "/all-nodes.json"

	c.RunWithArgs([]string{
		"debug",
		"recover",
		"collect-info",
		"--insecure",
		"--host",
		tc.Server(2).ServingRPCAddr(),
		replicaInfoFileName,
	})

	replicas, err := readReplicaInfoData([]string{replicaInfoFileName})
	require.NoError(t, err, "failed to read generated replica info")
	stores := map[roachpb.StoreID]interface{}{}
	totalReplicas := 0
	for _, li := range replicas.LocalInfo {
		for _, r := range li.Replicas {
			stores[r.StoreID] = struct{}{}
		}
		totalReplicas += len(li.Replicas)
	}
	require.Equal(t, 2, len(stores), "collected replicas from stores")
	require.Equal(t, 2, len(replicas.LocalInfo), "collected info is not split by node")
	require.Equal(t, totalRanges*2, totalReplicas, "number of collected replicas")
	require.Equal(t, totalRanges, len(replicas.Descriptors),
		"number of collected descriptors from metadata")
}

// TestLossOfQuorumRecovery performs a sanity check on end to end recovery workflow.
// This test doesn't try to validate all possible test cases, but instead check that
// artifacts are correctly produced and overall cluster recovery could be performed
// where it would be completely broken otherwise.
func TestLossOfQuorumRecovery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDeadlock(t, "slow under deadlock")

	ctx := context.Background()
	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	c := NewCLITest(TestCLIParams{
		NoServer: true,
	})
	defer c.Cleanup()

	// Test cluster contains 3 nodes that we would turn into a single node
	// cluster using loss of quorum recovery. After it is stopped, single node
	// would not be able to progress, but we will apply recovery procedure and
	// mark on replicas on node 1 as designated survivors. After that, starting
	// single node should succeed.
	tcBefore := testcluster.NewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {StoreSpecs: []base.StoreSpec{{Path: dir + "/store-1"}}},
		},
	})
	tcBefore.Start(t)
	s := sqlutils.MakeSQLRunner(tcBefore.Conns[0])
	s.Exec(t, "set cluster setting cluster.organization='remove dead replicas test'")
	defer tcBefore.Stopper().Stop(ctx)

	// We use scratch range to test special case for pending update on the
	// descriptor which has to be cleaned up before recovery could proceed.
	// For that we'll ensure it is not empty and then put an intent. After
	// recovery, we'll check that the range is still accessible for writes as
	// normal.
	sk := tcBefore.ScratchRange(t)
	require.NoError(t,
		tcBefore.Server(0).DB().Put(ctx, testutils.MakeKey(sk, []byte{1}), "value"),
		"failed to write value to scratch range")

	createIntentOnRangeDescriptor(ctx, t, tcBefore, sk)

	node1ID := tcBefore.Servers[0].NodeID()
	// Now that stores are prepared and replicated we can shut down cluster
	// and perform store manipulations.
	tcBefore.Stopper().Stop(ctx)

	server1StoreDir := dir + "/store-1"
	replicaInfoFileName := dir + "/node-1.json"
	c.RunWithArgs(
		[]string{"debug", "recover", "collect-info", "--store=" + server1StoreDir,
			replicaInfoFileName})

	// Generate recovery plan and try to verify that plan file was generated and contains
	// meaningful data. This is not strictly necessary for verifying end-to-end flow, but
	// having assertions on generated data helps to identify which stage of pipeline broke
	// if test fails.
	planFile := dir + "/recovery-plan.json"
	out, err := c.RunWithCaptureArgs(
		[]string{"debug", "recover", "make-plan", "--confirm=y", "--plan=" + planFile,
			replicaInfoFileName})
	require.NoError(t, err, "failed to run make-plan")
	require.Contains(t, out, fmt.Sprintf("- node n%d", node1ID),
		"planner didn't provide correct apply instructions")
	require.FileExists(t, planFile, "generated plan file")
	planFileContent, err := os.ReadFile(planFile)
	require.NoError(t, err, "test infra failed, can't open created plan file")
	plan := loqrecoverypb.ReplicaUpdatePlan{}
	jsonpb := protoutil.JSONPb{}
	require.NoError(t, jsonpb.Unmarshal(planFileContent, &plan),
		"failed to deserialize replica recovery plan")
	require.NotEmpty(t, plan.Updates, "resulting plan contains no updates")

	out, err = c.RunWithCaptureArgs(
		[]string{"debug", "recover", "apply-plan", "--confirm=y", "--store=" + server1StoreDir,
			planFile})
	require.NoError(t, err, "failed to run apply plan")
	// Check that there were at least one mention of replica being promoted.
	require.Contains(t, out, "will be updated", "no replica updated were recorded")
	require.Contains(t, out, fmt.Sprintf("Updated store(s): s%d", node1ID),
		"apply plan was not executed on requested node")

	tcAfter := testcluster.NewTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {StoreSpecs: []base.StoreSpec{{Path: dir + "/store-1"}}},
		},
	})
	// NB: If recovery is not performed, new cluster will just hang on startup.
	// This is caused by liveness range becoming unavailable and preventing any
	// progress. So it is likely that test will timeout if basic workflow fails.
	tcAfter.Start(t)
	defer tcAfter.Stopper().Stop(ctx)

	// In the new cluster, we will still have nodes 2 and 3 remaining from the first
	// attempt. That would increase number of replicas on system ranges to 5 and we
	// would not be able to upreplicate properly. So we need to decommission old nodes
	// first before proceeding.
	grpcConn, err := tcAfter.Server(0).RPCContext().GRPCDialNode(tcAfter.Server(0).ServingRPCAddr(), tcAfter.Server(0).NodeID(), rpc.DefaultClass).Connect(ctx)
	require.NoError(t, err, "Failed to create test cluster after recovery")
	adminClient := serverpb.NewAdminClient(grpcConn)

	require.NoError(t, runDecommissionNodeImpl(
		ctx, adminClient, nodeDecommissionWaitNone, nodeDecommissionChecksSkip, false,
		[]roachpb.NodeID{roachpb.NodeID(2), roachpb.NodeID(3)}, tcAfter.Server(0).NodeID()),
		"Failed to decommission removed nodes")

	for i := 0; i < len(tcAfter.Servers); i++ {
		require.NoError(t, tcAfter.Servers[i].Stores().VisitStores(func(store *kvserver.Store) error {
			store.SetReplicateQueueActive(true)
			return nil
		}), "Failed to activate replication queue")
	}
	require.NoError(t, err, tcAfter.WaitForFullReplication(), "Failed to perform full replication")

	for i := 0; i < len(tcAfter.Servers); i++ {
		require.NoError(t, tcAfter.Servers[i].Stores().VisitStores(func(store *kvserver.Store) error {
			return store.ForceConsistencyQueueProcess()
		}), "Failed to force replicas to consistency queue")
	}

	// As a validation step we will just pick one range and get its replicas to see
	// if they were up-replicated to the new nodes.
	s = sqlutils.MakeSQLRunner(tcAfter.Conns[0])
	r := s.QueryRow(t, "select replicas from crdb_internal.ranges limit 1")
	var replicas string
	r.Scan(&replicas)
	require.Equal(t, "{1,4,5}", replicas, "Replicas after loss of quorum recovery")

	// Validate that rangelog is updated by recovery records after cluster restarts.
	testutils.SucceedsSoon(t, func() error {
		r := s.QueryRow(t,
			`select count(*) from system.rangelog where "eventType" = 'unsafe_quorum_recovery'`)
		var recoveries int
		r.Scan(&recoveries)
		if recoveries != len(plan.Updates) {
			return errors.Errorf("found %d recovery events while expecting %d", recoveries,
				len(plan.Updates))
		}
		return nil
	})

	// We were using scratch range to test cleanup of pending transaction on
	// rangedescriptor key. We want to verify that after recovery, range is still
	// writable e.g. recovery succeeded.
	require.NoError(t,
		tcAfter.Server(0).DB().Put(ctx, testutils.MakeKey(sk, []byte{1}), "value2"),
		"failed to write value to scratch range after recovery")
}

func createIntentOnRangeDescriptor(
	ctx context.Context, t *testing.T, tcBefore *testcluster.TestCluster, sk roachpb.Key,
) {
	txn := kv.NewTxn(ctx, tcBefore.Servers[0].DB(), 1)
	var desc roachpb.RangeDescriptor
	// Pick one of the predefined split points.
	rdKey := keys.RangeDescriptorKey(roachpb.RKey(sk))
	if err := txn.GetProto(ctx, rdKey, &desc); err != nil {
		t.Fatal(err)
	}
	desc.NextReplicaID++
	if err := txn.Put(ctx, rdKey, &desc); err != nil {
		t.Fatal(err)
	}

	// At this point the intent has been written to Pebble but this
	// write was not synced (only the raft log append was synced). We
	// need to force another sync, but we're far from the storage
	// layer here so the easiest thing to do is simply perform a
	// second write. This will force the first write to be persisted
	// to disk (the second write may or may not make it to disk due to
	// timing).
	desc.NextReplicaID++
	if err := txn.Put(ctx, rdKey, &desc); err != nil {
		t.Fatal(err)
	}
}

// TestJsonSerialization verifies that all fields serialized in JSON could be
// read back. This specific test addresses issues where default naming scheme
// may not work in combination with other tags correctly. e.g. repeated used
// with omitempty seem to use camelcase unless explicitly specified.
func TestJsonSerialization(t *testing.T) {
	defer leaktest.AfterTest(t)()

	nr := loqrecoverypb.ClusterReplicaInfo{
		ClusterID: "id1",
		LocalInfo: []loqrecoverypb.NodeReplicaInfo{
			{
				Replicas: []loqrecoverypb.ReplicaInfo{
					{
						NodeID:  1,
						StoreID: 2,
						Desc: roachpb.RangeDescriptor{
							RangeID:  3,
							StartKey: roachpb.RKey(keys.MetaMin),
							EndKey:   roachpb.RKey(keys.MetaMax),
							InternalReplicas: []roachpb.ReplicaDescriptor{
								{
									NodeID:    1,
									StoreID:   2,
									ReplicaID: 3,
									Type:      roachpb.VOTER_INCOMING,
								},
							},
							NextReplicaID: 4,
							Generation:    7,
						},
						RaftAppliedIndex:   13,
						RaftCommittedIndex: 19,
						RaftLogDescriptorChanges: []loqrecoverypb.DescriptorChangeInfo{
							{
								ChangeType: 1,
								Desc:       &roachpb.RangeDescriptor{},
								OtherDesc:  &roachpb.RangeDescriptor{},
							},
						},
					},
				},
			},
		},
	}
	jsonpb := protoutil.JSONPb{Indent: "  "}
	data, err := jsonpb.Marshal(&nr)
	require.NoError(t, err)

	var crFromJSON loqrecoverypb.ClusterReplicaInfo
	require.NoError(t, jsonpb.Unmarshal(data, &crFromJSON))
	require.Equal(t, nr, crFromJSON, "objects before and after serialization")
}
