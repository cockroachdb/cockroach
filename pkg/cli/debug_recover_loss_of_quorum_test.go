// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/listenerutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
		ServerArgs: base.TestServerArgs{
			// This logic is specific to the storage layer.
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
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
	require.Equal(t, clusterversion.Latest.Version(), replicas.Version,
		"collected version info from stores")
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
			// This logic is specific to the storage layer.
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
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
		tc.Server(2).AdvRPCAddr(),
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
	require.Equal(t, clusterversion.Latest.Version(), replicas.Version,
		"collected version info from stores")
}

// TestLossOfQuorumRecovery performs a sanity check on end to end recovery workflow.
// This test doesn't try to validate all possible test cases, but instead check that
// artifacts are correctly produced and overall cluster recovery could be performed
// where it would be completely broken otherwise.
func TestLossOfQuorumRecovery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDeadlock(t, "slow under deadlock")
	skip.UnderStress(t, "slow under stress")

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
		ServerArgs: base.TestServerArgs{
			// This logic is specific to the storage layer.
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {StoreSpecs: []base.StoreSpec{{Path: dir + "/store-1"}}},
		},
	})
	tcBefore.Start(t)
	s := sqlutils.MakeSQLRunner(tcBefore.Conns[0])
	s.Exec(t, "SET CLUSTER SETTING cluster.organization = 'remove dead replicas test'")
	defer tcBefore.Stopper().Stop(ctx)

	// We use scratch range to test special case for pending update on the
	// descriptor which has to be cleaned up before recovery could proceed.
	// For that we'll ensure it is not empty and then put an intent. After
	// recovery, we'll check that the range is still accessible for writes as
	// normal.
	sk := tcBefore.ScratchRange(t)
	// The LOQ tooling does not work when a node fails during a split/merge
	// operation. Make sure that splitting the scratch range fully completes.
	require.NoError(t, tcBefore.WaitForSplitAndInitialization(sk))
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
	require.Contains(t, out, "will be updated", "no replica updates were recorded")
	require.Contains(t, out, fmt.Sprintf("Updated store(s): s%d", node1ID),
		"apply plan was not executed on requested node")

	tcAfter := testcluster.NewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// This logic is specific to the storage layer.
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
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
	adminClient := tcAfter.Server(0).GetAdminClient(t)

	require.NoError(t, runDecommissionNodeImpl(
		ctx, adminClient, nodeDecommissionWaitNone, nodeDecommissionChecksSkip, false,
		[]roachpb.NodeID{roachpb.NodeID(2), roachpb.NodeID(3)}, tcAfter.Server(0).NodeID()),
		"Failed to decommission removed nodes")

	for i := 0; i < len(tcAfter.Servers); i++ {
		require.NoError(t, tcAfter.Servers[i].GetStores().(*kvserver.Stores).VisitStores(func(store *kvserver.Store) error {
			store.TestingSetReplicateQueueActive(true)
			return nil
		}), "Failed to activate replication queue")
	}
	require.NoError(t, tcAfter.WaitForZoneConfigPropagation(),
		"Failed to ensure zone configs are propagated")
	require.NoError(t, tcAfter.WaitForFullReplication(), "Failed to perform full replication")

	for i := 0; i < len(tcAfter.Servers); i++ {
		require.NoError(t, tcAfter.Servers[i].GetStores().(*kvserver.Stores).VisitStores(func(store *kvserver.Store) error {
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

// TestStageVersionCheck verifies that we can force plan with different internal
// version onto cluster. To do this, we create a plan with internal version
// above current but matching major and minor. Then we check that staging fails
// and that force flag will update plan version to match local node.
func TestStageVersionCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDeadlock(t, "slow under deadlock")

	ctx := context.Background()
	_, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	c := NewCLITest(TestCLIParams{
		NoServer: true,
	})
	defer c.Cleanup()

	listenerReg := listenerutil.NewListenerRegistry()
	defer listenerReg.Close()

	storeReg := fs.NewStickyRegistry()
	tc := testcluster.NewTestCluster(t, 4, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// This logic is specific to the storage layer.
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
		ReplicationMode: base.ReplicationManual,
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						StickyVFSRegistry: storeReg,
					},
				},
				StoreSpecs: []base.StoreSpec{
					{InMemory: true, StickyVFSID: "1"},
				},
			},
		},
		ReusableListenerReg: listenerReg,
	})
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)
	tc.StopServer(3)

	adminClient := tc.Server(0).GetAdminClient(t)
	v := clusterversion.Latest.Version()
	v.Internal++
	// To avoid crafting real replicas we use StaleLeaseholderNodeIDs to force
	// node to stage plan for verification.
	p := loqrecoverypb.ReplicaUpdatePlan{
		PlanID:                  uuid.MakeV4(),
		Version:                 v,
		ClusterID:               tc.Server(0).StorageClusterID().String(),
		DecommissionedNodeIDs:   []roachpb.NodeID{4},
		StaleLeaseholderNodeIDs: []roachpb.NodeID{1},
	}
	// Attempts to stage plan with different internal version must fail.
	_, err := adminClient.RecoveryStagePlan(ctx, &serverpb.RecoveryStagePlanRequest{
		Plan:                      &p,
		AllNodes:                  true,
		ForcePlan:                 false,
		ForceLocalInternalVersion: false,
		MaxConcurrency:            -1, // no limit
	})
	require.ErrorContains(t, err, "doesn't match cluster active version")
	// Enable "stuck upgrade bypass" to stage plan on the cluster.
	_, err = adminClient.RecoveryStagePlan(ctx, &serverpb.RecoveryStagePlanRequest{
		Plan:                      &p,
		AllNodes:                  true,
		ForcePlan:                 false,
		ForceLocalInternalVersion: true,
		MaxConcurrency:            -1, // no limit
	})
	require.NoError(t, err, "force local must fix incorrect version")
	// Check that stored plan has version matching cluster version.
	ps := loqrecovery.NewPlanStore("", storeReg.Get("1"))
	p, ok, err := ps.LoadPlan()
	require.NoError(t, err, "failed to read node 0 plan")
	require.True(t, ok, "plan was not staged")
	require.Equal(t, clusterversion.Latest.Version(), p.Version,
		"plan version was not updated")
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

func TestHalfOnlineLossOfQuorumRecovery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDeadlock(t, "slow under deadlock")
	skip.UnderStress(t, "fails under stress and leader leases")

	ctx := context.Background()
	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	c := NewCLITest(TestCLIParams{
		NoServer: true,
	})
	defer c.Cleanup()

	listenerReg := listenerutil.NewListenerRegistry()
	defer listenerReg.Close()

	st := cluster.MakeTestingClusterSettings()
	// Test cluster contains 3 nodes that we would turn into a single node
	// cluster using loss of quorum recovery. To do that, we will terminate
	// two nodes and run recovery on remaining one. Restarting node should
	// bring it back to healthy (but underreplicated) state.
	// Note that we inject reusable listeners into all nodes to prevent tests
	// running in parallel from taking over ports of stopped nodes and responding
	// to gateway node with errors.
	// TODO(oleg): Make test run with 7 nodes to exercise cases where multiple
	// replicas survive. Current startup and allocator behaviour would make
	// this test flaky.
	var failCount atomic.Int64
	sa := make(map[int]base.TestServerArgs)
	for i := 0; i < 3; i++ {
		sa[i] = base.TestServerArgs{
			Settings: st,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyVFSRegistry: fs.NewStickyRegistry(),
				},
				LOQRecovery: &loqrecovery.TestingKnobs{
					MetadataScanTimeout: 15 * time.Second,
					ForwardReplicaFilter: func(
						response *serverpb.RecoveryCollectLocalReplicaInfoResponse,
					) error {
						// Artificially add an error that would cause the server to retry
						// the replica info for node 1. Note that we only add an error after
						// we return the first replica info for that node.
						// This helps in verifying that the replica info get discarded and
						// the node is revisited (based on the logs).
						if response != nil && response.ReplicaInfo.NodeID == 1 &&
							failCount.Add(1) < 3 && failCount.Load() > 1 {
							return errors.New("rpc stream stopped")
						}
						return nil
					},
				},
			},
			StoreSpecs: []base.StoreSpec{
				{
					InMemory: true,
				},
			},
		}
	}
	tc := testcluster.NewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// This logic is specific to the storage layer.
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
		ReusableListenerReg: listenerReg,
		ServerArgsPerNode:   sa,
	})
	tc.Start(t)
	s := sqlutils.MakeSQLRunner(tc.Conns[0])
	s.Exec(t, "SET CLUSTER SETTING cluster.organization = 'remove dead replicas test'")
	defer tc.Stopper().Stop(ctx)

	// We use scratch range to test special case for pending update on the
	// descriptor which has to be cleaned up before recovery could proceed.
	// For that we'll ensure it is not empty and then put an intent. After
	// recovery, we'll check that the range is still accessible for writes as
	// normal.
	sk := tc.ScratchRange(t)
	require.NoError(t,
		tc.Server(0).DB().Put(ctx, testutils.MakeKey(sk, []byte{1}), "value"),
		"failed to write value to scratch range")

	createIntentOnRangeDescriptor(ctx, t, tc, sk)

	node1ID := tc.Servers[0].NodeID()

	// Now that stores are prepared and replicated we can shut down cluster
	// and perform store manipulations.
	tc.StopServer(1)
	tc.StopServer(2)

	// Generate recovery plan and try to verify that plan file was generated and contains
	// meaningful data. This is not strictly necessary for verifying end-to-end flow, but
	// having assertions on generated data helps to identify which stage of pipeline broke
	// if test fails.
	planFile := dir + "/recovery-plan.json"
	out, err := c.RunWithCaptureArgs(
		[]string{
			"debug",
			"recover",
			"make-plan",
			"--confirm=y",
			"--certs-dir=test_certs",
			"--host=" + tc.Server(0).AdvRPCAddr(),
			"--plan=" + planFile,
		})
	require.NoError(t, err, "failed to run make-plan")
	require.Contains(t, out, "Started getting replica info for node_id:1",
		"planner didn't log the visited nodes properly")
	require.Contains(t, out, "Discarding replica info for node_id:1",
		"planner didn't log the discarded nodes properly")
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
		[]string{
			"debug", "recover", "apply-plan",
			"--certs-dir=test_certs",
			"--host=" + tc.Server(0).AdvRPCAddr(),
			"--confirm=y", planFile,
		})
	require.NoError(t, err, "failed to run apply plan")
	// Check that there were at least one mention of replica being promoted.
	require.Contains(t, out, "updating replica", "no replica updates were recorded")
	require.Contains(t, out,
		fmt.Sprintf("Plan staged. To complete recovery restart nodes n%d.", node1ID),
		"apply plan failed to stage on expected nodes")

	// Verify plan is staged on nodes
	out, err = c.RunWithCaptureArgs(
		[]string{
			"debug", "recover", "verify",
			"--certs-dir=test_certs",
			"--host=" + tc.Server(0).AdvRPCAddr(),
			planFile,
		})
	require.NoError(t, err, "failed to run verify plan")
	require.Contains(t, out, "ERROR: loss of quorum recovery is not finished yet")

	tc.StopServer(0)

	// NB: If recovery is not performed, server will just hang on startup.
	// This is caused by liveness range becoming unavailable and preventing any
	// progress. So it is likely that test will timeout if basic workflow fails.
	require.NoError(t, tc.RestartServer(0), "restart failed")
	s = sqlutils.MakeSQLRunner(tc.Conns[0])

	// Verifying that post start cleanup performed node decommissioning that
	// prevents old nodes from rejoining.
	ac := tc.GetAdminClient(t, 0)
	testutils.SucceedsSoon(t, func() error {
		dr, err := ac.DecommissionStatus(ctx,
			&serverpb.DecommissionStatusRequest{NodeIDs: []roachpb.NodeID{2, 3}})
		if err != nil {
			return err
		}
		for _, s := range dr.Status {
			if s.Membership != livenesspb.MembershipStatus_DECOMMISSIONED {
				return errors.Newf("expecting n%d to be decommissioned", s.NodeID)
			}
		}
		return nil
	})

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

	// Verify recovery complete.
	out, err = c.RunWithCaptureArgs(
		[]string{
			"debug", "recover", "verify",
			"--certs-dir=test_certs",
			"--host=" + tc.Server(0).AdvRPCAddr(),
			planFile,
		})
	require.NoError(t, err, "failed to run verify plan")
	require.Contains(t, out, "Loss of quorum recovery is complete.")

	// We were using scratch range to test cleanup of pending transaction on
	// rangedescriptor key. We want to verify that after recovery, range is still
	// writable e.g. recovery succeeded.
	require.NoError(t,
		tc.Server(0).DB().Put(ctx, testutils.MakeKey(sk, []byte{1}), "value2"),
		"failed to write value to scratch range after recovery")

	// Finally split scratch range to ensure metadata ranges are recovered.
	_, _, err = tc.Server(0).SplitRange(testutils.MakeKey(sk, []byte{42}))
	require.NoError(t, err, "failed to split range after recovery")
}

func TestUpdatePlanVsClusterDiff(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var empty uuid.UUID
	planID, _ := uuid.FromString("123e4567-e89b-12d3-a456-426614174000")
	otherPlanID, _ := uuid.FromString("123e4567-e89b-12d3-a456-426614174001")
	applyTime, _ := time.Parse(time.RFC3339, "2023-01-24T10:30:00Z")

	status := func(id roachpb.NodeID, pending, applied uuid.UUID, err string) loqrecoverypb.NodeRecoveryStatus {
		s := loqrecoverypb.NodeRecoveryStatus{
			NodeID: id,
		}
		if !pending.Equal(empty) {
			s.PendingPlanID = &pending
		}
		if !applied.Equal(empty) {
			s.AppliedPlanID = &applied
			s.ApplyTimestamp = &applyTime
		}
		s.Error = err
		return s
	}

	for _, d := range []struct {
		name         string
		updatedNodes []int
		staleLeases  []int
		status       []loqrecoverypb.NodeRecoveryStatus
		pending      int
		errors       int
		report       []string
	}{
		{
			name:         "after staging",
			updatedNodes: []int{1, 2},
			staleLeases:  []int{3},
			status: []loqrecoverypb.NodeRecoveryStatus{
				status(1, planID, empty, ""),
				status(2, planID, empty, ""),
				status(3, planID, empty, ""),
			},
			pending: 3,
			report: []string{
				" plan application pending on node n1",
				" plan application pending on node n2",
				" plan application pending on node n3",
			},
		},
		{
			name:         "partially applied",
			updatedNodes: []int{1, 2, 3},
			status: []loqrecoverypb.NodeRecoveryStatus{
				status(1, planID, empty, ""),
				status(2, empty, planID, ""),
				status(3, planID, empty, ""),
			},
			pending: 2,
			report: []string{
				" plan application pending on node n1",
				" plan applied successfully on node n2",
				" plan application pending on node n3",
			},
		},
		{
			name:         "fully applied",
			updatedNodes: []int{1, 2, 3},
			status: []loqrecoverypb.NodeRecoveryStatus{
				status(1, empty, planID, ""),
				status(2, empty, planID, ""),
				status(3, empty, planID, ""),
			},
			report: []string{
				" plan applied successfully on node n1",
				" plan applied successfully on node n2",
				" plan applied successfully on node n3",
			},
		},
		{
			name:         "staging lost no node",
			updatedNodes: []int{1, 2, 3},
			status: []loqrecoverypb.NodeRecoveryStatus{
				status(1, planID, empty, ""),
				status(3, planID, empty, ""),
			},
			pending: 2,
			errors:  1,
			report: []string{
				" plan application pending on node n1",
				" plan application pending on node n3",
				" failed to find node n2 where plan must be staged",
			},
		},
		{
			name:         "staging lost no plan",
			updatedNodes: []int{1, 2},
			staleLeases:  []int{3},
			status: []loqrecoverypb.NodeRecoveryStatus{
				status(1, planID, empty, ""),
				status(2, planID, empty, ""),
				status(3, empty, empty, ""),
			},
			pending: 2,
			errors:  1,
			report: []string{
				" plan application pending on node n1",
				" plan application pending on node n2",
				" failed to find staged plan on node n3",
			},
		},
		{
			name:         "partial failure",
			updatedNodes: []int{1, 2, 3},
			status: []loqrecoverypb.NodeRecoveryStatus{
				status(1, planID, empty, ""),
				status(2, empty, planID, "found stale replica"),
				status(3, planID, empty, ""),
			},
			pending: 2,
			errors:  1,
			report: []string{
				" plan application pending on node n1",
				" plan application failed on node n2: found stale replica",
				" plan application pending on node n3",
			},
		},
		{
			name: "no plan",
			status: []loqrecoverypb.NodeRecoveryStatus{
				status(1, planID, empty, ""),
				status(2, empty, planID, "found stale replica"),
				status(3, empty, otherPlanID, ""),
			},
			report: []string{
				" node n1 staged plan: 123e4567-e89b-12d3-a456-426614174000",
				" node n2 failed to apply plan 123e4567-e89b-12d3-a456-426614174000: found stale replica",
				" node n3 applied plan: 123e4567-e89b-12d3-a456-426614174001 at 2023-01-24 10:30:00 +0000 UTC",
			},
		},
		{
			name:         "wrong plan",
			updatedNodes: []int{1, 2},
			status: []loqrecoverypb.NodeRecoveryStatus{
				status(1, planID, empty, ""),
				status(2, otherPlanID, empty, ""),
				status(3, otherPlanID, empty, ""),
			},
			pending: 1,
			errors:  2,
			report: []string{
				" plan application pending on node n1",
				" unexpected staged plan 123e4567-e89b-12d3-a456-426614174001 on node n2",
				" unexpected staged plan 123e4567-e89b-12d3-a456-426614174001 on node n3",
			},
		},
	} {
		t.Run(d.name, func(t *testing.T) {
			plan := loqrecoverypb.ReplicaUpdatePlan{
				PlanID: planID,
			}
			// Plan will contain single replica update for each requested node.
			rangeSeq := 1
			for _, id := range d.updatedNodes {
				plan.Updates = append(plan.Updates, loqrecoverypb.ReplicaUpdate{
					RangeID:      roachpb.RangeID(rangeSeq),
					StartKey:     nil,
					OldReplicaID: roachpb.ReplicaID(1),
					NewReplica: roachpb.ReplicaDescriptor{
						NodeID:    roachpb.NodeID(id),
						StoreID:   roachpb.StoreID(id),
						ReplicaID: roachpb.ReplicaID(rangeSeq + 17),
					},
					NextReplicaID: roachpb.ReplicaID(rangeSeq + 18),
				})
			}
			for _, id := range d.staleLeases {
				plan.StaleLeaseholderNodeIDs = append(plan.StaleLeaseholderNodeIDs, roachpb.NodeID(id))
			}

			diff := diffPlanWithNodeStatus(plan, d.status)
			require.Equal(t, d.pending, diff.pending, "number of pending changes")
			require.Equal(t, d.errors, diff.errors, "number of node errors")
			if d.report != nil {
				require.Equal(t, len(d.report), len(diff.report), "number of lines in diff")
				for i := range d.report {
					require.Equal(t, d.report[i], diff.report[i], "wrong line %d of report", i)
				}
			}
		})
	}
}

func TestTruncateKeyOutput(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, d := range []struct {
		len    uint
		result string
	}{
		{
			len:    13,
			result: "/System/No...",
		},
		{
			len:    30,
			result: "/System/NodeLiveness",
		},
		{
			len:    3,
			result: "/Sy",
		},
		{
			len:    4,
			result: "/...",
		},
	} {
		t.Run("", func(t *testing.T) {
			helper := outputFormatHelper{
				maxPrintedKeyLength: d.len,
			}
			require.Equal(t, d.result, helper.formatKey(keys.NodeLivenessPrefix))
		})
	}
}

func TestTruncateSpanOutput(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, d := range []struct {
		len    uint
		result string
	}{
		{
			len:    30,
			result: "/System/{NodeLiveness-Syste...",
		},
		{
			len:    90,
			result: "/System/{NodeLiveness-SystemSpanConfigKeys}",
		},
		{
			len:    3,
			result: "/Sy",
		},
		{
			len:    4,
			result: "/...",
		},
	} {
		t.Run("", func(t *testing.T) {
			helper := outputFormatHelper{
				maxPrintedKeyLength: d.len,
			}
			require.Equal(t, d.result, helper.formatSpan(roachpb.Span{
				Key:    keys.NodeLivenessPrefix,
				EndKey: keys.SystemSpanConfigPrefix,
			}))
		})
	}
}
