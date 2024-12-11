// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package loqrecovery_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/listenerutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type clusterInfoCounters struct {
	nodes, stores, replicas, descriptors int
}

func TestReplicaCollection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	skip.UnderDeadlock(t, "occasionally flakes")

	// This test stops cluster servers. Use "reusable" listeners, otherwise the
	// ports can be reused by other test clusters, and we may accidentally connect
	// to a wrong node.
	// TODO(pav-kv): force all tests calling StopServer to use sticky listeners.
	listenerReg := listenerutil.NewListenerRegistry()
	defer listenerReg.Close()
	tc := testcluster.NewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{{InMemory: true}},
			Insecure:   true,
			Knobs: base.TestingKnobs{
				LOQRecovery: &loqrecovery.TestingKnobs{
					MetadataScanTimeout: 15 * time.Second,
				},
			},
		},
		ReusableListenerReg: listenerReg,
	})
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())
	tc.ToggleReplicateQueues(false)

	r := tc.ServerConn(0).QueryRow("select count(*) from crdb_internal.ranges_no_leases")
	var totalRanges int
	require.NoError(t, r.Scan(&totalRanges), "failed to query range count")
	adm := tc.GetAdminClient(t, 2)

	// Collect and assert replica metadata. For expectMeta case we sometimes have
	// meta and sometimes doesn't depending on which node holds the lease.
	// We just ignore descriptor counts if we are not expecting meta.
	assertReplicas := func(liveNodes int, expectRangeMeta bool) {
		var replicas loqrecoverypb.ClusterReplicaInfo
		var stats loqrecovery.CollectionStats

		replicas, stats, err := loqrecovery.CollectRemoteReplicaInfo(ctx, adm,
			-1 /* maxConcurrency */, nil /* logOutput */)
		require.NoError(t, err, "failed to retrieve replica info")

		// Check counters on retrieved replica info.
		cnt := getInfoCounters(replicas)
		require.Equal(t, liveNodes, cnt.stores, "collected replicas from stores")
		require.Equal(t, liveNodes, cnt.nodes, "collected replicas from nodes")
		if expectRangeMeta {
			require.Equal(t, totalRanges, cnt.descriptors,
				"number of collected descriptors from metadata")
		}
		require.Equal(t, totalRanges*liveNodes, cnt.replicas, "number of collected replicas")
		// Check stats counters as well.
		require.Equal(t, liveNodes, stats.Nodes, "node counter stats")
		require.Equal(t, liveNodes, stats.Stores, "store counter stats")
		if expectRangeMeta {
			require.Equal(t, totalRanges, stats.Descriptors, "range descriptor counter stats")
		}
		require.NotEqual(t, replicas.ClusterID, uuid.UUID{}.String(), "cluster UUID must not be empty")
		require.Equal(t, replicas.Version,
			clusterversion.Latest.Version(),
			"replica info version must match current binary version")
	}

	tc.StopServer(0)
	assertReplicas(2, true)
	tc.StopServer(1)
	assertReplicas(1, false)
}

// TestStreamRestart verifies that if connection is dropped mid way through
// replica stream, it would be handled correctly with a stream restart that
// allows caller to rewind back partial replica data and receive consistent
// stream of replcia infos.
func TestStreamRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var failCount atomic.Int64
	tc := testcluster.NewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{{InMemory: true}},
			Insecure:   true,
			Knobs: base.TestingKnobs{
				LOQRecovery: &loqrecovery.TestingKnobs{
					MetadataScanTimeout: 15 * time.Second,
					ForwardReplicaFilter: func(response *serverpb.RecoveryCollectLocalReplicaInfoResponse) error {
						if response.ReplicaInfo.NodeID == 2 && response.ReplicaInfo.Desc.RangeID == 14 && failCount.Add(1) < 3 {
							return errors.New("rpc stream stopped")
						}
						return nil
					},
				},
			},
		},
	})
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())
	tc.ToggleReplicateQueues(false)

	r := tc.ServerConn(0).QueryRow("select count(*) from crdb_internal.ranges_no_leases")
	var totalRanges int
	require.NoError(t, r.Scan(&totalRanges), "failed to query range count")
	adm := tc.GetAdminClient(t, 2)

	assertReplicas := func(liveNodes int) {
		var replicas loqrecoverypb.ClusterReplicaInfo
		var stats loqrecovery.CollectionStats

		replicas, stats, err := loqrecovery.CollectRemoteReplicaInfo(ctx, adm,
			-1 /* maxConcurrency */, nil /* logOutput */)
		require.NoError(t, err, "failed to retrieve replica info")

		// Check counters on retrieved replica info.
		cnt := getInfoCounters(replicas)
		require.Equal(t, liveNodes, cnt.stores, "collected replicas from stores")
		require.Equal(t, liveNodes, cnt.nodes, "collected replicas from nodes")
		require.Equal(t, totalRanges, cnt.descriptors,
			"number of collected descriptors from metadata")
		require.Equal(t, totalRanges*liveNodes, cnt.replicas,
			"number of collected replicas")
		// Check stats counters as well.
		require.Equal(t, liveNodes, stats.Nodes, "node counter stats")
		require.Equal(t, liveNodes, stats.Stores, "store counter stats")
		require.Equal(t, totalRanges, stats.Descriptors, "range descriptor counter stats")
	}

	assertReplicas(3)
}

func getInfoCounters(info loqrecoverypb.ClusterReplicaInfo) clusterInfoCounters {
	stores := map[roachpb.StoreID]interface{}{}
	nodes := map[roachpb.NodeID]interface{}{}
	totalReplicas := 0
	for _, nr := range info.LocalInfo {
		for _, r := range nr.Replicas {
			stores[r.StoreID] = struct{}{}
			nodes[r.NodeID] = struct{}{}
		}
		totalReplicas += len(nr.Replicas)
	}
	return clusterInfoCounters{
		nodes:       len(nodes),
		stores:      len(stores),
		replicas:    totalReplicas,
		descriptors: len(info.Descriptors),
	}
}

func TestGetPlanStagingState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc, _, planStores := prepTestCluster(ctx, t, 3)
	defer tc.Stopper().Stop(ctx)

	adm := tc.GetAdminClient(t, 0)

	resp, err := adm.RecoveryVerify(ctx, &serverpb.RecoveryVerifyRequest{MaxConcurrency: -1 /* no limit */})
	require.NoError(t, err)
	for _, s := range resp.Statuses {
		require.Nil(t, s.PendingPlanID, "no pending plan")
	}

	// Injecting plan into 2 nodes out of 3.
	plan := makeTestRecoveryPlan(ctx, t, adm)
	for i := 0; i < 2; i++ {
		require.NoError(t, planStores[i].SavePlan(plan), "failed to save plan on node n%d", i)
	}

	// First we test that plans are successfully picked up by status call.
	resp, err = adm.RecoveryVerify(ctx, &serverpb.RecoveryVerifyRequest{MaxConcurrency: -1 /* no limit */})
	require.NoError(t, err)
	statuses := aggregateStatusByNode(resp)
	require.Equal(t, &plan.PlanID, statuses[1].PendingPlanID, "incorrect plan id on node 1")
	require.Equal(t, &plan.PlanID, statuses[2].PendingPlanID, "incorrect plan id on node 2")
	require.Nil(t, statuses[3].PendingPlanID, "unexpected plan id on node 3")

	// Check we can collect partial results.
	tc.StopServer(1)

	testutils.SucceedsSoon(t, func() error {
		resp, err = adm.RecoveryVerify(ctx, &serverpb.RecoveryVerifyRequest{MaxConcurrency: -1 /* no limit */})
		if err != nil {
			return err
		}
		if len(resp.Statuses) > 2 {
			return errors.New("too many statuses in response")
		}
		return nil
	})

	statuses = aggregateStatusByNode(resp)
	require.Equal(t, &plan.PlanID, statuses[1].PendingPlanID, "incorrect plan id")
	require.Nil(t, statuses[3].PendingPlanID, "unexpected plan id")
}

func aggregateStatusByNode(
	resp *serverpb.RecoveryVerifyResponse,
) map[roachpb.NodeID]loqrecoverypb.NodeRecoveryStatus {
	statuses := make(map[roachpb.NodeID]loqrecoverypb.NodeRecoveryStatus)
	for _, s := range resp.Statuses {
		statuses[s.NodeID] = s
	}
	return statuses
}

func TestStageRecoveryPlans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc, _, _ := prepTestCluster(ctx, t, 3)
	defer tc.Stopper().Stop(ctx)

	adm := tc.GetAdminClient(t, 0)

	resp, err := adm.RecoveryVerify(ctx, &serverpb.RecoveryVerifyRequest{MaxConcurrency: -1 /* no limit */})
	require.NoError(t, err)
	for _, s := range resp.Statuses {
		require.Nil(t, s.PendingPlanID, "no pending plan")
	}

	sk := tc.ScratchRange(t)

	// Stage plan with update for node 3 using node 0 and check which nodes
	// saved plan.
	plan := makeTestRecoveryPlan(ctx, t, adm)
	plan.Updates = []loqrecoverypb.ReplicaUpdate{
		createRecoveryForRange(t, tc, sk, 3),
	}
	plan.StaleLeaseholderNodeIDs = []roachpb.NodeID{1}
	res, err := adm.RecoveryStagePlan(ctx, &serverpb.RecoveryStagePlanRequest{
		Plan:           &plan,
		AllNodes:       true,
		MaxConcurrency: -1, // no limit
	})
	require.NoError(t, err, "failed to stage plan")
	require.Empty(t, res.Errors, "unexpected errors in stage response")

	// First we test that plans are successfully picked up by status call.
	resp, err = adm.RecoveryVerify(ctx, &serverpb.RecoveryVerifyRequest{MaxConcurrency: -1 /* no limit */})
	require.NoError(t, err)
	statuses := aggregateStatusByNode(resp)
	require.Equal(t, &plan.PlanID, statuses[1].PendingPlanID, "incorrect plan id on node 1")
	require.Nil(t, statuses[2].PendingPlanID, "unexpected plan id on node 2")
	require.Equal(t, &plan.PlanID, statuses[3].PendingPlanID, "incorrect plan id on node 3")
}

func TestStageBadVersions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc, _, _ := prepTestCluster(ctx, t, 1)
	defer tc.Stopper().Stop(ctx)

	adm := tc.GetAdminClient(t, 0)

	sk := tc.ScratchRange(t)
	plan := makeTestRecoveryPlan(ctx, t, adm)
	plan.Updates = []loqrecoverypb.ReplicaUpdate{
		createRecoveryForRange(t, tc, sk, 1),
	}
	plan.Version = clusterversion.MinSupported.Version()
	plan.Version.Major -= 1

	_, err := adm.RecoveryStagePlan(ctx, &serverpb.RecoveryStagePlanRequest{
		Plan:           &plan,
		AllNodes:       true,
		MaxConcurrency: -1, // no limit
	})
	require.Error(t, err, "shouldn't stage plan with old version")

	plan.Version = clusterversion.Latest.Version()
	plan.Version.Major += 1
	_, err = adm.RecoveryStagePlan(ctx, &serverpb.RecoveryStagePlanRequest{
		Plan:           &plan,
		AllNodes:       true,
		MaxConcurrency: -1, // no limit
	})
	require.Error(t, err, "shouldn't stage plan with future version")
}

func TestStageConflictingPlans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc, _, _ := prepTestCluster(ctx, t, 3)
	defer tc.Stopper().Stop(ctx)

	adm := tc.GetAdminClient(t, 0)

	resp, err := adm.RecoveryVerify(ctx, &serverpb.RecoveryVerifyRequest{MaxConcurrency: -1 /* no limit */})
	require.NoError(t, err)
	for _, s := range resp.Statuses {
		require.Nil(t, s.PendingPlanID, "no pending plan")
	}

	sk := tc.ScratchRange(t)

	// Stage first plan.
	plan := makeTestRecoveryPlan(ctx, t, adm)
	plan.Updates = []loqrecoverypb.ReplicaUpdate{
		createRecoveryForRange(t, tc, sk, 3),
	}
	res, err := adm.RecoveryStagePlan(ctx, &serverpb.RecoveryStagePlanRequest{
		Plan:           &plan,
		AllNodes:       true,
		MaxConcurrency: -1, // no limit
	})
	require.NoError(t, err, "failed to stage plan")
	require.Empty(t, res.Errors, "unexpected errors in stage response")

	plan2 := makeTestRecoveryPlan(ctx, t, adm)
	plan2.Updates = []loqrecoverypb.ReplicaUpdate{
		createRecoveryForRange(t, tc, sk, 2),
	}
	_, err = adm.RecoveryStagePlan(ctx, &serverpb.RecoveryStagePlanRequest{
		Plan:           &plan2,
		AllNodes:       true,
		MaxConcurrency: -1, // no limit
	})
	require.ErrorContains(t, err,
		fmt.Sprintf("plan %s is already staged on node n3", plan.PlanID.String()),
		"conflicting plans must not be allowed")
}

func TestForcePlanUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc, _, _ := prepTestCluster(ctx, t, 3)
	defer tc.Stopper().Stop(ctx)

	adm := tc.GetAdminClient(t, 0)

	resV, err := adm.RecoveryVerify(ctx, &serverpb.RecoveryVerifyRequest{MaxConcurrency: -1 /* no limit */})
	require.NoError(t, err)
	for _, s := range resV.Statuses {
		require.Nil(t, s.PendingPlanID, "no pending plan")
	}

	sk := tc.ScratchRange(t)

	// Stage first plan.
	plan := makeTestRecoveryPlan(ctx, t, adm)
	plan.Updates = []loqrecoverypb.ReplicaUpdate{
		createRecoveryForRange(t, tc, sk, 3),
	}
	resS, err := adm.RecoveryStagePlan(ctx, &serverpb.RecoveryStagePlanRequest{
		Plan:           &plan,
		AllNodes:       true,
		MaxConcurrency: -1, // no limit
	})
	require.NoError(t, err, "failed to stage plan")
	require.Empty(t, resS.Errors, "unexpected errors in stage response")

	_, err = adm.RecoveryStagePlan(ctx, &serverpb.RecoveryStagePlanRequest{
		AllNodes:       true,
		ForcePlan:      true,
		MaxConcurrency: -1, // no limit
	})
	require.NoError(t, err, "force plan should reset previous plans")

	// Verify that plan was successfully replaced by an empty one.
	resV, err = adm.RecoveryVerify(ctx, &serverpb.RecoveryVerifyRequest{MaxConcurrency: -1 /* no limit */})
	require.NoError(t, err)
	statuses := aggregateStatusByNode(resV)
	require.Nil(t, statuses[1].PendingPlanID, "unexpected plan id on node 1")
	require.Nil(t, statuses[2].PendingPlanID, "unexpected plan id on node 2")
	require.Nil(t, statuses[3].PendingPlanID, "unexpected plan id on node 3")
}

func TestNodeDecommissioned(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc, _, _ := prepTestCluster(ctx, t, 3)
	defer tc.Stopper().Stop(ctx)

	adm := tc.GetAdminClient(t, 0)

	tc.StopServer(2)

	plan := makeTestRecoveryPlan(ctx, t, adm)
	plan.DecommissionedNodeIDs = []roachpb.NodeID{roachpb.NodeID(3)}
	testutils.SucceedsSoon(t, func() error {
		res, err := adm.RecoveryStagePlan(ctx, &serverpb.RecoveryStagePlanRequest{
			Plan:           &plan,
			AllNodes:       true,
			MaxConcurrency: -1, // no limit
		})
		if err != nil {
			return err
		}
		if len(res.Errors) > 0 {
			return errors.Newf("failed to stage plan: %s", strings.Join(res.Errors, "; "))
		}
		return nil
	})

	require.ErrorContains(t, tc.Server(0).RPCContext().OnOutgoingPing(ctx, &rpc.PingRequest{TargetNodeID: 3}),
		"permanently removed from the cluster", "ping of decommissioned node should fail")
}

func TestRejectDecommissionReachableNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc, _, _ := prepTestCluster(ctx, t, 3)
	defer tc.Stopper().Stop(ctx)

	adm := tc.GetAdminClient(t, 0)

	plan := makeTestRecoveryPlan(ctx, t, adm)
	plan.DecommissionedNodeIDs = []roachpb.NodeID{roachpb.NodeID(3)}
	_, err := adm.RecoveryStagePlan(ctx, &serverpb.RecoveryStagePlanRequest{
		Plan:           &plan,
		AllNodes:       true,
		MaxConcurrency: -1, // no limit
	})
	require.ErrorContains(t, err, "was planned for decommission, but is present in cluster",
		"staging plan decommissioning live nodes must not be allowed")
}

func TestStageRecoveryPlansToWrongCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc, _, _ := prepTestCluster(ctx, t, 3)
	defer tc.Stopper().Stop(ctx)

	adm := tc.GetAdminClient(t, 0)

	resp, err := adm.RecoveryVerify(ctx, &serverpb.RecoveryVerifyRequest{MaxConcurrency: -1 /* no limit */})
	require.NoError(t, err)
	for _, s := range resp.Statuses {
		require.Nil(t, s.PendingPlanID, "no pending plan")
	}

	sk := tc.ScratchRange(t)

	fakeClusterID := uuid.NewV4()
	// Stage plan with id of different cluster and see if error is raised.
	plan := makeTestRecoveryPlan(ctx, t, adm)
	plan.ClusterID = fakeClusterID.String()
	plan.Updates = []loqrecoverypb.ReplicaUpdate{
		createRecoveryForRange(t, tc, sk, 3),
	}
	_, err = adm.RecoveryStagePlan(ctx, &serverpb.RecoveryStagePlanRequest{
		Plan:           &plan,
		AllNodes:       true,
		MaxConcurrency: -1, // no limit
	})
	require.ErrorContains(t, err, "attempting to stage plan from cluster", "failed to stage plan")
}

func TestRetrieveRangeStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc, _, _ := prepTestCluster(ctx, t, 5)
	defer tc.Stopper().Stop(ctx)

	// Use scratch range to ensure we have a range that loses quorum.
	sk := tc.ScratchRange(t)
	require.NoError(t, tc.WaitFor5NodeReplication(),
		"failed to wait for full replication of 5 node cluster")
	tc.ToggleReplicateQueues(false)
	tc.SplitRangeOrFatal(t, testutils.MakeKey(sk, []byte{255}))

	d := tc.LookupRangeOrFatal(t, sk)

	rs := d.Replicas().Voters().Descriptors()
	require.Equal(t, 3, len(rs), "Number of scratch replicas")

	// Kill 2 of 3 scratch replicas.
	tc.StopServer(int(rs[0].NodeID - 1))
	tc.StopServer(int(rs[1].NodeID - 1))

	admServer := int(rs[2].NodeID - 1)
	adm := tc.GetAdminClient(t, admServer)

	r, err := adm.RecoveryVerify(ctx, &serverpb.RecoveryVerifyRequest{
		DecommissionedNodeIDs: []roachpb.NodeID{rs[0].NodeID, rs[1].NodeID},
		MaxReportedRanges:     999,
		MaxConcurrency:        -1, // no limit
	})
	require.NoError(t, err, "failed to get range status")

	// We verify that at least scratch range lost its quorum at this point.
	func() {
		for _, status := range r.UnavailableRanges.Ranges {
			if status.RangeID == d.RangeID {
				require.Equal(t, loqrecoverypb.RangeHealth_LOSS_OF_QUORUM.String(), status.Health.String())
				return
			}
		}
		t.Fatal("failed to find scratch range in unavailable ranges")
	}()
	require.Empty(t, r.UnavailableRanges.Error, "should have no error")

	// Try to check if limiting number of ranges will produce error on too many ranges.
	r, err = adm.RecoveryVerify(ctx, &serverpb.RecoveryVerifyRequest{
		DecommissionedNodeIDs: []roachpb.NodeID{rs[0].NodeID, rs[1].NodeID},
		MaxReportedRanges:     1,
		MaxConcurrency:        -1, // no limit
	})
	require.NoError(t, err, "failed to get range status")
	require.Equal(t, r.UnavailableRanges.Error, "found more failed ranges than limit 1")
}

func TestRetrieveApplyStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc, _, _ := prepTestCluster(ctx, t, 5)
	defer tc.Stopper().Stop(ctx)

	// Use scratch range to ensure we have a range that loses quorum.
	sk := tc.ScratchRange(t)
	require.NoError(t, tc.WaitFor5NodeReplication(),
		"failed to wait for full replication of 5 node cluster")
	tc.ToggleReplicateQueues(false)
	d := tc.LookupRangeOrFatal(t, sk)

	rs := d.Replicas().Voters().Descriptors()
	require.Equal(t, 3, len(rs), "Number of scratch replicas")

	admServer := int(rs[2].NodeID - 1)
	// Move liveness lease to a node that is not killed, otherwise test takes
	// very long time to finish.
	ld := tc.LookupRangeOrFatal(t, keys.NodeLivenessPrefix)
	tc.TransferRangeLeaseOrFatal(t, ld, tc.Target(admServer))

	tc.StopServer(int(rs[0].NodeID - 1))
	tc.StopServer(int(rs[1].NodeID - 1))

	adm := tc.GetAdminClient(t, admServer)

	var replicas loqrecoverypb.ClusterReplicaInfo
	testutils.SucceedsSoon(t, func() error {
		var err error
		replicas, _, err = loqrecovery.CollectRemoteReplicaInfo(ctx, adm,
			-1 /* maxConcurrency */, nil /* logOutput */)
		return err
	})
	plan, planDetails, err := loqrecovery.PlanReplicas(ctx, replicas, nil, nil, uuid.DefaultGenerator)
	require.NoError(t, err, "failed to create a plan")
	testutils.SucceedsSoon(t, func() error {
		res, err := adm.RecoveryStagePlan(ctx, &serverpb.RecoveryStagePlanRequest{
			Plan:           &plan,
			AllNodes:       true,
			MaxConcurrency: -1, // no limit
		})
		if err != nil {
			return err
		}
		if errMsg := strings.Join(res.Errors, ", "); len(errMsg) > 0 {
			return errors.Newf("%s", errMsg)
		}
		return nil
	})

	r, err := adm.RecoveryVerify(ctx, &serverpb.RecoveryVerifyRequest{
		DecommissionedNodeIDs: plan.DecommissionedNodeIDs,
		MaxConcurrency:        -1, // no limit
	})

	require.NoError(t, err, "failed to run recovery verify")
	updates := make(map[roachpb.NodeID]interface{})
	for _, n := range planDetails.UpdatedNodes {
		updates[n.NodeID] = struct{}{}
	}
	staged := 0
	for _, s := range r.Statuses {
		if s.PendingPlanID != nil {
			require.Equal(t, plan.PlanID, *s.PendingPlanID, "wrong plan staged")
			require.Contains(t, updates, s.NodeID,
				"plan should be staged on nodes where changes are planned")
			staged++
		}
	}
	require.Equal(t, len(planDetails.UpdatedNodes), staged, "number of staged plans")

	for _, id := range planDetails.UpdatedNodes {
		tc.StopServer(int(id.NodeID - 1))
		require.NoError(t, tc.RestartServer(int(id.NodeID-1)), "failed to restart node")
	}

	// Need to get new client connection as we one we used belonged to killed
	// server.
	adm = tc.GetAdminClient(t, admServer)

	r, err = adm.RecoveryVerify(ctx, &serverpb.RecoveryVerifyRequest{
		PendingPlanID:         &plan.PlanID,
		DecommissionedNodeIDs: plan.DecommissionedNodeIDs,
		MaxConcurrency:        -1, // no limit
	})
	require.NoError(t, err, "failed to run recovery verify")
	applied := 0
	for _, s := range r.Statuses {
		if s.AppliedPlanID != nil {
			require.Equal(t, plan.PlanID, *s.AppliedPlanID, "wrong plan staged")
			require.Contains(t, updates, s.NodeID,
				"plan should be staged on nodes where changes are planned")
			applied++
		}
	}
	require.Equal(t, len(planDetails.UpdatedNodes), applied, "number of applied plans")
}

func TestRejectBadVersionApplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc, _, pss := prepTestCluster(ctx, t, 3)
	defer tc.Stopper().Stop(ctx)

	adm := tc.GetAdminClient(t, 0)

	var replicas loqrecoverypb.ClusterReplicaInfo
	testutils.SucceedsSoon(t, func() error {
		var err error
		replicas, _, err = loqrecovery.CollectRemoteReplicaInfo(ctx, adm,
			-1 /* maxConcurrency */, nil /* logOutput */)
		return err
	})
	plan, _, err := loqrecovery.PlanReplicas(ctx, replicas, nil, nil, uuid.DefaultGenerator)
	require.NoError(t, err, "failed to create a plan")
	// Lower plan version below compatible one to trigger
	plan.Version.Major = 19

	tc.StopServer(1)
	require.NoError(t, pss[1].SavePlan(plan), "failed to inject plan into storage")
	require.NoError(t, tc.RestartServer(1), "failed to restart server")

	r, err := adm.RecoveryVerify(ctx, &serverpb.RecoveryVerifyRequest{MaxConcurrency: -1 /* no limit */})
	require.NoError(t, err, "failed to run recovery verify")
	found := false
	for _, s := range r.Statuses {
		if s.NodeID == 2 {
			require.NotNil(t, s.AppliedPlanID)
			require.Equal(t, plan.PlanID, *s.AppliedPlanID)
			require.Contains(t, s.Error, "failed to check cluster version against storage")
			found = true
		}
	}
	require.True(t, found, "restarted node not found in verify status")
}

func prepTestCluster(
	ctx context.Context, t *testing.T, nodes int,
) (*testcluster.TestCluster, fs.StickyRegistry, map[int]loqrecovery.PlanStore) {
	skip.UnderRace(t, "cluster frequently fails to start under stress race")

	reg := fs.NewStickyRegistry()

	lReg := listenerutil.NewListenerRegistry()

	args := base.TestClusterArgs{
		ServerArgsPerNode:   make(map[int]base.TestServerArgs),
		ReusableListenerReg: lReg,
	}

	st := cluster.MakeTestingClusterSettings()
	for i := 0; i < nodes; i++ {
		args.ServerArgsPerNode[i] = base.TestServerArgs{
			Settings: st,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyVFSRegistry: reg,
				},
				SpanConfig: &spanconfig.TestingKnobs{
					ConfigureScratchRange: true,
				},
			},
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: strconv.FormatInt(int64(i), 10),
				},
			},
		}
	}
	tc := testcluster.NewTestCluster(t, nodes, args)
	tc.Start(t)
	tc.Stopper().AddCloser(stop.CloserFn(lReg.Close))
	return tc, reg, prepInMemPlanStores(t, args.ServerArgsPerNode)
}

func prepInMemPlanStores(
	t *testing.T, serverArgs map[int]base.TestServerArgs,
) map[int]loqrecovery.PlanStore {
	pss := make(map[int]loqrecovery.PlanStore)
	for id, args := range serverArgs {
		reg := args.Knobs.Server.(*server.TestingKnobs).StickyVFSRegistry
		pss[id] = loqrecovery.NewPlanStore(".", reg.Get(args.StoreSpecs[0].StickyVFSID))
	}
	return pss
}

func createRecoveryForRange(
	t *testing.T, tc *testcluster.TestCluster, key roachpb.Key, storeID int,
) loqrecoverypb.ReplicaUpdate {
	rngD, err := tc.LookupRange(key)
	require.NoError(t, err, "can't find range for key %s", key)
	replD, ok := rngD.GetReplicaDescriptor(roachpb.StoreID(storeID))
	require.True(t, ok, "expecting scratch replica on store %d", storeID)
	replD.ReplicaID += 10
	return loqrecoverypb.ReplicaUpdate{
		RangeID:       rngD.RangeID,
		StartKey:      loqrecoverypb.RecoveryKey(rngD.StartKey),
		OldReplicaID:  replD.ReplicaID,
		NewReplica:    replD,
		NextReplicaID: replD.ReplicaID + 1,
	}
}

func makeTestRecoveryPlan(
	ctx context.Context, t *testing.T, ac serverpb.AdminClient,
) loqrecoverypb.ReplicaUpdatePlan {
	t.Helper()
	cr, err := ac.Cluster(ctx, &serverpb.ClusterRequest{})
	require.NoError(t, err, "failed to read cluster it")
	return loqrecoverypb.ReplicaUpdatePlan{
		PlanID:    uuid.MakeV4(),
		ClusterID: cr.ClusterID,
		Version:   clusterversion.Latest.Version(),
	}
}
