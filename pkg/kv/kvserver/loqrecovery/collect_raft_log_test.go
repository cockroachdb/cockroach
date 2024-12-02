// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package loqrecovery_test

import (
	"context"
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestFindUpdateDescriptor verifies that we can detect changes to range
// descriptor in the raft log.
// To do this we split and merge the range which updates descriptor prior to
// spawning or subsuming RHS.
func TestFindUpdateDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "lease-type", roachpb.TestingAllLeaseTypes(),
		func(t *testing.T, leaseType roachpb.LeaseType) {
			ctx := context.Background()

			const testNode = 0

			var testRangeID roachpb.RangeID
			var rHS roachpb.RangeDescriptor
			var lHSBefore roachpb.RangeDescriptor
			var lHSAfter roachpb.RangeDescriptor
			checkRaftLog(t, ctx, testNode,
				func(ctx context.Context, tc *testcluster.TestCluster) roachpb.RKey {
					scratchKey, err := tc.Server(0).ScratchRange()
					require.NoError(t, err, "failed to get scratch range")
					srk, err := keys.Addr(scratchKey)
					require.NoError(t, err, "failed to resolve scratch key")

					rd, err := tc.LookupRange(scratchKey)
					testRangeID = rd.RangeID
					require.NoError(t, err, "failed to get descriptor for scratch range")

					splitKey := testutils.MakeKey(scratchKey, []byte("z"))
					lHSBefore, rHS, err = tc.SplitRange(splitKey)
					require.NoError(t, err, "failed to split scratch range")

					lHSAfter, err = tc.Servers[0].MergeRanges(scratchKey)
					require.NoError(t, err, "failed to merge scratch range")

					require.NoError(t,
						tc.Server(testNode).DB().Put(ctx, testutils.MakeKey(scratchKey, []byte("|first")),
							"some data"),
						"failed to put test value in LHS")

					return srk
				},
				func(t *testing.T, ctx context.Context, reader storage.Reader) {
					seq, err := loqrecovery.GetDescriptorChangesFromRaftLog(
						ctx, testRangeID, 0, math.MaxInt64, reader)
					require.NoError(t, err, "failed to read raft log data")

					requireContainsDescriptor(t, loqrecoverypb.DescriptorChangeInfo{
						ChangeType: loqrecoverypb.DescriptorChangeType_Split,
						Desc:       &lHSBefore,
						OtherDesc:  &rHS,
					}, seq)
					requireContainsDescriptor(t, loqrecoverypb.DescriptorChangeInfo{
						ChangeType: loqrecoverypb.DescriptorChangeType_Merge,
						Desc:       &lHSAfter,
						OtherDesc:  &rHS,
					}, seq)
				}, leaseType)
		})
}

// TestFindUpdateRaft verifies that we can detect raft change commands in the
// raft log. To do this we change number of replicas and then assert if
// RaftChange updates are found in event sequence.
func TestFindUpdateRaft(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testutils.RunValues(t, "lease-type", roachpb.TestingAllLeaseTypes(),
		func(t *testing.T, leaseType roachpb.LeaseType) {
			ctx := context.Background()

			const testNode = 0

			var sRD roachpb.RangeDescriptor
			checkRaftLog(t, ctx, testNode,
				func(ctx context.Context, tc *testcluster.TestCluster) roachpb.RKey {
					scratchKey, err := tc.Server(0).ScratchRange()
					require.NoError(t, err, "failed to get scratch range")
					srk, err := keys.Addr(scratchKey)
					require.NoError(t, err, "failed to resolve scratch key")
					rd, err := tc.AddVoters(scratchKey, tc.Target(1))
					require.NoError(t, err, "failed to upreplicate scratch range")
					tc.TransferRangeLeaseOrFatal(t, rd, tc.Target(0))
					tc.RemoveVotersOrFatal(t, scratchKey, tc.Targets(1)...)

					sRD, err = tc.LookupRange(scratchKey)
					require.NoError(t, err, "failed to get descriptor after remove replicas")

					require.NoError(t,
						tc.Server(testNode).DB().Put(ctx, testutils.MakeKey(scratchKey, []byte("|first")),
							"some data"),
						"failed to put test value in range")

					return srk
				},
				func(t *testing.T, ctx context.Context, reader storage.Reader) {
					seq, err := loqrecovery.GetDescriptorChangesFromRaftLog(
						ctx, sRD.RangeID, 0, math.MaxInt64, reader)
					require.NoError(t, err, "failed to read raft log data")
					requireContainsDescriptor(t, loqrecoverypb.DescriptorChangeInfo{
						ChangeType: loqrecoverypb.DescriptorChangeType_ReplicaChange,
						Desc:       &sRD,
					}, seq)
				}, leaseType)
		})
}

func checkRaftLog(
	t *testing.T,
	ctx context.Context,
	nodeToMonitor int,
	action func(ctx context.Context, tc *testcluster.TestCluster) roachpb.RKey,
	assertRaftLog func(*testing.T, context.Context, storage.Reader),
	leaseType roachpb.LeaseType,
) {
	t.Helper()

	makeSnapshot := make(chan storage.Engine, 2)
	snapshots := make(chan storage.Reader, 2)

	raftFilter := func(args kvserverbase.ApplyFilterArgs) (int, *kvpb.Error) {
		t.Helper()
		select {
		case store := <-makeSnapshot:
			snapshots <- store.NewSnapshot()
		default:
		}
		return 0, nil
	}

	testRaftConfig := base.RaftConfig{
		// High enough interval to be longer than test but not overflow duration.
		RaftLogTruncationThreshold: math.MaxInt64,
	}

	// Leader leases typically has a stable leader without high election ticks.
	if leaseType != roachpb.LeaseLeader {
		testRaftConfig.RaftTickInterval = math.MaxInt32
		testRaftConfig.RaftElectionTimeoutTicks = 1000000
	}

	st := cluster.MakeTestingClusterSettings()
	kvserver.OverrideDefaultLeaseType(ctx, &st.SV, leaseType)
	tc := testcluster.NewTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: st,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					DisableGCQueue: true,
				},
			},
			StoreSpecs: []base.StoreSpec{{InMemory: true}},
			RaftConfig: testRaftConfig,
			Insecure:   true,
		},
		ReplicationMode: base.ReplicationManual,
		ServerArgsPerNode: map[int]base.TestServerArgs{
			nodeToMonitor: {
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						TestingPostApplyFilter: raftFilter,
						DisableGCQueue:         true,
					},
				},
				StoreSpecs: []base.StoreSpec{{InMemory: true}},
				RaftConfig: testRaftConfig,
				Insecure:   true,
				Settings:   st,
			},
		},
	})

	tc.Start(t)
	defer tc.Stopper().Stop(ctx)

	skey := action(ctx, tc)

	// TODO(sep-raft-log): the receiver doesn't use the engine, so remove this
	// altogether and use a `chan struct{}{}`.
	eng := tc.GetFirstStoreFromServer(t, nodeToMonitor).TODOEngine()
	makeSnapshot <- eng
	// After the test action is complete raft might be completely caught up with
	// its messages, so we will write a value into the range to ensure filter
	// fires up at least once after we requested capture.
	require.NoError(t,
		tc.Server(0).DB().Put(ctx, testutils.MakeKey(skey, []byte("second")), "some data"),
		"failed to put test value")
	reader := <-snapshots
	assertRaftLog(t, ctx, reader)
	defer reader.Close()
}

func requireContainsDescriptor(
	t *testing.T, value loqrecoverypb.DescriptorChangeInfo, seq []loqrecoverypb.DescriptorChangeInfo,
) {
	t.Helper()
	for _, v := range seq {
		if reflect.DeepEqual(value, v) {
			return
		}
	}
	t.Fatalf("descriptor change sequence %v doesn't contain %v", seq, value)
}

// TestCollectLeaseholderStatus verifies that leaseholder status is collected
// from replicas. It relies on range 1 always being present and fully replicated
// in ReplicationAuto mode. Assertion is checking number of replicas and only
// one of them thinking it is a leaseholder.
func TestCollectLeaseholderStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := testcluster.NewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					DisableGCQueue: true,
				},
			},
			StoreSpecs: []base.StoreSpec{{InMemory: true}},
			Insecure:   true,
		},
		ReplicationMode: base.ReplicationAuto,
	})
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)
	require.NoError(t, tc.WaitForFullReplication())

	adm := tc.GetAdminClient(t, 0)

	// Note: we need to retry because replica collection is not atomic and
	// leaseholder could move around so we could see none or more than one.
	testutils.SucceedsSoon(t, func() error {
		replicas, _, err := loqrecovery.CollectRemoteReplicaInfo(ctx, adm,
			-1 /* maxConcurrency */, nil /* logOutput */)
		require.NoError(t, err, "failed to collect replica info")

		foundLeaseholders := 0
		foundReplicas := 0
		for _, rs := range replicas.LocalInfo {
			for _, rs := range rs.Replicas {
				if rs.Desc.RangeID == 1 {
					foundReplicas++
					if rs.LocalAssumesLeaseholder {
						foundLeaseholders++
					}
				}
			}
		}
		if foundReplicas != 3 {
			return errors.Newf("expecting total 3 replicas in meta range on all nodes, found %d", foundReplicas)
		}
		if foundLeaseholders != 1 {
			return errors.Newf("expecting single leaseholder in meta range, found %d", foundLeaseholders)
		}
		return nil
	})
}
