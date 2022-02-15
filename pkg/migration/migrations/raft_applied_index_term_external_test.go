// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func forAllReplicas(tc *testcluster.TestCluster, f func(*kvserver.Replica) error) error {
	for i := 0; i < tc.NumServers(); i++ {
		err := tc.Server(i).GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
			var err error
			s.VisitReplicas(func(repl *kvserver.Replica) bool {
				err = f(repl)
				return err == nil
			})
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func getReplicaCounts(
	ctx context.Context, t *testing.T, tc *testcluster.TestCluster,
) (totalReplicas int, replicasWithTerm int) {
	t.Helper()
	require.NoError(t, forAllReplicas(tc, func(repl *kvserver.Replica) error {
		unlockFunc := repl.LockRaftMuForTesting()
		defer unlockFunc()
		totalReplicas++
		sl := stateloader.Make(repl.RangeID)
		appliedState, err := sl.LoadRangeAppliedState(ctx, repl.Engine())
		if err != nil {
			return err
		}
		// Basic sanity checking of the replica.
		if appliedState.RaftAppliedIndex == 0 {
			return errors.Errorf("expected initialized replica")
		}
		appliedIndexTermInReplicaStruct := repl.State(ctx).RaftAppliedIndexTerm
		// The in-memory ReplicaState maintained in Replica must be consistent
		// with that we are placing in RangeAppliedState.
		require.Equal(t, appliedState.RaftAppliedIndexTerm, appliedIndexTermInReplicaStruct)
		truncState, err := sl.LoadRaftTruncatedState(ctx, repl.Engine())
		if err != nil {
			return err
		}
		if truncState.Index > appliedState.RaftAppliedIndex {
			return errors.Errorf("truncated state index %d > applied index %d",
				truncState.Index, appliedState.RaftAppliedIndex)
		}
		// We could simply call repl.GetTerm at this point, but we do some more
		// sanity checking.
		var appliedIndexTerm uint64
		if truncState.Index == appliedState.RaftAppliedIndex {
			appliedIndexTerm = truncState.Term
		} else {
			lastIndex, err := sl.LoadLastIndex(ctx, repl.Engine())
			if err != nil {
				return err
			}
			if lastIndex < appliedState.RaftAppliedIndex {
				return errors.Errorf("last index in raft log %d < applied index %d",
					lastIndex, appliedState.RaftAppliedIndex)
			}
			if appliedIndexTerm, err = repl.GetTerm(appliedState.RaftAppliedIndex); err != nil {
				return err
			}
		}
		// Now we can decide whether term is being populated or not, or being
		// incorrectly populated.
		if appliedState.RaftAppliedIndexTerm == 0 {
			// Not populated
		} else if appliedState.RaftAppliedIndexTerm == appliedIndexTerm {
			replicasWithTerm++
		} else {
			return errors.Errorf("expected term %d but found %d", appliedIndexTerm,
				appliedState.RaftAppliedIndexTerm)
		}
		return nil
	}))
	return totalReplicas, replicasWithTerm
}

func TestRaftAppliedIndexTermMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderRace(t, "very slow under race")

	ctx := context.Background()
	binaryVersion := clusterversion.ByKey(clusterversion.PostAddRaftAppliedIndexTermMigration)
	bootstrapVersion := clusterversion.ByKey(clusterversion.AddRaftAppliedIndexTermMigration - 1)
	makeArgs := func() (args base.TestServerArgs) {
		args.Settings = cluster.MakeTestingClusterSettingsWithVersions(
			binaryVersion, bootstrapVersion, false,
		)
		args.Knobs.Server = &server.TestingKnobs{
			// Start at the version immediately preceding the migration.
			BinaryVersionOverride: bootstrapVersion,
			// We want to exercise manual control over the upgrade process.
			DisableAutomaticVersionUpgrade: make(chan struct{}),
		}
		return args
	}

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: makeArgs(),
			1: makeArgs(),
			2: makeArgs(),
		},
	})
	defer tc.Stopper().Stop(ctx)

	totalReplicas, replicasWithTerm := getReplicaCounts(ctx, t, tc)
	require.Equal(t, 0, replicasWithTerm)
	require.Less(t, 0, totalReplicas)
	t.Logf("totalReplicas: %d, replicasWithTerm: %d", totalReplicas, replicasWithTerm)
	var scratchKey roachpb.Key
	{
		// Do a split.
		scratchKey = tc.ScratchRange(t)
		desc, err := tc.LookupRange(scratchKey)
		require.NoError(t, err)
		span := desc.KeySpan()
		t.Logf("splitting range: %s", span.String())
		splitKey := append(span.Key, '0', '0', '0')
		leftDesc, rightDesc, err := tc.SplitRange(roachpb.Key(splitKey))
		require.NoError(t, err)
		t.Logf("split range into %s and %s", leftDesc.KeySpan(), rightDesc.KeySpan())
		total, withTerm := getReplicaCounts(ctx, t, tc)
		require.Less(t, totalReplicas, total)
		require.Equal(t, 0, withTerm)
		totalReplicas, replicasWithTerm = total, withTerm
		t.Logf("after split: totalReplicas: %d, replicasWithTerm: %d",
			totalReplicas, replicasWithTerm)
	}
	// Do the migration.
	_, err := tc.Conns[0].ExecContext(
		ctx, `SET CLUSTER SETTING version = $1`, binaryVersion.String(),
	)
	require.NoError(t, err)

	testutils.SucceedsSoon(t, func() error {
		return forAllReplicas(tc, func(repl *kvserver.Replica) error {
			// NB: repl.Version() is not the cluster version. It's the version of
			// the highest Migrate() command that has been executed on the range. So
			// the migration will set it to AddRaftAppliedIndexTermMigration, since
			// there is no Range migration associated with
			// PostAddRaftAppliedIndexTermMigration.
			if repl.Version().Less(clusterversion.ByKey(
				clusterversion.AddRaftAppliedIndexTermMigration)) {
				return errors.Newf("unexpected version %s", repl.Version())
			}
			return nil
		})
	})
	totalReplicas, replicasWithTerm = getReplicaCounts(ctx, t, tc)
	require.Equal(t, totalReplicas, replicasWithTerm)
	require.Less(t, 0, totalReplicas)
	t.Logf("totalReplicas: %d, replicasWithTerm: %d", totalReplicas, replicasWithTerm)
	// Do another split
	{
		desc, err := tc.LookupRange(scratchKey)
		require.NoError(t, err)
		span := desc.KeySpan()
		t.Logf("splitting range: %s", span.String())
		splitKey := append(span.Key, '0', '0')
		leftDesc, rightDesc, err := tc.SplitRange(roachpb.Key(splitKey))
		require.NoError(t, err)
		t.Logf("split range into %s and %s", leftDesc.KeySpan(), rightDesc.KeySpan())
		total, withTerm := getReplicaCounts(ctx, t, tc)
		require.Less(t, totalReplicas, total)
		require.Equal(t, total, withTerm)
		totalReplicas, replicasWithTerm = total, withTerm
		t.Logf("after second split: totalReplicas: %d, replicasWithTerm: %d",
			totalReplicas, replicasWithTerm)
	}
	// Check that the term is maintained when a write happens.
	kvDB := tc.Server(0).DB()
	rScratchKey, err := keys.Addr(scratchKey)
	require.NoError(t, err)
	repl := tc.GetRaftLeader(t, rScratchKey)
	rs1 := repl.State(ctx).ReplicaState
	require.NoError(t, kvDB.Put(ctx, scratchKey, 10))
	testutils.SucceedsSoon(t, func() error {
		rs2 := repl.State(ctx).ReplicaState
		if rs1.RaftAppliedIndex == rs2.RaftAppliedIndex {
			return errors.Errorf("waiting for application")
		}
		return nil
	})
	rs2 := repl.State(ctx).ReplicaState
	require.Less(t, rs1.RaftAppliedIndex, rs2.RaftAppliedIndex)
	require.Equal(t, rs1.RaftAppliedIndexTerm, rs2.RaftAppliedIndexTerm)
}

func TestLatestClusterDoesNotNeedRaftAppliedIndexTermMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderRace(t, "very slow under race")

	ctx := context.Background()
	binaryVersion := clusterversion.ByKey(clusterversion.PostAddRaftAppliedIndexTermMigration)
	makeArgs := func() (args base.TestServerArgs) {
		args.Settings = cluster.MakeTestingClusterSettingsWithVersions(
			binaryVersion, binaryVersion, false,
		)
		args.Knobs.Server = &server.TestingKnobs{
			BinaryVersionOverride: binaryVersion,
			// We want to exercise manual control over the upgrade process.
			DisableAutomaticVersionUpgrade: make(chan struct{}),
		}
		return args
	}

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: makeArgs(),
			1: makeArgs(),
			2: makeArgs(),
		},
	})
	defer tc.Stopper().Stop(ctx)

	totalReplicas, replicasWithTerm := getReplicaCounts(ctx, t, tc)
	require.Equal(t, totalReplicas, replicasWithTerm)
	require.Less(t, 0, totalReplicas)
	t.Logf("totalReplicas: %d, replicasWithTerm: %d", totalReplicas, replicasWithTerm)
}
