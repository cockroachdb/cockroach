// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migration_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestTruncatedStateMigration(t *testing.T) {
	defer leaktest.AfterTest(t)
	ctx := context.Background()

	for _, testCase := range []struct {
		name string
		typ  stateloader.TruncatedStateType
	}{
		{"ts=new,as=new", stateloader.TruncatedStateUnreplicated},
		{"ts=legacy,as=new", stateloader.TruncatedStateLegacyReplicated},
		{"ts=legacy,as=legacy", stateloader.TruncatedStateLegacyReplicatedAndNoAppliedKey},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			args := base.TestClusterArgs{}
			args.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{TruncatedStateTypeOverride: &testCase.typ}
			args.ServerArgs.Knobs.Server = &server.TestingKnobs{
				// TruncatedAndRangeAppliedStateMigration is part of the
				// migration that lets us stop using the legacy truncated state.
				// When the active cluster version is greater than it, we assert
				// against the presence of legacy truncated state and ensure
				// we're using the range applied state key. In this test we'll
				// start of at the version immediately preceding the migration,
				// and migrate past it.
				BinaryVersionOverride: clusterversion.ByKey(clusterversion.TruncatedAndRangeAppliedStateMigration - 1),
				// We want to exercise manual control over the upgrade process.
				DisableAutomaticVersionUpgrade: 1,
			}
			tc := testcluster.StartTestCluster(t, 3, args)
			defer tc.Stopper().Stop(ctx)

			forAllReplicas := func(f func(*kvserver.Replica) error) error {
				for i := 0; i < tc.NumServers(); i++ {
					err := tc.Server(i).GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
						var err error
						s.VisitReplicas(func(repl *kvserver.Replica) (wantMore bool) {
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

			getLegacyRanges := func() []string {
				t.Helper()
				var out []string
				require.NoError(t, forAllReplicas(func(repl *kvserver.Replica) error {
					sl := stateloader.Make(repl.RangeID)

					_, legacy, err := sl.LoadRaftTruncatedState(ctx, repl.Engine())
					if err != nil {
						return err
					}
					if legacy {
						// We're using the legacy truncated state, record ourselves.
						out = append(out, fmt.Sprintf("ts(r%d)", repl.RangeID))
					}

					as, err := sl.LoadRangeAppliedState(ctx, repl.Engine())
					if err != nil {
						return err
					}
					if as == nil {
						// We're not using the new applied state key, record ourselves.
						out = append(out, fmt.Sprintf("as(r%d)", repl.RangeID))
					}
					return nil
				}))
				return out
			}

			legacyRanges := getLegacyRanges()
			switch testCase.typ {
			case stateloader.TruncatedStateUnreplicated:
				if len(legacyRanges) != 0 {
					t.Fatalf("expected no ranges with legacy keys if bootstrapped with unreplicated truncated state, got: %v", legacyRanges)
				}
			case stateloader.TruncatedStateLegacyReplicated, stateloader.TruncatedStateLegacyReplicatedAndNoAppliedKey:
				if len(legacyRanges) == 0 {
					t.Fatalf("expected ranges with legacy keys if bootstrapped with replicated truncated state, got none")
				}
			}

			// NB: we'll never spot a legacy applied state here. This is
			// because that migration is so aggressive that it has already
			// happened as part of the initial up-replication.
			t.Logf("ranges with legacy keys before migration: %v", legacyRanges)

			_, err := tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`,
				clusterversion.ByKey(clusterversion.TruncatedAndRangeAppliedStateMigration+1).String())
			require.NoError(t, err)
			require.Zero(t, getLegacyRanges())

			require.NoError(t, forAllReplicas(func(repl *kvserver.Replica) error {
				truncStateVersion := clusterversion.ByKey(clusterversion.TruncatedAndRangeAppliedStateMigration)
				if repl.Version().Less(truncStateVersion) {
					return errors.Newf("unexpected version %s", repl.Version())
				}
				return nil
			}))
		})
	}
}

func TestMigrateUpdatesReplicaVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We're going to be migrating from startCV to endCV.
	startCV := clusterversion.ClusterVersion{Version: roachpb.Version{Major: 41}}
	endCV := clusterversion.ClusterVersion{Version: roachpb.Version{Major: 42}}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: cluster.MakeTestingClusterSettingsWithVersions(endCV.Version, startCV.Version, false),
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					BinaryVersionOverride:          startCV.Version,
					DisableAutomaticVersionUpgrade: 1,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	// We'll take a specific range, still running at startCV, generate an
	// outgoing snapshot and then suspend it temporarily. We'll then bump the
	// cluster version on all the stores, as part of the migration process, and
	// then resume the snapshot process. Seeing as how the snapshot was
	// generated pre-version bump, off of a version of the range that hadn't
	// observed the migration corresponding to the latest cluster version, we
	// expect the store to reject it.

	key := tc.ScratchRange(t)
	require.NoError(t, tc.WaitForSplitAndInitialization(key))
	desc, err := tc.LookupRange(key)
	require.NoError(t, err)
	rangeID := desc.RangeID

	// Enqueue the replica in the raftsnapshot queue. We use SucceedsSoon
	// because it may take a bit for raft to figure out that we need to be
	// generating a snapshot.
	store := tc.GetFirstStoreFromServer(t, 0)
	repl, err := store.GetReplica(rangeID)
	require.NoError(t, err)

	if got := repl.Version(); got != startCV.Version {
		t.Fatalf("got replica version %s, expected %s", got, startCV.Version)
	}

	// Register the below raft migration.
	unregisterKVMigration := batcheval.TestingRegisterMigrationInterceptor(endCV.Version, func() {})
	defer unregisterKVMigration()

	// Register the top-level migration.
	unregister := migration.TestingRegisterMigrationInterceptor(endCV, func(ctx context.Context, h *migration.Helper) error {
		return h.DB().Migrate(ctx, desc.StartKey, desc.EndKey, h.ClusterVersion().Version)
	})
	defer unregister()

	// Wait until all nodes have are considered live.
	nl := tc.Server(0).NodeLiveness().(*liveness.NodeLiveness)
	testutils.SucceedsSoon(t, func() error {
		for _, s := range tc.Servers {
			id := s.NodeID()
			live, err := nl.IsLive(id)
			if err != nil {
				return err
			}
			if !live {
				return errors.Newf("n%s not live yet", id)
			}
		}
		return nil
	})

	// Kick off the migration process.
	_, err = tc.Conns[0].ExecContext(ctx, `SET CLUSTER SETTING version = $1`, endCV.String())
	require.NoError(t, err)

	if got := repl.Version(); got != endCV.Version {
		t.Fatalf("got replica version %s, expected %s", got, endCV.Version)
	}
}
