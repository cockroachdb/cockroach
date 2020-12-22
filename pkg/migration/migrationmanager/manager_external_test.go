// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrationmanager_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/migration/migrations"
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
	unregister := migrations.TestingRegisterMigrationInterceptor(endCV, func(
		ctx context.Context, cv clusterversion.ClusterVersion, c migration.Cluster,
	) error {
		return c.DB().Migrate(ctx, desc.StartKey, desc.EndKey, cv.Version)
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
