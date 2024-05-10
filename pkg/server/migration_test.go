// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestValidateTargetClusterVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	v := func(major, minor int32) roachpb.Version {
		return roachpb.Version{Major: major, Minor: minor}
	}
	cv := func(major, minor int32) clusterversion.ClusterVersion {
		return clusterversion.ClusterVersion{Version: v(major, minor)}
	}

	var tests = []struct {
		binaryVersion             roachpb.Version
		binaryMinSupportedVersion roachpb.Version
		targetClusterVersion      clusterversion.ClusterVersion
		expErrMatch               string // empty if expecting a nil error
	}{
		{
			binaryVersion:             v(20, 2),
			binaryMinSupportedVersion: v(20, 1),
			targetClusterVersion:      cv(20, 1),
			expErrMatch:               "",
		},
		{
			binaryVersion:             v(20, 2),
			binaryMinSupportedVersion: v(20, 1),
			targetClusterVersion:      cv(20, 2),
			expErrMatch:               "",
		},
		{
			binaryVersion:             v(20, 2),
			binaryMinSupportedVersion: v(20, 1),
			targetClusterVersion:      cv(21, 1),
			expErrMatch:               "binary version.*less than target cluster version",
		},
		{
			binaryVersion:             v(20, 2),
			binaryMinSupportedVersion: v(20, 1),
			targetClusterVersion:      cv(19, 2),
			expErrMatch:               "target cluster version.*less than binary's min supported version",
		},
	}

	//   node's minimum supported version <= target version <= node's binary version

	for i, test := range tests {
		st := cluster.MakeTestingClusterSettingsWithVersions(
			test.binaryVersion,
			test.binaryMinSupportedVersion,
			false, /* initializeVersion */
		)

		s := serverutils.StartServerOnly(t, base.TestServerArgs{
			Settings: st,
			Knobs: base.TestingKnobs{
				Server: &TestingKnobs{
					BinaryVersionOverride: test.binaryVersion,
				},
			},
		})

		migrationServer := s.MigrationServer().(*migrationServer)
		req := &serverpb.ValidateTargetClusterVersionRequest{
			ClusterVersion: &test.targetClusterVersion,
		}
		_, err := migrationServer.ValidateTargetClusterVersion(context.Background(), req)
		if !testutils.IsError(err, test.expErrMatch) {
			t.Fatalf("test %d: got error %s, wanted error matching '%s'", i, err, test.expErrMatch)
		}

		s.Stopper().Stop(context.Background())
	}
}

func TestSyncAllEngines(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	vfsRegistry := fs.NewStickyRegistry(fs.UseStrictMemFS)
	storeSpec := base.DefaultTestStoreSpec
	storeSpec.StickyVFSID = "sync-all-engines"
	testServerArgs := base.TestServerArgs{
		Settings: cluster.MakeTestingClusterSettingsWithVersions(
			roachpb.Version{Major: 24, Minor: 1},
			roachpb.Version{Major: 23, Minor: 1},
			false, /* initializeVersion */
		),
		StoreSpecs: []base.StoreSpec{storeSpec},
		Knobs: base.TestingKnobs{
			Server: &TestingKnobs{
				BinaryVersionOverride: roachpb.Version{Major: 24, Minor: 1},
				StickyVFSRegistry:     vfsRegistry,
			},
		},
	}

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, testServerArgs)
	storeID := s.StorageLayer().GetFirstStoreID()
	store, err := s.StorageLayer().GetStores().(*kvserver.Stores).GetStore(storeID)
	require.NoError(t, err)
	key := storage.EngineKey{Key: roachpb.Key("a")}
	{
		// Write a key to an engine unsynced.
		wb := store.StateEngine().NewWriteBatch()
		require.NoError(t, wb.PutEngineKey(key, []byte("a")))
		require.NoError(t, wb.Commit(false /* sync */))
	}
	// Ask the migration server to sync all engines.
	migrationServer := s.MigrationServer().(*migrationServer)
	_, err = migrationServer.SyncAllEngines(ctx, &serverpb.SyncAllEnginesRequest{})
	require.NoError(t, err)

	// Simulate a power failure immediately after SyncAllEngines completes. If
	// SyncAllEngines did not sync the engines, the previous write key "a"
	// will have been lost.
	{
		memFS := vfsRegistry.Get(storeSpec.StickyVFSID)
		memFS.SetIgnoreSyncs(true)
		s.Stopper().Stop(ctx)
		memFS.ResetToSyncedState()
	}

	// Restart the server.
	s = serverutils.StartServerOnly(t, testServerArgs)
	defer s.Stopper().Stop(ctx)
	store, err = s.StorageLayer().GetStores().(*kvserver.Stores).GetStore(storeID)
	require.NoError(t, err)
	// Verify that the restarted engine has the key "a".
	{
		engIter, err := store.StateEngine().NewEngineIterator(ctx, storage.IterOptions{
			LowerBound: roachpb.Key([]byte("a")),
			UpperBound: roachpb.Key([]byte("b")),
		})
		require.NoError(t, err)
		valid, err := engIter.SeekEngineKeyGE(key)
		require.NoError(t, err)
		require.True(t, valid)
		got, err := engIter.UnsafeEngineKey()
		require.NoError(t, err)
		require.Equal(t, key, got)
		defer engIter.Close()
	}
}

// TestBumpClusterVersion verifies that the BumpClusterVersion RPC correctly
// persists the cluster version to disk and updates the active in-memory
// version.
func TestBumpClusterVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	v := func(major, minor int32) roachpb.Version {
		return roachpb.Version{Major: major, Minor: minor}
	}
	cv := func(major, minor int32) clusterversion.ClusterVersion {
		return clusterversion.ClusterVersion{Version: v(major, minor)}
	}

	var tests = []struct {
		binaryVersion        roachpb.Version
		activeClusterVersion clusterversion.ClusterVersion // akin to min supported binary version
		bumpClusterVersion   clusterversion.ClusterVersion
		expClusterVersion    clusterversion.ClusterVersion
	}{
		{
			binaryVersion:        v(21, 1),
			activeClusterVersion: cv(20, 2),
			bumpClusterVersion:   cv(20, 2),
			expClusterVersion:    cv(20, 2),
		},
		{
			binaryVersion:        v(21, 1),
			activeClusterVersion: cv(20, 2),
			bumpClusterVersion:   cv(21, 1),
			expClusterVersion:    cv(21, 1),
		},
		{
			binaryVersion:        v(21, 1),
			activeClusterVersion: cv(21, 1),
			bumpClusterVersion:   cv(20, 2),
			expClusterVersion:    cv(21, 1),
		},
	}

	ctx := context.Background()
	for i, test := range tests {
		t.Run(fmt.Sprintf("config=%d", i), func(t *testing.T) {
			st := cluster.MakeTestingClusterSettingsWithVersions(
				test.binaryVersion,
				test.activeClusterVersion.Version,
				false, /* initializeVersion */
			)

			s := serverutils.StartServerOnly(t, base.TestServerArgs{
				Settings: st,
				Knobs: base.TestingKnobs{
					Server: &TestingKnobs{
						// This test wants to bootstrap at the previously active
						// cluster version, so we can actually bump the cluster
						// version to the binary version. Think a cluster with
						// active cluster version v20.1, but running v20.2 binaries.
						BinaryVersionOverride: test.activeClusterVersion.Version,
						// We're bumping cluster versions manually ourselves. We
						// want avoid racing with the auto-upgrade process.
						DisableAutomaticVersionUpgrade: make(chan struct{}),
					},
				},
			})
			defer s.Stopper().Stop(context.Background())

			// Check to see our pre-bump active cluster version is what we expect.
			if got := s.ClusterSettings().Version.ActiveVersion(ctx); got != test.activeClusterVersion {
				t.Fatalf("expected active cluster version %s, got %s", test.activeClusterVersion, got)
			}

			migrationServer := s.MigrationServer().(*migrationServer)
			req := &serverpb.BumpClusterVersionRequest{
				ClusterVersion: &test.bumpClusterVersion,
			}
			if _, err := migrationServer.BumpClusterVersion(ctx, req); err != nil {
				t.Fatal(err)
			}

			// Check to see if our post-bump active cluster version is what we
			// expect.
			if got := s.ClusterSettings().Version.ActiveVersion(ctx); got != test.expClusterVersion {
				t.Fatalf("expected active cluster version %s, got %s", test.expClusterVersion, got)
			}

			// Check to see that our bumped cluster version was persisted to disk.
			synthesizedCV, err := kvstorage.SynthesizeClusterVersionFromEngines(
				ctx, s.Engines(), test.binaryVersion,
				test.activeClusterVersion.Version,
			)
			if err != nil {
				t.Fatal(err)
			}
			if synthesizedCV != test.expClusterVersion {
				t.Fatalf("expected synthesized cluster version %s, got %s", test.expClusterVersion, synthesizedCV)
			}
		})
	}
}

func TestMigrationPurgeOutdatedReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numStores = 3
	var storeSpecs []base.StoreSpec
	for i := 0; i < numStores; i++ {
		storeSpecs = append(storeSpecs, base.StoreSpec{InMemory: true})
	}

	intercepted := 0
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		StoreSpecs: storeSpecs,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				PurgeOutdatedReplicasInterceptor: func() {
					intercepted++
				},
			},
		},
	})
	defer s.Stopper().Stop(context.Background())

	migrationServer := s.MigrationServer().(*migrationServer)
	version := clusterversion.Latest.Version()
	if _, err := migrationServer.PurgeOutdatedReplicas(context.Background(), &serverpb.PurgeOutdatedReplicasRequest{
		Version: &version,
	}); err != nil {
		t.Fatal(err)
	}

	if intercepted != numStores {
		t.Fatalf("expected to have GC-ed replicas on %d stores, found %d", numStores, intercepted)
	}
}

// TestUpgradeHappensAfterMigration is a regression test to ensure that
// upgrades run prior to attempting to upgrade the cluster to the current
// version. It will also verify that any migrations that modify the system
// database schema properly update the SystemDatabaseSchemaVersion on the
// system database descriptor.
func TestUpgradeHappensAfterMigrations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.Latest.Version(),
		clusterversion.MinSupported.Version(),
		false, /* initializeVersion */
	)
	automaticUpgrade := make(chan struct{})
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: st,
		Knobs: base.TestingKnobs{
			Server: &TestingKnobs{
				DisableAutomaticVersionUpgrade: automaticUpgrade,
				BinaryVersionOverride:          clusterversion.MinSupported.Version(),
			},
			UpgradeManager: &upgradebase.TestingKnobs{
				AfterRunPermanentUpgrades: func() {
					// Try to encourage other goroutines to run.
					const N = 100
					for i := 0; i < N; i++ {
						runtime.Gosched()
					}
					require.True(t, st.Version.ActiveVersion(ctx).Less(clusterversion.Latest.Version()))
				},
			},
		},
	})

	internalDB := s.ApplicationLayer().InternalDB().(descs.DB)
	err := internalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		systemDBDesc, err := txn.Descriptors().ByID(txn.KV()).Get().Database(ctx, keys.SystemDatabaseID)
		if err != nil {
			return err
		}
		systemDBVersion := systemDBDesc.DatabaseDesc().GetSystemDatabaseSchemaVersion()
		// NB: When MinSupported is changed to 24_2, this check can change to
		// an equality check. This is because 24.2 is the first release where
		// https://github.com/cockroachdb/cockroach/issues/121914 has been
		// resolved.
		require.False(
			t, clusterversion.MinSupported.Version().Less(*systemDBVersion),
			"before upgrade, expected system database version (%v) to be less than or equal to clusterversion.MinSupported (%v)",
			*systemDBVersion, clusterversion.MinSupported.Version(),
		)
		return nil
	})
	require.NoError(t, err)

	close(automaticUpgrade)
	sr := sqlutils.MakeSQLRunner(db)

	// Allow more than the default 45 seconds for the upgrades to run. Migrations
	// can take some time, especially under stress.
	sr.SucceedsSoonDuration = 3 * time.Minute
	sr.CheckQueryResultsRetry(t, `
SELECT version = crdb_internal.node_executable_version()
  FROM [SHOW CLUSTER SETTING version]`,
		[][]string{{"true"}})

	// After the upgrade, make sure the new system database version is equal to
	// the SystemDatabaseSchemaBootstrapVersion. This serves two purposes:
	// - reminder to bump SystemDatabaseSchemaBootstrapVersion.
	// - ensure that upgrades have run and updated the system database version.
	err = internalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		systemDBDesc, err := txn.Descriptors().ByID(txn.KV()).Get().Database(ctx, keys.SystemDatabaseID)
		if err != nil {
			return err
		}
		systemDBVersion := systemDBDesc.DatabaseDesc().GetSystemDatabaseSchemaVersion()
		require.Equalf(
			t, systemschema.SystemDatabaseSchemaBootstrapVersion, *systemDBVersion,
			"after upgrade, expected system database version (%v) to be equal to systemschema.SystemDatabaseSchemaBootstrapVersion (%v)",
			*systemDBVersion, systemschema.SystemDatabaseSchemaBootstrapVersion,
		)
		return nil
	})
	require.NoError(t, err)

	s.Stopper().Stop(context.Background())
}
