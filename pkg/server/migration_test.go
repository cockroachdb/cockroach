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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestValidateTargetClusterVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

		s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
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

// TestBumpClusterVersion verifies that the BumpClusterVersion RPC correctly
// persists the cluster version to disk and updates the active in-memory
// version.
func TestBumpClusterVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

			s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
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
						DisableAutomaticVersionUpgrade: 1,
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
			synthesizedCV, err := kvserver.SynthesizeClusterVersionFromEngines(
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

	const numStores = 3
	var storeSpecs []base.StoreSpec
	for i := 0; i < numStores; i++ {
		storeSpecs = append(storeSpecs, base.StoreSpec{InMemory: true})
	}

	intercepted := 0
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
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
	if _, err := migrationServer.PurgeOutdatedReplicas(context.Background(), &serverpb.PurgeOutdatedReplicasRequest{
		Version: &clusterversion.TestingBinaryVersion,
	}); err != nil {
		t.Fatal(err)
	}

	if intercepted != numStores {
		t.Fatalf("expected to have GC-ed replicas on %d stores, found %d", numStores, intercepted)
	}
}

// TestUpgradeHappensAfterMigration is a regression test to ensure that
// migrations run prior to attempting to upgrade the cluster to the current
// version.
func TestUpgradeHappensAfterMigrations(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.TestingBinaryMinSupportedVersion,
		false, /* initializeVersion */
	)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: st,
		Knobs: base.TestingKnobs{
			Server: &TestingKnobs{
				BinaryVersionOverride: clusterversion.TestingBinaryMinSupportedVersion,
			},
			SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
				AfterEnsureMigrations: func() {
					// Try to encourage other goroutines to run.
					const N = 100
					for i := 0; i < N; i++ {
						runtime.Gosched()
					}
					require.True(t, st.Version.ActiveVersion(ctx).Less(clusterversion.TestingBinaryVersion))
				},
			},
		},
	})
	sqlutils.MakeSQLRunner(db).
		CheckQueryResultsRetry(t, `
SELECT version = crdb_internal.node_executable_version()
  FROM [SHOW CLUSTER SETTING version]`,
			[][]string{{"true"}})
	s.Stopper().Stop(context.Background())
}
