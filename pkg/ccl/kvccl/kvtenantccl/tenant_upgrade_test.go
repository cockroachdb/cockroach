// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvtenantccl_test

import (
	"context"
	gosql "database/sql"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/migration/migrations"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

// TestTenantUpgrade exercises the case where a system tenant is in a
// non-finalized version state and creates a tenant. The test ensures
// that that newly created tenant begins in that same version.
//
// The first subtest creates the tenant in the mixed version state,
// then upgrades the system tenant, then upgrades the secondary tenant,
// and ensures everything is happy. It then restarts the tenant and ensures
// that the cluster version is properly set.
//
// The second subtest creates a new tenant after the system tenant has been
// upgraded and ensures that it is created at the final cluster version. It
// also verifies that the version is correct after a restart
func TestTenantUpgrade(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.TestingBinaryMinSupportedVersion,
		false, // initializeVersion
	)
	// Initialize the version to the BinaryMinSupportedVersion.
	require.NoError(t, clusterversion.Initialize(ctx,
		clusterversion.TestingBinaryMinSupportedVersion, &settings.SV))
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.TestingBinaryMinSupportedVersion,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	connectToTenant := func(t *testing.T, addr string) (_ *gosql.DB, cleanup func()) {
		pgURL, cleanupPGUrl := sqlutils.PGUrl(t, addr, "Tenant", url.User(security.RootUser))
		tenantDB, err := gosql.Open("postgres", pgURL.String())
		require.NoError(t, err)
		return tenantDB, func() {
			tenantDB.Close()
			cleanupPGUrl()
		}
	}
	mkTenant := func(t *testing.T, id uint64) (tenantDB *gosql.DB, cleanup func()) {
		settings := cluster.MakeTestingClusterSettingsWithVersions(
			clusterversion.TestingBinaryVersion,
			clusterversion.TestingBinaryMinSupportedVersion,
			false, // initializeVersion
		)
		// Initialize the version to the minimum it could be.
		require.NoError(t, clusterversion.Initialize(ctx,
			clusterversion.TestingBinaryMinSupportedVersion, &settings.SV))
		tenantArgs := base.TestTenantArgs{
			TenantID:     roachpb.MakeTenantID(id),
			TestingKnobs: base.TestingKnobs{},
			Settings:     settings,
		}
		tenant, err := tc.Server(0).StartTenant(ctx, tenantArgs)
		require.NoError(t, err)
		return connectToTenant(t, tenant.SQLAddr())
	}

	t.Run("upgrade tenant", func(t *testing.T) {
		// Create a tenant before upgrading anything and verify its version.
		const initialTenantID = 10
		initialTenant, cleanup := mkTenant(t, initialTenantID)
		initialTenantRunner := sqlutils.MakeSQLRunner(initialTenant)

		// Ensure that the tenant works.
		initialTenantRunner.CheckQueryResults(t, "SHOW CLUSTER SETTING version",
			[][]string{{clusterversion.TestingBinaryMinSupportedVersion.String()}})
		initialTenantRunner.Exec(t, "CREATE TABLE t (i INT PRIMARY KEY)")
		initialTenantRunner.Exec(t, "INSERT INTO t VALUES (1), (2)")

		// Upgrade the host cluster.
		sqlutils.MakeSQLRunner(tc.ServerConn(0)).Exec(t,
			"SET CLUSTER SETTING version = $1",
			clusterversion.TestingBinaryVersion.String())

		// Ensure that the tenant still works.
		initialTenantRunner.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1"}, {"2"}})

		// Upgrade the tenant cluster.
		initialTenantRunner.Exec(t,
			"SET CLUSTER SETTING version = $1",
			clusterversion.TestingBinaryVersion.String())

		// Ensure that the tenant still works.
		initialTenantRunner.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1"}, {"2"}})
		initialTenantRunner.CheckQueryResults(t, "SHOW CLUSTER SETTING version",
			[][]string{{clusterversion.TestingBinaryVersion.String()}})

		// Restart the tenant and ensure that the version is correct.
		cleanup()
		{
			tenantServer, err := tc.Server(0).StartTenant(ctx, base.TestTenantArgs{
				TenantID: roachpb.MakeTenantID(initialTenantID),
				Existing: true,
			})
			require.NoError(t, err)
			initialTenant, cleanup = connectToTenant(t, tenantServer.SQLAddr())
			defer cleanup()
			initialTenantRunner = sqlutils.MakeSQLRunner(initialTenant)
		}
		initialTenantRunner.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1"}, {"2"}})
		initialTenantRunner.CheckQueryResults(t, "SHOW CLUSTER SETTING version",
			[][]string{{clusterversion.TestingBinaryVersion.String()}})
	})

	t.Run("post-upgrade tenant", func(t *testing.T) {
		// Create a new tenant and ensure it has the right version.
		const postUpgradeTenantID = 11
		postUpgradeTenant, cleanup := mkTenant(t, postUpgradeTenantID)
		sqlutils.MakeSQLRunner(postUpgradeTenant).CheckQueryResults(t,
			"SHOW CLUSTER SETTING version",
			[][]string{{clusterversion.TestingBinaryVersion.String()}})

		// Restart the new tenant and ensure it has the right version.
		cleanup()
		{
			tenantServer, err := tc.Server(0).StartTenant(ctx, base.TestTenantArgs{
				TenantID: roachpb.MakeTenantID(postUpgradeTenantID),
				Existing: true,
			})
			require.NoError(t, err)
			postUpgradeTenant, cleanup = connectToTenant(t, tenantServer.SQLAddr())
			defer cleanup()
		}
		sqlutils.MakeSQLRunner(postUpgradeTenant).CheckQueryResults(t,
			"SHOW CLUSTER SETTING version",
			[][]string{{clusterversion.TestingBinaryVersion.String()}})
	})

}

// Returns two versions v0, v1, v2 which correspond to adjacent releases. v1 will
// equal the TestingBinaryMinSupportedVersion to avoid rot in tests using this
// (as we retire old versions).
func v0v1v2() (roachpb.Version, roachpb.Version, roachpb.Version) {
	v1 := clusterversion.TestingBinaryVersion
	v0 := clusterversion.TestingBinaryMinSupportedVersion
	v2 := clusterversion.TestingBinaryVersion
	if v0.Minor > 0 {
		v0.Minor--
	} else {
		v0.Major--
	}
	if v1.Minor > 0 {
		v1.Minor--
	} else {
		v1.Major--
	}
	return v0, v1, v2
}

// TestTenantUpgradeFailure exercises cases where the tenant dies
// between version upgrades.
func TestTenantUpgradeFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Contains information for starting a tenant
	// and maintaining a stopper.
	type tenantInfo struct {
		v2onMigrationStopper *stop.Stopper
		tenantArgs           *base.TestTenantArgs
	}
	v0, v1, v2 := v0v1v2()
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.TestingBinaryMinSupportedVersion,
		false, // initializeVersion
	)
	// Initialize the version to the BinaryMinSupportedVersion.
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.TestingBinaryMinSupportedVersion,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)
	// Channel for stopping a tenant.
	tenantStopperChannel := make(chan struct{})
	startAndConnectToTenant := func(t *testing.T, tenantInfo *tenantInfo) (_ *gosql.DB, cleanup func()) {
		tenant, err := tc.Server(0).StartTenant(ctx, *tenantInfo.tenantArgs)
		require.NoError(t, err)
		tenantInfo.tenantArgs.Existing = true
		pgURL, cleanupPGUrl := sqlutils.PGUrl(t, tenant.SQLAddr(), "Tenant", url.User(security.RootUser))
		tenantDB, err := gosql.Open("postgres", pgURL.String())
		require.NoError(t, err)
		return tenantDB, func() {
			tenantDB.Close()
			cleanupPGUrl()
		}
	}
	mkTenant := func(t *testing.T, id uint64, existing bool) *tenantInfo {
		settings := cluster.MakeTestingClusterSettingsWithVersions(
			v2,
			v0,
			false, // initializeVersion
		)
		v2onMigrationStopper := stop.NewStopper()
		// Initialize the version to the minimum it could be.
		require.NoError(t, clusterversion.Initialize(ctx,
			clusterversion.TestingBinaryMinSupportedVersion, &settings.SV))
		tenantArgs := base.TestTenantArgs{
			Stopper:  v2onMigrationStopper,
			TenantID: roachpb.MakeTenantID(id),
			Existing: existing,
			TestingKnobs: base.TestingKnobs{
				MigrationManager: &migration.TestingKnobs{
					ListBetweenOverride: func(from, to clusterversion.ClusterVersion) []clusterversion.ClusterVersion {
						return []clusterversion.ClusterVersion{{Version: v1}, {Version: v2}}
					},
					RegistryOverride: func(cv clusterversion.ClusterVersion) (migration.Migration, bool) {
						switch cv.Version {
						case v1:
							return migration.NewTenantMigration("testing", clusterversion.ClusterVersion{
								Version: v1,
							},
								migrations.NoPrecondition,
								func(
									ctx context.Context, version clusterversion.ClusterVersion, deps migration.TenantDeps, _ *jobs.Job,
								) error {
									return nil
								}), true
						case v2:
							return migration.NewTenantMigration("testing next", clusterversion.ClusterVersion{
								Version: v2,
							},
								migrations.NoPrecondition,
								func(
									ctx context.Context, version clusterversion.ClusterVersion, deps migration.TenantDeps, _ *jobs.Job,
								) error {
									tenantStopperChannel <- struct{}{}
									return nil
								}), true
						default:
							panic("Unexpected version number observed.")
						}
					},
				},
			},
			Settings: settings,
		}
		return &tenantInfo{tenantArgs: &tenantArgs,
			v2onMigrationStopper: v2onMigrationStopper}
	}

	t.Run("upgrade tenant have it crash then resume", func(t *testing.T) {
		// Create a tenant before upgrading anything and verify its version.
		const initialTenantID = 10
		tenantInfo := mkTenant(t, initialTenantID, false /*existing*/)
		tenant, cleanup := startAndConnectToTenant(t, tenantInfo)
		initialTenantRunner := sqlutils.MakeSQLRunner(tenant)
		// Ensure that the tenant works.
		initialTenantRunner.CheckQueryResults(t, "SHOW CLUSTER SETTING version",
			[][]string{{clusterversion.TestingBinaryMinSupportedVersion.String()}})
		initialTenantRunner.Exec(t, "CREATE TABLE t (i INT PRIMARY KEY)")
		initialTenantRunner.Exec(t, "INSERT INTO t VALUES (1), (2)")
		// Upgrade the host cluster and have it crash on v1.
		sqlutils.MakeSQLRunner(tc.ServerConn(0)).Exec(t,
			"SET CLUSTER SETTING version = $1",
			v2.String())
		// Ensure that the tenant still works.
		initialTenantRunner.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1"}, {"2"}})
		// Use to wait for tenant crash leading to a clean up.
		waitForTenantClose := make(chan struct{})
		// Cause the upgrade to crash on v1.
		go func() {
			<-tenantStopperChannel
			tenant.Close()
			tenantInfo.v2onMigrationStopper.Stop(ctx)
			waitForTenantClose <- struct{}{}
		}()
		// Upgrade the tenant cluster, but the upgrade
		// will fail on v1.
		initialTenantRunner.ExpectErr(t,
			".*(database is closed|failed to connect|closed network connection)+",
			"SET CLUSTER SETTING version = $1",
			clusterversion.TestingBinaryVersion.String())
		<-waitForTenantClose
		cleanup()
		tenantInfo = mkTenant(t, initialTenantID, true /*existing*/)
		tenant, cleanup = startAndConnectToTenant(t, tenantInfo)
		initialTenantRunner = sqlutils.MakeSQLRunner(tenant)
		// Ensure that the tenant still works and the target
		// version wasn't reached.
		initialTenantRunner.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1"}, {"2"}})
		initialTenantRunner.CheckQueryResults(t, "SHOW CLUSTER SETTING version",
			[][]string{{v1.String()}})

		// Restart the tenant and ensure that the version is correct.
		cleanup()
		{
			tca, cleanup := startAndConnectToTenant(t, tenantInfo)
			defer cleanup()
			initialTenantRunner = sqlutils.MakeSQLRunner(tca)
		}
		// Keep trying to resume the stopper channel until the channel is closed,
		// since we may repeatedly wait on it due to transaction retries. In
		// the other case the stopper is used, so no such risk exists.
		go func() {
			for {
				_, ok := <-tenantStopperChannel
				if !ok {
					return
				}
			}
		}()
		// Upgrade the tenant cluster.
		initialTenantRunner.Exec(t,
			"SET CLUSTER SETTING version = $1",
			clusterversion.TestingBinaryVersion.String())
		close(tenantStopperChannel)
		// Validate the target version has been reached.
		initialTenantRunner.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1"}, {"2"}})
		initialTenantRunner.CheckQueryResults(t, "SHOW CLUSTER SETTING version",
			[][]string{{v2.String()}})
		tenantInfo.v2onMigrationStopper.Stop(ctx)
	})
}
