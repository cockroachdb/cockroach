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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
					DisableAutomaticVersionUpgrade: 1,
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
		// Prevent a logging assertion that the server ID is initialized multiple times.
		log.TestingClearServerIdentifiers()
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
			log.TestingClearServerIdentifiers()
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
			log.TestingClearServerIdentifiers()
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
