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
// that that newly created tenant begins in that same version. The test
// the upgrades the system tenant, upgrades the secondary tenant, and
// ensures everything is happy. Finally, the test creates a new tenant
// and ensures that it is created at the final cluster version.
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
		tenant, err := tc.Server(0).StartTenant(tenantArgs)
		require.NoError(t, err)
		pgURL, cleanupPGUrl := sqlutils.PGUrl(t, tenant.SQLAddr(), "Tenant", url.User(security.RootUser))
		tenantDB, err = gosql.Open("postgres", pgURL.String())
		require.NoError(t, err)
		return tenantDB, func() {
			tenantDB.Close()
			cleanupPGUrl()
		}
	}

	// Create a tenant before upgrading anything and verify its version.
	initialTenant, cleanup := mkTenant(t, 10)
	defer cleanup()
	{
		var versionStr string
		require.NoError(t, initialTenant.QueryRow("SHOW CLUSTER SETTING version").
			Scan(&versionStr))
		require.Equal(t, clusterversion.TestingBinaryMinSupportedVersion.String(), versionStr)
	}

	// Upgrade the host cluster.
	{
		_, err := tc.ServerConn(0).Exec("SET CLUSTER SETTING version = $1",
			clusterversion.TestingBinaryVersion.String())
		require.NoError(t, err)
	}

	// Upgrade the tenant cluster.
	{
		_, err := initialTenant.Exec("SET CLUSTER SETTING version = $1",
			clusterversion.TestingBinaryVersion.String())
		require.NoError(t, err)
	}

	// Create a new tenant and ensure it has the right version.
	postUpgradeTenant, cleanup := mkTenant(t, 11)
	defer cleanup()
	{
		var versionStr string
		require.NoError(t, postUpgradeTenant.QueryRow("SHOW CLUSTER SETTING version").
			Scan(&versionStr))
		require.Equal(t, clusterversion.TestingBinaryVersion.String(), versionStr)
	}
}
