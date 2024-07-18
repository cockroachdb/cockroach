// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCreateTenantSystemTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// Set up the storage cluster at v1.
	v1 := clusterversion.MinSupported.Version()
	v2 := clusterversion.V24_2_TenantSystemTables.Version()

	settings := cluster.MakeTestingClusterSettingsWithVersions(
		v2,
		v1,
		false, // initializeVersion
	)

	require.NoError(t, clusterversion.Initialize(ctx, v1, &settings.SV))

	t.Log("starting server")
	ts := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Settings:          settings,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         v1,
			},
			// Make the upgrade faster by accelerating jobs.
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer ts.Stopper().Stop(ctx)

	// Set up the tenant cluster at v1.
	tenantSettings := cluster.MakeTestingClusterSettingsWithVersions(
		v2,
		v1,
		false, // initializeVersion
	)

	require.NoError(t,
		clusterversion.Initialize(ctx, clusterversion.MinSupported.Version(), &tenantSettings.SV))
	tenantArgs := base.TestTenantArgs{
		TenantID: roachpb.MustMakeTenantID(10),
		TestingKnobs: base.TestingKnobs{
			// Make the upgrade faster by accelerating jobs.
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
			},
		},
		Settings: tenantSettings,
	}
	tenant, err := ts.TenantController().StartTenant(ctx, tenantArgs)
	require.NoError(t, err)

	// Upgrade the storage cluster to v2
	sysDB := sqlutils.MakeSQLRunner(ts.SQLConn(t))
	sysDB.Exec(t, "SET CLUSTER SETTING version = $1", v2.String())

	// Upgrade the tenant cluster to v2.
	tenantDB := tenant.SQLConn(t)

	checkTable := func(tableName string, shouldExist bool) {
		_, err = tenantDB.Exec(fmt.Sprintf("SELECT * FROM %s", tableName))
		if shouldExist {
			require.NoError(t, err, fmt.Sprintf("%s does not exist", tableName))
		} else {
			require.Error(t, err, fmt.Sprintf("%s exists", tableName))
		}
	}

	checkTables := func(shouldExist bool) {
		checkTable("system.tenants", shouldExist)
		checkTable("system.tenant_settings", shouldExist)
		checkTable("system.tenant_usage", shouldExist)
		checkTable("system.span_configurations", shouldExist)
		checkTable("system.task_payloads", shouldExist)
		checkTable("system.tenant_tasks", shouldExist)
		checkTable("system.tenant_id_seq", shouldExist)
	}

	checkTables(false /* shouldExist */)
	upgrades.Upgrade(t, tenantDB, clusterversion.V24_2_TenantSystemTables, nil, false)
	checkTables(true /* shouldExist */)

	// Check that the system and secondary tenant have the same tables in the
	// system database.
	tenantRunner := sqlutils.MakeSQLRunner(tenantDB)
	systemTableNames := "SELECT name FROM crdb_internal.tables WHERE database_name ='system' ORDER BY name"
	tenantRunner.CheckQueryResults(t,
		systemTableNames,
		sysDB.QueryStr(t, systemTableNames))
}
