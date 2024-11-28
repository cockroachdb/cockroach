// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestAllSystemTablesHaveBackupConfig is a test that enumerates all system
// table names and ensures that a config is specified for each tables.
func TestAllSystemTablesHaveBackupConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TestControlsTenantsExplicitly,
			}})
	defer tc.Stopper().Stop(ctx)
	systemSQL := sqlutils.MakeSQLRunner(tc.Conns[0])

	_, tSQL := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{
		TenantID:     roachpb.MustMakeTenantID(10),
		TestingKnobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()},
	})
	defer tSQL.Close()

	secondaryTenantSQL := sqlutils.MakeSQLRunner(tSQL)

	verifySystemTables := func(tableNames [][]string) {
		for _, systemTableNameRow := range tableNames {
			systemTableName := systemTableNameRow[0]
			if systemTableBackupConfiguration[systemTableName].shouldIncludeInClusterBackup == invalidBackupInclusion {
				t.Fatalf("cluster backup inclusion not specified for system table %s", systemTableName)
			}
		}
	}
	tableNamesQuery := `USE system; SELECT table_name FROM [SHOW TABLES];`

	verifySystemTables(systemSQL.QueryStr(t, tableNamesQuery))
	verifySystemTables(secondaryTenantSQL.QueryStr(t, tableNamesQuery))

}

func TestConfigurationDetailsOnlySetForIncludedTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for systemTable, configuration := range systemTableBackupConfiguration {
		if configuration.customRestoreFunc != nil {
			// If some restore options were specified, we probably want to also
			// include in in the set of system tables that are looked at by cluster
			// backup/restore.
			if optInToClusterBackup != configuration.shouldIncludeInClusterBackup {
				t.Fatalf("custom restore function specified for table %q, but it's not included in cluster backups",
					systemTable)
			}
		}
	}
}
