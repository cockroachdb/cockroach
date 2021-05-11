// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
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
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])

	systemTableNames := sqlDB.QueryStr(t, `USE system; SELECT table_name FROM [SHOW TABLES];`)
	for _, systemTableNameRow := range systemTableNames {
		systemTableName := systemTableNameRow[0]
		if systemTableBackupConfiguration[systemTableName].shouldIncludeInClusterBackup == invalidBackupInclusion {
			t.Fatalf("cluster backup inclusion not specified for system table %s", systemTableName)
		}
	}
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
