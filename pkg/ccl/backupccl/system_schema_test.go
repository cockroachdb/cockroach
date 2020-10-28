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
	tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])

	systemTableNames := sqlDB.QueryStr(t, `USE system; SELECT table_name FROM [SHOW TABLES];`)
	for _, systemTableNameRow := range systemTableNames {
		systemTableName := systemTableNameRow[0]
		if systemTableBackupConfiguration[systemTableName].includeInClusterBackup == invalid {
			t.Fatalf("cluster backup inclusion not specified for system table %s", systemTableName)
		}
	}
}
