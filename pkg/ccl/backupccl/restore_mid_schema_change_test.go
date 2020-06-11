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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestRestoreMidSchemaChanges attempts to RESTORE several BACKUPs that are
// already constructed and store in
// ccl/backupccl/testdata/restore_mid_schema_change. These backups were taken on
// tables that were in the process of performing a schema change. In particular,
// the schema changes were temporarily blocked after it completed its backfill
// stage.
//
// This test ensures that these BACKUPS can be:
// 1) Restore
// 2) Can read the data from the restored table
// 3) The schema changes that were in progress on the table complete and are
// applied.
// 4) Can apply new schema changes. (This is used to ensure that there are no
// more mutations hanging on the table descriptor.)
//
// The test cases are organized based on cluster version of the cluster that
// took the BACKUP. Each test-case represents a BACKUP. They were created using
// the statements provided in the create.sql file in the appropriate testdata
// dir. All backups backed up defaultdb.*, which contain the relevant tables.
// Most backups contain a single table whose name matches the backup name. If
// the backup is expected to contain several tables, the table names will be
// backupName1, backupName2, ...
func TestRestoreMidSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const (
		testdataBase = "testdata/restore_mid_schema_change"
		exportDirs   = testdataBase + "/exports"
	)
	versionDirs, err := ioutil.ReadDir(exportDirs)
	require.NoError(t, err)
	for _, clusterVersionDir := range versionDirs {
		require.True(t, clusterVersionDir.IsDir())
		fullClusterVersionDir, err := filepath.Abs(filepath.Join(exportDirs, clusterVersionDir.Name()))
		require.NoError(t, err)
		backupDirs, err := ioutil.ReadDir(fullClusterVersionDir)
		require.NoError(t, err)
		// In each version folder (e.g. "19.2", "20.1"), there is a backup for each schema change.
		for _, backupDir := range backupDirs {
			fullBackupDir, err := filepath.Abs(filepath.Join(fullClusterVersionDir, backupDir.Name()))
			require.NoError(t, err)
			t.Run(clusterVersionDir.Name()+"-"+backupDir.Name(), restoreMidSchemaChange(fullBackupDir, backupDir.Name()))
		}
	}
}

func restoreMidSchemaChange(backupDir, schemaChangeName string) func(t *testing.T) {
	verify := func(t *testing.T, scName string, sqlDB *sqlutils.SQLRunner) {
		var expectedData [][]string
		tableName := fmt.Sprintf("defaultdb.%s", scName)
		sqlDB.CheckQueryResultsRetry(t, "SELECT * FROM crdb_internal.jobs WHERE job_type = 'SCHEMA CHANGE' AND status <> 'succeeded'", [][]string{})
		numSchemaChangeJobs := 1
		// This enumerates the tests cases and specifies how each case should be
		// handled.
		switch scName {
		case "midaddcol":
			expectedData = [][]string{{"1", "1.3"}, {"2", "1.3"}, {"3", "1.3"}}
		case "midaddconst":
			expectedData = [][]string{{"1"}, {"2"}, {"3"}}
			sqlDB.CheckQueryResults(t, "SELECT count(*) FROM [SHOW CONSTRAINTS FROM defaultdb.midaddconst] WHERE constraint_name = 'my_const'", [][]string{{"1"}})
		case "midaddindex":
			expectedData = [][]string{{"1"}, {"2"}, {"3"}}
			sqlDB.CheckQueryResults(t, "SELECT count(*) FROM [SHOW INDEXES FROM defaultdb.midaddindex] WHERE column_name = 'a'", [][]string{{"1"}})
		case "middropcol":
			expectedData = [][]string{{"1"}, {"1"}, {"1"}, {"2"}, {"2"}, {"2"}, {"3"}, {"3"}, {"3"}}
		case "midmany":
			numSchemaChangeJobs = 3
			expectedData = [][]string{{"1", "1.3"}, {"2", "1.3"}, {"3", "1.3"}}
			sqlDB.CheckQueryResults(t, "SELECT count(*) FROM [SHOW CONSTRAINTS FROM defaultdb.midmany] WHERE constraint_name = 'my_const'", [][]string{{"1"}})
			sqlDB.CheckQueryResults(t, "SELECT count(*) FROM [SHOW INDEXES FROM defaultdb.midmany] WHERE column_name = 'a'", [][]string{{"1"}})
		case "midmultitxn":
			expectedData = [][]string{{"1", "1.3"}, {"2", "1.3"}, {"3", "1.3"}}
			sqlDB.CheckQueryResults(t, "SELECT count(*) FROM [SHOW CONSTRAINTS FROM defaultdb.midmultitxn] WHERE constraint_name = 'my_const'", [][]string{{"1"}})
			sqlDB.CheckQueryResults(t, "SELECT count(*) FROM [SHOW INDEXES FROM defaultdb.midmultitxn] WHERE column_name = 'a'", [][]string{{"1"}})
		case "midmultitable":
			numSchemaChangeJobs = 2
			expectedData = [][]string{{"1", "1.3"}, {"2", "1.3"}, {"3", "1.3"}}
			sqlDB.CheckQueryResults(t, fmt.Sprintf("SELECT * FROM %s1", tableName), expectedData)
			expectedData = [][]string{{"1"}, {"2"}, {"3"}}
			sqlDB.CheckQueryResults(t, fmt.Sprintf("SELECT * FROM %s2", tableName), expectedData)
			tableName += "1"
		case "midprimarykeyswap":
			// The primary key swap will also create a cleanup job.
			numSchemaChangeJobs = 2
			expectedData = [][]string{{"1"}, {"2"}, {"3"}}
		case "midprimarykeyswapcleanup":
			// This backup only contains the cleanup job mentioned above.
			expectedData = [][]string{{"1"}, {"2"}, {"3"}}
		}
		if scName != "midmultitable" {
			sqlDB.CheckQueryResults(t, fmt.Sprintf("SELECT * FROM %s", tableName), expectedData)
		}
		sqlDB.CheckQueryResults(t, "SELECT count(*) FROM crdb_internal.jobs WHERE job_type = 'SCHEMA CHANGE'", [][]string{{strconv.Itoa(numSchemaChangeJobs)}})
		// Ensure that a schema change can complete on the restored table.
		schemaChangeQuery := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT post_restore_const CHECK (a > 0)", tableName)
		sqlDB.Exec(t, schemaChangeQuery)
	}

	return func(t *testing.T) {
		params := base.TestServerArgs{}
		defer func(oldInterval time.Duration) {
			jobs.DefaultAdoptInterval = oldInterval
		}(jobs.DefaultAdoptInterval)
		jobs.DefaultAdoptInterval = 100 * time.Millisecond
		const numAccounts = 1000
		_, _, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(t, singleNode, numAccounts,
			InitNone, base.TestClusterArgs{ServerArgs: params})
		defer cleanup()
		symlink := filepath.Join(dir, "foo")
		err := os.Symlink(backupDir, symlink)
		require.NoError(t, err)
		sqlDB.Exec(t, "USE defaultdb")
		restoreQuery := fmt.Sprintf("RESTORE defaultdb.* from $1")
		log.Infof(context.Background(), "%+v", sqlDB.QueryStr(t, "SHOW BACKUP $1", LocalFoo))
		sqlDB.Exec(t, restoreQuery, LocalFoo)
		verify(t, schemaChangeName, sqlDB)
	}
}
