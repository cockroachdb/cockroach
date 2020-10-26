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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestRestoreMidSchemaChanges attempts to RESTORE several BACKUPs that are
// already constructed and store in
// ccl/backupccl/testdata/restore_mid_schema_change. These backups were taken on
// tables that were in the process of performing a schema change. In particular,
// the schema changes were temporarily blocked either before of after it
// completed its backfill stage.
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
// dir. All backups backed up either to defaultdb.*, which contain the relevant
// tables or a cluster backup. Most backups contain a single table whose name
// matches the backup name. If the backup is expected to contain several tables,
// the table names will be backupName1, backupName2, ...
func TestRestoreMidSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const (
		testdataBase = "testdata/restore_mid_schema_change"
		exportDirs   = testdataBase + "/exports"
	)
	for _, isClusterRestore := range []bool{true, false} {
		name := "table"
		if isClusterRestore {
			name = "cluster"
		}
		t.Run(name, func(t *testing.T) {
			// blockLocations indicates whether the backup taken was blocked before or
			// after the backfill portion of the schema change.
			for _, blockLocation := range []string{"before", "after"} {
				t.Run(blockLocation, func(t *testing.T) {
					versionDirs, err := ioutil.ReadDir(filepath.Join(exportDirs, blockLocation))
					require.NoError(t, err)

					for _, clusterVersionDir := range versionDirs {
						if clusterVersionDir.Name() == "19.2" && isClusterRestore {
							// 19.2 does not support cluster backups.
							continue
						}

						t.Run(clusterVersionDir.Name(), func(t *testing.T) {
							require.True(t, clusterVersionDir.IsDir())
							fullClusterVersionDir, err := filepath.Abs(
								filepath.Join(exportDirs, blockLocation, clusterVersionDir.Name()))
							require.NoError(t, err)

							// In each version folder (e.g. "19.2", "20.1"), there is a backup for
							// each schema change.
							backupDirs, err := ioutil.ReadDir(fullClusterVersionDir)
							require.NoError(t, err)

							for _, backupDir := range backupDirs {
								fullBackupDir, err := filepath.Abs(filepath.Join(fullClusterVersionDir, backupDir.Name()))
								require.NoError(t, err)
								t.Run(backupDir.Name(), restoreMidSchemaChange(fullBackupDir, backupDir.Name(), isClusterRestore))
							}
						})
					}
				})
			}
		})
	}
}

func verifyMidSchemaChange(
	t *testing.T, scName string, sqlDB *sqlutils.SQLRunner, isClusterRestore bool,
) {
	var expectedData [][]string
	tableName := fmt.Sprintf("defaultdb.%s", scName)
	// numJobsInCluster is the number of completed jobs that will be restored
	// during a cluster restore.
	var numJobsInCluster int
	expNumSchemaChangeJobs := 1
	// This enumerates the tests cases and specifies how each case should be
	// handled.
	switch scName {
	case "midaddcol":
		numJobsInCluster = 1 // the CREATE TABLE job
		expectedData = [][]string{{"1", "1.3"}, {"2", "1.3"}, {"3", "1.3"}}
	case "midaddconst":
		numJobsInCluster = 1 // the CREATE TABLE job
		expectedData = [][]string{{"1"}, {"2"}, {"3"}}
		sqlDB.CheckQueryResults(t, "SELECT count(*) FROM [SHOW CONSTRAINTS FROM defaultdb.midaddconst] WHERE constraint_name = 'my_const'", [][]string{{"1"}})
	case "midaddindex":
		numJobsInCluster = 1 // the CREATE TABLE job
		expectedData = [][]string{{"1"}, {"2"}, {"3"}}
		sqlDB.CheckQueryResults(t, "SELECT count(*) FROM [SHOW INDEXES FROM defaultdb.midaddindex] WHERE column_name = 'a'", [][]string{{"1"}})
	case "middropcol":
		numJobsInCluster = 1 // the CREATE TABLE job
		expectedData = [][]string{{"1"}, {"1"}, {"1"}, {"2"}, {"2"}, {"2"}, {"3"}, {"3"}, {"3"}}
	case "midmany":
		numJobsInCluster = 1 // the CREATE TABLE job
		expNumSchemaChangeJobs = 3
		expectedData = [][]string{{"1", "1.3"}, {"2", "1.3"}, {"3", "1.3"}}
		sqlDB.CheckQueryResults(t, "SELECT count(*) FROM [SHOW CONSTRAINTS FROM defaultdb.midmany] WHERE constraint_name = 'my_const'", [][]string{{"1"}})
		sqlDB.CheckQueryResults(t, "SELECT count(*) FROM [SHOW INDEXES FROM defaultdb.midmany] WHERE column_name = 'a'", [][]string{{"1"}})
	case "midmultitxn":
		numJobsInCluster = 1 // the CREATE TABLE job
		expectedData = [][]string{{"1", "1.3"}, {"2", "1.3"}, {"3", "1.3"}}
		sqlDB.CheckQueryResults(t, "SELECT count(*) FROM [SHOW CONSTRAINTS FROM defaultdb.midmultitxn] WHERE constraint_name = 'my_const'", [][]string{{"1"}})
		sqlDB.CheckQueryResults(t, "SELECT count(*) FROM [SHOW INDEXES FROM defaultdb.midmultitxn] WHERE column_name = 'a'", [][]string{{"1"}})
	case "midmultitable":
		numJobsInCluster = 2 // the 2 CREATE TABLE jobs
		expNumSchemaChangeJobs = 2
		expectedData = [][]string{{"1", "1.3"}, {"2", "1.3"}, {"3", "1.3"}}
		sqlDB.CheckQueryResults(t, fmt.Sprintf("SELECT * FROM %s1", tableName), expectedData)
		expectedData = [][]string{{"1"}, {"2"}, {"3"}}
		sqlDB.CheckQueryResults(t, fmt.Sprintf("SELECT * FROM %s2", tableName), expectedData)
		tableName += "1"
	case "midprimarykeyswap":
		numJobsInCluster = 2 // the CREATE TABLE job and the ALTER COLUMN
		// The primary key swap will also create a cleanup job.
		expNumSchemaChangeJobs = 2
		expectedData = [][]string{{"1"}, {"2"}, {"3"}}
	case "midprimarykeyswapcleanup":
		// The CREATE TABLE job, the ALTER COLUMN, and the original ALTER PRIMARY
		// KEY that is being cleaned up.
		numJobsInCluster = 3
		// This backup only contains the cleanup job mentioned above.
		expectedData = [][]string{{"1"}, {"2"}, {"3"}}
	}
	if scName != "midmultitable" {
		sqlDB.CheckQueryResults(t, fmt.Sprintf("SELECT * FROM %s", tableName), expectedData)
	}
	if isClusterRestore {
		// If we're performing a cluster restore, we also need to include the drop
		// crdb_temp_system job.
		expNumSchemaChangeJobs++
		// And the create table jobs included from the backups.
		expNumSchemaChangeJobs += numJobsInCluster
	}
	schemaChangeJobs := sqlDB.QueryStr(t, "SELECT description FROM crdb_internal.jobs WHERE job_type = 'SCHEMA CHANGE'")
	require.Equal(t, expNumSchemaChangeJobs, len(schemaChangeJobs),
		"Expected %d schema change jobs but found %v", expNumSchemaChangeJobs, schemaChangeJobs)
	if isClusterRestore {
		// Cluster restores should be restoring the exact job entries that were
		// backed up, and therefore should not create jobs that contains "RESTORING"
		// in the description.
		schemaChangeJobs := sqlDB.QueryStr(t,
			"SELECT description FROM crdb_internal.jobs WHERE job_type = 'SCHEMA CHANGE' AND description NOT LIKE '%RESTORING%'")
		require.Equal(t, expNumSchemaChangeJobs, len(schemaChangeJobs),
			"Expected %d schema change jobs but found %v", expNumSchemaChangeJobs, schemaChangeJobs)
	} else {
		// Non-cluster restores should create jobs with "RESTORE" in the job
		// description.
		schemaChangeJobs := sqlDB.QueryStr(t,
			"SELECT description FROM crdb_internal.jobs WHERE job_type = 'SCHEMA CHANGE' AND description LIKE '%RESTORING%'")
		require.Equal(t, expNumSchemaChangeJobs, len(schemaChangeJobs),
			"Expected %d schema change jobs but found %v", expNumSchemaChangeJobs, schemaChangeJobs)
	}
	// Ensure that a schema change can complete on the restored table.
	schemaChangeQuery := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT post_restore_const CHECK (a > 0)", tableName)
	sqlDB.Exec(t, schemaChangeQuery)
}

func restoreMidSchemaChange(
	backupDir, schemaChangeName string, isClusterRestore bool,
) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		defer jobs.TestingSetAdoptAndCancelIntervals(100*time.Millisecond, 100*time.Millisecond)()

		dir, dirCleanupFn := testutils.TempDir(t)
		params := base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{ExternalIODir: dir},
		}
		tc := testcluster.StartTestCluster(t, singleNode, params)
		defer func() {
			tc.Stopper().Stop(ctx)
			dirCleanupFn()
		}()
		sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])

		symlink := filepath.Join(dir, "foo")
		err := os.Symlink(backupDir, symlink)
		require.NoError(t, err)

		sqlDB.Exec(t, "USE defaultdb")
		restoreQuery := fmt.Sprintf("RESTORE defaultdb.* from $1")
		if isClusterRestore {
			restoreQuery = fmt.Sprintf("RESTORE from $1")
		}
		log.Infof(context.Background(), "%+v", sqlDB.QueryStr(t, "SHOW BACKUP $1", LocalFoo))
		sqlDB.Exec(t, restoreQuery, LocalFoo)
		sqlDB.CheckQueryResultsRetry(t, "SELECT * FROM crdb_internal.jobs WHERE job_type = 'SCHEMA CHANGE' AND status <> 'succeeded'", [][]string{})
		verifyMidSchemaChange(t, schemaChangeName, sqlDB, isClusterRestore)
	}
}
