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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
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

	skip.UnderRaceWithIssue(t, 56584)

	var (
		testdataBase = testutils.TestDataPath(t, "restore_mid_schema_change")
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
								t.Run(backupDir.Name(), restoreMidSchemaChange(fullBackupDir, backupDir.Name(), isClusterRestore, blockLocation == "after"))
							}
						})
					}
				})
			}
		})
	}
}

// expectedSCJobCount returns the expected number of schema change jobs
// we expect to find.
func expectedSCJobCount(scName string, isClusterRestore, after bool) int {
	// The number of schema change under test. These will be the ones that are
	// synthesized in database restore.
	var expNumSCJobs int
	var numBackgroundSCJobs int

	// Some test cases may have more than 1 background schema change job.
	switch scName {
	case "midmany":
		numBackgroundSCJobs = 1 // the create table
		// This test runs 3 schema changes on a single table.
		expNumSCJobs = 3
	case "midmultitable":
		numBackgroundSCJobs = 2 // this test creates 2 tables
		expNumSCJobs = 2        // this test perform a schema change for each table
	case "midprimarykeyswap":
		// Create table + alter column is done in the prep stage of this test.
		numBackgroundSCJobs = 2
		// PK change + PK cleanup
		expNumSCJobs = 2
		if isClusterRestore && after {
			expNumSCJobs = 1
		}
	case "midprimarykeyswapcleanup":
		// This test performs an ALTER COLUMN, and the original ALTER PRIMARY
		// KEY that is being cleaned up.
		numBackgroundSCJobs = 3
		expNumSCJobs = 1
	default:
		// Most test cases only have 1 schema change under test.
		expNumSCJobs = 1
		// Most test cases have just a CREATE TABLE job that created the table
		// under test.
		numBackgroundSCJobs = 1
	}

	// We drop defaultdb and postgres for full cluster restores
	numBackgroundDropDatabaseSCJobs := 2
	// Since we're doing a cluster restore, we need to account for all of
	// the schema change jobs that existed in the backup.
	if isClusterRestore {
		expNumSCJobs += numBackgroundSCJobs + numBackgroundDropDatabaseSCJobs

		// If we're performing a cluster restore, we also need to include the drop
		// crdb_temp_system job.
		expNumSCJobs++
	}

	return expNumSCJobs
}

func validateTable(
	t *testing.T, kvDB *kv.DB, sqlDB *sqlutils.SQLRunner, dbName string, tableName string,
) {
	desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, dbName, tableName)
	// There should be no mutations on these table descriptors at this point.
	require.Equal(t, 0, len(desc.TableDesc().Mutations))

	var rowCount int
	sqlDB.QueryRow(t, fmt.Sprintf(`SELECT count(*) FROM %s.%s`, dbName, tableName)).Scan(&rowCount)
	require.Greater(t, rowCount, 0, "expected table to have some rows")
	// The number of entries in all indexes should be the same.
	for _, index := range desc.AllIndexes() {
		var indexCount int
		sqlDB.QueryRow(t, fmt.Sprintf(`SELECT count(*) FROM %s.%s@[%d]`, dbName, tableName, index.GetID())).Scan(&indexCount)
		require.Equal(t, rowCount, indexCount, `index should have the same number of rows as PK`)
	}
}

func getTablesInTest(scName string) (tableNames []string) {
	// Most of the backups name their table the test name.
	tableNames = []string{scName}

	// Some create multiple tables thouhg.
	switch scName {
	case "midmultitable":
		tableNames = []string{"midmultitable1", "midmultitable2"}
	}

	return
}

func verifyMidSchemaChange(
	t *testing.T, scName string, kvDB *kv.DB, sqlDB *sqlutils.SQLRunner, isClusterRestore, after bool,
) {
	tables := getTablesInTest(scName)

	// Check that we are left with the expected number of schema change jobs.
	expNumSchemaChangeJobs := expectedSCJobCount(scName, isClusterRestore, after)
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

	for _, tableName := range tables {
		validateTable(t, kvDB, sqlDB, "defaultdb", tableName)
		// Ensure that a schema change can complete on the restored table.
		schemaChangeQuery := fmt.Sprintf("ALTER TABLE defaultdb.%s ADD CONSTRAINT post_restore_const CHECK (a > 0)", tableName)
		sqlDB.Exec(t, schemaChangeQuery)
	}

}

func restoreMidSchemaChange(
	backupDir, schemaChangeName string, isClusterRestore bool, after bool,
) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		dir, dirCleanupFn := testutils.TempDir(t)
		params := base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				ExternalIODir: dir,
				Knobs:         base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()},
			},
		}
		tc := testcluster.StartTestCluster(t, singleNode, params)
		defer func() {
			tc.Stopper().Stop(ctx)
			dirCleanupFn()
		}()
		sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
		kvDB := tc.Server(0).DB()

		symlink := filepath.Join(dir, "foo")
		err := os.Symlink(backupDir, symlink)
		require.NoError(t, err)

		sqlDB.Exec(t, "USE defaultdb")
		restoreQuery := "RESTORE defaultdb.* from $1"
		if isClusterRestore {
			restoreQuery = "RESTORE from $1"
		}
		log.Infof(context.Background(), "%+v", sqlDB.QueryStr(t, "SHOW BACKUP $1", localFoo))
		sqlDB.Exec(t, restoreQuery, localFoo)
		// Wait for all jobs to terminate. Some may fail since we don't restore
		// adding spans.
		sqlDB.CheckQueryResultsRetry(t, "SELECT * FROM crdb_internal.jobs WHERE job_type = 'SCHEMA CHANGE' AND NOT (status = 'succeeded' OR status = 'failed')", [][]string{})
		verifyMidSchemaChange(t, schemaChangeName, kvDB, sqlDB, isClusterRestore, after)
	}
}
