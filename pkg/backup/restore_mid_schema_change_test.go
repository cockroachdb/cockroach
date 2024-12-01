// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/stretchr/testify/require"
)

func TestRestoreMidSchemaChangeClusterSchemaOnly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRaceWithIssue(t, 56584)

	runTestRestoreMidSchemaChange(t, true, true)
}

func TestRestoreMidSchemaChangeCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRaceWithIssue(t, 56584)

	runTestRestoreMidSchemaChange(t, false, true)
}
func TestRestoreMidSchemaChangeSchemaOnly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRaceWithIssue(t, 56584)

	runTestRestoreMidSchemaChange(t, true, false)
}
func TestRestoreMidSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRaceWithIssue(t, 56584)

	runTestRestoreMidSchemaChange(t, false, false)
}

// runTestRestoreMidSchemaChange attempts to RESTORE several BACKUPs that are
// already constructed and store in
// backup/testdata/restore_mid_schema_change. These backups were taken on
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
func runTestRestoreMidSchemaChange(t *testing.T, isSchemaOnly, isClusterRestore bool) {
	var (
		testdataBase = datapathutils.TestDataPath(t, "restore_mid_schema_change")
		exportDirs   = testdataBase + "/exports"
	)
	name := "regular-"
	if isSchemaOnly {
		name = "schema-only-"
	}
	name = name + "table"
	if isClusterRestore {
		name = name + "cluster"
	}
	t.Run(name, func(t *testing.T) {
		// blockLocations indicates whether the backup taken was blocked before or
		// after the backfill portion of the schema change.
		for _, blockLocation := range []string{"before", "after"} {
			t.Run(blockLocation, func(t *testing.T) {
				versionDirs, err := os.ReadDir(filepath.Join(exportDirs, blockLocation))
				require.NoError(t, err)
				for _, clusterVersionDir := range versionDirs {
					clusterVersion, err := parseMajorVersion(clusterVersionDir.Name())
					require.NoError(t, err)

					t.Run(clusterVersionDir.Name(), func(t *testing.T) {
						require.True(t, clusterVersionDir.IsDir())
						fullClusterVersionDir, err := filepath.Abs(
							filepath.Join(exportDirs, blockLocation, clusterVersionDir.Name()))
						require.NoError(t, err)

						// In each version folder (e.g. "19.2", "20.1"), there is a backup for
						// each schema change.
						backupDirs, err := os.ReadDir(fullClusterVersionDir)
						require.NoError(t, err)

						for _, backupDir := range backupDirs {
							fullBackupDir, err := filepath.Abs(filepath.Join(fullClusterVersionDir, backupDir.Name()))
							require.NoError(t, err)
							t.Run(backupDir.Name(), restoreMidSchemaChange(fullBackupDir, backupDir.Name(),
								isClusterRestore, isSchemaOnly, clusterVersion))
						}
					})
				}
			})
		}
	})
}

// parseMajorVersion parses our major-versioned directory names as if they were
// full crdb versions.
func parseMajorVersion(verStr string) (*version.Version, error) {
	return version.Parse(fmt.Sprintf("v%s.0", verStr))
}

// expectedSCJobCount returns the expected number of schema change jobs
// we expect to find.
func expectedSCJobCount(scName string, ver *version.Version) int {
	// The number of schema change under test. These will be the ones that are
	// synthesized in database restore.
	var expNumSCJobs int

	// Some test cases may have more than 1 background schema change job.
	switch scName {
	case "midmany":
		expNumSCJobs = 3
	case "midmultitable":
		expNumSCJobs = 2 // this test perform a schema change for each table
	case "midprimarykeyswap":
		// PK change and PK cleanup
		expNumSCJobs = 2
	case "midprimarykeyswapcleanup":
		expNumSCJobs = 2
	default:
		// Most test cases only have 1 schema change under test.
		expNumSCJobs = 1
	}

	return expNumSCJobs
}

func validateTable(
	t *testing.T,
	tc *testcluster.TestCluster,
	sqlDB *sqlutils.SQLRunner,
	dbName string,
	tableName string,
	isSchemaOnly bool,
) {
	srv := tc.ApplicationLayer(0)
	desc := desctestutils.TestingGetPublicTableDescriptor(srv.DB(), srv.Codec(), dbName, tableName)
	// There should be no mutations on these table descriptors at this point.
	require.Equal(t, 0, len(desc.TableDesc().Mutations))

	var rowCount int
	sqlDB.QueryRow(t, fmt.Sprintf(`SELECT count(*) FROM %s.%s`, dbName, tableName)).Scan(&rowCount)
	if isSchemaOnly {
		require.Equal(t, rowCount, 0, "expected table to have no rows")
	} else {
		require.Greater(t, rowCount, 0, "expected table to have some rows")
	}
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
	t *testing.T,
	scName string,
	tc *testcluster.TestCluster,
	sqlDB *sqlutils.SQLRunner,
	isSchemaOnly bool,
	majorVer *version.Version,
) {
	tables := getTablesInTest(scName)

	// Check that we are left with the expected number of schema change jobs.
	expNumSchemaChangeJobs := expectedSCJobCount(scName, majorVer)

	synthesizedSchemaChangeJobs := sqlDB.QueryStr(t,
		`SELECT description FROM "".crdb_internal.jobs WHERE job_type = 'SCHEMA CHANGE' AND description LIKE '%RESTORING%'`)
	require.Equal(t, expNumSchemaChangeJobs, len(synthesizedSchemaChangeJobs),
		"Expected %d schema change jobs but found %v", expNumSchemaChangeJobs, synthesizedSchemaChangeJobs)

	for _, tableName := range tables {
		validateTable(t, tc, sqlDB, "defaultdb", tableName, isSchemaOnly)
		// Ensure that a schema change can complete on the restored table.
		schemaChangeQuery := fmt.Sprintf("ALTER TABLE defaultdb.%s ADD CONSTRAINT post_restore_const CHECK (a > 0)", tableName)
		sqlDB.Exec(t, schemaChangeQuery)
	}

}

func restoreMidSchemaChange(
	backupDir, schemaChangeName string,
	isClusterRestore bool,
	isSchemaOnly bool,
	majorVer *version.Version,
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

		symlink := filepath.Join(dir, "foo")
		err := os.Symlink(backupDir, symlink)
		require.NoError(t, err)

		sqlDB.Exec(t, "USE defaultdb")
		// The restore queries are run with `UNSAFE_RESTORE_INCOMPATIBLE_VERSION`
		// option to ensure the restore is successful on development branches. This
		// is because, while the backups were generated on release branches and have
		// versions such as 22.2 in their manifest, the development branch will have
		// a MinSupportedVersion offset by the clusterversion.DevOffset described in
		// `pkg/clusterversion/cockroach_versions.go`. This will mean that the
		// manifest version is always less than the MinSupportedVersion which will
		// in turn fail the restore unless we pass in the specified option to elide
		// the compatibility check.
		restoreQuery := "RESTORE defaultdb.* FROM LATEST IN $1 WITH UNSAFE_RESTORE_INCOMPATIBLE_VERSION"
		if isClusterRestore {
			restoreQuery = "RESTORE FROM LATEST IN $1 WITH UNSAFE_RESTORE_INCOMPATIBLE_VERSION"
		}
		if isSchemaOnly {
			restoreQuery = restoreQuery + ", schema_only"
		}
		log.Infof(context.Background(), "%+v", sqlDB.QueryStr(t, "SHOW BACKUP FROM LATEST IN $1", localFoo))
		sqlDB.Exec(t, restoreQuery, localFoo)
		// Wait for all jobs to terminate. Some may fail since we don't restore
		// adding spans.
		sqlDB.CheckQueryResultsRetry(t, "SELECT * FROM crdb_internal.jobs WHERE job_type = 'SCHEMA CHANGE' AND NOT (status = 'succeeded' OR status = 'failed')", [][]string{})
		verifyMidSchemaChange(t, schemaChangeName, tc, sqlDB, isSchemaOnly, majorVer)

		// Because crdb_internal.invalid_objects is a virtual table, by default, the
		// query will take a lease on the database sqlDB is connected to and only run
		// the query on the given database. The "" prefix prevents this lease
		// acquisition and allows the query to fetch all descriptors in the cluster.
		sqlDB.CheckQueryResultsRetry(t, `SELECT * from "".crdb_internal.invalid_objects`, [][]string{})
	}
}
