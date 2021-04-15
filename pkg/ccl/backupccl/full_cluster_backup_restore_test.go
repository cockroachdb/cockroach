// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl_test

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func backupRestoreTestSetupEmptyWithParams(
	t testing.TB,
	clusterSize int,
	dir string,
	init func(tc *testcluster.TestCluster),
	params base.TestClusterArgs,
) (ctx context.Context, tc *testcluster.TestCluster, sqlDB *sqlutils.SQLRunner, cleanup func()) {
	ctx = context.Background()

	params.ServerArgs.ExternalIODir = dir
	tc = testcluster.StartTestCluster(t, clusterSize, params)
	init(tc)

	sqlDB = sqlutils.MakeSQLRunner(tc.Conns[0])

	cleanupFn := func() {
		tc.Stopper().Stop(ctx) // cleans up in memory storage's auxiliary dirs
	}

	return ctx, tc, sqlDB, cleanupFn
}

func createEmptyCluster(
	t testing.TB, clusterSize int,
) (sqlDB *sqlutils.SQLRunner, tempDir string, cleanup func()) {
	ctx := context.Background()

	dir, dirCleanupFn := testutils.TempDir(t)
	params := base.TestClusterArgs{}
	params.ServerArgs.ExternalIODir = dir
	tc := testcluster.StartTestCluster(t, clusterSize, params)

	sqlDB = sqlutils.MakeSQLRunner(tc.Conns[0])

	cleanupFn := func() {
		tc.Stopper().Stop(ctx) // cleans up in memory storage's auxiliary dirs
		dirCleanupFn()         // cleans up dir, which is the nodelocal:// storage
	}

	return sqlDB, dir, cleanupFn
}

// Large test to ensure that all of the system table data is being restored in
// the new cluster. Ensures that all the moving pieces are working together.
func TestFullClusterBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	// Disable automatic stats collection on the backup and restoring clusters to ensure
	// the test is deterministic.
	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false`)
	sqlDBRestore.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false`)

	// Create some other databases and tables.
	sqlDB.Exec(t, `CREATE TABLE defaultdb.foo (a int);`)
	sqlDB.Exec(t, `CREATE DATABASE data2;`)
	sqlDB.Exec(t, `CREATE TABLE data2.foo (a int);`)

	// Setup the system systemTablesToVerify to ensure that they are copied to the new cluster.
	// Populate system.users.
	for i := 0; i < 1000; i++ {
		sqlDB.Exec(t, fmt.Sprintf("CREATE USER maxroach%d", i))
		sqlDB.Exec(t, fmt.Sprintf("ALTER USER maxroach%d CREATEROLE", i))
	}
	// Populate system.zones.
	sqlDB.Exec(t, `ALTER TABLE data.bank CONFIGURE ZONE USING gc.ttlseconds = 3600`)
	sqlDB.Exec(t, `ALTER TABLE defaultdb.foo CONFIGURE ZONE USING gc.ttlseconds = 45`)
	sqlDB.Exec(t, `ALTER DATABASE data2 CONFIGURE ZONE USING gc.ttlseconds = 900`)
	// Populate system.jobs.
	// Note: this is not the backup under test, this just serves as a job which should appear in the restore.
	sqlDB.Exec(t, `BACKUP data.bank TO 'nodelocal://0/throwawayjob'`)
	preBackupJobs := sqlDB.QueryStr(t, "SELECT * FROM system.jobs")
	// Populate system.settings.
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.bulk_io_write.concurrent_addsstable_requests = 5`)
	sqlDB.Exec(t, `INSERT INTO system.ui (key, value, "lastUpdated") VALUES ($1, $2, now())`, "some_key", "some_val")
	// Populate system.comments.
	sqlDB.Exec(t, `COMMENT ON TABLE data.bank IS 'table comment string'`)
	sqlDB.Exec(t, `COMMENT ON DATABASE data IS 'database comment string'`)

	sqlDB.Exec(t,
		`INSERT INTO system.locations ("localityKey", "localityValue", latitude, longitude) VALUES ($1, $2, $3, $4)`,
		"city", "New York City", 40.71427, -74.00597,
	)
	// Populate system.role_members.
	sqlDB.Exec(t, `CREATE ROLE system_ops;`)
	sqlDB.Exec(t, `GRANT CREATE, SELECT ON DATABASE data TO system_ops;`)
	sqlDB.Exec(t, `GRANT system_ops TO maxroach1;`)

	injectStats(t, sqlDB, "data.bank", "id")
	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)

	// Create a bunch of user tables on the restoring cluster that we're going
	// to delete.
	for i := 0; i < 50; i++ {
		sqlDBRestore.Exec(t, `CREATE DATABASE db_to_drop`)
		sqlDBRestore.Exec(t, `CREATE TABLE db_to_drop.table_to_drop (a int)`)
		sqlDBRestore.Exec(t, `ALTER TABLE db_to_drop.table_to_drop CONFIGURE ZONE USING gc.ttlseconds=1`)
		sqlDBRestore.Exec(t, `DROP DATABASE db_to_drop`)
	}
	// Wait for the GC job to finish to ensure the descriptors no longer exist.
	sqlDBRestore.CheckQueryResultsRetry(
		t, "SELECT count(*) FROM [SHOW JOBS] WHERE job_type = 'SCHEMA CHANGE GC' AND status = 'running'",
		[][]string{{"0"}},
	)

	sqlDBRestore.Exec(t, `RESTORE FROM $1`, localFoo)

	t.Run("ensure all databases restored", func(t *testing.T) {
		sqlDBRestore.CheckQueryResults(t,
			`SHOW DATABASES`,
			[][]string{
				{"data"},
				{"data2"},
				{"defaultdb"},
				{"postgres"},
				{"system"},
			})
	})

	t.Run("ensure system table data restored", func(t *testing.T) {
		// Note the absence of the jobs table. Jobs are tested by another test as
		// jobs are created during the RESTORE process.
		systemTablesToVerify := []string{
			sqlbase.CommentsTable.Name,
			sqlbase.LocationsTable.Name,
			sqlbase.RoleMembersTable.Name,
			sqlbase.RoleOptionsTable.Name,
			sqlbase.SettingsTable.Name,
			sqlbase.TableStatisticsTable.Name,
			sqlbase.UITable.Name,
			sqlbase.UsersTable.Name,
			sqlbase.ZonesTable.Name,
		}

		verificationQueries := make([]string, len(systemTablesToVerify))
		// Populate the list of tables we expect to be restored as well as queries
		// that can be used to ensure that data in those tables is restored.
		for i, table := range systemTablesToVerify {
			switch table {
			case sqlbase.TableStatisticsTable.Name:
				// createdAt and statisticsID are re-generated on RESTORE.
				query := fmt.Sprintf("SELECT \"tableID\", name, \"columnIDs\", \"rowCount\" FROM system.table_statistics")
				verificationQueries[i] = query
			case sqlbase.SettingsTable.Name:
				// We don't include the cluster version.
				query := fmt.Sprintf("SELECT * FROM system.%s WHERE name <> 'version'", table)
				verificationQueries[i] = query
			default:
				query := fmt.Sprintf("SELECT * FROM system.%s", table)
				verificationQueries[i] = query
			}
		}

		for _, read := range verificationQueries {
			sqlDBRestore.CheckQueryResults(t, read, sqlDB.QueryStr(t, read))
		}
	})

	t.Run("ensure table IDs have not changed", func(t *testing.T) {
		// Check that all tables have been restored. DISTINCT is needed in order to
		// deal with the inclusion of schemas in the system.namespace table.
		tableIDCheck := "SELECT DISTINCT name, id FROM system.namespace"
		sqlDBRestore.CheckQueryResults(t, tableIDCheck, sqlDB.QueryStr(t, tableIDCheck))
	})

	t.Run("ensure user table data restored", func(t *testing.T) {
		expectedUserTables := [][]string{
			{"data", "bank"},
			{"data2", "foo"},
			{"defaultdb", "foo"},
		}

		for _, table := range expectedUserTables {
			query := fmt.Sprintf("SELECT * FROM %s.%s", table[0], table[1])
			sqlDBRestore.CheckQueryResults(t, query, sqlDB.QueryStr(t, query))
		}
	})

	t.Run("ensure that grants are restored", func(t *testing.T) {
		grantCheck := "use system; SHOW grants"
		sqlDBRestore.CheckQueryResults(t, grantCheck, sqlDB.QueryStr(t, grantCheck))
		grantCheck = "use data; SHOW grants"
		sqlDBRestore.CheckQueryResults(t, grantCheck, sqlDB.QueryStr(t, grantCheck))
	})

	t.Run("ensure that jobs are restored", func(t *testing.T) {
		// Ensure that the jobs in the RESTORE cluster is a superset of the jobs
		// that were in the BACKUP cluster (before the full cluster BACKUP job was
		// run). There may be more jobs now because the restore can run jobs of
		// its own.
		newJobs := sqlDBRestore.QueryStr(t, "SELECT * FROM system.jobs")
		for _, oldJob := range preBackupJobs {
			present := false
			for _, newJob := range newJobs {
				if reflect.DeepEqual(oldJob, newJob) {
					present = true
				}
			}
			if !present {
				t.Errorf("Expected to find job %+v in RESTORE cluster, but not found", oldJob)
			}
		}
	})

	t.Run("ensure that tables can be created at the execpted ID", func(t *testing.T) {
		maxID, err := strconv.Atoi(sqlDBRestore.QueryStr(t, "SELECT max(id) FROM system.namespace")[0][0])
		if err != nil {
			t.Fatal(err)
		}
		dbName, tableName := "new_db", "new_table"
		// N.B. We skip the database ID that was allocated too the temporary
		// system table and all of the temporary system tables (1 + 9).
		numIDsToSkip := 10
		expectedDBID := maxID + numIDsToSkip + 1
		expectedTableID := maxID + numIDsToSkip + 2
		sqlDBRestore.Exec(t, fmt.Sprintf("CREATE DATABASE %s", dbName))
		sqlDBRestore.Exec(t, fmt.Sprintf("CREATE TABLE %s.%s (a int)", dbName, tableName))
		sqlDBRestore.CheckQueryResults(
			t, fmt.Sprintf("SELECT id FROM system.namespace WHERE name = '%s'", dbName),
			[][]string{{strconv.Itoa(expectedDBID)}},
		)
		sqlDBRestore.CheckQueryResults(
			t, fmt.Sprintf("SELECT id FROM system.namespace WHERE name = '%s'", tableName),
			[][]string{{strconv.Itoa(expectedTableID)}},
		)
	})
}

func TestFullClusterBackupDroppedTables(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	_, tablesToCheck := generateInterleavedData(sqlDB, t, numAccounts)

	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	sqlDBRestore.Exec(t, `RESTORE FROM $1`, localFoo)

	for _, table := range tablesToCheck {
		query := fmt.Sprintf("SELECT * FROM data.%s", table)
		sqlDBRestore.CheckQueryResults(t, query, sqlDB.QueryStr(t, query))
	}
}

func TestIncrementalFullClusterBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	const incrementalBackupLocation = "nodelocal://0/inc-full-backup"
	_, _, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	sqlDB.Exec(t, fmt.Sprintf("CREATE USER maxroach1"))

	sqlDB.Exec(t, `BACKUP TO $1 INCREMENTAL FROM $2`, incrementalBackupLocation, localFoo)
	sqlDBRestore.Exec(t, `RESTORE FROM $1, $2`, localFoo, incrementalBackupLocation)

	checkQuery := "SELECT * FROM system.users"
	sqlDBRestore.CheckQueryResults(t, checkQuery, sqlDB.QueryStr(t, checkQuery))
}

// TestEmptyFullClusterResotre ensures that we can backup and restore a full
// cluster backup with only metadata (no user data). Regression test for #49573.
func TestEmptyFullClusterRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sqlDB, tempDir, cleanupFn := createEmptyCluster(t, singleNode)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	sqlDB.Exec(t, `CREATE USER alice`)
	sqlDB.Exec(t, `CREATE USER bob`)
	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	sqlDBRestore.Exec(t, `RESTORE FROM $1`, localFoo)

	checkQuery := "SELECT * FROM system.users"
	sqlDBRestore.CheckQueryResults(t, checkQuery, sqlDB.QueryStr(t, checkQuery))
}

// Regression test for #50561.
func TestClusterRestoreEmptyDB(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	sqlDB.Exec(t, `CREATE DATABASE some_db`)
	sqlDB.Exec(t, `CREATE DATABASE some_db_2`)
	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	sqlDBRestore.Exec(t, `RESTORE FROM $1`, localFoo)

	checkQuery := "SHOW DATABASES"
	sqlDBRestore.CheckQueryResults(t, checkQuery, sqlDB.QueryStr(t, checkQuery))
}

func TestDisallowFullClusterRestoreOnNonFreshCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	sqlDBRestore.Exec(t, `CREATE DATABASE foo`)
	sqlDBRestore.ExpectErr(
		t, "pq: full cluster restore can only be run on a cluster with no tables or databases but found 1 descriptors",
		`RESTORE FROM $1`, localFoo,
	)
}

func TestDisallowFullClusterRestoreOfNonFullBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	sqlDB.Exec(t, `BACKUP data.bank TO $1`, localFoo)
	sqlDBRestore.ExpectErr(
		t, "pq: full cluster RESTORE can only be used on full cluster BACKUP files",
		`RESTORE FROM $1`, localFoo,
	)
}

func TestAllowNonFullClusterRestoreOfFullBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	sqlDB.Exec(t, `CREATE DATABASE data2`)
	sqlDB.Exec(t, `RESTORE data.bank FROM $1 WITH into_db='data2'`, localFoo)

	checkResults := "SELECT * FROM data.bank"
	sqlDB.CheckQueryResults(t, checkResults, sqlDB.QueryStr(t, checkResults))
}

func TestResotreDatabaseFromFullClusterBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	sqlDB.Exec(t, `DROP DATABASE data`)
	sqlDB.Exec(t, `RESTORE DATABASE data FROM $1`, localFoo)

	sqlDB.CheckQueryResults(t, "SELECT count(*) FROM data.bank", [][]string{{"10"}})
}

func TestRestoreSystemTableFromFullClusterBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()

	sqlDB.Exec(t, `CREATE USER maxroach`)
	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	sqlDB.Exec(t, `CREATE DATABASE temp_sys`)
	sqlDB.Exec(t, `RESTORE system.users FROM $1 WITH into_db='temp_sys'`, localFoo)

	sqlDB.CheckQueryResults(t, "SELECT * FROM temp_sys.users", sqlDB.QueryStr(t, "SELECT * FROM system.users"))
}

func TestCreateDBAndTableIncrementalFullClusterBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	_, _, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, initNone)
	defer cleanupFn()

	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	sqlDB.Exec(t, `CREATE DATABASE foo`)
	sqlDB.Exec(t, `CREATE TABLE foo.bar (a int)`)

	// Ensure that the new backup succeeds.
	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
}

// TestClusterRevisionHistory tests that cluster backups can be taken with
// revision_history and correctly restore into various points in time.
func TestClusterRevisionHistory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type testCase struct {
		ts    string
		check func(t *testing.T, runner *sqlutils.SQLRunner)
	}

	testCases := make([]testCase, 0, 6)
	ts := make([]string, 6)

	var tc testCase
	const numAccounts = 1
	_, _, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	defer cleanupFn()
	sqlDB.Exec(t, `CREATE DATABASE d1`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[0])
	tc = testCase{
		ts: ts[0],
		check: func(t *testing.T, checkSQLDB *sqlutils.SQLRunner) {
			checkSQLDB.ExpectErr(t, `database "d1" already exists`, `CREATE DATABASE d1`)
			checkSQLDB.Exec(t, `CREATE DATABASE d2`)
		},
	}
	testCases = append(testCases, tc)

	sqlDB.Exec(t, `CREATE DATABASE d2`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[1])
	tc = testCase{
		ts: ts[1],
		check: func(t *testing.T, checkSQLDB *sqlutils.SQLRunner) {
			// Expect both databases to exist at this point.
			checkSQLDB.ExpectErr(t, `database "d1" already exists`, `CREATE DATABASE d1`)
			checkSQLDB.ExpectErr(t, `database "d2" already exists`, `CREATE DATABASE d2`)
		},
	}
	testCases = append(testCases, tc)

	sqlDB.Exec(t, `DROP DATABASE d1`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[2])
	tc = testCase{
		ts: ts[2],
		check: func(t *testing.T, checkSQLDB *sqlutils.SQLRunner) {
			checkSQLDB.Exec(t, `CREATE DATABASE d1`)
			checkSQLDB.ExpectErr(t, `database "d2" already exists`, `CREATE DATABASE d2`)
		},
	}
	testCases = append(testCases, tc)
	sqlDB.Exec(t, `BACKUP TO $1 WITH revision_history`, localFoo)

	// Now let's test an incremental backup with revision history. At the start of
	// the incremental backup, we expect only d2 to exist.
	sqlDB.Exec(t, `DROP DATABASE d2;`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[3])
	tc = testCase{
		ts: ts[3],
		check: func(t *testing.T, checkSQLDB *sqlutils.SQLRunner) {
			// Neither database should exist at this point in time.
			checkSQLDB.Exec(t, `CREATE DATABASE d1`)
			checkSQLDB.Exec(t, `CREATE DATABASE d2`)
		},
	}
	testCases = append(testCases, tc)
	sqlDB.Exec(t, `BACKUP TO $1 WITH revision_history`, localFoo)

	sqlDB.Exec(t, `CREATE DATABASE d1`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[4])
	tc = testCase{
		ts: ts[4],
		check: func(t *testing.T, checkSQLDB *sqlutils.SQLRunner) {
			checkSQLDB.ExpectErr(t, `database "d1" already exists`, `CREATE DATABASE d1`)
			checkSQLDB.Exec(t, `CREATE DATABASE d2`)
		},
	}
	testCases = append(testCases, tc)

	sqlDB.Exec(t, `DROP DATABASE d1`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[5])
	tc = testCase{
		ts: ts[5],
		check: func(t *testing.T, checkSQLDB *sqlutils.SQLRunner) {
			checkSQLDB.Exec(t, `CREATE DATABASE d1`)
			checkSQLDB.Exec(t, `CREATE DATABASE d2`)
		},
	}
	testCases = append(testCases, tc)
	sqlDB.Exec(t, `BACKUP TO $1 WITH revision_history`, localFoo)

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("t%d", i), func(t *testing.T) {
			_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone, base.TestClusterArgs{})
			defer cleanupEmptyCluster()

			sqlDBRestore.Exec(t, `RESTORE FROM $1 AS OF SYSTEM TIME `+testCase.ts, localFoo)
			testCase.check(t, sqlDBRestore)
		})
	}

}

// TestReintroduceOfflineSpans is a regression test for #62564, which tracks a
// bug where AddSSTable requests to OFFLINE tables may be missed by cluster
// incremental backups since they can write at a timestamp older than the last
// backup.
func TestReintroduceOfflineSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "likely slow under race")

	// Block restores on the source cluster.
	blockDBRestore := make(chan struct{})
	dbRestoreStarted := make(chan struct{})
	// The data is split such that there will be 10 span entries to process.
	restoreBlockEntiresThreshold := 4
	entriesCount := 0
	params := base.TestClusterArgs{}
	knobs := base.TestingKnobs{
		Store: &kvserver.StoreTestingKnobs{
			TestingResponseFilter: func(ba roachpb.BatchRequest, br *roachpb.BatchResponse) *roachpb.Error {
				for _, ru := range br.Responses {
					switch ru.GetInner().(type) {
					case *roachpb.ImportResponse:
						if entriesCount == 0 {
							close(dbRestoreStarted)
						}
						if entriesCount == restoreBlockEntiresThreshold {
							<-blockDBRestore
						}

						entriesCount++
					}
				}
				return nil
			},
		},
	}
	params.ServerArgs.Knobs = knobs

	const numAccounts = 1000
	ctx, _, srcDB, tempDir, cleanupSrc := backupRestoreTestSetupWithParams(t, singleNode, numAccounts, initNone, params)
	defer cleanupSrc()

	dbBackupLoc := "nodelocal://0/my_db_backup"
	clusterBackupLoc := "nodelocal://0/my_cluster_backup"

	// Take a backup that we'll use to create an OFFLINE descriptor.
	srcDB.Exec(t, `CREATE INDEX new_idx ON data.bank (balance)`)
	srcDB.Exec(t, `BACKUP DATABASE data TO $1 WITH revision_history`, dbBackupLoc)

	srcDB.Exec(t, `CREATE DATABASE restoredb;`)

	// Take a base full backup.
	srcDB.Exec(t, `BACKUP TO $1 WITH revision_history`, clusterBackupLoc)

	var g errgroup.Group
	g.Go(func() error {
		_, err := srcDB.DB.ExecContext(ctx, `RESTORE data.bank FROM $1 WITH into_db='restoredb'`, dbBackupLoc)
		return err
	})

	// Take an incremental backup after the database restore starts.
	<-dbRestoreStarted
	srcDB.Exec(t, `BACKUP TO $1 WITH revision_history`, clusterBackupLoc)

	var tsMidRestore string
	srcDB.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&tsMidRestore)

	// Allow the restore to finish. This will issue AddSSTable requests at a
	// timestamp that is before the last incremental we just took.
	close(blockDBRestore)

	// Wait for the database restore to finish, and take another incremental
	// backup that will miss the AddSSTable writes.
	require.NoError(t, g.Wait())

	var tsBefore string
	srcDB.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&tsBefore)

	// Drop an index on the restored table to ensure that the dropped index was
	// also re-included.
	srcDB.Exec(t, `DROP INDEX new_idx`)

	srcDB.Exec(t, `BACKUP TO $1 WITH revision_history`, clusterBackupLoc)

	t.Run("spans-reintroduced", func(t *testing.T) {
		_, _, destDB, cleanupDst := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone, base.TestClusterArgs{})
		defer cleanupDst()

		// Restore the incremental backup chain that has missing writes.
		destDB.Exec(t, `RESTORE FROM $1 AS OF SYSTEM TIME `+tsBefore, clusterBackupLoc)

		// Assert that the restored database has the same number of rows in both the
		// source and destination cluster.
		checkQuery := `SELECT count(*) FROM restoredb.bank AS OF SYSTEM TIME ` + tsBefore
		expectedCount := srcDB.QueryStr(t, checkQuery)
		destDB.CheckQueryResults(t, `SELECT count(*) FROM restoredb.bank`, expectedCount)

		checkQuery = `SELECT count(*) FROM restoredb.bank@new_idx AS OF SYSTEM TIME ` + tsBefore
		expectedCount = srcDB.QueryStr(t, checkQuery)
		destDB.CheckQueryResults(t, `SELECT count(*) FROM restoredb.bank@new_idx`, expectedCount)
	})

	t.Run("restore-canceled", func(t *testing.T) {
		defer func(oldInterval time.Duration) {
			jobs.DefaultAdoptInterval = oldInterval
		}(jobs.DefaultAdoptInterval)
		jobs.DefaultAdoptInterval = 100 * time.Millisecond

		_, _, destDB, cleanupDst := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone, base.TestClusterArgs{})
		defer cleanupDst()

		destDB.Exec(t, `RESTORE FROM $1 AS OF SYSTEM TIME `+tsMidRestore, clusterBackupLoc)

		// Wait for the cluster restore job to finish, as well as the restored RESTORE TABLE
		// job to revert.
		destDB.CheckQueryResultsRetry(t, `
		SELECT description, status FROM [SHOW JOBS]
		WHERE job_type = 'RESTORE' AND status NOT IN ('succeeded', 'canceled')`,
			[][]string{},
		)
		// The cluster restore should succeed, but the table restore should have failed.
		destDB.CheckQueryResults(t,
			`SELECT status, count(*) FROM [SHOW JOBS] WHERE job_type = 'RESTORE' GROUP BY status ORDER BY status`,
			[][]string{{"canceled", "1"}, {"succeeded", "1"}})

		destDB.ExpectErr(t, `relation "restoredb.bank" does not exist`, `SELECT count(*) FROM restoredb.bank`)
	})
}
