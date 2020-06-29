// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
)

// Large test to ensure that all of the system table data is being restored in
// the new cluster. Ensures that all the moving pieces are working together.
func TestFullClusterBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitNone)
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
	numUsers := 1000
	if util.RaceEnabled {
		numUsers = 10
	}
	for i := 0; i < numUsers; i++ {
		sqlDB.Exec(t, fmt.Sprintf("CREATE USER maxroach%d", i))
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

	sqlDB.Exec(t, `CREATE STATISTICS my_stats FROM data.bank`)
	sqlDB.Exec(t, `BACKUP TO $1`, LocalFoo)

	// Create a bunch of user tables on the restoring cluster that we're going
	// to delete.
	numTables := 50
	if util.RaceEnabled {
		numTables = 2
	}
	for i := 0; i < numTables; i++ {
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

	sqlDBRestore.Exec(t, `RESTORE FROM $1`, LocalFoo)

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
		// system table and all of the temporary system tables (1 + 8).
		numIDsToSkip := 9
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
	_, _, sqlDB, tempDir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitNone)
	defer cleanupFn()
	defer cleanupEmptyCluster()

	_, tablesToCheck := generateInterleavedData(sqlDB, t, numAccounts)

	sqlDB.Exec(t, `BACKUP TO $1`, LocalFoo)
	sqlDBRestore.Exec(t, `RESTORE FROM $1`, LocalFoo)

	for _, table := range tablesToCheck {
		query := fmt.Sprintf("SELECT * FROM data.%s", table)
		sqlDBRestore.CheckQueryResults(t, query, sqlDB.QueryStr(t, query))
	}
}

func TestIncrementalFullClusterBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	const incrementalBackupLocation = "nodelocal://0/inc-full-backup"
	_, _, sqlDB, tempDir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitNone)
	defer cleanupFn()
	defer cleanupEmptyCluster()

	sqlDB.Exec(t, `BACKUP TO $1`, LocalFoo)
	sqlDB.Exec(t, fmt.Sprintf("CREATE USER maxroach1"))

	sqlDB.Exec(t, `BACKUP TO $1 INCREMENTAL FROM $2`, incrementalBackupLocation, LocalFoo)
	sqlDBRestore.Exec(t, `RESTORE FROM $1, $2`, LocalFoo, incrementalBackupLocation)

	checkQuery := "SELECT * FROM system.users"
	sqlDBRestore.CheckQueryResults(t, checkQuery, sqlDB.QueryStr(t, checkQuery))
}

// TestEmptyFullClusterResotre ensures that we can backup and restore a full
// cluster backup with only metadata (no user data). Regression test for #49573.
func TestEmptyFullClusterRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sqlDB, tempDir, cleanupFn := createEmptyCluster(t, singleNode)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitNone)
	defer cleanupFn()
	defer cleanupEmptyCluster()

	sqlDB.Exec(t, `CREATE USER alice`)
	sqlDB.Exec(t, `CREATE USER bob`)
	sqlDB.Exec(t, `BACKUP TO $1`, LocalFoo)
	sqlDBRestore.Exec(t, `RESTORE FROM $1`, LocalFoo)

	checkQuery := "SELECT * FROM system.users"
	sqlDBRestore.CheckQueryResults(t, checkQuery, sqlDB.QueryStr(t, checkQuery))
}

// Regression test for #50561.
func TestClusterRestoreEmptyDB(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitNone)
	defer cleanupFn()
	defer cleanupEmptyCluster()

	sqlDB.Exec(t, `CREATE DATABASE some_db`)
	sqlDB.Exec(t, `CREATE DATABASE some_db_2`)
	sqlDB.Exec(t, `BACKUP TO $1`, LocalFoo)
	sqlDBRestore.Exec(t, `RESTORE FROM $1`, LocalFoo)

	checkQuery := "SHOW DATABASES"
	sqlDBRestore.CheckQueryResults(t, checkQuery, sqlDB.QueryStr(t, checkQuery))
}

func TestDisallowFullClusterRestoreOnNonFreshCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitNone)
	defer cleanupFn()
	defer cleanupEmptyCluster()

	sqlDB.Exec(t, `BACKUP TO $1`, LocalFoo)
	sqlDBRestore.Exec(t, `CREATE DATABASE foo`)
	sqlDBRestore.ExpectErr(
		t, "pq: full cluster restore can only be run on a cluster with no tables or databases but found 1 descriptors",
		`RESTORE FROM $1`, LocalFoo,
	)
}

func TestDisallowFullClusterRestoreOfNonFullBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitNone)
	defer cleanupFn()
	defer cleanupEmptyCluster()

	sqlDB.Exec(t, `BACKUP data.bank TO $1`, LocalFoo)
	sqlDBRestore.ExpectErr(
		t, "pq: full cluster RESTORE can only be used on full cluster BACKUP files",
		`RESTORE FROM $1`, LocalFoo,
	)
}

func TestAllowNonFullClusterRestoreOfFullBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	sqlDB.Exec(t, `BACKUP TO $1`, LocalFoo)
	sqlDB.Exec(t, `CREATE DATABASE data2`)
	sqlDB.Exec(t, `RESTORE data.bank FROM $1 WITH into_db='data2'`, LocalFoo)

	checkResults := "SELECT * FROM data.bank"
	sqlDB.CheckQueryResults(t, checkResults, sqlDB.QueryStr(t, checkResults))
}

func TestResotreDatabaseFromFullClusterBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	sqlDB.Exec(t, `BACKUP TO $1`, LocalFoo)
	sqlDB.Exec(t, `DROP DATABASE data`)
	sqlDB.Exec(t, `RESTORE DATABASE data FROM $1`, LocalFoo)

	sqlDB.CheckQueryResults(t, "SELECT count(*) FROM data.bank", [][]string{{"10"}})
}

func TestRestoreSystemTableFromFullClusterBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	defer cleanupFn()

	sqlDB.Exec(t, `CREATE USER maxroach`)
	sqlDB.Exec(t, `BACKUP TO $1`, LocalFoo)
	sqlDB.Exec(t, `CREATE DATABASE temp_sys`)
	sqlDB.Exec(t, `RESTORE system.users FROM $1 WITH into_db='temp_sys'`, LocalFoo)

	sqlDB.CheckQueryResults(t, "SELECT * FROM temp_sys.users", sqlDB.QueryStr(t, "SELECT * FROM system.users"))
}

func TestCreateDBAndTableIncrementalFullClusterBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	_, _, sqlDB, _, cleanupFn := BackupRestoreTestSetup(t, singleNode, 0, InitNone)
	defer cleanupFn()

	sqlDB.Exec(t, `BACKUP TO $1`, LocalFoo)
	sqlDB.Exec(t, `CREATE DATABASE foo`)
	sqlDB.Exec(t, `CREATE TABLE foo.bar (a int)`)

	// Ensure that the new backup succeeds.
	sqlDB.Exec(t, `BACKUP TO $1`, LocalFoo)
}

// TestClusterRestoreFailCleanup tests that a failed RESTORE is cleaned up.
func TestClusterRestoreFailCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params := base.TestServerArgs{}
	// Disable GC job so that the final check of crdb_internal.tables is
	// guaranteed to not be cleaned up. Although this was never observed by a
	// stress test, it is here for safety.
	blockCh := make(chan struct{})
	defer close(blockCh)
	params.Knobs.GCJob = &sql.GCJobTestingKnobs{
		RunBeforeResume: func(_ int64) error { <-blockCh; return nil },
	}

	const numAccounts = 1000
	_, _, sqlDB, tempDir, cleanupFn := BackupRestoreTestSetup(t, singleNode, numAccounts, InitNone)
	_, tcRestore, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(
		t, singleNode, tempDir, InitNone,
	)
	defer cleanupFn()
	defer cleanupEmptyCluster()

	// Setup the system systemTablesToVerify to ensure that they are copied to the new cluster.
	// Populate system.users.
	for i := 0; i < 1000; i++ {
		sqlDB.Exec(t, fmt.Sprintf("CREATE USER maxroach%d", i))
	}
	sqlDB.Exec(t, `BACKUP TO $1`, LocalFoo)

	// Bugger the backup by injecting a failure while restoring the system data.
	for _, server := range tcRestore.Servers {
		registry := server.JobRegistry().(*jobs.Registry)
		registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
			jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
				r := raw.(*restoreResumer)
				r.testingKnobs.duringSystemTableRestoration = func() error {
					return errors.New("injected error")
				}
				return r
			},
		}
	}

	sqlDBRestore.ExpectErr(
		t, "injected error",
		`RESTORE FROM $1`, LocalFoo,
	)
	// Verify the failed RESTORE added some DROP tables.
	// Note that the system tables here correspond to the temporary tables
	// imported, not the system tables themselves.
	sqlDBRestore.CheckQueryResults(t,
		`SELECT name FROM crdb_internal.tables WHERE state = 'DROP' ORDER BY name`,
		[][]string{
			{"bank"},
			{"comments"},
			{"jobs"},
			{"locations"},
			{"role_members"},
			{"settings"},
			{"ui"},
			{"users"},
			{"zones"},
		},
	)
}
