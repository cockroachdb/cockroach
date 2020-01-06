// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl_test

import (
	"fmt"
	"reflect"
	"testing"

	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/roleccl"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// Large test to ensure that all of the system table data is being restored in
// the new cluster. Ensures that all the moving pieces are working together.
func TestFullClusterBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone)
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
	}
	// Populate system.zones.
	sqlDB.Exec(t, `ALTER TABLE data.bank CONFIGURE ZONE USING gc.ttlseconds = 3600`)
	sqlDB.Exec(t, `ALTER TABLE defaultdb.foo CONFIGURE ZONE USING gc.ttlseconds = 45`)
	sqlDB.Exec(t, `ALTER DATABASE data2 CONFIGURE ZONE USING gc.ttlseconds = 900`)
	// Populate system.jobs.
	// Note: this is not the backup under test, this just serves as a job which should appear in the restore.
	sqlDB.Exec(t, `BACKUP data.bank TO 'nodelocal:///throwawayjob'`)
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
	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
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
}

func TestFullClusterBackupDroppedTables(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone)
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
	const incrementalBackupLocation = "nodelocal:///inc-full-backup"
	_, _, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone)
	defer cleanupFn()
	defer cleanupEmptyCluster()

	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	sqlDB.Exec(t, fmt.Sprintf("CREATE USER maxroach1"))

	sqlDB.Exec(t, `BACKUP TO $1 INCREMENTAL FROM $2`, incrementalBackupLocation, localFoo)
	sqlDBRestore.Exec(t, `RESTORE FROM $1, $2`, localFoo, incrementalBackupLocation)

	checkQuery := "SELECT * FROM system.users"
	sqlDBRestore.CheckQueryResults(t, checkQuery, sqlDB.QueryStr(t, checkQuery))
}

func TestDisallowFullClusterRestoreOnNonFreshCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone)
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
	_, _, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, initNone)
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
