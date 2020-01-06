// Copyright 2016 The Cockroach Authors.
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
	_, _, sqlDB2, cleanupFresh := backupRestoreTestSetupFresh(t, singleNode, tempDir, initNone)
	defer cleanupFn()
	defer cleanupFresh()

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
	// Populate system.roles.
	sqlDB.Exec(t, `CREATE ROLE system_ops;`)
	sqlDB.Exec(t, `GRANT CREATE, SELECT ON DATABASE data TO system_ops;`)
	sqlDB.Exec(t, `GRANT system_ops TO maxroach1;`)

	sqlDB.Exec(t, `CREATE STATISTICS my_stats FROM data.bank`)
	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	sqlDB2.Exec(t, `RESTORE FROM $1`, localFoo)
	var restoreID uint64
	sqlDB2.QueryRow(t, `SELECT job_id FROM crdb_internal.jobs ORDER BY created DESC LIMIT 1`).Scan(&restoreID)

	sqlDB2.CheckQueryResults(t,
		`SHOW DATABASES`,
		[][]string{
			{"data"},
			{"data2"},
			{"defaultdb"},
			{"postgres"},
			{"system"},
		})

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
	expectedUserTables := [][]string{
		{"data", "bank"},
		{"data2", "foo"},
		{"defaultdb", "foo"},
	}
	expectedTables := make([][]string, len(systemTablesToVerify)+len(expectedUserTables))

	verifyTableContentQueries := make([]string, 0, len(systemTablesToVerify)+len(expectedUserTables))
	// Populate the list of tables we expect to be restored as well as queries
	// that can be used to ensure that data in those tables is restored.
	for i, table := range systemTablesToVerify {
		expectedTables[i] = []string{table}

		switch table {
		case sqlbase.TableStatisticsTable.Name:
			// createdAt and statisticsID are re-generated on RESTORE.
			query := fmt.Sprintf("SELECT \"tableID\", name, \"columnIDs\", \"rowCount\" FROM system.table_statistics")
			verifyTableContentQueries = append(verifyTableContentQueries, query)
		default:
			query := fmt.Sprintf("SELECT * FROM system.%s", table)
			verifyTableContentQueries = append(verifyTableContentQueries, query)
		}
	}
	for _, table := range expectedUserTables {
		expectedTables = append(expectedTables, []string{table[1]})
		verifyTableContentQueries = append(verifyTableContentQueries, fmt.Sprintf("SELECT * FROM %s.%s", table[0], table[1]))
	}

	for _, read := range verifyTableContentQueries {
		sqlDB2.CheckQueryResults(t, read, sqlDB.QueryStr(t, read))
	}

	// Check that all tables have been restored. DISTINCT is needed in order to
	// deal with the inclusion of schemas in the system.namespace table.
	tableIDCheck := "SELECT DISTINCT name, id FROM system.namespace"
	sqlDB2.CheckQueryResults(t, tableIDCheck, sqlDB.QueryStr(t, tableIDCheck))

	// Ensure that the jobs in the RESTORE cluster is a superset of the jobs
	// that were in the BACKUP cluster (before the full cluster BACKUP job was
	// run). There may be more jobs now because the restore can run jobs of
	// its own.
	newJobs := sqlDB.QueryStr(t, "SELECT * FROM system.jobs")
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
}

func TestIncrementalFullClusterBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	const incrementalBackupLocation = "nodelocal:///inc-full-backup"
	_, _, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDB2, cleanupFresh := backupRestoreTestSetupFresh(t, singleNode, tempDir, initNone)
	defer cleanupFn()
	defer cleanupFresh()

	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	// Setup the system systemTablesToVerify to ensure that they are copied to the new cluster.
	// Populate system.users.
	for i := 0; i < 1000; i++ {
		sqlDB.Exec(t, fmt.Sprintf("CREATE USER maxroach%d", i))
	}

	sqlDB.Exec(t, `BACKUP TO $1 INCREMENTAL FROM $2`, incrementalBackupLocation, localFoo)
	sqlDB2.Exec(t, `RESTORE FROM $1, $2`, localFoo, incrementalBackupLocation)

	checkQuery := "SELECT * FROM system.users"
	sqlDB2.CheckQueryResults(t, checkQuery, sqlDB.QueryStr(t, checkQuery))
}

func TestAllowNonFullClusterRestoreOfFullBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDB2, cleanupFresh := backupRestoreTestSetupFresh(t, singleNode, tempDir, initNone)
	defer cleanupFn()
	defer cleanupFresh()

	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	sqlDB2.Exec(t, `CREATE DATABASE data`)
	sqlDB2.Exec(t, `RESTORE data.bank FROM $1`, localFoo)

	checkResults := "SELECT * FROM data.bank"
	sqlDB2.CheckQueryResults(t, checkResults, sqlDB.QueryStr(t, checkResults))
}

func TestResotreDatabaseFromFullClusterBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDB2, cleanupFresh := backupRestoreTestSetupFresh(t, singleNode, tempDir, initNone)
	defer cleanupFn()
	defer cleanupFresh()

	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	sqlDB2.Exec(t, `RESTORE DATABASE data FROM $1`, localFoo)

	checkResults := "SELECT * FROM data.bank"
	sqlDB2.CheckQueryResults(t, checkResults, sqlDB.QueryStr(t, checkResults))
}

func TestRestoreSystemTableFromFullClusterBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDB2, cleanupFresh := backupRestoreTestSetupFresh(t, singleNode, tempDir, initNone)
	defer cleanupFn()
	defer cleanupFresh()
	const systemTable = "system.users"

	sqlDB.Exec(t, `CREATE USER maxroach`)
	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	sqlDB2.Exec(t, `CREATE DATABASE temp_sys`)
	sqlDB2.Exec(t, `RESTORE system.users FROM $1 WITH into_db='temp_sys'`, localFoo)

	sqlDB2.CheckQueryResults(t, "SELECT * FROM temp_sys.users", sqlDB.QueryStr(t, "SELECT * FROM system.users"))
}

func TestDisallowFullClusterRestoreOnNonFreshCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDB2, cleanupFresh := backupRestoreTestSetupFresh(t, singleNode, tempDir, initNone)
	defer cleanupFn()
	defer cleanupFresh()

	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	sqlDB2.Exec(t, `CREATE DATABASE foo`)
	sqlDB2.ExpectErr(
		t, "pq: full cluster restore can only be run on a cluster with no tables or databases but found 1 descriptors",
		`RESTORE FROM $1`, localFoo,
	)
}

func TestDisallowFullClusterRestoreOfNonFullBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numAccounts = 10
	_, _, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, initNone)
	_, _, sqlDB2, cleanupFresh := backupRestoreTestSetupFresh(t, singleNode, tempDir, initNone)
	defer cleanupFn()
	defer cleanupFresh()

	sqlDB.Exec(t, `BACKUP data.bank TO $1`, localFoo)
	sqlDB2.ExpectErr(
		t, "pq: full cluster RESTORE can only be used on full cluster BACKUP files",
		`RESTORE FROM $1`, localFoo,
	)
}
