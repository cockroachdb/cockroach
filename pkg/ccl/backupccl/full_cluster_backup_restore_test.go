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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// Large test to ensure that all of the system table data is being restored in
// the new cluster. Ensures that all the moving pieces are working together.
func TestFullClusterBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DisableSpanConfigs: true, // TODO(irfansharif): #75060.
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					// We compare job progress before and after a restore. Disable
					// the automatic jobs checkpointing which could possibly mutate
					// the progress data during the backup/restore process.
					JobDisablePersistingCheckpoints: true,
				},
			},
		}}
	const numAccounts = 10
	tcBackup, sqlDB, tempDir, cleanupFn := backupRestoreTestSetupWithParams(t, singleNode, numAccounts, InitManualReplication, params)
	tcRestore, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, params)
	defer cleanupFn()
	defer cleanupEmptyCluster()

	backupKVDB := tcBackup.Server(0).DB()

	// Closed when the restore is allowed to progress with the rest of the backup.
	allowProgressAfterPreRestore := make(chan struct{})
	// Closed to signal the the zones have been restored.
	restoredZones := make(chan struct{})
	for _, server := range tcRestore.Servers {
		registry := server.JobRegistry().(*jobs.Registry)
		registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
			jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
				r := raw.(*restoreResumer)
				r.testingKnobs.afterPreRestore = func() error {
					close(restoredZones)
					<-allowProgressAfterPreRestore
					return nil
				}
				return r
			},
		}
	}

	// The claim_session_id field in jobs is a uuid and so needs to be excluded
	// when comparing jobs pre/post restore.
	const jobsQuery = `
SELECT id, status, created, payload, progress, created_by_type, created_by_id, claim_instance_id
FROM system.jobs
	`
	// Pause SQL Stats compaction job to ensure the test is deterministic.
	sqlDB.Exec(t, `PAUSE SCHEDULES SELECT id FROM [SHOW SCHEDULES FOR SQL STATISTICS]`)

	// Disable automatic stats collection on the backup and restoring clusters to ensure
	// the test is deterministic.
	sqlDB.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false`)
	sqlDBRestore.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false`)

	// Create some other descriptors as well.
	sqlDB.Exec(t, `
USE data;
CREATE SCHEMA test_data_schema;
CREATE TABLE data.test_data_schema.test_table (a int);
INSERT INTO data.test_data_schema.test_table VALUES (1), (2);

USE defaultdb;
CREATE SCHEMA test_schema;
CREATE TABLE defaultdb.test_schema.test_table (a int);
INSERT INTO defaultdb.test_schema.test_table VALUES (1), (2);
CREATE TABLE defaultdb.foo (a int);
CREATE TYPE greeting AS ENUM ('hi');
CREATE TABLE welcomes (a greeting);

CREATE DATABASE data2;
USE data2;
CREATE SCHEMA empty_schema;
CREATE TABLE data2.foo (a int);
`)
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(backupKVDB, keys.SystemSQLCodec, "data2", "foo")
	// Store the highest user-table ID for later assertions.
	maxBackupTableID := tableDesc.GetID()

	// Setup the system systemTablesToVerify to ensure that they are copied to the new cluster.
	// Populate system.users.
	numUsers := 1000
	if util.RaceEnabled {
		numUsers = 10
	}
	for i := 0; i < numUsers; i++ {
		sqlDB.Exec(t, fmt.Sprintf("CREATE USER maxroach%d", i))
		sqlDB.Exec(t, fmt.Sprintf("ALTER USER maxroach%d CREATEDB", i))
	}
	// Populate system.zones.
	sqlDB.Exec(t, `ALTER TABLE data.bank CONFIGURE ZONE USING gc.ttlseconds = 3600`)
	sqlDB.Exec(t, `ALTER TABLE defaultdb.foo CONFIGURE ZONE USING gc.ttlseconds = 45`)
	sqlDB.Exec(t, `ALTER DATABASE data2 CONFIGURE ZONE USING gc.ttlseconds = 900`)
	// Populate system.jobs.
	// Note: this is not the backup under test, this just serves as a job which
	// should appear in the restore.
	// This job will eventually fail since it will run from a new cluster.
	sqlDB.Exec(t, `BACKUP data.bank TO 'nodelocal://0/throwawayjob'`)
	preBackupJobs := sqlDB.QueryStr(t, jobsQuery)
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
	sqlDB.Exec(t, `GRANT system_ops TO maxroach1;`)

	// Populate system.scheduled_jobs table with a first run in the future to prevent immediate adoption.
	firstRun := timeutil.Now().Add(time.Hour).Format(timeutil.TimestampWithoutTZFormat)
	sqlDB.Exec(t, `CREATE SCHEDULE FOR BACKUP data.bank INTO $1 RECURRING '@hourly' FULL BACKUP ALWAYS WITH SCHEDULE OPTIONS first_run = $2`, localFoo, firstRun)
	sqlDB.Exec(t, `PAUSE SCHEDULES SELECT id FROM [SHOW SCHEDULES FOR BACKUP]`)

	injectStats(t, sqlDB, "data.bank", "id")
	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)

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

	doneRestore := make(chan struct{})
	go func() {
		sqlDBRestore.Exec(t, `RESTORE FROM $1`, localFoo)
		close(doneRestore)
	}()

	// Check that zones are restored during pre-restore.
	t.Run("ensure zones are restored during pre-restore", func(t *testing.T) {
		<-restoredZones
		// Not specifying the schema makes the query search using defaultdb first.
		// which ends up returning the error
		// pq: database "defaultdb" is offline: restoring
		checkZones := "SELECT * FROM system.public.zones"
		sqlDBRestore.CheckQueryResults(t, checkZones, sqlDB.QueryStr(t, checkZones))

		// Check that the user tables are still offline.
		sqlDBRestore.ExpectErr(t, "database \"data\" is offline: restoring", "SELECT * FROM data.public.bank")

		// Check there is no data in the span that we expect user data to be imported.
		store := tcRestore.GetFirstStoreFromServer(t, 0)
		startKey := keys.SystemSQLCodec.TablePrefix(bootstrap.TestingUserDescID(0))
		endKey := keys.SystemSQLCodec.TablePrefix(uint32(maxBackupTableID)).PrefixEnd()
		it := store.Engine().NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
			UpperBound: endKey,
		})
		defer it.Close()
		it.SeekGE(storage.MVCCKey{Key: startKey})
		hasKey, err := it.Valid()
		require.NoError(t, err)
		require.False(t, hasKey)
	})

	// Allow the restore to make progress after we've checked the pre-restore
	// stage.
	close(allowProgressAfterPreRestore)

	// Wait for the restore to finish before checking that it did the right thing.
	<-doneRestore

	t.Run("ensure all databases restored", func(t *testing.T) {
		sqlDBRestore.CheckQueryResults(t,
			`SELECT database_name, owner FROM [SHOW DATABASES]`,
			[][]string{
				{"data", security.RootUser},
				{"data2", security.RootUser},
				{"defaultdb", security.RootUser},
				{"postgres", security.RootUser},
				{"system", security.NodeUser},
			})
	})

	t.Run("ensure all schemas are restored", func(t *testing.T) {
		expectedSchemas := map[string][][]string{
			"defaultdb": {{"crdb_internal"}, {"information_schema"}, {"pg_catalog"}, {"pg_extension"}, {"public"}, {"test_schema"}},
			"data":      {{"crdb_internal"}, {"information_schema"}, {"pg_catalog"}, {"pg_extension"}, {"public"}, {"test_data_schema"}},
			"data2":     {{"crdb_internal"}, {"empty_schema"}, {"information_schema"}, {"pg_catalog"}, {"pg_extension"}, {"public"}},
		}
		for dbName, expectedSchemas := range expectedSchemas {
			sqlDBRestore.CheckQueryResults(t,
				fmt.Sprintf(`USE %s; SELECT schema_name FROM [SHOW SCHEMAS] ORDER BY schema_name;`, dbName),
				expectedSchemas)
		}
	})

	t.Run("ensure system table data restored", func(t *testing.T) {
		// Note the absence of the jobs table. Jobs are tested by another test as
		// jobs are created during the RESTORE process.
		systemTablesToVerify := []string{
			systemschema.CommentsTable.GetName(),
			systemschema.LocationsTable.GetName(),
			systemschema.RoleMembersTable.GetName(),
			systemschema.RoleOptionsTable.GetName(),
			systemschema.SettingsTable.GetName(),
			systemschema.TableStatisticsTable.GetName(),
			systemschema.UITable.GetName(),
			systemschema.UsersTable.GetName(),
			systemschema.ScheduledJobsTable.GetName(),
		}

		verificationQueries := make([]string, len(systemTablesToVerify))
		// Populate the list of tables we expect to be restored as well as queries
		// that can be used to ensure that data in those tables is restored.
		for i, table := range systemTablesToVerify {
			switch table {
			case systemschema.TableStatisticsTable.GetName():
				// createdAt and statisticsID are re-generated on RESTORE.
				query := `SELECT "tableID", name, "columnIDs", "rowCount" FROM system.table_statistics`
				verificationQueries[i] = query
			case systemschema.SettingsTable.GetName():
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
		tableIDCheck := "SELECT * FROM system.namespace ORDER BY id"
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
		newJobsStr := sqlDBRestore.QueryStr(t, jobsQuery)
		newJobs := make(map[string][]string)

		for _, newJob := range newJobsStr {
			// The first element of the slice is the job id.
			newJobs[newJob[0]] = newJob
		}
		for _, oldJob := range preBackupJobs {
			newJob, ok := newJobs[oldJob[0]]
			if !ok {
				t.Errorf("Expected to find job %+v in RESTORE cluster, but not found", oldJob)
			}
			require.Equal(t, oldJob, newJob)
		}
	})

	t.Run("zone_configs", func(t *testing.T) {
		// The restored zones should be a superset of the zones in the backed up
		// cluster.
		zoneIDsResult := sqlDB.QueryStr(t, `SELECT id FROM system.zones`)
		var q strings.Builder
		q.WriteString("SELECT * FROM system.zones WHERE id IN (")
		for i, restoreZoneIDRow := range zoneIDsResult {
			if i > 0 {
				q.WriteString(", ")
			}
			q.WriteString(restoreZoneIDRow[0])
		}
		q.WriteString(")")
		sqlDBRestore.CheckQueryResults(t, q.String(), sqlDB.QueryStr(t, q.String()))
	})

	t.Run("ensure that tables can be created at the excepted ID", func(t *testing.T) {
		var maxID, dbID, tableID int
		sqlDBRestore.QueryRow(t, "SELECT max(id) FROM system.namespace").Scan(&maxID)
		dbName, tableName := "new_db", "new_table"
		sqlDBRestore.Exec(t, fmt.Sprintf("CREATE DATABASE %s", dbName))
		sqlDBRestore.Exec(t, fmt.Sprintf("CREATE TABLE %s.%s (a int)", dbName, tableName))
		sqlDBRestore.QueryRow(t,
			fmt.Sprintf("SELECT id FROM system.namespace WHERE name = '%s'", dbName)).Scan(&dbID)
		require.True(t, dbID > maxID)
		sqlDBRestore.QueryRow(t,
			fmt.Sprintf("SELECT id FROM system.namespace WHERE name = '%s'", tableName)).Scan(&tableID)
		require.True(t, tableID > maxID)
		require.NotEqual(t, dbID, tableID)
	})
}

func TestIncrementalFullClusterBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	const incrementalBackupLocation = "nodelocal://0/inc-full-backup"
	_, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	_, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	sqlDB.Exec(t, "CREATE USER maxroach1")

	sqlDB.Exec(t, `BACKUP TO $1 INCREMENTAL FROM $2`, incrementalBackupLocation, localFoo)
	sqlDBRestore.Exec(t, `RESTORE FROM $1, $2`, localFoo, incrementalBackupLocation)

	checkQuery := "SELECT * FROM system.users"
	sqlDBRestore.CheckQueryResults(t, checkQuery, sqlDB.QueryStr(t, checkQuery))
}

// TestEmptyFullClusterResotre ensures that we can backup and restore a full
// cluster backup with only metadata (no user data). Regression test for #49573.
func TestEmptyFullClusterRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sqlDB, tempDir, cleanupFn := createEmptyCluster(t, singleNode)
	_, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{})
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
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	_, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	_, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{})
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
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	_, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	_, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	sqlDBRestore.Exec(t, `CREATE DATABASE foo`)
	sqlDBRestore.ExpectErr(t,
		"pq: full cluster restore can only be run on a cluster with no tables or databases but found 2 descriptors: foo, public",
		`RESTORE FROM $1`, localFoo,
	)
}

func TestClusterRestoreSystemTableOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	_, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	tcRestore, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode,
		tempDir,
		InitManualReplication, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	restoredSystemTables := make([]string, 0)
	for _, server := range tcRestore.Servers {
		registry := server.JobRegistry().(*jobs.Registry)
		registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
			jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
				r := raw.(*restoreResumer)
				r.testingKnobs.duringSystemTableRestoration = func(systemTableName string) error {
					restoredSystemTables = append(restoredSystemTables, systemTableName)
					return nil
				}
				return r
			},
		}
	}

	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	sqlDBRestore.Exec(t, `RESTORE FROM $1`, localFoo)
	// Check that the settings table is the last of the system tables to be
	// restored.
	require.Equal(t, restoredSystemTables[len(restoredSystemTables)-1],
		systemschema.SettingsTable.GetName())
}

func TestDisallowFullClusterRestoreOfNonFullBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	_, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	_, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{})
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
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	sqlDB.Exec(t, `CREATE DATABASE data2`)
	sqlDB.Exec(t, `RESTORE data.bank FROM $1 WITH into_db='data2'`, localFoo)

	checkResults := "SELECT * FROM data.bank"
	sqlDB.CheckQueryResults(t, checkResults, sqlDB.QueryStr(t, checkResults))
}

func TestRestoreFromFullClusterBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 10
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	sqlDB.Exec(t, `DROP DATABASE data`)

	t.Run("database", func(t *testing.T) {
		sqlDB.Exec(t, `RESTORE DATABASE data FROM $1`, localFoo)
		defer sqlDB.Exec(t, `DROP DATABASE data`)
		sqlDB.CheckQueryResults(t, "SELECT count(*) FROM data.bank", [][]string{{"10"}})
	})

	t.Run("table", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE DATABASE data`)
		defer sqlDB.Exec(t, `DROP DATABASE data`)
		sqlDB.Exec(t, `RESTORE data.bank FROM $1`, localFoo)
		sqlDB.CheckQueryResults(t, "SELECT count(*) FROM data.bank", [][]string{{"10"}})
	})

	t.Run("tables", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE DATABASE data`)
		defer sqlDB.Exec(t, `DROP DATABASE data`)
		sqlDB.Exec(t, `RESTORE data.* FROM $1`, localFoo)
		sqlDB.CheckQueryResults(t, "SELECT count(*) FROM data.bank", [][]string{{"10"}})
	})

	t.Run("system tables", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE DATABASE temp_sys`)
		sqlDB.Exec(t, `RESTORE system.users FROM $1 WITH into_db='temp_sys'`, localFoo)
		sqlDB.CheckQueryResults(t, "SELECT * FROM temp_sys.users", sqlDB.QueryStr(t, "SELECT * FROM system.users"))
	})
}

func TestCreateDBAndTableIncrementalFullClusterBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, 0, InitManualReplication)
	defer cleanupFn()

	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
	sqlDB.Exec(t, `CREATE DATABASE foo`)
	sqlDB.Exec(t, `CREATE TABLE foo.bar (a int)`)

	// Ensure that the new backup succeeds.
	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)
}

// TestClusterRestoreFailCleanup tests that a failed RESTORE is cleaned up.
func TestClusterRestoreFailCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "takes >1 min under race")
	params := base.TestServerArgs{}
	// Disable GC job so that the final check of crdb_internal.tables is
	// guaranteed to not be cleaned up. Although this was never observed by a
	// stress test, it is here for safety.
	blockCh := make(chan struct{})
	defer close(blockCh)
	params.Knobs.GCJob = &sql.GCJobTestingKnobs{
		RunBeforeResume: func(_ jobspb.JobID) error { <-blockCh; return nil },
	}

	const numAccounts = 1000
	_, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	// Setup the system systemTablesToVerify to ensure that they are copied to the new cluster.
	// Populate system.users.
	for i := 0; i < 1000; i++ {
		sqlDB.Exec(t, fmt.Sprintf("CREATE USER maxroach%d", i))
	}

	sqlDB.Exec(t, `BACKUP TO 'nodelocal://1/missing-ssts'`)

	// Bugger the backup by removing the SST files. (Note this messes up all of
	// the backups, but there is only one at this point.)
	if err := filepath.Walk(tempDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			t.Fatal(err)
		}
		if info.Name() == backupManifestName || !strings.HasSuffix(path, ".sst") {
			return nil
		}
		return os.Remove(path)
	}); err != nil {
		t.Fatal(err)
	}

	// Create a non-corrupted backup.
	// Populate system.jobs.
	// Note: this is not the backup under test, this just serves as a job which
	// should appear in the restore.
	// This job will eventually fail since it will run from a new cluster.
	sqlDB.Exec(t, `BACKUP data.bank TO 'nodelocal://0/throwawayjob'`)
	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)

	t.Run("during restoration of data", func(t *testing.T) {
		_, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{})
		defer cleanupEmptyCluster()
		sqlDBRestore.ExpectErr(t, "sst: no such file", `RESTORE FROM 'nodelocal://1/missing-ssts'`)
		// Verify the failed RESTORE added some DROP tables.
		// Note that the system tables here correspond to the temporary tables
		// imported, not the system tables themselves.
		sqlDBRestore.CheckQueryResults(t,
			`SELECT name FROM system.crdb_internal.tables WHERE state = 'DROP' ORDER BY name`,
			[][]string{
				{"bank"},
				{"comments"},
				{"database_role_settings"},
				{"jobs"},
				{"locations"},
				{"role_members"},
				{"role_options"},
				{"scheduled_jobs"},
				{"settings"},
				{"tenant_settings"},
				{"ui"},
				{"users"},
				{"zones"},
			},
		)
	})

	// This test retries the job (by injected a retry error) after restoring a
	// every system table that has a custom restore function. This tried to tease
	// out any errors that may occur if some of the system table restoration
	// functions are not idempotent.
	t.Run("retry-during-custom-system-table-restore", func(t *testing.T) {
		customRestoreSystemTables := make([]string, 0)
		for table, config := range systemTableBackupConfiguration {
			if config.customRestoreFunc != nil {
				customRestoreSystemTables = append(customRestoreSystemTables, table)
			}
		}
		for _, customRestoreSystemTable := range customRestoreSystemTables {
			t.Run(customRestoreSystemTable, func(t *testing.T) {
				args := base.TestClusterArgs{ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()},
				}}
				tcRestore, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, args)
				defer cleanupEmptyCluster()

				// Inject a retry error, that returns once.
				alreadyErrored := false
				for _, server := range tcRestore.Servers {
					registry := server.JobRegistry().(*jobs.Registry)
					registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
						jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
							r := raw.(*restoreResumer)
							r.testingKnobs.duringSystemTableRestoration = func(systemTableName string) error {
								if !alreadyErrored && systemTableName == customRestoreSystemTable {
									alreadyErrored = true
									return jobs.MarkAsRetryJobError(errors.New("injected error"))
								}
								return nil
							}
							return r
						},
					}
				}
				// The initial restore will return an error, and restart.
				sqlDBRestore.ExpectErr(t, `running execution from '.*' to '.*' on \d+ failed: injected error`, `RESTORE FROM $1`, localFoo)
				// Reduce retry delays.
				sqlDBRestore.Exec(t, "SET CLUSTER SETTING jobs.registry.retry.initial_delay = '1ms'")
				// Expect the restore to succeed.
				sqlDBRestore.CheckQueryResultsRetry(t,
					`SELECT count(*) FROM [SHOW JOBS] WHERE job_type = 'RESTORE' AND status = 'succeeded'`,
					[][]string{{"1"}})
			})
		}
	})

	t.Run("during system table restoration", func(t *testing.T) {
		tcRestore, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{})
		defer cleanupEmptyCluster()

		// Bugger the backup by injecting a failure while restoring the system data.
		for _, server := range tcRestore.Servers {
			registry := server.JobRegistry().(*jobs.Registry)
			registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
				jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
					r := raw.(*restoreResumer)
					r.testingKnobs.duringSystemTableRestoration = func(_ string) error {
						return errors.New("injected error")
					}
					return r
				},
			}
		}

		sqlDBRestore.ExpectErr(t, "injected error", `RESTORE FROM $1`, localFoo)
		// Verify the failed RESTORE added some DROP tables.
		// Note that the system tables here correspond to the temporary tables
		// imported, not the system tables themselves.
		sqlDBRestore.CheckQueryResults(t,
			`SELECT name FROM system.crdb_internal.tables WHERE state = 'DROP' ORDER BY name`,
			[][]string{
				{"bank"},
				{"comments"},
				{"database_role_settings"},
				{"jobs"},
				{"locations"},
				{"role_members"},
				{"role_options"},
				{"scheduled_jobs"},
				{"settings"},
				{"tenant_settings"},
				{"ui"},
				{"users"},
				{"zones"},
			},
		)
	})

	t.Run("after offline tables", func(t *testing.T) {
		tcRestore, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{})
		defer cleanupEmptyCluster()

		// Bugger the backup by injecting a failure while restoring the system data.
		for _, server := range tcRestore.Servers {
			registry := server.JobRegistry().(*jobs.Registry)
			registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
				jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
					r := raw.(*restoreResumer)
					r.testingKnobs.afterOfflineTableCreation = func() error {
						return errors.New("injected error")
					}
					return r
				},
			}
		}

		sqlDBRestore.ExpectErr(t, "injected error", `RESTORE FROM $1`, localFoo)
	})
}

// A regression test where dropped descriptors would appear in the set of
// `Descriptors`.
func TestDropDatabaseRevisionHistory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1
	_, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	sqlDB.Exec(t, `BACKUP TO $1 WITH revision_history`, localFoo)
	sqlDB.Exec(t, `
CREATE DATABASE same_name_db;
DROP DATABASE same_name_db;
CREATE DATABASE same_name_db;
`)
	sqlDB.Exec(t, `BACKUP TO $1 WITH revision_history`, localFoo)

	_, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{})
	defer cleanupEmptyCluster()
	sqlDBRestore.Exec(t, `RESTORE FROM $1`, localFoo)
	sqlDBRestore.ExpectErr(t, `database "same_name_db" already exists`, `CREATE DATABASE same_name_db`)
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

	testCases := make([]testCase, 0)
	ts := make([]string, 6)

	var tc testCase
	const numAccounts = 1
	_, sqlDB, tempDir, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()
	sqlDB.Exec(t, `CREATE DATABASE d1`)
	sqlDB.Exec(t, `CREATE TABLE d1.t (a INT)`)
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
	sqlDB.Exec(t, `CREATE TABLE d2.t (a INT)`)
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
			checkSQLDB.Exec(t, `CREATE TABLE d1.t (a INT)`)
			checkSQLDB.ExpectErr(t, `database "d2" already exists`, `CREATE DATABASE d2`)
			checkSQLDB.ExpectErr(t, `relation "d2.public.t" already exists`, `CREATE TABLE d2.t (a INT)`)
		},
	}
	testCases = append(testCases, tc)
	sqlDB.Exec(t, `BACKUP TO $1 WITH revision_history`, localFoo)

	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[3])
	sqlDB.Exec(t, `DROP DATABASE d2;`)
	tc = testCase{
		ts: ts[3],
		check: func(t *testing.T, checkSQLDB *sqlutils.SQLRunner) {
			checkSQLDB.Exec(t, `CREATE DATABASE d1`)
			checkSQLDB.Exec(t, `CREATE TABLE d1.t (a INT)`)
			checkSQLDB.ExpectErr(t, `database "d2" already exists`, `CREATE DATABASE d2`)
			checkSQLDB.ExpectErr(t, `relation "d2.public.t" already exists`, `CREATE TABLE d2.t (a INT)`)
		},
	}
	testCases = append(testCases, tc)
	sqlDB.Exec(t, `BACKUP TO $1 WITH revision_history`, localFoo)

	sqlDB.Exec(t, `CREATE DATABASE d1`)
	sqlDB.Exec(t, `CREATE TABLE d1.t (a INT)`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[4])
	tc = testCase{
		ts: ts[4],
		check: func(t *testing.T, checkSQLDB *sqlutils.SQLRunner) {
			checkSQLDB.ExpectErr(t, `database "d1" already exists`, `CREATE DATABASE d1`)
			checkSQLDB.ExpectErr(t, `relation "d1.public.t" already exists`, `CREATE TABLE d1.t (a INT)`)
			checkSQLDB.Exec(t, `CREATE DATABASE d2`)
			checkSQLDB.Exec(t, `CREATE TABLE d2.t (a INT)`)
		},
	}
	testCases = append(testCases, tc)

	sqlDB.Exec(t, `DROP DATABASE d1`)
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts[5])
	tc = testCase{
		ts: ts[5],
		check: func(t *testing.T, checkSQLDB *sqlutils.SQLRunner) {
			checkSQLDB.Exec(t, `CREATE DATABASE d1`)
			checkSQLDB.Exec(t, `CREATE TABLE d1.t (a INT)`)
			checkSQLDB.Exec(t, `CREATE DATABASE d2`)
			checkSQLDB.Exec(t, `CREATE TABLE d2.t (a INT)`)
		},
	}
	testCases = append(testCases, tc)
	sqlDB.Exec(t, `BACKUP TO $1 WITH revision_history`, localFoo)

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("t%d", i), func(t *testing.T) {
			_, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{})
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
		DistSQL: &execinfra.TestingKnobs{
			BackupRestoreTestingKnobs: &sql.BackupRestoreTestingKnobs{
				RunAfterProcessingRestoreSpanEntry: func(_ context.Context) {
					if entriesCount == 0 {
						close(dbRestoreStarted)
					}
					if entriesCount == restoreBlockEntiresThreshold {
						<-blockDBRestore
					}

					entriesCount++
				},
			}},
	}
	params.ServerArgs.Knobs = knobs

	const numAccounts = 1000
	ctx := context.Background()
	_, srcDB, tempDir, cleanupSrc := backupRestoreTestSetupWithParams(t, singleNode, numAccounts, InitManualReplication, params)
	defer cleanupSrc()

	dbBackupLoc := "nodelocal://0/my_db_backup"
	clusterBackupLoc := "nodelocal://0/my_cluster_backup"

	// the small test-case will get entirely buffered/merged by small-file merging
	// and not report any progress in the meantime unless it is disabled.
	srcDB.Exec(t, `SET CLUSTER SETTING bulkio.backup.file_size = '1'`)

	// Test servers only have 128MB root memory monitors, reduce the buffer size
	// so we don't see memory errors.
	srcDB.Exec(t, `SET CLUSTER SETTING bulkio.backup.merge_file_buffer_size = '1MiB'`)

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
		_, destDB, cleanupDst := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{})
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
		args := base.TestClusterArgs{ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()}},
		}
		_, destDB, cleanupDst := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, args)
		defer cleanupDst()

		destDB.Exec(t, `RESTORE FROM $1 AS OF SYSTEM TIME `+tsMidRestore, clusterBackupLoc)

		// Wait for the cluster restore job to finish, as well as the restored RESTORE TABLE
		// job to cancel.
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

// TestClusterRevisionDoesNotBackupOptOutSystemTables is a regression test for a
// bug that was introduced where we would include revisions for descriptors that
// are not supposed to be backed up egs: system tables that are opted out.
//
// The test would previously fail with an error that the descriptors table (an
// opt out system table) did not have a span covering the time between the
// `EndTime` of the first backup and second backup, since there are no revisions
// to it between those backups.
func TestClusterRevisionDoesNotBackupOptOutSystemTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc, _, _, cleanup := backupRestoreTestSetup(t, singleNode, 10, InitManualReplication)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)
	defer cleanup()

	sqlDB.Exec(t, `
CREATE DATABASE test;
USE test;
CREATE TABLE foo (id INT);
BACKUP TO 'nodelocal://1/foo' WITH revision_history;
BACKUP TO 'nodelocal://1/foo' WITH revision_history;
CREATE TABLE bar (id INT);
BACKUP TO 'nodelocal://1/foo' WITH revision_history;
`)
}

func TestRestoreWithRecreatedDefaultDB(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sqlDB, tempDir, cleanupFn := createEmptyCluster(t, singleNode)
	_, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	sqlDB.Exec(t, `
DROP DATABASE defaultdb;
CREATE DATABASE defaultdb; 
`)
	row := sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'defaultdb'`)
	var expectedDefaultDBID string
	row.Scan(&expectedDefaultDBID)
	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)

	sqlDBRestore.Exec(t, `RESTORE FROM $1`, localFoo)

	sqlDBRestore.CheckQueryResults(t, `SELECT * FROM system.namespace WHERE name = 'defaultdb'`, [][]string{
		{"0", "0", "defaultdb", expectedDefaultDBID},
	})
}

func TestRestoreWithDroppedDefaultDB(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sqlDB, tempDir, cleanupFn := createEmptyCluster(t, singleNode)
	_, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	sqlDB.Exec(t, `
DROP DATABASE defaultdb;
`)
	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)

	sqlDBRestore.Exec(t, `RESTORE FROM $1`, localFoo)

	sqlDBRestore.CheckQueryResults(t, `SELECT count(*) FROM system.namespace WHERE name = 'defaultdb'`, [][]string{
		{"0"},
	})
}

func TestRestoreToClusterWithDroppedDefaultDB(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sqlDB, tempDir, cleanupFn := createEmptyCluster(t, singleNode)
	_, sqlDBRestore, cleanupEmptyCluster := backupRestoreTestSetupEmpty(t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{})
	defer cleanupFn()
	defer cleanupEmptyCluster()

	expectedRow := sqlDB.QueryRow(t, `SELECT * FROM system.namespace WHERE name = 'defaultdb'`)
	var parentID, parentSchemaID, ID int
	var name string
	expectedRow.Scan(&parentID, &parentSchemaID, &name, &ID)

	sqlDB.Exec(t, `BACKUP TO $1`, localFoo)

	sqlDBRestore.Exec(t, `
DROP DATABASE defaultdb;
`)
	sqlDBRestore.Exec(t, `RESTORE FROM $1`, localFoo)
	sqlDBRestore.CheckQueryResults(t, `SELECT * FROM system.namespace WHERE name = 'defaultdb'`, [][]string{
		{fmt.Sprint(parentID), fmt.Sprint(parentSchemaID), name, fmt.Sprint(ID)},
	})
}
