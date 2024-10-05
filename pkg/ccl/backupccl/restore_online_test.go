// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupccl

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/fingerprintutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

func TestOnlineRestoreBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()

	ctx := context.Background()

	const numAccounts = 1000
	params := base.TestClusterArgs{
		// Online restore is not supported in a secondary tenant yet.
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	}
	tc, sqlDB, dir, cleanupFn := backupRestoreTestSetupWithParams(t, singleNode, numAccounts, InitManualReplication, params)
	defer cleanupFn()
	externalStorage := "nodelocal://1/backup"

	createStmt := `SELECT create_statement FROM [SHOW CREATE TABLE data.bank]`
	createStmtRes := sqlDB.QueryStr(t, createStmt)

	sqlDB.Exec(t, fmt.Sprintf("BACKUP INTO '%s'", externalStorage))

	rtc, rSQLDB, cleanupFnRestored := backupRestoreTestSetupEmpty(t, 1, dir, InitManualReplication, params)
	defer cleanupFnRestored()
	var preRestoreTs float64
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&preRestoreTs)

	bankOnlineRestore(t, rSQLDB, numAccounts, externalStorage)

	fpSrc, err := fingerprintutils.FingerprintDatabase(ctx, tc.Conns[0], "data", fingerprintutils.Stripped())
	require.NoError(t, err)
	fpDst, err := fingerprintutils.FingerprintDatabase(ctx, rtc.Conns[0], "data", fingerprintutils.Stripped())
	require.NoError(t, err)
	require.NoError(t, fingerprintutils.CompareDatabaseFingerprints(fpSrc, fpDst))

	assertMVCCOnlineRestore(t, rSQLDB, preRestoreTs)
	assertOnlineRestoreWithRekeying(t, sqlDB, rSQLDB)

	// Wait for the download job to complete.
	var downloadJobID jobspb.JobID
	rSQLDB.QueryRow(t, `SELECT job_id FROM [SHOW JOBS] WHERE description LIKE '%Background Data Download%'`).Scan(&downloadJobID)
	jobutils.WaitForJobToSucceed(t, rSQLDB, downloadJobID)

	rSQLDB.CheckQueryResults(t, createStmt, createStmtRes)
}

func TestOnlineRestorePartitioned(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()

	srv, sqlDB, _, cleanupFn := backupRestoreTestSetupWithParams(t, 3, 100,
		InitManualReplication,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant},
		},
	)
	defer cleanupFn()

	sqlDB.Exec(t, `BACKUP DATABASE data TO ('nodelocal://1/a?COCKROACH_LOCALITY=default', 
		'nodelocal://1/b?COCKROACH_LOCALITY=dc%3Ddc2', 
		'nodelocal://1/c?COCKROACH_LOCALITY=dc%3Ddc3')`)

	j := sqlDB.QueryStr(t, `RESTORE DATABASE data FROM ('nodelocal://1/a?COCKROACH_LOCALITY=default', 
		'nodelocal://1/b?COCKROACH_LOCALITY=dc%3Ddc2', 
		'nodelocal://1/c?COCKROACH_LOCALITY=dc%3Ddc3') WITH new_db_name='d2', EXPERIMENTAL DEFERRED COPY`)

	srv.Servers[0].JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()

	sqlDB.Exec(t, fmt.Sprintf(`SHOW JOB WHEN COMPLETE %s`, j[0][4]))
}

func TestOnlineRestoreStatementResult(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()

	const numAccounts = 1
	_, sqlDB, _, cleanupFn := backupRestoreTestSetupWithParams(
		t,
		singleNode,
		numAccounts,
		InitManualReplication,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			},
		},
	)
	defer cleanupFn()

	sqlDB.ExecMultiple(
		t,
		"USE data",
		"CREATE TABLE foo (x INT PRIMARY KEY, y INT)",
		"INSERT INTO foo VALUES (1, 2)",
	)
	sqlDB.Exec(t, "BACKUP DATABASE data INTO $1", localFoo)

	rows := sqlDB.Query(
		t,
		"RESTORE DATABASE data FROM LATEST IN $1 WITH OPTIONS (new_db_name='data2', experimental deferred copy)",
		localFoo,
	)
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if a, e := columns, []string{
		"job_id", "tables", "approx_rows", "approx_bytes", "background_download_job_id",
	}; !reflect.DeepEqual(e, a) {
		t.Fatalf("unexpected columns:\n%s", strings.Join(pretty.Diff(e, a), "\n"))
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		t.Fatal("zero rows in result")
	}

	var id, expectedID, tables, approxRows, approxBytes, downloadJobID int64
	require.NoError(t, rows.Scan(
		&id, &tables, &approxRows, &approxBytes, &downloadJobID,
	))
	sqlDB.QueryRow(t,
		`SELECT job_id FROM crdb_internal.jobs WHERE job_id = $1`, id,
	).Scan(
		&expectedID,
	)

	require.Equal(t, expectedID, id, "result does not match system.jobs")
	require.Equal(t, id+1, downloadJobID, "download job id should be one greater than restore job id")
	require.Equal(t, int64(2), tables, "expected 2 tables to be in result")
	require.Greater(t, approxRows, int64(0), "no rows estimated in result")
	require.Greater(t, approxBytes, int64(0), "no bytes estimated in result")

	if rows.Next() {
		t.Fatal("more than one row in result")
	}
}

// TestOnlineRestoreWaitForDownload checks that the download job succeeeds even
// if no queries are run on the restoring key space.
func TestOnlineRestoreWaitForDownload(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()

	const numAccounts = 1000
	_, sqlDB, _, cleanupFn := backupRestoreTestSetupWithParams(t, singleNode, numAccounts, InitManualReplication, base.TestClusterArgs{
		// Online restore is not supported in a secondary tenant yet.
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	})
	defer cleanupFn()
	externalStorage := "nodelocal://1/backup"

	sqlDB.Exec(t, fmt.Sprintf("BACKUP INTO '%s'", externalStorage))

	sqlDB.Exec(t, fmt.Sprintf("RESTORE DATABASE data FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY, new_db_name=data2", externalStorage))

	// Wait for the download job to complete.
	var downloadJobID jobspb.JobID
	sqlDB.QueryRow(t, `SELECT job_id FROM [SHOW JOBS] WHERE description LIKE '%Background Data Download%' ORDER BY created DESC LIMIT 1`).Scan(&downloadJobID)
	jobutils.WaitForJobToSucceed(t, sqlDB, downloadJobID)
	sqlDB.CheckQueryResults(t, `SELECT * FROM crdb_internal.tenant_span_stats(
		ARRAY(SELECT(crdb_internal.table_span('data2.bank'::regclass::oid::int)[1], crdb_internal.table_span('data2.bank'::regclass::oid::int)[2]))
	) WHERE (stats->>'external_file_bytes')::int > 0`, [][]string{})
}

// TestOnlineRestoreTenant runs an online restore of a tenant and ensures the
// restore is not MVCC compliant.
//
// NB: With prefix synthesis, we temporarliy do not support online restore of tenants.
func TestOnlineRestoreTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()

	externalStorage := "nodelocal://1/backup"

	params := base.TestClusterArgs{ServerArgs: base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			TenantTestingKnobs: &sql.TenantTestingKnobs{
				// The tests expect specific tenant IDs to show up.
				EnableTenantIDReuse: true,
			},
		},

		DefaultTestTenant: base.TestControlsTenantsExplicitly},
	}
	const numAccounts = 1

	tc, systemDB, dir, cleanupFn := backupRestoreTestSetupWithParams(
		t, singleNode, numAccounts, InitManualReplication, params,
	)
	_, _ = tc, systemDB
	defer cleanupFn()
	srv := tc.Server(0)

	_ = securitytest.EmbeddedTenantIDs()

	_, conn10 := serverutils.StartTenant(t, srv, base.TestTenantArgs{TenantID: roachpb.MustMakeTenantID(10)})
	defer conn10.Close()
	tenant10 := sqlutils.MakeSQLRunner(conn10)
	tenant10.Exec(t, `CREATE DATABASE foo; CREATE TABLE foo.bar(i int primary key); INSERT INTO foo.bar VALUES (110), (210)`)

	systemDB.Exec(t, fmt.Sprintf(`BACKUP TENANT 10 INTO '%s'`, externalStorage))

	restoreTC, rSQLDB, cleanupFnRestored := backupRestoreTestSetupEmpty(t, 1, dir, InitManualReplication, params)
	defer cleanupFnRestored()

	var preRestoreTs float64
	tenant10.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&preRestoreTs)

	// Restore the tenant twice: once below and once above the old ID, to show
	// that we can rewrite it in either direction.
	rSQLDB.Exec(t, fmt.Sprintf("RESTORE TENANT 10 FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY, TENANT_NAME = 'below', TENANT = '2'", externalStorage))
	rSQLDB.Exec(t, fmt.Sprintf("RESTORE TENANT 10 FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY, TENANT_NAME = 'above', TENANT = '20'", externalStorage))
	rSQLDB.Exec(t, "ALTER TENANT below STOP SERVICE")
	rSQLDB.Exec(t, "ALTER TENANT above STOP SERVICE")
	rSQLDB.CheckQueryResults(t, "SELECT fingerprint FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TENANT below]",
		rSQLDB.QueryStr(t, `SELECT fingerprint FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TENANT above]`))

	secondaryStopper := stop.NewStopper()
	_, cBelow := serverutils.StartTenant(
		t, restoreTC.Server(0), base.TestTenantArgs{
			TenantName: "below",
			TenantID:   roachpb.MustMakeTenantID(2),
			Stopper:    secondaryStopper,
		})
	_, cAbove := serverutils.StartTenant(
		t, restoreTC.Server(0), base.TestTenantArgs{
			TenantName: "above",
			TenantID:   roachpb.MustMakeTenantID(20),
			Stopper:    secondaryStopper,
		})

	defer func() {
		cBelow.Close()
		cAbove.Close()
		secondaryStopper.Stop(context.Background())
	}()
	dbBelow, dbAbove := sqlutils.MakeSQLRunner(cBelow), sqlutils.MakeSQLRunner(cAbove)
	dbBelow.CheckQueryResults(t, `select * from foo.bar`, tenant10.QueryStr(t, `select * from foo.bar`))
	dbAbove.CheckQueryResults(t, `select * from foo.bar`, tenant10.QueryStr(t, `select * from foo.bar`))

	// Ensure the restore of a tenant was not mvcc
	var maxRestoreMVCCTimestamp float64
	dbBelow.QueryRow(t, "SELECT max(crdb_internal_mvcc_timestamp) FROM foo.bar").Scan(&maxRestoreMVCCTimestamp)
	require.Greater(t, preRestoreTs, maxRestoreMVCCTimestamp)
	dbAbove.QueryRow(t, "SELECT max(crdb_internal_mvcc_timestamp) FROM foo.bar").Scan(&maxRestoreMVCCTimestamp)
	require.Greater(t, preRestoreTs, maxRestoreMVCCTimestamp)
}

func TestOnlineRestoreErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()

	_, sqlDB, dir, cleanupFn := backupRestoreTestSetup(t, singleNode, 1, InitManualReplication)
	defer cleanupFn()
	params := base.TestClusterArgs{
		// Online restore is not supported in a secondary tenant yet.
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	}
	_, rSQLDB, cleanupFnRestored := backupRestoreTestSetupEmpty(t, 1, dir, InitManualReplication, params)
	defer cleanupFnRestored()
	rSQLDB.Exec(t, "CREATE DATABASE data")
	var (
		fullBackup                = "nodelocal://1/full-backup"
		fullBackupWithRevs        = "nodelocal://1/full-backup-with-revs"
		incrementalBackup         = "nodelocal://1/incremental-backup"
		incrementalBackupWithRevs = "nodelocal://1/incremental-backup-with-revs"
	)

	t.Run("incremental backups are unsupported", func(t *testing.T) {
		sqlDB.Exec(t, fmt.Sprintf("BACKUP INTO '%s'", incrementalBackup))
		sqlDB.Exec(t, fmt.Sprintf("BACKUP INTO LATEST IN '%s'", incrementalBackup))
		rSQLDB.ExpectErr(t, "incremental backup not supported",
			fmt.Sprintf("RESTORE TABLE data.bank FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY", incrementalBackup))
	})
	t.Run("full backups with revision history are unsupported", func(t *testing.T) {
		var systemTime string
		sqlDB.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&systemTime)
		sqlDB.Exec(t, fmt.Sprintf("BACKUP INTO '%s' AS OF SYSTEM TIME '%s' WITH revision_history", fullBackupWithRevs, systemTime))
		rSQLDB.ExpectErr(t, "revision history backup not supported",
			fmt.Sprintf("RESTORE TABLE data.bank FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY", fullBackupWithRevs))
	})
	t.Run("incremental backups with revision history are unsupported", func(t *testing.T) {
		sqlDB.Exec(t, fmt.Sprintf("BACKUP INTO '%s' WITH revision_history", incrementalBackupWithRevs))
		sqlDB.Exec(t, fmt.Sprintf("BACKUP INTO LATEST IN '%s' WITH revision_history", incrementalBackupWithRevs))
		rSQLDB.ExpectErr(t, "incremental backup not supported",
			fmt.Sprintf("RESTORE TABLE data.bank FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY", incrementalBackupWithRevs))
	})
	t.Run("external storage locations that don't support early boot are unsupported", func(t *testing.T) {
		rSQLDB.Exec(t, "CREATE DATABASE bank")
		rSQLDB.Exec(t, "BACKUP INTO 'userfile:///my_backups'")
		rSQLDB.ExpectErr(t, "scheme userfile is not accessible during node startup",
			"RESTORE DATABASE bank FROM LATEST IN 'userfile:///my_backups' WITH EXPERIMENTAL DEFERRED COPY")
	})
	t.Run("verify_backup_table_data not supported", func(t *testing.T) {
		sqlDB.Exec(t, fmt.Sprintf("BACKUP INTO '%s'", fullBackup))
		sqlDB.ExpectErr(t, "cannot run online restore with verify_backup_table_data",
			fmt.Sprintf("RESTORE data FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY, schema_only, verify_backup_table_data", fullBackup))
	})
}

func bankOnlineRestore(
	t *testing.T, sqlDB *sqlutils.SQLRunner, numAccounts int, externalStorage string,
) {
	// Create a table in the default database to force table id rewriting.
	sqlDB.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY, s STRING);")

	sqlDB.Exec(t, "CREATE DATABASE data")
	sqlDB.Exec(t, fmt.Sprintf("RESTORE TABLE data.bank FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY", externalStorage))

	var restoreRowCount int
	sqlDB.QueryRow(t, "SELECT count(*) FROM data.bank").Scan(&restoreRowCount)
	require.Equal(t, numAccounts, restoreRowCount)
}

// assertMVCCOnlineRestore checks that online restore conducted mvcc compatible
// addsstable requests. Note that the restoring database is written to, so no
// fingerprinting can be done after this command.
func assertMVCCOnlineRestore(t *testing.T, sqlDB *sqlutils.SQLRunner, preRestoreTs float64) {
	// Check that Online Restore was MVCC
	var minRestoreMVCCTimestamp float64
	sqlDB.QueryRow(t, "SELECT min(crdb_internal_mvcc_timestamp) FROM data.bank").Scan(&minRestoreMVCCTimestamp)
	require.Greater(t, minRestoreMVCCTimestamp, preRestoreTs)

	// Check that we can write on top of OR data
	var maxRestoreMVCCTimestamp float64
	sqlDB.QueryRow(t, "SELECT max(crdb_internal_mvcc_timestamp) FROM data.bank").Scan(&maxRestoreMVCCTimestamp)
	sqlDB.Exec(t, "SET sql_safe_updates = false;")
	sqlDB.Exec(t, "UPDATE data.bank SET balance = balance+1;")

	var updateMVCCTimestamp float64
	sqlDB.QueryRow(t, "SELECT min(crdb_internal_mvcc_timestamp) FROM data.bank").Scan(&updateMVCCTimestamp)
	require.Greater(t, updateMVCCTimestamp, maxRestoreMVCCTimestamp)
}

func assertOnlineRestoreWithRekeying(
	t *testing.T, sqlDB *sqlutils.SQLRunner, rSQLDB *sqlutils.SQLRunner,
) {
	bankTableIDQuery := "SELECT id FROM system.namespace WHERE name = 'bank'"
	var (
		originalID int
		restoreID  int
	)
	sqlDB.QueryRow(t, bankTableIDQuery).Scan(&originalID)
	rSQLDB.QueryRow(t, bankTableIDQuery).Scan(&restoreID)
	require.NotEqual(t, originalID, restoreID)
}
