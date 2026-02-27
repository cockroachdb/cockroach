// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/fingerprintutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

var orParams = base.TestClusterArgs{
	// Online restore is not supported in a secondary tenant yet.
	ServerArgs: base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	},
}

var latestDownloadJobIDQuery = `SELECT id FROM system.jobs WHERE description LIKE '%Background Data Download%' ORDER BY created DESC LIMIT 1`

func onlineImpl(rng *rand.Rand) string {
	opt := "EXPERIMENTAL DEFERRED COPY"
	if rng.Intn(2) == 0 {
		opt = "EXPERIMENTAL COPY"
	}
	return opt
}

func TestOnlineRestoreBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()

	ctx := context.Background()

	const numAccounts = 1000

	tc, sqlDB, dir, cleanupFn := backupRestoreTestSetupWithParams(t, singleNode, numAccounts, InitManualReplication, orParams)
	defer cleanupFn()

	rtc, rSQLDB, cleanupFnRestored := backupRestoreTestSetupEmpty(t, 1, dir, InitManualReplication, orParams)
	defer cleanupFnRestored()

	externalStorage := backuptestutils.GetExternalStorageURI(t, "nodelocal://1/backup", "backup", sqlDB, rSQLDB)

	createStmt := `SELECT create_statement FROM [SHOW CREATE TABLE data.bank]`
	createStmtRes := sqlDB.QueryStr(t, createStmt)

	testutils.RunTrueAndFalse(t, "incremental", func(t *testing.T, incremental bool) {
		testutils.RunTrueAndFalse(t, "blocking download", func(t *testing.T, blockingDownload bool) {
			sqlDB.Exec(t, fmt.Sprintf("BACKUP DATABASE data INTO '%s'", externalStorage))

			if incremental {
				sqlDB.Exec(t, "UPDATE data.bank SET balance = balance+123 where true;")
				sqlDB.Exec(t, fmt.Sprintf("BACKUP DATABASE data INTO LATEST IN '%s'", externalStorage))
			}

			var preRestoreTs float64
			sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&preRestoreTs)

			bankOnlineRestore(t, rSQLDB, numAccounts, externalStorage, blockingDownload)

			fpSrc, err := fingerprintutils.FingerprintDatabase(ctx, tc.Conns[0], "data", fingerprintutils.Stripped())
			require.NoError(t, err)
			fpDst, err := fingerprintutils.FingerprintDatabase(ctx, rtc.Conns[0], "data", fingerprintutils.Stripped())
			require.NoError(t, err)
			require.NoError(t, fingerprintutils.CompareDatabaseFingerprints(fpSrc, fpDst))

			assertMVCCOnlineRestore(t, rSQLDB, preRestoreTs)
			assertOnlineRestoreWithRekeying(t, sqlDB, rSQLDB)

			if !blockingDownload {
				waitForLatestDownloadJobToSucceed(t, rSQLDB)
			}

			rSQLDB.CheckQueryResults(t, createStmt, createStmtRes)
			sqlDB.CheckQueryResults(t, jobutils.GetExternalBytesForConnectedTenant, [][]string{{"0"}})

			rSQLDB.Exec(t, "DROP DATABASE data CASCADE")
		})
	})

}

func TestOnlineRestoreRecovery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tmpDir := t.TempDir()

	defer nodelocal.ReplaceNodeLocalForTesting(tmpDir)()

	const numAccounts = 1000

	_, sqlDB, _, cleanupFn := backupRestoreTestSetupWithParams(t, singleNode, numAccounts, InitManualReplication, orParams)
	defer cleanupFn()

	trueExternalStorage := "nodelocal://1/backup"
	externalStorage := backuptestutils.GetExternalStorageURI(t, trueExternalStorage, "backup", sqlDB)

	restoreToPausedDownloadJob := func(t *testing.T, newDBName string) int {
		defer func() {
			sqlDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
		}()
		sqlDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'restore.before_download'")
		sqlDB.Exec(t, fmt.Sprintf("BACKUP DATABASE data INTO '%s'", externalStorage))
		var linkJobID int
		sqlDB.QueryRow(t, fmt.Sprintf("RESTORE DATABASE data FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY, new_db_name=%s, detached", externalStorage, newDBName)).Scan(&linkJobID)
		jobutils.WaitForJobToSucceed(t, sqlDB, jobspb.JobID(linkJobID))
		var downloadJobID int
		sqlDB.QueryRow(t, latestDownloadJobIDQuery).Scan(&downloadJobID)
		jobutils.WaitForJobToPause(t, sqlDB, jobspb.JobID(downloadJobID))

		var dbExists bool
		sqlDB.QueryRow(t, fmt.Sprintf("SELECT count(*) > 0 FROM system.namespace WHERE name = '%s'", newDBName)).Scan(&dbExists)
		require.True(t, dbExists, "database should exist")

		var externalBytes int64
		sqlDB.QueryRow(t, jobutils.GetExternalBytesForConnectedTenant).Scan(&externalBytes)
		require.Greater(t, externalBytes, int64(0), "external bytes should be greater than 0")
		return downloadJobID
	}

	checkRecovery := func(t *testing.T, dbName string) {
		sqlDB.CheckQueryResults(t, jobutils.GetExternalBytesForConnectedTenant, [][]string{{"0"}})
		var dbExists bool
		sqlDB.QueryRow(t, fmt.Sprintf("SELECT count(*) > 0 FROM system.namespace WHERE name = '%s'", dbName)).Scan(&dbExists)
		require.False(t, dbExists, "database %s should not exist", dbName)
	}

	t.Run("cancel download job", func(t *testing.T) {
		dbName := "data_cancel"
		downloadJobID := restoreToPausedDownloadJob(t, dbName)
		sqlDB.Exec(t, fmt.Sprintf("CANCEL JOB %d", downloadJobID))
		jobutils.WaitForJobToCancel(t, sqlDB, jobspb.JobID(downloadJobID))
		checkRecovery(t, dbName)
	})
	t.Run("delete file", func(t *testing.T) {
		dbName := "data_delete"
		downloadJobID := restoreToPausedDownloadJob(t, dbName)
		corruptBackup(t, sqlDB, tmpDir, trueExternalStorage)
		sqlDB.ExpectErr(t, "no such file or directory", "SELECT count(*) FROM data_delete.bank")
		sqlDB.Exec(t, fmt.Sprintf("RESUME JOB %d", downloadJobID))
		jobutils.WaitForJobToFail(t, sqlDB, jobspb.JobID(downloadJobID))
		checkRecovery(t, dbName)
	})
	t.Run("cancel link job", func(t *testing.T) {
		dbName := "data_link"
		defer func() {
			sqlDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
		}()
		sqlDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'restore.before_publishing_descriptors'")
		sqlDB.Exec(t, fmt.Sprintf("BACKUP DATABASE data INTO '%s'", externalStorage))
		var linkJobID int
		sqlDB.QueryRow(t, fmt.Sprintf("RESTORE DATABASE data FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY, new_db_name=%s, detached", externalStorage, dbName)).Scan(&linkJobID)
		jobutils.WaitForJobToPause(t, sqlDB, jobspb.JobID(linkJobID))
		sqlDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
		sqlDB.Exec(t, fmt.Sprintf("CANCEL JOB %d", linkJobID))
		jobutils.WaitForJobToCancel(t, sqlDB, jobspb.JobID(linkJobID))
		checkRecovery(t, dbName)
	})
	t.Run("delete file block job", func(t *testing.T) {
		dbName := "data_block"
		defer func() {
			sqlDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
		}()
		sqlDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'restore.before_download'")
		sqlDB.Exec(t, fmt.Sprintf("BACKUP DATABASE data INTO '%s'", externalStorage))
		var blockingJobID int
		sqlDB.QueryRow(t, fmt.Sprintf("RESTORE DATABASE data FROM LATEST IN '%s' WITH EXPERIMENTAL COPY, new_db_name=%s, detached", externalStorage, dbName)).Scan(&blockingJobID)
		jobutils.WaitForJobToPause(t, sqlDB, jobspb.JobID(blockingJobID))
		corruptBackup(t, sqlDB, tmpDir, trueExternalStorage)
		sqlDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
		sqlDB.Exec(t, fmt.Sprintf("RESUME JOB %d", blockingJobID))
		jobutils.WaitForJobToFail(t, sqlDB, jobspb.JobID(blockingJobID))
		checkRecovery(t, dbName)
	})
}

// We run full cluster online restore recovery in a separate environment since
// it requires dropping all databases and will impact other tests.
func TestFullClusterOnlineRestoreRecovery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tmpDir := t.TempDir()

	defer nodelocal.ReplaceNodeLocalForTesting(tmpDir)()

	const numAccounts = 1000

	_, sqlDB, _, cleanupFn := backupRestoreTestSetupWithParams(t, singleNode, numAccounts, InitManualReplication, orParams)
	defer cleanupFn()

	trueExternalStorage := "nodelocal://1/backup"
	externalStorage := backuptestutils.GetExternalStorageURI(t, trueExternalStorage, "backup", sqlDB)

	sqlDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'restore.before_download'")
	sqlDB.Exec(t, fmt.Sprintf("BACKUP INTO '%s'", externalStorage))

	// Reset cluster for full cluster restore.
	dbs := sqlDB.QueryStr(
		t, "SELECT database_name FROM [SHOW DATABASES] WHERE database_name != 'system'",
	)
	sqlDB.Exec(t, "USE system")
	for _, db := range dbs {
		sqlDB.Exec(t, fmt.Sprintf("DROP DATABASE %s CASCADE", db[0]))
	}

	var linkJobID jobspb.JobID
	sqlDB.QueryRow(
		t,
		fmt.Sprintf(
			"RESTORE FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY, detached", externalStorage,
		),
	).Scan(&linkJobID)
	jobutils.WaitForJobToSucceed(t, sqlDB, linkJobID)
	var downloadJobID jobspb.JobID
	sqlDB.QueryRow(t, latestDownloadJobIDQuery).Scan(&downloadJobID)
	jobutils.WaitForJobToPause(t, sqlDB, downloadJobID)
	corruptBackup(t, sqlDB, tmpDir, trueExternalStorage)
	sqlDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
	sqlDB.Exec(t, fmt.Sprintf("RESUME JOB %d", downloadJobID))
	jobutils.WaitForJobToFail(t, sqlDB, downloadJobID)
}

func corruptBackup(t *testing.T, sqlDB *sqlutils.SQLRunner, ioDir string, uri string) {
	var filePath, spanStart, spanEnd string
	// We delete the last SST file in SHOW BACKUP to ensure the deletion of an SST
	// file that backs a user-table.
	// https://github.com/cockroachdb/cockroach/issues/148408 illustrates how
	// deleting the backing SST file of a system table will not necessarily cause
	// a download job to fail.
	filePathQuery := fmt.Sprintf(
		`SELECT path, start_pretty, end_pretty FROM
		(
			SELECT row_number() OVER (), *
			FROM [SHOW BACKUP FILES FROM LATEST IN '%s']
		)
		ORDER BY row_number DESC
		LIMIT 1`,
		uri,
	)
	parsedURI, err := url.Parse(strings.ReplaceAll(uri, "'", ""))
	sqlDB.QueryRow(t, filePathQuery).Scan(&filePath, &spanStart, &spanEnd)
	fullPath := filepath.Join(ioDir, parsedURI.Path, filePath)
	t.Logf("deleting backup file %s covering span [%s, %s)", fullPath, spanStart, spanEnd)
	require.NoError(t, err)
	require.NoError(t, os.Remove(fullPath))
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

	a := backuptestutils.GetExternalStorageURI(t, "nodelocal://1/a", "conn-a", sqlDB) + "?COCKROACH_LOCALITY=default"
	b := backuptestutils.GetExternalStorageURI(t, "nodelocal://1/b", "conn-b", sqlDB) + "?COCKROACH_LOCALITY=dc%3Ddc2"
	c := backuptestutils.GetExternalStorageURI(t, "nodelocal://1/c", "conn-c", sqlDB) + "?COCKROACH_LOCALITY=dc%3Ddc3"

	sqlDB.Exec(t, fmt.Sprintf("BACKUP DATABASE data INTO ('%s', '%s', '%s')", a, b, c))

	j := sqlDB.QueryStr(t, fmt.Sprintf(
		`RESTORE DATABASE data FROM LATEST IN ('%s', '%s', '%s')
		WITH new_db_name='d2', EXPERIMENTAL DEFERRED COPY`,
		a, b, c,
	))

	srv.Servers[0].JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()

	sqlDB.Exec(t, fmt.Sprintf(`SHOW JOB WHEN COMPLETE %s`, j[0][4]))
}

func TestOnlineRestoreLinkCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()

	rng, _ := randutil.NewTestRand()

	const numAccounts = 10
	_, sqlDB, _, cleanupFn := backupRestoreTestSetupWithParams(
		t,
		singleNode,
		numAccounts,
		InitManualReplication,
		orParams,
	)
	defer cleanupFn()

	externalStorage := backuptestutils.GetExternalStorageURI(t, "nodelocal://1/backup", "backup", sqlDB)

	sqlDB.Exec(t, "BACKUP DATABASE data INTO $1", externalStorage)
	sqlDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints  = 'restore.before_publishing_descriptors'")
	var jobID jobspb.JobID
	stmt := fmt.Sprintf("RESTORE DATABASE data FROM LATEST IN $1 WITH OPTIONS (new_db_name='data2', %s, detached)", onlineImpl(rng))
	sqlDB.QueryRow(t, stmt, externalStorage).Scan(&jobID)
	jobutils.WaitForJobToPause(t, sqlDB, jobID)

	// Set a pauspoint during the link phase which should not get hit because of
	// checkpointing.
	sqlDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints  = 'restore.before_link'")
	sqlDB.Exec(t, "RESUME JOB $1", jobID)
	jobutils.WaitForJobToSucceed(t, sqlDB, jobID)
}

func TestOnlineRestoreStatementResult(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()

	const numAccounts = 2
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

	externalStorage := backuptestutils.GetExternalStorageURI(t, "nodelocal://1/backup", "backup", sqlDB)

	sqlDB.ExecMultiple(
		t,
		"USE data",
		"CREATE TABLE foo (x INT PRIMARY KEY, y INT)",
		"INSERT INTO foo VALUES (1, 2)",
	)
	sqlDB.Exec(t, "BACKUP DATABASE data INTO $1", externalStorage)

	rows := sqlDB.Query(
		t,
		"RESTORE DATABASE data FROM LATEST IN $1 WITH OPTIONS (new_db_name='data2', experimental deferred copy)",
		externalStorage,
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
	externalStorage := backuptestutils.GetExternalStorageURI(t, "nodelocal://1/backup", "backup", sqlDB)

	sqlDB.Exec(t, fmt.Sprintf("BACKUP INTO '%s'", externalStorage))

	sqlDB.Exec(t, fmt.Sprintf("RESTORE DATABASE data FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY, new_db_name=data2", externalStorage))
	waitForLatestDownloadJobToSucceed(t, sqlDB)
	sqlDB.CheckQueryResults(t, jobutils.GetExternalBytesForConnectedTenant, [][]string{{"0"}})

}

func waitForLatestDownloadJobToSucceed(t *testing.T, sqlDB *sqlutils.SQLRunner) {
	var downloadJobID jobspb.JobID
	sqlDB.QueryRow(t,
		latestDownloadJobIDQuery,
	).Scan(&downloadJobID)
	jobutils.WaitForJobToSucceed(t, sqlDB, downloadJobID)
}

// TestOnlineRestoreTenant runs an online restore of a tenant and ensures the
// restore is not MVCC compliant.
func TestOnlineRestoreTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()

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
	const numAccounts = 2

	tc, systemDB, dir, cleanupFn := backupRestoreTestSetupWithParams(
		t, singleNode, numAccounts, InitManualReplication, params,
	)
	_, _ = tc, systemDB
	defer cleanupFn()
	srv := tc.Server(0)

	externalStorage := backuptestutils.GetExternalStorageURI(t, "nodelocal://1/backup", "backup", systemDB)

	_ = securitytest.EmbeddedTenantIDs()

	_, conn10 := serverutils.StartTenant(t, srv, base.TestTenantArgs{TenantID: roachpb.MustMakeTenantID(10)})
	defer conn10.Close()
	tenant10 := sqlutils.MakeSQLRunner(conn10)
	tenant10.Exec(t, `CREATE DATABASE foo; CREATE TABLE foo.bar(i int primary key); INSERT INTO foo.bar VALUES (110), (210)`)

	testutils.RunTrueAndFalse(t, "incremental", func(t *testing.T, incremental bool) {

		restoreTC, rSQLDB, cleanupFnRestored := backupRestoreTestSetupEmpty(t, 1, dir, InitManualReplication, params)
		defer cleanupFnRestored()

		// just using this to run CREATE EXTERNAL CONNECTION on the recovery db if we're using one
		_ = backuptestutils.GetExternalStorageURI(t, "nodelocal://1/backup", "backup", rSQLDB)

		systemDB.Exec(t, fmt.Sprintf(`BACKUP TENANT 10 INTO '%s'`, externalStorage))

		if incremental {
			tenant10.Exec(t, "INSERT INTO foo.bar VALUES (111), (211)")
			systemDB.Exec(t, fmt.Sprintf(`BACKUP TENANT 10 INTO LATEST IN '%s'`, externalStorage))
		}

		var preRestoreTs float64
		tenant10.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&preRestoreTs)

		belowID := uint64(2)
		aboveID := uint64(20)

		// Restore the tenant twice: once below and once above the old ID, to show
		// that we can rewrite it in either direction.
		rSQLDB.Exec(t, fmt.Sprintf("RESTORE TENANT 10 FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY, TENANT_NAME = 'below', TENANT = '%d'", externalStorage, belowID))
		rSQLDB.Exec(t, fmt.Sprintf("RESTORE TENANT 10 FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY, TENANT_NAME = 'above', TENANT = '%d'", externalStorage, aboveID))
		rSQLDB.Exec(t, "ALTER TENANT below STOP SERVICE")
		rSQLDB.Exec(t, "ALTER TENANT above STOP SERVICE")
		rSQLDB.CheckQueryResults(t, "SELECT fingerprint FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TENANT below]",
			rSQLDB.QueryStr(t, `SELECT fingerprint FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TENANT above]`))

		secondaryStopper := stop.NewStopper()
		_, cBelow := serverutils.StartTenant(
			t, restoreTC.Server(0), base.TestTenantArgs{
				TenantName: "below",
				TenantID:   roachpb.MustMakeTenantID(belowID),
				Stopper:    secondaryStopper,
			})
		_, cAbove := serverutils.StartTenant(
			t, restoreTC.Server(0), base.TestTenantArgs{
				TenantName: "above",
				TenantID:   roachpb.MustMakeTenantID(aboveID),
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

		dbAbove.CheckQueryResults(t, jobutils.GetExternalBytesForConnectedTenant, [][]string{{"0"}})
		dbBelow.CheckQueryResults(t, jobutils.GetExternalBytesForConnectedTenant, [][]string{{"0"}})
		rSQLDB.CheckQueryResults(t, jobutils.GetExternalBytesTenantKeySpace, [][]string{{"0"}})
	})
}

func TestOnlineRestoreErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()

	_, sqlDB, dir, cleanupFn := backupRestoreTestSetup(t, singleNode, 2, InitManualReplication)
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
		fullBackup = backuptestutils.GetExternalStorageURI(
			t, "nodelocal://1/full-backup", "full-backup", sqlDB, rSQLDB,
		)
		fullBackupWithRevs = backuptestutils.GetExternalStorageURI(
			t, "nodelocal://1/full-backup-with-revs", "full-backup-with-revs", sqlDB, rSQLDB,
		)
		incrementalBackupWithRevs = backuptestutils.GetExternalStorageURI(
			t, "nodelocal://1/incremental-backup-with-revs", "incremental-backup-with-revs", sqlDB, rSQLDB,
		)
	)
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
		rSQLDB.ExpectErr(t, "revision history backup not supported",
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

func TestOnlineRestoreRetryingDownloadRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()

	rng, seed := randutil.NewPseudoRand()
	t.Logf("random seed: %d", seed)

	alwaysFail := rng.Intn(2) == 0
	t.Logf("always fail download requests: %t", alwaysFail)
	totalFailures := int32(rng.Intn(maxDownloadAttempts-1) + 1)
	var currentFailures atomic.Int32

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			Knobs: base.TestingKnobs{
				BackupRestore: &sql.BackupRestoreTestingKnobs{
					RunBeforeSendingDownloadSpan: func() error {
						if alwaysFail {
							return errors.Newf("always fail download request")
						}
						if currentFailures.Load() >= totalFailures {
							return nil
						}
						currentFailures.Add(1)
						return errors.Newf("injected download request failure")
					},
				},
			},
		},
	}

	const numAccounts = 2
	_, sqlDB, _, cleanupFn := backupRestoreTestSetupWithParams(
		t, singleNode, numAccounts, InitManualReplication, clusterArgs,
	)
	defer cleanupFn()

	externalStorage := backuptestutils.GetExternalStorageURI(t, "nodelocal://1/backup", "backup", sqlDB)
	sqlDB.Exec(t, fmt.Sprintf("BACKUP INTO '%s'", externalStorage))
	sqlDB.Exec(
		t,
		fmt.Sprintf(`
		RESTORE DATABASE data FROM LATEST IN '%s'
		WITH EXPERIMENTAL DEFERRED COPY, new_db_name=data2
		`, externalStorage),
	)

	var downloadJobID jobspb.JobID
	sqlDB.QueryRow(t, latestDownloadJobIDQuery).Scan(&downloadJobID)
	if alwaysFail {
		jobutils.WaitForJobToFail(t, sqlDB, downloadJobID)
	} else {
		jobutils.WaitForJobToSucceed(t, sqlDB, downloadJobID)
	}
}

func TestOnlineRestoreDownloadRetryReset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()

	var attemptCount int
	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			Knobs: base.TestingKnobs{
				BackupRestore: &sql.BackupRestoreTestingKnobs{
					// We want the retry loop to fail until its final attempt, and then
					// succeed on the last attempt. This will allow the download job to
					// make progress, in which case the retry loop _should_ reset. Then
					// we continue allowing the retry loop to fail until its last
					// attempt, in which case it will succeed again.
					RunBeforeSendingDownloadSpan: func() error {
						attemptCount++
						if attemptCount < maxDownloadAttempts {
							return errors.Newf("injected download request failure")
						}
						return nil
					},
					RunBeforeDownloadCleanup: func() error {
						if attemptCount < maxDownloadAttempts*2 {
							return errors.Newf("injected download cleanup failure")
						}
						return nil
					},
				},
			},
		},
	}
	const numAccounts = 2
	_, sqlDB, _, cleanupFn := backupRestoreTestSetupWithParams(
		t, singleNode, numAccounts, InitManualReplication, clusterArgs,
	)
	defer cleanupFn()

	externalStorage := backuptestutils.GetExternalStorageURI(t, "nodelocal://1/backup", "backup", sqlDB)
	sqlDB.Exec(t, fmt.Sprintf("BACKUP INTO '%s'", externalStorage))
	sqlDB.Exec(
		t,
		fmt.Sprintf(`
		RESTORE DATABASE data FROM LATEST IN '%s'
		WITH EXPERIMENTAL DEFERRED COPY, new_db_name=data2
		`, externalStorage),
	)

	var downloadJobID jobspb.JobID
	sqlDB.QueryRow(t, latestDownloadJobIDQuery).Scan(&downloadJobID)
	jobutils.WaitForJobToSucceed(t, sqlDB, downloadJobID)
	require.Equal(t, maxDownloadAttempts*2, attemptCount)
}

func TestOnlineRestoreFailScatterNonEmptyRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()

	// This test first runs online restore and waits for an external SST to be
	// added to ensure at least one range has ingested an external SST. It then
	// pauses the job and resumes it. During this second phase, we count the
	// number of KV scatter requets that are sent and the number of successful
	// scatter requests. Since at least one range contains an external SST, the
	// number of scatter requests should be greater than the number of successful
	// scatter requests.
	// NB: This relies on the fact that the `TestingResponseFilter` hook is not
	// called if an admin scatter fails due to the size limit being hit.
	reachedPause := make(chan struct{})
	defer close(reachedPause)
	pauseCh := make(chan struct{})
	defer close(pauseCh)

	var resumed atomic.Bool
	var postPauseScatterRequests atomic.Int32
	var postPauseSuccessfulScatters atomic.Int32

	params := orParams
	params.ServerArgs.Knobs = base.TestingKnobs{
		BackupRestore: &sql.BackupRestoreTestingKnobs{
			AfterAddRemoteSST: func() error {
				if resumed.Load() {
					return nil
				}
				reachedPause <- struct{}{}
				<-pauseCh
				return nil
			},
		},
		Store: &kvserver.StoreTestingKnobs{
			TestingRequestFilter: func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
				if !resumed.Load() {
					return nil
				}
				for _, req := range ba.Requests {
					if req.GetInner().Method() == kvpb.AdminScatter {
						postPauseScatterRequests.Add(1)
					}
				}
				return nil
			},
			TestingResponseFilter: func(
				ctx context.Context, req *kvpb.BatchRequest, resp *kvpb.BatchResponse,
			) *kvpb.Error {
				if !resumed.Load() {
					return nil
				}
				for _, r := range req.Requests {
					if r.GetInner().Method() == kvpb.AdminScatter {
						postPauseSuccessfulScatters.Add(1)
					}
				}
				return nil
			},
		},
	}

	const numAccounts = 100
	_, sqlDB, _, cleanupFn := backupRestoreTestSetupWithParams(
		t, singleNode, numAccounts, InitManualReplication, params,
	)
	defer cleanupFn()

	externalStorage := backuptestutils.GetExternalStorageURI(t, "nodelocal://1/backup", "backup", sqlDB)
	sqlDB.Exec(t, fmt.Sprintf("BACKUP INTO '%s'", externalStorage))

	var linkJobID jobspb.JobID
	sqlDB.QueryRow(
		t,
		`RESTORE DATABASE data FROM LATEST IN $1
		WITH EXPERIMENTAL DEFERRED COPY, DETACHED, new_db_name='data2'`,
		externalStorage,
	).Scan(&linkJobID)

	<-reachedPause
	sqlDB.Exec(t, "PAUSE JOB $1", linkJobID)
	jobutils.WaitForJobToPause(t, sqlDB, linkJobID)
	pauseCh <- struct{}{}

	resumed.Store(true)
	sqlDB.Exec(t, "RESUME JOB $1", linkJobID)
	jobutils.WaitForJobToSucceed(t, sqlDB, linkJobID)

	require.Greater(t, postPauseScatterRequests.Load(), postPauseSuccessfulScatters.Load())
}

func bankOnlineRestore(
	t *testing.T,
	sqlDB *sqlutils.SQLRunner,
	numAccounts int,
	externalStorage string,
	blockingDownload bool,
) {
	// Create a table in the default database to force table id rewriting.
	sqlDB.Exec(t, "CREATE TABLE IF NOT EXISTS foo (i INT PRIMARY KEY, s STRING);")

	sqlDB.Exec(t, "CREATE DATABASE data")
	stmt := fmt.Sprintf("RESTORE TABLE data.bank FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY", externalStorage)
	if blockingDownload {
		stmt = fmt.Sprintf("RESTORE TABLE data.bank FROM LATEST IN '%s' WITH EXPERIMENTAL COPY", externalStorage)
	}
	sqlDB.Exec(t, stmt)

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

	// The where true conditional avoids the need to set sql_updates to true.
	sqlDB.Exec(t, "UPDATE data.bank SET balance = balance+1 where true;")

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

// TestOnlineRestoreRevisionHistoryLayers tests online restore with backup chains
// that include revision history layers. When a backup chain contains layers with
// revision history, those layers must be ingested (not linked) by the distributed
// restore flow. This test exercises various combinations of:
//   - Layers without revision history (linkable)
//   - Layers with revision history (must be ingested)
//   - Restores to specific timestamps covered by revision history
//   - DELETE operations that create tombstones which must shadow linked data
//
// The test creates a table split into multiple ranges and makes mutations across
// different ranges in different backup layers to exercise the hybrid link/ingest
// path thoroughly.
func TestOnlineRestoreRevisionHistoryLayers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()

	ctx := context.Background()

	// Use enough accounts to create multiple ranges.
	const numAccounts = 500

	srcTC, sqlDB, dir, cleanupFn := backupRestoreTestSetupWithParams(t, singleNode, numAccounts, InitManualReplication, orParams)
	defer cleanupFn()

	dstTC, rSQLDB, cleanupFnRestored := backupRestoreTestSetupEmpty(t, 1, dir, InitManualReplication, orParams)
	defer cleanupFnRestored()

	externalStorage := backuptestutils.GetExternalStorageURI(t, "nodelocal://1/backup-rev-history", "backup-rev-history", sqlDB, rSQLDB)

	// Split the bank table into multiple ranges so we have mutations affecting
	// different ranges in different layers.
	sqlDB.Exec(t, "ALTER TABLE data.bank SPLIT AT VALUES (100), (200), (300), (400)")

	// Layer 0: Full backup (no revision history).
	// Initial balance is the account id (set by backupRestoreTestSetup).
	sqlDB.Exec(t, fmt.Sprintf("BACKUP DATABASE data INTO '%s'", externalStorage))

	// Layer 1: First incremental without revision history.
	// Update balance for range 0-100 only.
	sqlDB.Exec(t, "UPDATE data.bank SET balance = 1000 WHERE id < 100")
	sqlDB.Exec(t, fmt.Sprintf("BACKUP DATABASE data INTO LATEST IN '%s'", externalStorage))

	// Layer 2: Second incremental without revision history.
	// Update balance for range 100-200 only.
	sqlDB.Exec(t, "UPDATE data.bank SET balance = 2000 WHERE id >= 100 AND id < 200")
	sqlDB.Exec(t, fmt.Sprintf("BACKUP DATABASE data INTO LATEST IN '%s'", externalStorage))

	// Now we start revision history layers.
	// Layer 3: First incremental WITH revision history.
	// Update balance for range 200-300.
	sqlDB.Exec(t, "UPDATE data.bank SET balance = 3000 WHERE id >= 200 AND id < 300")
	var ts3 string
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts3)
	sqlDB.Exec(t, fmt.Sprintf("BACKUP DATABASE data INTO LATEST IN '%s' WITH revision_history", externalStorage))

	// Capture a timestamp in the middle of the revision history for AOST testing.
	// Make another mutation WITHIN the revision history period.
	sqlDB.Exec(t, "UPDATE data.bank SET balance = 3500 WHERE id >= 200 AND id < 250")
	var tsMidRevision string
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&tsMidRevision)

	// Layer 4: Second incremental WITH revision history.
	// Update balance for range 300-400 and DELETE some rows.
	// The deletions test that tombstones in revision history layers correctly
	// shadow keys in the linked layers below.
	sqlDB.Exec(t, "UPDATE data.bank SET balance = 4000 WHERE id >= 300 AND id < 400")
	// Delete rows 350-359 (10 rows) - these existed in linked layers and must be
	// shadowed by tombstones from the ingested revision history layer.
	sqlDB.Exec(t, "DELETE FROM data.bank WHERE id >= 350 AND id < 360")
	var tsAfterDelete string
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&tsAfterDelete)
	sqlDB.Exec(t, fmt.Sprintf("BACKUP DATABASE data INTO LATEST IN '%s' WITH revision_history", externalStorage))

	// Layer 5: Third incremental WITH revision history.
	// Update balance for range 400-500 and delete more rows.
	sqlDB.Exec(t, "UPDATE data.bank SET balance = 5000 WHERE id >= 400")
	// Delete rows 450-454 (5 rows).
	sqlDB.Exec(t, "DELETE FROM data.bank WHERE id >= 450 AND id < 455")
	var ts5 string
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts5)
	sqlDB.Exec(t, fmt.Sprintf("BACKUP DATABASE data INTO LATEST IN '%s' WITH revision_history", externalStorage))

	// Layer 6: Incremental WITH revision history that writes over tombstones.
	// Re-insert some of the deleted rows with new values. This tests that
	// writes over tombstones are correctly handled.
	// Re-insert rows 350-354 (5 of the 10 deleted rows from layer 4).
	for id := 350; id < 355; id++ {
		sqlDB.Exec(t, "INSERT INTO data.bank (id, balance, payload) VALUES ($1, 7000, 'reinserted')", id)
	}
	var tsReinsert string
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&tsReinsert)
	sqlDB.Exec(t, fmt.Sprintf("BACKUP DATABASE data INTO LATEST IN '%s' WITH revision_history", externalStorage))

	// Layer 7: Final incremental WITHOUT revision history (after the revision history layers).
	// Update all balances to a final value.
	sqlDB.Exec(t, "UPDATE data.bank SET balance = 6000 WHERE true")
	sqlDB.Exec(t, fmt.Sprintf("BACKUP DATABASE data INTO LATEST IN '%s'", externalStorage))

	// Deleted rows: 10 (ids 350-359) + 5 (ids 450-454) = 15, but 5 were re-inserted (350-354)
	const deletedInLayer4 = 10   // ids 350-359
	const deletedInLayer5 = 5    // ids 450-454
	const reinsertedInLayer6 = 5 // ids 350-354 re-inserted
	const totalDeleted = deletedInLayer4 + deletedInLayer5 - reinsertedInLayer6

	// verifyData is a helper to verify the restored data matches expectations.
	// This is called twice: once while the download job is paused (data is linked)
	// and again after the download completes (data is local).
	type testCase struct {
		name             string
		restoreTime      string  // AS OF SYSTEM TIME, empty for latest
		expectedBalances [][]int // expected [minID, maxID, balance] ranges
		expectedRows     int     // expected row count
		deletedIDs       []int   // IDs that should NOT exist after restore
		reinsertedIDs    []int   // IDs that were re-inserted over tombstones
		expectError      string  // expected error, empty if success expected
		desc             string  // description for debugging
	}

	verifyData := func(t *testing.T, tc testCase, phase string) {
		// Verify row count.
		var restoreRowCount int
		rSQLDB.QueryRow(t, "SELECT count(*) FROM data.bank").Scan(&restoreRowCount)
		require.Equal(t, tc.expectedRows, restoreRowCount, "%s: row count mismatch for %s", phase, tc.desc)

		// Verify expected balances for each range.
		for i, expected := range tc.expectedBalances {
			minID, maxID, expectedBalance := expected[0], expected[1], expected[2]
			var actualBalance int
			rSQLDB.QueryRow(t,
				"SELECT balance FROM data.bank WHERE id = $1", minID,
			).Scan(&actualBalance)
			require.Equal(t, expectedBalance, actualBalance,
				"%s: balance mismatch at id=%d (range %d) for %s", phase, minID, i, tc.desc)

			// Check a few more points in the range.
			if maxID > minID {
				midID := (minID + maxID) / 2
				rSQLDB.QueryRow(t,
					"SELECT balance FROM data.bank WHERE id = $1", midID,
				).Scan(&actualBalance)
				require.Equal(t, expectedBalance, actualBalance,
					"%s: balance mismatch at id=%d (range %d) for %s", phase, midID, i, tc.desc)
			}
		}

		// Verify deleted rows are not present.
		for i, deletedID := range tc.deletedIDs {
			var count int
			rSQLDB.QueryRow(t,
				"SELECT count(*) FROM data.bank WHERE id = $1", deletedID,
			).Scan(&count)
			require.Equal(t, 0, count,
				"%s: deleted id=%d (index %d) should not exist for %s", phase, deletedID, i, tc.desc)
		}

		// Verify re-inserted rows exist and have correct balance.
		for i, reinsertedID := range tc.reinsertedIDs {
			var count int
			rSQLDB.QueryRow(t,
				"SELECT count(*) FROM data.bank WHERE id = $1", reinsertedID,
			).Scan(&count)
			require.Equal(t, 1, count,
				"%s: re-inserted id=%d (index %d) should exist for %s", phase, reinsertedID, i, tc.desc)
		}
	}

	// Test cases for different restore scenarios.
	testCases := []testCase{
		{
			name:        "restore-to-latest",
			restoreTime: "",
			expectedBalances: [][]int{
				{0, 349, 6000},               // accounts before first deletion
				{350, 354, 6000},             // re-inserted rows (updated in layer 7)
				{360, 449, 6000},             // accounts between deletions
				{455, numAccounts - 1, 6000}, // accounts after second deletion
			},
			expectedRows:  numAccounts - totalDeleted,
			deletedIDs:    []int{355, 356, 357, 358, 359, 450, 454}, // sample of still-deleted IDs
			reinsertedIDs: []int{350, 351, 352, 353, 354},           // re-inserted over tombstones
			desc:          "restore to latest backup (with re-inserts over tombstones)",
		},
		{
			name:        "restore-to-layer-3-end",
			restoreTime: ts3,
			expectedBalances: [][]int{
				{0, 99, 1000},
				{100, 199, 2000},
				{200, 299, 3000}, // layer 3 update (first rev history layer)
			},
			expectedRows: numAccounts, // no deletions yet at this timestamp
			deletedIDs:   nil,
			desc:         "restore to end of first revision history layer",
		},
		{
			name:        "restore-to-mid-revision",
			restoreTime: tsMidRevision,
			expectedBalances: [][]int{
				{0, 99, 1000},
				{100, 199, 2000},
				{200, 249, 3500}, // mid-revision update
				{250, 299, 3000}, // still at layer 3 value
			},
			expectedRows: numAccounts, // no deletions yet at this timestamp
			deletedIDs:   nil,
			desc:         "restore to a point in time within revision history",
		},
		{
			name:        "restore-after-delete",
			restoreTime: tsAfterDelete,
			expectedBalances: [][]int{
				{0, 99, 1000},
				{100, 199, 2000},
				{200, 249, 3500},
				{250, 299, 3000},
				{300, 349, 4000},
				{360, 399, 4000}, // 350-359 deleted
			},
			expectedRows: numAccounts - deletedInLayer4,
			deletedIDs:   []int{350, 355, 359}, // sample of deleted IDs from layer 4
			desc:         "restore to timestamp after first deletion",
		},
		{
			name:        "restore-to-layer-5-end",
			restoreTime: ts5,
			expectedBalances: [][]int{
				{0, 99, 1000},
				{100, 199, 2000},
				{200, 249, 3500},
				{250, 299, 3000},
				{300, 349, 4000},
				{360, 399, 4000},
				{400, 449, 5000},
				{455, numAccounts - 1, 5000}, // 450-454 deleted
			},
			expectedRows: numAccounts - deletedInLayer4 - deletedInLayer5, // all deletions, no re-insertions yet
			deletedIDs:   []int{350, 355, 359, 450, 454},                  // sample of deleted IDs
			desc:         "restore to end of last revision history layer before re-inserts",
		},
		{
			name:        "restore-after-reinsert",
			restoreTime: tsReinsert,
			expectedBalances: [][]int{
				{0, 99, 1000},
				{100, 199, 2000},
				{200, 249, 3500},
				{250, 299, 3000},
				{300, 349, 4000},
				{350, 354, 7000}, // re-inserted rows
				{360, 399, 4000},
				{400, 449, 5000},
				{455, numAccounts - 1, 5000},
			},
			expectedRows:  numAccounts - totalDeleted,               // 5 still deleted (355-359) + 5 (450-454)
			deletedIDs:    []int{355, 356, 357, 358, 359, 450, 454}, // sample of still-deleted IDs
			reinsertedIDs: []int{350, 351, 352, 353, 354},           // re-inserted over tombstones
			desc:          "restore after re-inserting rows over tombstones",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clean up before each test case.
			rSQLDB.Exec(t, "DROP DATABASE IF EXISTS data CASCADE")

			// Enable the distributed flow for online restore (required for revision history).
			rSQLDB.Exec(t, "SET CLUSTER SETTING backup.restore.online_use_dist_flow.enabled = true")

			// Set a pause point at the start of the download job so we can verify
			// data correctness before download (while data is still linked) and
			// after download (when data is local).
			rSQLDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'restore.before_download'")
			defer rSQLDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")

			// Create database and perform online restore.
			rSQLDB.Exec(t, "CREATE DATABASE data")

			restoreStmt := fmt.Sprintf("RESTORE TABLE data.bank FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY, detached", externalStorage)
			if tc.restoreTime != "" {
				restoreStmt = fmt.Sprintf("RESTORE TABLE data.bank FROM LATEST IN '%s' AS OF SYSTEM TIME '%s' WITH EXPERIMENTAL DEFERRED COPY, detached",
					externalStorage, tc.restoreTime)
			}

			if tc.expectError != "" {
				rSQLDB.ExpectErr(t, tc.expectError, restoreStmt)
				return
			}

			var linkJobID jobspb.JobID
			rSQLDB.QueryRow(t, restoreStmt).Scan(&linkJobID)
			jobutils.WaitForJobToSucceed(t, rSQLDB, linkJobID)

			// Wait for download job to pause at the pause point.
			var downloadJobID jobspb.JobID
			rSQLDB.QueryRow(t, latestDownloadJobIDQuery).Scan(&downloadJobID)
			jobutils.WaitForJobToPause(t, rSQLDB, downloadJobID)

			// Verify data while download job is paused (data is linked, not yet downloaded).
			verifyData(t, tc, "before-download")

			// For latest restore, also verify via fingerprint comparison.
			if tc.restoreTime == "" {
				fpSrc, err := fingerprintutils.FingerprintDatabase(ctx, srcTC.Conns[0], "data", fingerprintutils.Stripped())
				require.NoError(t, err)
				fpDst, err := fingerprintutils.FingerprintDatabase(ctx, dstTC.Conns[0], "data", fingerprintutils.Stripped())
				require.NoError(t, err)
				require.NoError(t, fingerprintutils.CompareDatabaseFingerprints(fpSrc, fpDst))
			}

			// Clear the pause point and resume the download job.
			rSQLDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
			rSQLDB.Exec(t, fmt.Sprintf("RESUME JOB %d", downloadJobID))
			jobutils.WaitForJobToSucceed(t, rSQLDB, downloadJobID)

			// Verify data after download job completes (data is now local).
			verifyData(t, tc, "after-download")

			// For latest restore, verify fingerprints again after download.
			if tc.restoreTime == "" {
				fpSrc, err := fingerprintutils.FingerprintDatabase(ctx, srcTC.Conns[0], "data", fingerprintutils.Stripped())
				require.NoError(t, err)
				fpDst, err := fingerprintutils.FingerprintDatabase(ctx, dstTC.Conns[0], "data", fingerprintutils.Stripped())
				require.NoError(t, err)
				require.NoError(t, fingerprintutils.CompareDatabaseFingerprints(fpSrc, fpDst))
			}
		})
	}
}

// TestOnlineRestoreLinkingNonexistentFiles verifies that the link phase of
// online restore is a metadata-only operation that does not interact with the
// data file contents. It proves this by deleting the data SST files after
// creating the backup, then running the link phase — which should succeed
// because linking only records file references in the LSM without reading the
// actual data files.
//
// The test creates incremental backups with data-tight SST bounds (by
// disabling backup presplitting), so that incremental SSTs have bounds
// strictly enclosed by the base SST. This geometry is required to exercise
// Pebble's overlap checker.
func TestOnlineRestoreLinkingNonexistentFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tmpDir := t.TempDir()
	defer nodelocal.ReplaceNodeLocalForTesting(tmpDir)()

	const numAccounts = 1000

	_, sqlDB, _, cleanupFn := backupRestoreTestSetupWithParams(t, singleNode, numAccounts, InitManualReplication, orParams)
	defer cleanupFn()

	// Force each backup layer into a single wide SST by setting a large file
	// size target. This ensures the base SST's bounds span the entire table,
	// so incremental SSTs have bounds strictly enclosed by the base.
	sqlDB.Exec(t, "SET CLUSTER SETTING bulkio.backup.file_size = '128MB'")
	// Disable presplit exports so that backup SSTs have data-tight bounds
	// rather than range-aligned bounds. With range-aligned bounds, the base
	// and incremental SSTs end up with the same boundaries and the enclosing
	// case never triggers. With data-tight bounds, the base SST covers the
	// full table while incremental SSTs cover narrow subsets, creating the
	// enclosing geometry.
	sqlDB.Exec(t, "SET CLUSTER SETTING bulkio.backup.presplit_request_spans.enabled = false")

	backupURI := "nodelocal://1/backup"

	// Full backup producing one SST covering the entire table key range.
	sqlDB.Exec(t, fmt.Sprintf("BACKUP DATABASE data INTO '%s'", backupURI))

	// Each incremental updates a narrow band in the middle of the table,
	// producing SSTs whose bounds are strictly enclosed by the base SST's
	// wide bounds. When linked, Pebble's overlap checker sees the base file
	// enclosing the incremental and opens it to probe for data overlap.
	sqlDB.Exec(t, "SET CLUSTER SETTING backup.restore.online_layer_limit = 25")
	for i := 0; i < 20; i++ {
		lo := 300 + i*20
		hi := lo + 10
		sqlDB.Exec(t, fmt.Sprintf("UPDATE data.bank SET balance = balance + 1 WHERE id >= %d AND id < %d", lo, hi))
		sqlDB.Exec(t, fmt.Sprintf("BACKUP DATABASE data INTO LATEST IN '%s'", backupURI))
	}

	// Delete the data SST files from the backup. The metadata SSTs
	// (filelist.sst, descriptorslist.sst) are preserved so restore can read
	// the manifest. When Pebble's overlap checker tries to open a previously-
	// linked backing file during the link phase, the I/O will fail.
	backupDir := filepath.Join(tmpDir, "backup")
	var deleted int
	require.NoError(t, filepath.WalkDir(backupDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Ext(path) == ".sst" && filepath.Base(filepath.Dir(path)) == "data" {
			deleted++
			return os.Remove(path)
		}
		return nil
	}))
	require.Greater(t, deleted, 0, "expected data SST files in backup")
	sqlDB.Exec(t, "DROP DATABASE data CASCADE")
	sqlDB.Exec(t, fmt.Sprintf(
		"RESTORE DATABASE data FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY", backupURI))
}

// TestOnlineRestoreWithDistFlow tests that online restore works correctly when
// using the distributed restore flow (distRestore with RestoreDataProcessor)
// instead of the simpler sendAddRemoteSSTs loop. This is enabled via the
// backup.restore.online_use_dist_flow.enabled cluster setting.
//
// The test runs restores both with and without the dist flow enabled and
// compares the results to ensure they are consistent, including the approx_rows
// and approx_bytes reported in the restore result.
func TestOnlineRestoreWithDistFlow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()

	ctx := context.Background()

	const numAccounts = 100

	tc, sqlDB, dir, cleanupFn := backupRestoreTestSetupWithParams(t, singleNode, numAccounts, InitManualReplication, orParams)
	defer cleanupFn()

	rtc, rSQLDB, cleanupFnRestored := backupRestoreTestSetupEmpty(t, 1, dir, InitManualReplication, orParams)
	defer cleanupFnRestored()

	testutils.RunTrueAndFalse(t, "incremental", func(t *testing.T, incremental bool) {
		// Defer cleanup so we clean up even if the test fails.
		defer rSQLDB.Exec(t, "DROP DATABASE IF EXISTS data CASCADE")

		// Create the backup once per incremental setting. Use a subdir to
		// separate full from incremental test runs.
		backupSubdir := "full"
		if incremental {
			backupSubdir = "incr"
		}
		backupURI := fmt.Sprintf("nodelocal://1/backup-dist-flow/%s", backupSubdir)
		sqlDB.Exec(t, fmt.Sprintf("BACKUP DATABASE data INTO '%s'", backupURI))
		if incremental {
			sqlDB.Exec(t, "UPDATE data.bank SET balance = balance + 100 WHERE true")
			sqlDB.Exec(t, fmt.Sprintf("BACKUP DATABASE data INTO LATEST IN '%s'", backupURI))
		}

		// restoreResult holds the results of a restore operation.
		type restoreResult struct {
			approxRows  int64
			approxBytes int64
		}

		// runRestore performs an online restore with the given dist flow setting
		// and returns the result.
		runRestore := func(t *testing.T, useDistFlow bool) restoreResult {
			// Clean up before restore.
			rSQLDB.Exec(t, "DROP DATABASE IF EXISTS data CASCADE")

			// Set the dist flow setting.
			rSQLDB.Exec(t, fmt.Sprintf(
				"SET CLUSTER SETTING backup.restore.online_use_dist_flow.enabled = %t", useDistFlow))

			// Create database and perform online restore.
			rSQLDB.Exec(t, "CREATE DATABASE data")
			rows := rSQLDB.Query(t, fmt.Sprintf(
				"RESTORE TABLE data.bank FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY", backupURI))
			defer rows.Close()

			// Parse the restore result.
			require.True(t, rows.Next(), "expected restore to return a row")
			var jobID, tables, approxRows, approxBytes, downloadJobID int64
			require.NoError(t, rows.Scan(&jobID, &tables, &approxRows, &approxBytes, &downloadJobID))
			require.False(t, rows.Next(), "expected only one row from restore")

			// Verify data is accessible.
			var restoreRowCount int
			rSQLDB.QueryRow(t, "SELECT count(*) FROM data.bank").Scan(&restoreRowCount)
			require.Equal(t, numAccounts, restoreRowCount)

			// Verify data integrity by comparing fingerprints.
			fpSrc, err := fingerprintutils.FingerprintDatabase(ctx, tc.Conns[0], "data", fingerprintutils.Stripped())
			require.NoError(t, err)
			fpDst, err := fingerprintutils.FingerprintDatabase(ctx, rtc.Conns[0], "data", fingerprintutils.Stripped())
			require.NoError(t, err)
			require.NoError(t, fingerprintutils.CompareDatabaseFingerprints(fpSrc, fpDst))

			// Wait for the download job to complete.
			waitForLatestDownloadJobToSucceed(t, rSQLDB)

			return restoreResult{approxRows: approxRows, approxBytes: approxBytes}
		}

		// Run restore with dist flow disabled (coordinator path).
		coordResult := runRestore(t, false /* useDistFlow */)

		// Run restore with dist flow enabled (distributed path).
		distResult := runRestore(t, true /* useDistFlow */)

		// Compare the results. Both paths should report the same approximate
		// rows and bytes since they're restoring the same data.
		require.Equal(t, coordResult.approxRows, distResult.approxRows,
			"approx_rows mismatch between coordinator (%d) and dist flow (%d)",
			coordResult.approxRows, distResult.approxRows)
		require.Equal(t, coordResult.approxBytes, distResult.approxBytes,
			"approx_bytes mismatch between coordinator (%d) and dist flow (%d)",
			coordResult.approxBytes, distResult.approxBytes)

		// Sanity check that we actually got non-zero values.
		require.Greater(t, coordResult.approxRows, int64(0),
			"expected non-zero approx_rows from coordinator path")
		require.Greater(t, coordResult.approxBytes, int64(0),
			"expected non-zero approx_bytes from coordinator path")
	})
}
