// Copyright 2023 The Cockroach Authors.
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
	"github.com/stretchr/testify/require"
)

func TestOnlineRestoreBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()

	ctx := context.Background()

	const numAccounts = 1000
	tc, sqlDB, dir, cleanupFn := backupRestoreTestSetupWithParams(t, singleNode, numAccounts, InitManualReplication, base.TestClusterArgs{
		// Online restore is not supported in a secondary tenant yet.
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	})
	defer cleanupFn()
	externalStorage := "nodelocal://1/backup"

	sqlDB.Exec(t, fmt.Sprintf("BACKUP INTO '%s'", externalStorage))

	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	}
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
	sqlDB.QueryRow(t, `SELECT job_id FROM [SHOW JOBS] WHERE description LIKE '%Background Data Download%'`).Scan(&downloadJobID)
	jobutils.WaitForJobToSucceed(t, sqlDB, downloadJobID)
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

	_, rSQLDB, cleanupFnRestored := backupRestoreTestSetupEmpty(t, 1, dir, InitManualReplication, params)
	defer cleanupFnRestored()

	var preRestoreTs float64
	tenant10.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&preRestoreTs)

	rSQLDB.ExpectErr(t, "cannot run Online Restore on a tenant", fmt.Sprintf("RESTORE TENANT 10 FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY", externalStorage))
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

}

func bankOnlineRestore(
	t *testing.T, sqlDB *sqlutils.SQLRunner, numAccounts int, externalStorage string,
) {
	// Create a table in the default database to force table id rewriting.
	sqlDB.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY, s STRING);")

	sqlDB.Exec(t, "CREATE DATABASE data")
	sqlDB.Exec(t, fmt.Sprintf("RESTORE TABLE data.bank FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY", externalStorage))

	require.Equal(t, checkLinkingProgress(t, sqlDB), float32(1.0))

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

func checkLinkingProgress(t *testing.T, sqlDB *sqlutils.SQLRunner) float32 {
	var linkingJobID jobspb.JobID
	sqlDB.QueryRow(t, `SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'RESTORE' ORDER BY created LIMIT 1`).Scan(&linkingJobID)
	prog := jobutils.GetJobProgress(t, sqlDB, linkingJobID)
	return prog.GetFractionCompleted()
}
