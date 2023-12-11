// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestOnlineRestoreBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1000
	_, sqlDB, dir, cleanupFn := backupRestoreTestSetupWithParams(t, singleNode, numAccounts, InitManualReplication, base.TestClusterArgs{
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
	_, rSQLDB, cleanupFnRestored := backupRestoreTestSetupEmpty(t, 1, dir, InitManualReplication, params)
	defer cleanupFnRestored()
	bankOnlineRestore(t, rSQLDB, numAccounts, externalStorage)

	// Wait for the download job to complete.
	//
	// TODO(adityamaru): Change to wait for successful completion once
	// storage lands https://github.com/cockroachdb/pebble/pull/3067.
	var downloadJobID jobspb.JobID
	rSQLDB.QueryRow(t, `SELECT job_id FROM [SHOW JOBS] WHERE description LIKE '%Background Data Download%'`).Scan(&downloadJobID)
	jobutils.WaitForJobToFail(t, rSQLDB, downloadJobID)
	var error string
	rSQLDB.QueryRow(t, `SELECT error FROM [SHOW JOBS] WHERE description LIKE '%Background Data Download%'`).Scan(&error)
	require.Contains(t, error, "not implemented")
}

func TestOnlineRestoreErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	_, sqlDB, dir, cleanupFn := backupRestoreTestSetupWithParams(t, singleNode, 1, InitManualReplication, base.TestClusterArgs{
		// Online restore is not supported in a secondary tenant yet.
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	})
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
		rSQLDB.ExpectErr(t, "incremental backup not supported", fmt.Sprintf("RESTORE TABLE data.bank FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY", incrementalBackup))
	})
	t.Run("full backups with revision history are unsupported", func(t *testing.T) {
		var systemTime string
		sqlDB.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&systemTime)
		sqlDB.Exec(t, fmt.Sprintf("BACKUP INTO '%s' AS OF SYSTEM TIME '%s' WITH revision_history", fullBackupWithRevs, systemTime))
		rSQLDB.ExpectErr(t, "revision history backup not supported", fmt.Sprintf("RESTORE TABLE data.bank FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY", fullBackupWithRevs))
	})
	t.Run("icremental backups with revision history are unsupported", func(t *testing.T) {
		sqlDB.Exec(t, fmt.Sprintf("BACKUP INTO '%s' WITH revision_history", incrementalBackupWithRevs))
		sqlDB.Exec(t, fmt.Sprintf("BACKUP INTO LATEST IN '%s' WITH revision_history", incrementalBackupWithRevs))
		rSQLDB.ExpectErr(t, "incremental backup not supported", fmt.Sprintf("RESTORE TABLE data.bank FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY", incrementalBackupWithRevs))
	})
	t.Run("descriptor rewrites are unsupported", func(t *testing.T) {
		sqlDB.Exec(t, fmt.Sprintf("BACKUP INTO '%s'", fullBackup))
		rSQLDB.Exec(t, "CREATE DATABASE new_data")
		rSQLDB.ExpectErr(t, "descriptor rewrites not supported", fmt.Sprintf("RESTORE TABLE data.bank FROM LATEST IN '%s' WITH into_db=new_data,EXPERIMENTAL DEFERRED COPY", fullBackup))
	})

}

func bankOnlineRestore(
	t *testing.T, sqlDB *sqlutils.SQLRunner, numAccounts int, externalStorage string,
) {
	sqlDB.Exec(t, "CREATE DATABASE data")
	sqlDB.Exec(t, fmt.Sprintf("RESTORE TABLE data.bank FROM LATEST IN '%s' WITH EXPERIMENTAL DEFERRED COPY", externalStorage))

	require.Equal(t, checkLinkingProgress(t, sqlDB), float32(1.0))

	var restoreRowCount int
	sqlDB.QueryRow(t, "SELECT count(*) FROM data.bank").Scan(&restoreRowCount)
	require.Equal(t, numAccounts, restoreRowCount)
}

func checkLinkingProgress(t *testing.T, sqlDB *sqlutils.SQLRunner) float32 {
	var linkingJobID jobspb.JobID
	sqlDB.QueryRow(t, `SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'RESTORE' ORDER BY created LIMIT 1`).Scan(&linkingJobID)
	prog := jobutils.GetJobProgress(t, sqlDB, linkingJobID)
	return prog.GetFractionCompleted()
}
