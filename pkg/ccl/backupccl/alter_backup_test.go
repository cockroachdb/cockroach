// Copyright 2022 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// getAWSEncryptionOption wraps getAWSKMSURI in single quotes so it can be used in queries as a string
func getAWSEncryptionOption(
	t *testing.T, regionEnvVariable string, keyIDEnvVariable string,
) string {
	uri, _ := getAWSKMSURI(t, regionEnvVariable, keyIDEnvVariable)
	return fmt.Sprintf("'%s'", uri)
}

// TestAlterBackupStatement tests to see that the ALTER BACKUP statement is correctly creating and naming
// new encryption-files.
func TestAlterBackupStatement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	key1 := getAWSEncryptionOption(t, "OLD_AWS_KMS_REGION", "OLD_AWS_KEY_ID")
	key2 := getAWSEncryptionOption(t, "NEW_AWS_KMS_REGION", "NEW_AWS_KEY_ID")

	const userfile = "'userfile:///a'"
	const numAccounts = 1

	tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	execCfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)
	defer cleanupFn()
	query := fmt.Sprintf("BACKUP TABLE bank INTO %s WITH kms = %s", userfile, key1)
	sqlDB.Exec(t, query)

	query = fmt.Sprintf("ALTER BACKUP LATEST in %s ADD NEW_KMS=%s WITH OLD_KMS=%s", userfile, key2, key1)
	sqlDB.Exec(t, query)

	ctx := context.Background()
	store, err := execCfg.DistSQLSrv.ExternalStorageFromURI(ctx, "userfile:///a", security.RootUserName())
	require.NoError(t, err)

	files, err := getEncryptionInfoFiles(ctx, store)
	require.NoError(t, err)
	require.True(t, len(files) == 2)

	query = fmt.Sprintf("ALTER BACKUP LATEST in %s ADD NEW_KMS=%s WITH OLD_KMS=%s", userfile, key1, key2)
	sqlDB.Exec(t, query)
	files, err = getEncryptionInfoFiles(ctx, store)
	require.NoError(t, err)
	require.True(t, len(files) == 3)
}

// TestAlterBackupRestore tests to see that an altered backup can be correctly restored using the new key,
// and that the old key will be correctly rejected.
func TestAlterBackupRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	oldURI := getAWSEncryptionOption(t, "OLD_AWS_KMS_REGION", "OLD_AWS_KEY_ID")
	newURI := getAWSEncryptionOption(t, "NEW_AWS_KMS_REGION", "NEW_AWS_KEY_ID")

	const userfile = "'userfile:///a'"
	const numAccounts = 1
	t.Run(fmt.Sprintf("alter-backup-restore-with-%s", newURI), func(t *testing.T) {
		_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
		defer cleanupFn()
		query := fmt.Sprintf("BACKUP TABLE bank INTO %s WITH kms = %s", userfile, oldURI)
		sqlDB.Exec(t, query)

		query = fmt.Sprintf("ALTER BACKUP LATEST in %s ADD NEW_KMS=%s WITH OLD_KMS=%s", userfile, newURI, oldURI)
		sqlDB.Exec(t, query)

		sqlDB.Exec(t, "DROP TABLE bank")
		query = fmt.Sprintf("RESTORE TABLE bank FROM LATEST in %s WITH KMS=%s", userfile, newURI)
		sqlDB.Exec(t, query)
	})

	t.Run(fmt.Sprintf("alter-backup-restore-with-%s", oldURI), func(t *testing.T) {
		_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
		defer cleanupFn()
		query := fmt.Sprintf("BACKUP TABLE bank INTO %s WITH kms = %s", userfile, oldURI)
		sqlDB.Exec(t, query)

		query = fmt.Sprintf("ALTER BACKUP LATEST IN %s ADD NEW_KMS= %s WITH OLD_KMS = %s", userfile, newURI, oldURI)
		sqlDB.Exec(t, query)

		sqlDB.Exec(t, "DROP TABLE bank")
		query = fmt.Sprintf("RESTORE TABLE bank FROM LATEST in %s WITH KMS = %s", userfile, oldURI)
		sqlDB.Exec(t, query)
	})
}

// TestAlterBackupShowBackup tests to see that show backup correctly recognizes the new encryption-info file
// when SHOW BACKUP is called on an encrypted backup that was altered.
func TestAlterBackupShowBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	oldURI := getAWSEncryptionOption(t, "OLD_AWS_KMS_REGION", "OLD_AWS_KEY_ID")
	newURI := getAWSEncryptionOption(t, "NEW_AWS_KMS_REGION", "NEW_AWS_KEY_ID")

	const userfile = "'userfile:///a'"
	const numAccounts = 1

	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()
	query := fmt.Sprintf("BACKUP TABLE bank INTO %s WITH KMS = %s", userfile, oldURI)
	sqlDB.Exec(t, query)

	query = fmt.Sprintf("ALTER BACKUP LATEST IN %s ADD NEW_KMS = %s WITH OLD_KMS = %s", userfile, newURI, oldURI)
	sqlDB.Exec(t, query)

	query = fmt.Sprintf("SHOW BACKUP LATEST IN %s WITH KMS = %s", userfile, newURI)
	sqlDB.Exec(t, query)

	query = fmt.Sprintf("SHOW BACKUP LATEST IN %s WITH KMS = %s", userfile, oldURI)
	sqlDB.Exec(t, query)
}

// TestAlterBackupIncremental tests to see that incremental backups know to look for the new encryption-file when
// backing up to a backup that was altered.
func TestAlterBackupIncremental(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	oldURI := getAWSEncryptionOption(t, "OLD_AWS_KMS_REGION", "OLD_AWS_KEY_ID")
	newURI := getAWSEncryptionOption(t, "NEW_AWS_KMS_REGION", "NEW_AWS_KEY_ID")

	const userfile = "'userfile:///a'"
	const numAccounts = 1

	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()
	query := fmt.Sprintf("BACKUP TABLE bank INTO %s WITH KMS = %s", userfile, oldURI)
	sqlDB.Exec(t, query)

	query = fmt.Sprintf("ALTER BACKUP LATEST IN %s ADD NEW_KMS = %s WITH OLD_KMS = %s", userfile, newURI, oldURI)
	sqlDB.Exec(t, query)

	// Delete an arbitrary row to test incremental backups.
	sqlDB.Exec(t, "DELETE FROM bank WHERE 1=1 LIMIT 1")

	query = fmt.Sprintf("BACKUP TABLE bank INTO LATEST IN %s WITH KMS = %s", userfile, newURI)
	sqlDB.Exec(t, query)

	sqlDB.Exec(t, "DROP TABLE bank")
	query = fmt.Sprintf("RESTORE TABLE bank FROM LATEST in %s WITH KMS=%s", userfile, newURI)
	sqlDB.Exec(t, query)

	sqlDB.Exec(t, "DROP TABLE bank")
	query = fmt.Sprintf("RESTORE TABLE bank FROM LATEST in %s WITH KMS=%s", userfile, oldURI)
	sqlDB.Exec(t, query)
}
