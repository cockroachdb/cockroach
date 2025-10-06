// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupccl

import (
	"context"
	gosql "database/sql"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestRestoreWithOpenTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterSize := 1
	tc, sqlDB, _, cleanupFn := backuptestutils.StartBackupRestoreTestCluster(t, clusterSize)
	defer cleanupFn()

	sqlDB.Exec(t, `CREATE ROLE testuser WITH LOGIN PASSWORD 'password'`)
	sqlDB.Exec(t, `CREATE DATABASE restoretarget;`)

	userConn := sqlutils.MakeSQLRunner(tc.Servers[0].SQLConn(t, serverutils.UserPassword("testuser", "password")))
	userConn.Exec(t, "CREATE TABLE ids (id UUID PRIMARY KEY NOT NULL DEFAULT gen_random_uuid());")
	sqlDB.Exec(t, `BACKUP TABLE ids INTO 'nodelocal://1/ids'`)

	userConn.Exec(t, "BEGIN")
	// Query the id table to take out a lease and perform role access checks.
	_ = userConn.QueryStr(t, "SELECT * FROM ids")

	result := make(chan error)
	go func() {
		_, err := sqlDB.DB.ExecContext(context.Background(), `RESTORE TABLE ids FROM LATEST IN 'nodelocal://1/ids' WITH into_db = 'restoretarget'`)
		result <- err
	}()

	select {
	case <-time.After(2 * time.Minute):
		// This is a regression test for misbehavior in restore. Restore was
		// incrementing the role table's descriptor version in order to flush the
		// role cache. This is necessary for full cluster restores, since they
		// modify the role table, but caused a regression for table and database
		// level restores. Table and database restores would hang if there were any
		// open long running transactions.
		t.Fatal("restore is blocked by an open transaction")
	case err := <-result:
		require.NoError(t, err)
	}

	userConn.Exec(t, "COMMIT")
}

func TestRestoreDuplicateTempTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This is a regression test for #153722. It verifies that restoring a backup
	// that contains two temporary tables with the same name does not cause the
	// restore to fail with an error of the form: "restoring 17 TableDescriptors
	// from 4 databases: restoring table desc and namespace entries: table
	// already exists"

	clusterSize := 1
	tc, sqlDB, _, cleanupFn := backuptestutils.StartBackupRestoreTestCluster(t, clusterSize)
	defer cleanupFn()

	sqlDB.Exec(t, `SET experimental_enable_temp_tables=true`)
	sqlDB.Exec(t, `CREATE DATABASE test_db`)
	sqlDB.Exec(t, `USE test_db`)
	sqlDB.Exec(t, `CREATE TABLE permanent_table (id INT PRIMARY KEY, name TEXT)`)

	sessions := make([]*gosql.DB, 2)
	for i := range sessions {
		sessions[i] = tc.Servers[0].SQLConn(t)
		sql := sqlutils.MakeSQLRunner(sessions[i])
		sql.Exec(t, `SET experimental_enable_temp_tables=true`)
		sql.Exec(t, `USE test_db`)
		sql.Exec(t, `CREATE TEMP TABLE duplicate_temp (id INT PRIMARY KEY, value TEXT)`)
		sql.Exec(t, `INSERT INTO duplicate_temp VALUES (1, 'value')`)
	}

	sqlDB.Exec(t, `BACKUP INTO 'nodelocal://1/duplicate_temp_backup'`)

	for _, session := range sessions {
		require.NoError(t, session.Close())
	}

	// The cluster must be empty for a full cluster restore.
	sqlDB.Exec(t, `DROP DATABASE test_db CASCADE`)
	sqlDB.Exec(t, `RESTORE FROM LATEST IN 'nodelocal://1/duplicate_temp_backup'`)

	sqlDB.Exec(t, `DROP DATABASE test_db CASCADE`)
	sqlDB.Exec(t, `RESTORE DATABASE test_db FROM LATEST IN 'nodelocal://1/duplicate_temp_backup'`)

	result := sqlDB.QueryStr(t, `SELECT table_name FROM [SHOW TABLES] ORDER BY table_name`)
	require.Equal(t, [][]string{{"permanent_table"}}, result)
}
