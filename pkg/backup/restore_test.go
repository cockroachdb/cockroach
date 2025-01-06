// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backuptestutils"
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
