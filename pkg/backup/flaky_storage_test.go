// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/fault"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestBackupRestore_FlakyStorage tests backup and restore operations with flaky storage enabled.
// It creates a table with rows, performs backups with retries,
// drops the table, performs restore with retries,
// and verifies the restored data matches the original.
func TestBackupRestore_FlakyStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Set up a slim test server with flaky storage enabled
	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				CloudStorageKnobs: &cloud.TestingKnobs{
					OpFaults: fault.NewProbabilisticFaults(0.01),
					IoFaults: fault.NewProbabilisticFaults(0.001),
				},
			},
		},
	}

	ctx := context.Background()

	tc, sqlDB, _, cleanupFn := backupRestoreTestSetupWithParams(
		t, singleNode, 0, InitManualReplication, params)
	defer cleanupFn()

	sql := sqlutils.MakeSQLRunner(tc.Conns[0])

	nextRow := 1
	writeRows := func(t *testing.T, sql *sqlutils.SQLRunner) {
		for i := 0; i < 10; i++ {
			sql.Exec(t, fmt.Sprintf(`INSERT INTO testdb.test_table VALUES (%d, 'row_%d', %d)`, nextRow, nextRow, nextRow*10))
			nextRow++
		}
	}

	sql.Exec(t, `SET CLUSTER SETTING backup.compaction.threshold = 5`)
	sql.Exec(t, `SET CLUSTER SETTING backup.compaction.window_size = 3`)

	// Create a test table
	sql.Exec(t, `SET CLUSTER SETTING rpc.experimental_drpc.enabled = false`)
	sql.Exec(t, `CREATE DATABASE testdb`)
	sql.Exec(t, `CREATE TABLE testdb.test_table (id INT PRIMARY KEY, name STRING, value INT)`)

	writeRows(t, sql)
	testutils.SucceedsSoon(t, func() error {
		_, err := sqlDB.DB.ExecContext(ctx, `BACKUP TABLE testdb.test_table INTO 'nodelocal://1/backup'`)
		return err
	})

	for i := 0; i < 10; i++ {
		writeRows(t, sql)
		testutils.SucceedsSoon(t, func() error {
			_, err := sqlDB.DB.ExecContext(ctx, `BACKUP TABLE testdb.test_table INTO LATEST IN 'nodelocal://1/backup'`)
			return err
		})
	}

	originalFingerprint := sql.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE testdb.test_table`)

	sqlDB.Exec(t, `DROP TABLE testdb.test_table`)

	testutils.SucceedsSoon(t, func() error {
		_, err := sqlDB.DB.ExecContext(ctx, `RESTORE TABLE testdb.test_table FROM LATEST IN 'nodelocal://1/backup'`)
		return err
	})

	restoredFingerprint := sqlDB.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE testdb.test_table`)
	require.Equal(t, originalFingerprint, restoredFingerprint, "Fingerprints should match")
}
