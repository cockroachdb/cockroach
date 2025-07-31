package backup

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestFlakyStorage tests backup and restore operations with flaky storage enabled.
// It creates a table with 100 rows, performs a detached backup with retries,
// waits for completion, drops the table, performs a detached restore with retries,
// waits for completion, and verifies the restored data matches the original.
func TestFlakyStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Set up a slim test server with flaky storage enabled
	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	}

	ctx := context.Background()

	tc, sqlDB, _, cleanupFn := backupRestoreTestSetupWithParams(
		t, singleNode, 0, InitManualReplication, params)
	defer cleanupFn()

	sql := sqlutils.MakeSQLRunner(tc.Conns[0])

	// Enable flaky storage and crank up the error frequency
	sql.Exec(t, `SET CLUSTER SETTING cloud.flaky_storage.enabled = 'true'`)
	sql.Exec(t, `SET CLUSTER SETTING cloud.flaky_storage.min_error_interval = '2s'`)

	// Create a test table with 100 rows
	sql.Exec(t, `CREATE DATABASE testdb`)
	sql.Exec(t, `CREATE TABLE testdb.test_table (id INT PRIMARY KEY, name STRING, value INT)`)
	for i := 1; i <= 100; i++ {
		sql.Exec(t, fmt.Sprintf(`INSERT INTO testdb.test_table VALUES (%d, 'row_%d', %d)`, i, i, i*10))
	}

	originalFingerprint := sql.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE testdb.test_table`)

	testutils.SucceedsSoon(t, func() error {
		_, err := sqlDB.DB.ExecContext(ctx, `BACKUP TABLE testdb.test_table INTO 'nodelocal://1/backup'`)
		return err
	})

	sqlDB.Exec(t, `DROP TABLE testdb.test_table`)

	testutils.SucceedsSoon(t, func() error {
		_, err := sqlDB.DB.ExecContext(ctx, `RESTORE TABLE testdb.test_table FROM LATEST IN 'nodelocal://1/backup'`)
		return err
	})

	restoredFingerprint := sqlDB.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE testdb.test_table`)
	require.Equal(t, originalFingerprint, restoredFingerprint, "Fingerprints should match")
}
