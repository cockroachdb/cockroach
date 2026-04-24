// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// waitForRevlogPastTimestamp polls SHOW JOBS WITH RESOLVED TIMESTAMP
// until the revlog job's resolved timestamp is at or past the given
// HLC timestamp (expressed as a decimal string from
// cluster_logical_timestamp()).
func waitForRevlogPastTimestamp(
	t *testing.T, sqlDB *sqlutils.SQLRunner, siblingID jobspb.JobID, target string,
) {
	t.Helper()
	require.NoError(t, testutils.SucceedsWithinError(func() error {
		var pastTarget *bool
		sqlDB.QueryRow(t,
			`SELECT resolved_timestamp >= $2::DECIMAL `+
				`FROM [SHOW JOBS WITH RESOLVED TIMESTAMP] WHERE job_id = $1`,
			siblingID, target,
		).Scan(&pastTarget)
		if pastTarget == nil || !*pastTarget {
			return errors.Newf(
				"revlog has not resolved past target %s", target,
			)
		}
		return nil
	}, 90*time.Second))
}

// clusterTimestamp returns the current cluster_logical_timestamp()
// as a decimal string suitable for AS OF SYSTEM TIME.
func clusterTimestamp(t *testing.T, sqlDB *sqlutils.SQLRunner) string {
	t.Helper()
	var ts string
	sqlDB.QueryRow(t,
		`SELECT cluster_logical_timestamp()::STRING`,
	).Scan(&ts)
	return ts
}

// TestRevlogAOSTExceedsResolved verifies that restoring to an AOST
// beyond the revision log's last resolved tick fails with an
// appropriate error during planning.
func TestRevlogAOSTExceedsResolved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	}
	_, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(
		t, singleNode, 0 /* numAccounts */, InitManualReplication, params,
	)
	defer cleanup()

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	sqlDB.Exec(t, `CREATE DATABASE aost_exceed`)
	sqlDB.Exec(t, `CREATE TABLE aost_exceed.tbl (k INT PRIMARY KEY)`)

	const destSubdir = "aost-exceed"
	dest := "nodelocal://1/" + destSubdir
	sqlDB.Exec(t, `BACKUP DATABASE aost_exceed INTO $1 WITH REVISION STREAM`, dest)

	collectionDir := filepath.Join(dir, destSubdir)
	siblingID := readSiblingJobID(t, collectionDir)
	jobutils.WaitForJobToRun(t, sqlDB, siblingID)

	// Insert some data and wait for at least one resolved tick.
	sqlDB.Exec(t, `INSERT INTO aost_exceed.tbl VALUES (1)`)
	afterInsert := clusterTimestamp(t, sqlDB)
	waitForRevlogPastTimestamp(t, sqlDB, siblingID, afterInsert)

	// Cancel the revlog job so it stops resolving.
	sqlDB.Exec(t, `CANCEL JOB $1`, siblingID)
	jobutils.WaitForJobToCancel(t, sqlDB, siblingID)

	// Take a cluster timestamp well after the revlog stopped
	// resolving. Since the revlog job is canceled, this
	// timestamp will be beyond the revlog's last resolved tick.
	beyondRevlog := clusterTimestamp(t, sqlDB)

	// Attempt to restore past the revlog's resolved point.
	sqlDB.Exec(t, `DROP DATABASE aost_exceed CASCADE`)
	sqlDB.ExpectErr(t,
		"revision log has not resolved",
		fmt.Sprintf(
			`RESTORE DATABASE aost_exceed FROM LATEST IN '%s' AS OF SYSTEM TIME '%s'`,
			dest, beyondRevlog,
		),
	)
}

// TestRevlogDescriptorResolution tests descriptor resolution during
// restore planning when a revision log is present, ensuring that
// schema changes during the revlog window are correctly merged.
func TestRevlogDescriptorResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	}
	_, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(
		t, singleNode, 0 /* numAccounts */, InitManualReplication, params,
	)
	defer cleanup()

	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)

	t.Run("RESTORE DATABASE with table created during revlog", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE DATABASE db_create`)
		sqlDB.Exec(t, `CREATE TABLE db_create.original (k INT PRIMARY KEY, v STRING)`)
		sqlDB.Exec(t, `INSERT INTO db_create.original VALUES (1, 'one')`)

		const destSubdir = "db-create"
		dest := "nodelocal://1/" + destSubdir
		sqlDB.Exec(t, `BACKUP DATABASE db_create INTO $1 WITH REVISION STREAM`, dest)

		collectionDir := filepath.Join(dir, destSubdir)
		siblingID := readSiblingJobID(t, collectionDir)
		jobutils.WaitForJobToRun(t, sqlDB, siblingID)

		// Create a new table after the backup. The revlog's
		// descriptor rangefeed will capture this schema change.
		sqlDB.Exec(t, `CREATE TABLE db_create.new_tbl (k INT PRIMARY KEY, v STRING)`)
		sqlDB.Exec(t, `INSERT INTO db_create.new_tbl VALUES (10, 'ten')`)

		afterCreate := clusterTimestamp(t, sqlDB)
		waitForRevlogPastTimestamp(t, sqlDB, siblingID, afterCreate)

		sqlDB.Exec(t, `CANCEL JOB $1`, siblingID)
		jobutils.WaitForJobToCancel(t, sqlDB, siblingID)

		// Drop the database, then restore to the post-create AOST.
		sqlDB.Exec(t, `DROP DATABASE db_create CASCADE`)
		sqlDB.Exec(t, fmt.Sprintf(
			`RESTORE DATABASE db_create FROM LATEST IN '%s' AS OF SYSTEM TIME '%s'`,
			dest, afterCreate,
		))

		// Both the original and the new table should exist.
		// The original table's data comes from the backup.
		var count int
		sqlDB.QueryRow(t, `SELECT count(*) FROM db_create.original`).Scan(&count)
		require.Equal(t, 1, count, "original table should have 1 row")
		// The new table's descriptor was resolved from the revlog
		// schema changes and created during restore. Its data
		// would come from the revlog data ticks, which is handled
		// by the backup job's revision log stream (not tested
		// here).
		sqlDB.Exec(t, `SELECT * FROM db_create.new_tbl`)

		sqlDB.Exec(t, `DROP DATABASE db_create CASCADE`)
	})

	t.Run("RESTORE TABLE for table created during revlog", func(t *testing.T) {
		sqlDB.Exec(t, `CREATE DATABASE tbl_create`)
		sqlDB.Exec(t, `CREATE TABLE tbl_create.original (k INT PRIMARY KEY)`)

		const destSubdir = "tbl-create"
		dest := "nodelocal://1/" + destSubdir
		sqlDB.Exec(t, `BACKUP DATABASE tbl_create INTO $1 WITH REVISION STREAM`, dest)

		collectionDir := filepath.Join(dir, destSubdir)
		siblingID := readSiblingJobID(t, collectionDir)
		jobutils.WaitForJobToRun(t, sqlDB, siblingID)

		sqlDB.Exec(t, `CREATE TABLE tbl_create.new_tbl (k INT PRIMARY KEY, v STRING)`)
		sqlDB.Exec(t, `INSERT INTO tbl_create.new_tbl VALUES (42, 'hello')`)

		afterCreate := clusterTimestamp(t, sqlDB)
		waitForRevlogPastTimestamp(t, sqlDB, siblingID, afterCreate)

		sqlDB.Exec(t, `CANCEL JOB $1`, siblingID)
		jobutils.WaitForJobToCancel(t, sqlDB, siblingID)

		// Drop the table, then restore just that table from the
		// revlog-augmented descriptor set.
		sqlDB.Exec(t, `DROP TABLE tbl_create.new_tbl`)
		sqlDB.Exec(t, fmt.Sprintf(
			`RESTORE TABLE tbl_create.new_tbl FROM LATEST IN '%s' AS OF SYSTEM TIME '%s'`,
			dest, afterCreate,
		))

		// Verify the table was created by the descriptor
		// resolution. Data comes from revlog ticks.
		sqlDB.Exec(t, `SELECT * FROM tbl_create.new_tbl`)

		sqlDB.Exec(t, `DROP DATABASE tbl_create CASCADE`)
	})

	t.Run("RESTORE TABLE after drop fails", func(t *testing.T) {
		// TODO(kev-cao): The revlog stream currently fails when a
		// table is dropped, so the resolved timestamp never
		// advances past the drop. Skip until the revlog writer
		// handles drops correctly.
		skip.IgnoreLint(t, "blocked on revlog stream drop handling")

		sqlDB.Exec(t, `CREATE DATABASE tbl_drop`)
		sqlDB.Exec(t, `CREATE TABLE tbl_drop.ephemeral (k INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO tbl_drop.ephemeral VALUES (1)`)
		// An anchor table keeps the revlog scope alive after
		// ephemeral is dropped, so the revlog job continues to
		// resolve through the drop timestamp.
		sqlDB.Exec(t, `CREATE TABLE tbl_drop.anchor (k INT PRIMARY KEY)`)

		const destSubdir = "tbl-drop"
		dest := "nodelocal://1/" + destSubdir
		sqlDB.Exec(t, `BACKUP DATABASE tbl_drop INTO $1 WITH REVISION STREAM`, dest)

		collectionDir := filepath.Join(dir, destSubdir)
		siblingID := readSiblingJobID(t, collectionDir)
		jobutils.WaitForJobToRun(t, sqlDB, siblingID)

		// Drop the table while the revlog is watching.
		beforeDrop := clusterTimestamp(t, sqlDB)
		sqlDB.Exec(t, `DROP TABLE tbl_drop.ephemeral`)
		afterDrop := clusterTimestamp(t, sqlDB)
		waitForRevlogPastTimestamp(t, sqlDB, siblingID, afterDrop)

		sqlDB.Exec(t, `CANCEL JOB $1`, siblingID)
		jobutils.WaitForJobToCancel(t, sqlDB, siblingID)

		// Restoring the dropped table to a time AFTER the drop
		// should fail because the table no longer exists.
		sqlDB.ExpectErr(t,
			"no tables or databases matched",
			fmt.Sprintf(
				`RESTORE TABLE tbl_drop.ephemeral FROM LATEST IN '%s' AS OF SYSTEM TIME '%s'`,
				dest, afterDrop,
			),
		)

		// Restoring the same table to a time BEFORE the drop
		// should succeed.
		sqlDB.Exec(t, fmt.Sprintf(
			`RESTORE TABLE tbl_drop.ephemeral FROM LATEST IN '%s' AS OF SYSTEM TIME '%s'`,
			dest, beforeDrop,
		))
		var count int
		sqlDB.QueryRow(t, `SELECT count(*) FROM tbl_drop.ephemeral`).Scan(&count)
		require.Equal(t, 1, count)

		sqlDB.Exec(t, `DROP DATABASE tbl_drop CASCADE`)
	})
}
