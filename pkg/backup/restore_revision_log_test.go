// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/revlog/restorerevlog"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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

// TestMergeTickEventsFromBackup exercises the merge algorithm against
// real revision log data produced by BACKUP ... WITH REVISION STREAM.
//
//  1. Start a single-node cluster and create a table.
//  2. Run INSERT, UPDATE, and DELETE to generate MVCC history.
//  3. BACKUP INTO ... WITH REVISION STREAM to create a backup with a
//     sibling revision log job.
//  4. Wait for the revlog job to produce at least one closed tick.
//  5. Read all ticks from the revlog and run MergeTickEvents.
//  6. Validate: output is non-empty, keys are sorted, each key
//     appears exactly once, and values are properly encoded.
func TestMergeTickEventsFromBackup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// System tenant is required because kv.rangefeed.enabled is
	// operator-only, and the revlog producer needs rangefeeds.
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
	sqlDB.Exec(t, `CREATE TABLE data.kv (k INT PRIMARY KEY, v STRING)`)

	// Run backup with revision stream to start the sibling revlog
	// job. The revlog producer watches via rangefeed from this
	// point forward.
	const destSubdir = "revlog-merge-test"
	dest := "nodelocal://1/" + destSubdir
	sqlDB.Exec(t, `BACKUP INTO $1 WITH REVISION STREAM`, dest)

	collectionDir := filepath.Join(dir, destSubdir)
	siblingID := readSiblingJobID(t, collectionDir)
	jobutils.WaitForJobToRun(t, sqlDB, siblingID)

	// Mutations happen after the revlog job starts so the rangefeed
	// captures them.
	sqlDB.Exec(t, `INSERT INTO data.kv VALUES (1, 'a'), (2, 'b'), (3, 'c')`)
	sqlDB.Exec(t, `UPDATE data.kv SET v = 'a2' WHERE k = 1`)
	sqlDB.Exec(t, `DELETE FROM data.kv WHERE k = 2`)

	// Wait for the revlog job to resolve past the mutations by
	// polling its high_water_timestamp via SHOW JOBS.
	require.NoError(t, testutils.SucceedsWithinError(func() error {
		var resolvedTS *string
		sqlDB.QueryRow(t,
			`SELECT resolved_timestamp::STRING `+
				`FROM [SHOW JOBS WITH RESOLVED TIMESTAMP] WHERE job_id = $1`,
			siblingID,
		).Scan(&resolvedTS)
		if resolvedTS == nil {
			return errors.New("revlog job has no resolved timestamp yet")
		}
		return nil
	}, 90*time.Second))

	// Open the collection storage and discover ticks.
	es := nodelocal.TestingMakeNodelocalStorage(
		collectionDir,
		cluster.MakeTestingClusterSettings(),
		cloudpb.ExternalStorage{},
	)
	defer es.Close()

	lr := revlog.NewLogReader(es)
	var manifests []revlogpb.Manifest
	for tick, tickErr := range lr.Ticks(
		ctx, hlc.Timestamp{WallTime: 1}, hlc.MaxTimestamp,
	) {
		require.NoError(t, tickErr)
		manifests = append(manifests, tick.Manifest)
	}
	require.NotEmpty(t, manifests, "expected at least one tick manifest")
	t.Logf("discovered %d tick(s)", len(manifests))

	// Run the merge algorithm.
	spec := execinfrapb.RevlogLocalMergeSpec{
		Ticks: manifests,
	}
	var merged []restorerevlog.MergedEntry
	for entry, err := range restorerevlog.MergeTickEvents(ctx, es, spec) {
		require.NoError(t, err)
		merged = append(merged, entry)
	}
	require.NotEmpty(t, merged, "expected non-empty merge output")

	// Validate: keys must be in ascending order and unique.
	seen := make(map[string]bool)
	for i, e := range merged {
		keyStr := string(e.Key.Key)
		require.False(t, seen[keyStr],
			"duplicate key %s in merge output", e.Key.Key)
		seen[keyStr] = true

		if i > 0 {
			require.True(t,
				merged[i-1].Key.Key.Compare(e.Key.Key) < 0,
				"keys not sorted: %s >= %s",
				merged[i-1].Key.Key, e.Key.Key,
			)
		}

		// Non-tombstone values must be valid roachpb.Value
		// encoding (4-byte checksum + 1-byte tag + data).
		if len(e.Value) > 0 {
			require.GreaterOrEqual(t, len(e.Value), 5,
				"value for key %s too short to be a roachpb.Value",
				e.Key.Key)
		}
	}

	t.Logf("merge produced %d deduplicated entries", len(merged))

	// Cancel the sibling job so cleanup doesn't hang.
	sqlDB.Exec(t, `CANCEL JOB $1`, siblingID)
	jobutils.WaitForJobToCancel(t, sqlDB, siblingID)
}

// TestRestoreFromRevlog runs a full restore through the revlog path
// and verifies that the restored data in KV reflects the mutations
// captured by the revision log. The database has two tables but the
// restore targets only one, exercising the rekey skip logic for
// unscoped revlog events.
func TestRestoreFromRevlog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// System tenant is required because kv.rangefeed.enabled is
	// operator-only, and the revlog producer needs rangefeeds.
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
	sqlDB.Exec(t, `CREATE DATABASE src`)
	sqlDB.Exec(t, `CREATE TABLE src.foo (k INT PRIMARY KEY, v STRING)`)
	sqlDB.Exec(t, `INSERT INTO src.foo VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')`)
	// A second table in the same database ensures the revlog
	// contains events for tables outside the restore scope.
	sqlDB.Exec(t, `CREATE TABLE src.bar (k INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO src.bar VALUES (1), (2)`)

	// Run backup with revision stream to start the sibling revlog
	// job. The revlog producer watches via rangefeed from this
	// point forward.
	const destSubdir = "revlog-merge-test"
	dest := "nodelocal://1/" + destSubdir
	sqlDB.Exec(t, `BACKUP INTO $1 WITH REVISION STREAM`, dest)

	collectionDir := filepath.Join(dir, destSubdir)
	siblingID := readSiblingJobID(t, collectionDir)
	jobutils.WaitForJobToRun(t, sqlDB, siblingID)

	// Mutations happen after the revlog job starts so the rangefeed
	// captures them.
	sqlDB.Exec(t, `UPDATE src.foo SET v = 'updated' WHERE k = 1`)
	sqlDB.Exec(t, `DELETE FROM src.foo WHERE k = 2`)
	sqlDB.Exec(t, `INSERT INTO src.foo VALUES (100, 'new')`)

	aost := clusterTimestamp(t, sqlDB)
	waitForRevlogPastTimestamp(t, sqlDB, siblingID, aost)

	sqlDB.Exec(t, `CANCEL JOB $1`, siblingID)
	jobutils.WaitForJobToCancel(t, sqlDB, siblingID)

	// Restore only src.foo — the revlog data contains events for
	// both foo and bar, but only foo has rekeys.
	sqlDB.Exec(t, `CREATE DATABASE restored`)
	sqlDB.Exec(t, fmt.Sprintf(
		`RESTORE TABLE src.foo FROM LATEST IN '%s' AS OF SYSTEM TIME '%s' WITH into_db = 'restored'`,
		dest, aost,
	))

	// Verify the restored data matches the expected state at the
	// AOST: k=1 updated, k=2 deleted, k=3 unchanged, k=100 inserted.
	rows := sqlDB.QueryStr(t,
		`SELECT k, v FROM restored.foo ORDER BY k`,
	)
	expected := [][]string{
		{"1", "updated"},
		{"3", "gamma"},
		{"100", "new"},
	}
	require.Equal(t, expected, rows)

	// Verify that intermediate SSTs were cleaned up after restore.
	// The local merge writes to nodelocal://{id}/job/{jobID}/map/,
	// which maps to {dir}/job/{jobID}/ on disk.
	jobDir := filepath.Join(dir, "job")
	var leftoverSSTs []string
	_ = filepath.WalkDir(jobDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Ext(path) == ".sst" {
			leftoverSSTs = append(leftoverSSTs, path)
		}
		return nil
	})
	require.Empty(t, leftoverSSTs,
		"expected no leftover SSTs under %s", jobDir,
	)
}
