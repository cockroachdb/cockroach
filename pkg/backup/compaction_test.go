// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/fatih/structs"
	"github.com/stretchr/testify/require"
)

func TestBackupCompaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tempDir, tempDirCleanup := testutils.TempDir(t)
	defer tempDirCleanup()
	st := cluster.MakeTestingClusterSettings()
	_, db, cleanupDB := backupRestoreTestSetupEmpty(
		t, singleNode, tempDir, InitManualReplication, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Settings: st,
			},
		},
	)
	defer cleanupDB()

	bucketNum := 1
	getLatestFullDir := func(collectionURI []string) string {
		t.Helper()
		var backupPath string
		db.QueryRow(
			t,
			fmt.Sprintf("SHOW BACKUPS IN (%s)", stringifyCollectionURI(collectionURI)),
		).Scan(&backupPath)
		return backupPath
	}

	// Note: Each subtest should create their backups in their own subdirectory to
	// avoid false negatives from subtests relying on backups from other subtests.
	t.Run("basic operations insert, update, and delete", func(t *testing.T) {
		bucketNum++
		collectionURI := []string{fmt.Sprintf("nodelocal://1/backup/%d", bucketNum)}

		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE IF EXISTS foo")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		start := getTime()
		backupStmt := fullBackupQuery(fullCluster, collectionURI, start, noOpts)
		db.Exec(t, backupStmt)

		var fullJobId jobspb.JobID
		db.QueryRow(t, "SELECT system.jobs.id from system.jobs WHERE system.jobs.job_type = 'BACKUP'").Scan(&fullJobId)
		fullRedactedUri := getDescUri(t, db, fullJobId)

		// Run twice to test compaction on top of compaction.
		for i := range 2 {
			db.Exec(t, "INSERT INTO foo VALUES (2, 2), (3, 3)")
			db.Exec(t, incBackupQuery(fullCluster, collectionURI, noAOST, noOpts))
			db.Exec(t, "UPDATE foo SET b = b + 1 WHERE a = 2")
			db.Exec(t, incBackupQuery(fullCluster, collectionURI, noAOST, noOpts))
			db.Exec(t, "DELETE FROM foo WHERE a = 3")
			end := getTime()
			db.Exec(t, incBackupQuery(fullCluster, collectionURI, end, noOpts))

			compactionJobId := triggerCompaction(t, db, backupStmt, getLatestFullDir(collectionURI), start, end)
			compactionRedactedUri := getDescUri(t, db, compactionJobId)
			require.Equal(t, fullRedactedUri, compactionRedactedUri)

			jobutils.WaitForJobToSucceed(t, db, compactionJobId)

			validateCompactedBackupForTables(t, db, collectionURI, []string{"foo"}, start, end, noOpts, noOpts, 2+i)
			start = end
		}

		// Ensure that additional backups were created.
		var numBackups int
		db.QueryRow(
			t, fmt.Sprintf("SELECT count(DISTINCT (start_time, end_time)) FROM [%s]", showBackupQuery(collectionURI, noOpts)),
		).Scan(&numBackups)
		require.Equal(t, 9, numBackups)
	})

	t.Run("create and drop tables", func(t *testing.T) {
		bucketNum++
		collectionURI := []string{fmt.Sprintf("nodelocal://1/backup/%d", bucketNum)}

		defer func() {
			db.Exec(t, "DROP TABLE IF EXISTS foo, bar, baz")
		}()
		db.Exec(t, "CREATE TABLE foo (a INT PRIMARY KEY, b INT)")
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		start := getTime()
		backupStmt := fullBackupQuery(fullCluster, collectionURI, start, noOpts)
		db.Exec(t, backupStmt)

		db.Exec(t, "CREATE TABLE bar (a INT, b INT)")
		db.Exec(t, "INSERT INTO bar VALUES (1, 1)")
		db.Exec(t, incBackupQuery(fullCluster, collectionURI, noAOST, noOpts))

		db.Exec(t, "INSERT INTO bar VALUES (2, 2)")
		db.Exec(t, incBackupQuery(fullCluster, collectionURI, noAOST, noOpts))

		db.Exec(t, "CREATE TABLE baz (a INT, b INT)")
		db.Exec(t, "INSERT INTO baz VALUES (3, 3)")
		end := getTime()
		db.Exec(t, incBackupQuery(fullCluster, collectionURI, end, noOpts))

		jobutils.WaitForJobToSucceed(t, db, triggerCompaction(t, db, backupStmt, getLatestFullDir(collectionURI), start, end))
		validateCompactedBackupForTables(t, db, collectionURI, []string{"foo", "bar", "baz"}, start, end, noOpts, noOpts, 2)

		db.Exec(t, "DROP TABLE bar")
		end = getTime()
		db.Exec(t, incBackupQuery(fullCluster, collectionURI, end, noOpts))
		jobutils.WaitForJobToSucceed(t, db, triggerCompaction(t, db, backupStmt, getLatestFullDir(collectionURI), start, end))

		db.Exec(t, "DROP TABLE foo, baz")
		db.Exec(t, restoreQuery(t, fullCluster, collectionURI, noAOST, noOpts))
		rows := db.QueryStr(t, "SELECT * FROM [SHOW TABLES] WHERE table_name = 'bar'")
		require.Empty(t, rows)
	})

	t.Run("create indexes", func(t *testing.T) {
		bucketNum++
		collectionURI := []string{fmt.Sprintf("nodelocal://1/backup/%d", bucketNum)}

		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1), (2, 2), (3, 3)")
		start := getTime()
		backupStmt := fullBackupQuery(fullCluster, collectionURI, start, noOpts)
		db.Exec(t, backupStmt)

		db.Exec(t, "CREATE INDEX bar ON foo (a)")
		db.Exec(t, "CREATE INDEX baz ON foo (a)")
		db.Exec(t, incBackupQuery(fullCluster, collectionURI, noAOST, noOpts))

		db.Exec(t, "CREATE INDEX qux ON foo (b)")
		db.Exec(t, "DROP INDEX foo@bar")
		end := getTime()
		db.Exec(t, incBackupQuery(fullCluster, collectionURI, end, noOpts))

		jobutils.WaitForJobToSucceed(t, db, triggerCompaction(t, db, backupStmt, getLatestFullDir(collectionURI), start, end))

		var numIndexes, restoredNumIndexes int
		db.QueryRow(t, "SELECT count(*) FROM [SHOW INDEXES FROM foo]").Scan(&numIndexes)
		db.Exec(t, "DROP TABLE foo")
		db.Exec(t, restoreQuery(t, "TABLE foo", collectionURI, noAOST, noOpts))
		db.QueryRow(t, "SELECT count(*) FROM [SHOW INDEXES FROM foo]").Scan(&restoredNumIndexes)
		require.Equal(t, numIndexes, restoredNumIndexes)
	})

	t.Run("compact middle of backup chain", func(t *testing.T) {
		bucketNum++
		collectionURI := []string{fmt.Sprintf("nodelocal://1/backup/%d", bucketNum)}

		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		backupStmt := fullBackupQuery(fullCluster, collectionURI, noAOST, noOpts)
		db.Exec(t, backupStmt)

		db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
		start := getTime()
		db.Exec(t, incBackupQuery(fullCluster, collectionURI, start, noOpts))

		db.Exec(t, "INSERT INTO foo VALUES (3, 3)")
		db.Exec(t, incBackupQuery(fullCluster, collectionURI, noAOST, noOpts))

		db.Exec(t, "INSERT INTO foo VALUES (4, 4)")
		db.Exec(t, incBackupQuery(fullCluster, collectionURI, noAOST, noOpts))

		db.Exec(t, "INSERT INTO foo VALUES (5, 5)")
		end := getTime()
		db.Exec(t, incBackupQuery(fullCluster, collectionURI, end, noOpts))

		db.Exec(t, "INSERT INTO foo VALUES (6, 6)")
		db.Exec(t, incBackupQuery(fullCluster, collectionURI, noAOST, noOpts))

		jobutils.WaitForJobToSucceed(t, db, triggerCompaction(t, db, backupStmt, getLatestFullDir(collectionURI), start, end))
		validateCompactedBackupForTables(t, db, collectionURI, []string{"foo"}, start, end, noOpts, noOpts, 4)
	})

	t.Run("table-level backups", func(t *testing.T) {
		bucketNum++
		collectionURI := []string{fmt.Sprintf("nodelocal://1/backup/%d", bucketNum)}

		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		start := getTime()
		backupStmt := fullBackupQuery(fullCluster, collectionURI, start, noOpts)
		db.Exec(t, backupStmt)

		db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
		db.Exec(t, incBackupQuery(fullCluster, collectionURI, noAOST, noOpts))
		db.Exec(t, "UPDATE foo SET b = b + 1 WHERE a = 2")
		db.Exec(t, "DELETE FROM foo WHERE a = 1")
		end := getTime()
		db.Exec(t, incBackupQuery(fullCluster, collectionURI, end, noOpts))

		jobutils.WaitForJobToSucceed(t, db, triggerCompaction(t, db, backupStmt, getLatestFullDir(collectionURI), start, end))
		validateCompactedBackupForTables(t, db, collectionURI, []string{"foo"}, start, end, noOpts, noOpts, 2)
	})

	t.Run("encrypted backups", func(t *testing.T) {
		bucketNum++
		collectionURI := []string{fmt.Sprintf("nodelocal://1/backup/%d", bucketNum)}
		encryptOpts := "WITH encryption_passphrase = 'correct-horse-battery-staple'"

		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
			db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		start := getTime()
		backupStmt := fullBackupQuery(fullCluster, collectionURI, start, encryptOpts)
		db.Exec(t, backupStmt)

		db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
		db.Exec(t, incBackupQuery(fullCluster, collectionURI, noAOST, encryptOpts))

		db.Exec(t, "UPDATE foo SET b = b + 1 WHERE a = 2")
		end := getTime()
		db.Exec(t, incBackupQuery(fullCluster, collectionURI, end, encryptOpts))

		pause := rand.Intn(2) == 0
		if pause {
			db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'backup_compaction.after.details_has_checkpoint'")
		}
		jobID := triggerCompaction(t, db, backupStmt, getLatestFullDir(collectionURI), start, end)
		if pause {
			jobutils.WaitForJobToPause(t, db, jobID)
			db.Exec(t, "RESUME JOB $1", jobID)
		}
		jobutils.WaitForJobToSucceed(t, db, jobID)
		validateCompactedBackupForTables(t, db, collectionURI, []string{"foo"}, start, end, encryptOpts, encryptOpts, 2)
	})

	t.Run("pause resume and cancel", func(t *testing.T) {
		bucketNum++
		collectionURI := []string{fmt.Sprintf("nodelocal://1/backup/%d", bucketNum)}

		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
			db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
		}()
		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		start := getTime()
		backupStmt := fullBackupQuery(fullCluster, collectionURI, start, noOpts)
		db.Exec(t, backupStmt)
		db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
		db.Exec(t, incBackupQuery(fullCluster, collectionURI, noAOST, noOpts))
		db.Exec(t, "INSERT INTO foo VALUES (3, 3)")
		end := getTime()
		db.Exec(t, incBackupQuery(fullCluster, collectionURI, end, noOpts))
		db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'backup_compaction.after.details_has_checkpoint'")

		jobID := triggerCompaction(t, db, backupStmt, getLatestFullDir(collectionURI), start, end)
		jobutils.WaitForJobToPause(t, db, jobID)
		db.Exec(t, "RESUME JOB $1", jobID)
		jobutils.WaitForJobToSucceed(t, db, jobID)
		validateCompactedBackupForTables(t, db, collectionURI, []string{"foo"}, start, end, noOpts, noOpts, 2)

		db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
		db.Exec(t, "INSERT INTO foo VALUES (4, 4)")
		end = getTime()
		db.Exec(t, incBackupQuery(fullCluster, collectionURI, end, noOpts))
		db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'backup_compaction.after.details_has_checkpoint'")
		jobID = triggerCompaction(t, db, backupStmt, getLatestFullDir(collectionURI), start, end)
		jobutils.WaitForJobToPause(t, db, jobID)
		db.Exec(t, "CANCEL JOB $1", jobID)
		jobutils.WaitForJobToCancel(t, db, jobID)
	})

	t.Run("compaction of chain ending in a compacted backup", func(t *testing.T) {
		// This test is to ensure that the second compaction job does not
		// clobber the first compaction in the bucket. To do so, we take the
		// following chain:
		// F -> I1 -> I2 -> I3
		// We compact I2 and I3 to get C1 and then compact I1 and C1 to get C2.
		// Both C1 and C2 will have the same end time, but C2 should not clobber C1.
		bucketNum++
		collectionURI := []string{fmt.Sprintf("nodelocal://1/backup/%d", bucketNum)}

		db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
		defer func() {
			db.Exec(t, "DROP TABLE foo")
		}()
		start := getTime()
		backupStmt := fullBackupQuery(fullCluster, collectionURI, start, noOpts)
		db.Exec(t, backupStmt)

		db.Exec(t, "INSERT INTO foo VALUES (1, 1)")
		mid := getTime()
		db.Exec(t, incBackupQuery(fullCluster, collectionURI, mid, noOpts))

		db.Exec(t, "INSERT INTO foo VALUES (2, 2)")
		db.Exec(t, incBackupQuery(fullCluster, collectionURI, noAOST, noOpts))

		db.Exec(t, "INSERT INTO foo VALUES (3, 3)")
		end := getTime()
		db.Exec(t, incBackupQuery(fullCluster, collectionURI, end, noOpts))

		fullDir := getLatestFullDir(collectionURI)
		c1JobID := triggerCompaction(t, db, backupStmt, fullDir, mid, end)
		jobutils.WaitForJobToSucceed(t, db, c1JobID)

		c2JobID := triggerCompaction(t, db, backupStmt, fullDir, start, end)
		jobutils.WaitForJobToSucceed(t, db, c2JobID)
		ensureBackupExists(t, db, collectionURI, mid, end, noOpts)
		ensureBackupExists(t, db, collectionURI, start, end, noOpts)
	})
	// TODO (kev-cao): Once range keys are supported by the compaction
	// iterator, add tests for dropped tables/indexes.
}

func TestScheduledBackupCompaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var blockCompaction chan struct{}
	var testKnobs []func(*base.TestingKnobs)
	if rand.Intn(2) == 0 {
		// Artificially force a scheduled full backup to also execute before the
		// compaction job resolves its destination, which will cause the LATEST
		// file to be updated. This is to ensure that the compaction job correctly
		// identifies the full backup it belongs to and does not attempt to chain
		// off of the second full backup.
		blockCompaction = make(chan struct{})
		defer close(blockCompaction)
		testKnobs = append(testKnobs, func(testKnobs *base.TestingKnobs) {
			testKnobs.BackupRestore = &sql.BackupRestoreTestingKnobs{
				RunBeforeResolvingCompactionDest: func() error {
					<-blockCompaction
					return nil
				},
			}
		})
	}
	th, cleanup := newTestHelper(t, testKnobs...)
	defer cleanup()

	th.setOverrideAsOfClauseKnob(t)
	// Time is set to a time such that no full backup will unexpectedly run as we
	// artificially time travel. This ensures deterministic behavior that is not
	// impacted by when the test runs.
	th.env.SetTime(time.Date(2025, 3, 27, 1, 0, 0, 0, time.UTC))

	th.sqlDB.Exec(t, "SET CLUSTER SETTING backup.compaction.threshold = 4")
	th.sqlDB.Exec(t, "SET CLUSTER SETTING backup.compaction.window_size = 3")
	schedules, err := th.createBackupSchedule(
		t, "CREATE SCHEDULE FOR BACKUP INTO $1 RECURRING '@hourly'", "nodelocal://1/backup",
	)
	require.NoError(t, err)
	require.Equal(t, 2, len(schedules))

	full, inc := schedules[0], schedules[1]
	if full.IsPaused() {
		full, inc = inc, full
	}

	th.env.SetTime(full.NextRun().Add(time.Second))
	require.NoError(t, th.executeSchedules())
	th.waitForSuccessfulScheduledJob(t, full.ScheduleID())
	var backupPath string
	th.sqlDB.QueryRow(t, "SHOW BACKUPS IN 'nodelocal://1/backup'").Scan(&backupPath)

	for range 3 {
		inc, err = jobs.ScheduledJobDB(th.internalDB()).
			Load(context.Background(), th.env, inc.ScheduleID())
		require.NoError(t, err)

		th.env.SetTime(inc.NextRun().Add(time.Second))
		require.NoError(t, th.executeSchedules())
		th.waitForSuccessfulScheduledJob(t, inc.ScheduleID())
	}

	if blockCompaction != nil {
		t.Log("executing second full backup before compaction job resolves destination")
		// Instead of fast forwarding to the full backup's next run, which can result in additional
		// incrementeals being triggered by `executeSchedules`, we update the full's next run to the
		// current time.
		th.sqlDB.QueryStr(t, fmt.Sprintf("ALTER BACKUP SCHEDULE %d EXECUTE FULL IMMEDIATELY", full.ScheduleID()))
		full, err = jobs.ScheduledJobDB(th.internalDB()).
			Load(context.Background(), th.env, full.ScheduleID())
		require.NoError(t, err)
		th.env.SetTime(full.NextRun().Add(time.Second))
		require.NoError(t, th.executeSchedules())
		th.waitForSuccessfulScheduledJob(t, full.ScheduleID())
		blockCompaction <- struct{}{}
	}

	var jobID jobspb.JobID
	require.NoError(
		t, th.sqlDB.DB.QueryRowContext(
			ctx,
			`SELECT job_id FROM [SHOW JOBS] WHERE description ILIKE 'COMPACT%' AND job_type = 'BACKUP'`,
		).Scan(&jobID),
	)

	testutils.SucceedsSoon(t, func() error {
		th.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
		var unused int64
		return th.sqlDB.DB.QueryRowContext(
			ctx,
			"SELECT job_id FROM [SHOW JOBS] WHERE job_id = $1 AND status = $2",
			jobID, jobs.StateSucceeded,
		).Scan(&unused)
	})

	var numBackups int
	th.sqlDB.QueryRow(
		t,
		fmt.Sprintf("SELECT count(DISTINCT (start_time, end_time)) FROM "+
			"[SHOW BACKUP FROM '%s' IN 'nodelocal://1/backup']", backupPath),
	).Scan(&numBackups)
	require.Equal(t, 5, numBackups)
}

func TestCompactionTriggeringCompaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	th, cleanup := newTestHelper(t)
	defer cleanup()
	th.setOverrideAsOfClauseKnob(t)
	// Set to a time such that full backups do not unexpectedly run based on test
	// time.
	th.env.SetTime(time.Date(2025, 05, 01, 1, 0, 0, 0, time.UTC))
	// First disable compactions so that we can create a sufficiently long chain
	// to require multiple compactions.
	th.sqlDB.Exec(t, "SET CLUSTER SETTING backup.compaction.threshold = 0")
	schedules, err := th.createBackupSchedule(
		t, "CREATE SCHEDULE FOR BACKUP INTO $1 RECURRING '@hourly'", "nodelocal://1/backup",
	)
	require.NoError(t, err)
	require.Equal(t, 2, len(schedules))

	full, inc := schedules[0], schedules[1]
	if full.IsPaused() {
		full, inc = inc, full
	}

	th.env.SetTime(full.NextRun().Add(time.Second))
	require.NoError(t, th.executeSchedules())
	th.waitForSuccessfulScheduledJob(t, full.ScheduleID())

	executeIncremental := func() {
		t.Helper()
		inc, err = jobs.ScheduledJobDB(th.internalDB()).Load(ctx, th.env, inc.ScheduleID())
		require.NoError(t, err)
		th.env.SetTime(inc.NextRun().Add(time.Second))
		require.NoError(t, th.executeSchedules())
		th.waitForSuccessfulScheduledJob(t, inc.ScheduleID())
	}

	for range 5 {
		executeIncremental()
	}

	// Now enable compactions such that the next incremental will trigger a
	// compaction, which should then trigger more compactions until the chain is
	// shorter than the threshold.
	th.sqlDB.Exec(t, "SET CLUSTER SETTING backup.compaction.threshold = 4")
	th.sqlDB.Exec(t, "SET CLUSTER SETTING backup.compaction.window_size = 3")
	executeIncremental()

	// Validate that a compaction job was created.
	var unused jobspb.JobID
	require.NoError(
		t,
		th.sqlDB.DB.QueryRowContext(
			ctx,
			`SELECT job_id FROM [SHOW JOBS] WHERE description ILIKE 'COMPACT%' AND job_type = 'BACKUP'`,
		).Scan(&unused),
	)

	// Poll until there are no more compaction jobs running.
	testutils.SucceedsSoon(t, func() error {
		th.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
		var numCompactions int
		require.NoError(
			t,
			th.sqlDB.DB.QueryRowContext(
				ctx,
				`SELECT count(*) FROM [SHOW JOBS] WHERE description ILIKE 'COMPACT%' AND job_type = 'BACKUP' AND status = $1`,
				jobs.StateRunning,
			).Scan(&numCompactions),
		)
		if numCompactions == 0 {
			return nil
		}
		return fmt.Errorf("waiting for compactions to complete, %d still running", numCompactions)
	})

	// Validate that all compaction jobs succeeded.
	var numNotSucceeded int
	require.NoError(
		t,
		th.sqlDB.DB.QueryRowContext(
			ctx,
			`SELECT count(*) FROM [SHOW JOBS] WHERE description ILIKE 'COMPACT%' AND job_type = 'BACKUP' AND status != $1`,
			jobs.StateSucceeded,
		).Scan(&numNotSucceeded),
	)
	require.Equal(t, 0, numNotSucceeded)

	// Validate that multiple compactions were indeed run.
	var numCompactions int
	require.NoError(
		t,
		th.sqlDB.DB.QueryRowContext(
			ctx,
			`SELECT count(*) FROM [SHOW JOBS] WHERE description ILIKE 'COMPACT%' AND job_type = 'BACKUP'`,
		).Scan(&numCompactions),
	)
	require.Greater(t, numCompactions, 1)
}

func TestBlockConcurrentScheduledCompactions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test covers the case outlined in #145410 where a scheduled compaction
	// is triggered before another compaction completes. This test ensures that we
	// avoid a race condition where the completion of a compaction job impacts
	// another compaction job by mutating the backup chain while the other
	// compaction job is resolving its compaction chain.
	ctx := context.Background()
	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.setOverrideAsOfClauseKnob(t)
	// Set to a time such that full backups do not unexpectedly run based on test
	// time.
	th.env.SetTime(time.Date(2025, 05, 01, 1, 0, 0, 0, time.UTC))

	th.sqlDB.Exec(t, "SET CLUSTER SETTING backup.compaction.threshold = 4")
	th.sqlDB.Exec(t, "SET CLUSTER SETTING backup.compaction.window_size = 3")
	th.sqlDB.Exec(
		t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'backup_compaction.after.details_has_checkpoint'",
	)
	schedules, err := th.createBackupSchedule(
		t, "CREATE SCHEDULE FOR BACKUP INTO $1 RECURRING '@hourly'", "nodelocal://1/backup",
	)
	require.NoError(t, err)
	require.Equal(t, 2, len(schedules))

	full, inc := schedules[0], schedules[1]
	if full.IsPaused() {
		full, inc = inc, full
	}

	th.env.SetTime(full.NextRun().Add(time.Second))
	require.NoError(t, th.executeSchedules())
	th.waitForSuccessfulScheduledJob(t, full.ScheduleID())

	executeIncremental := func() {
		t.Helper()
		inc, err = jobs.ScheduledJobDB(th.internalDB()).
			Load(context.Background(), th.env, inc.ScheduleID())
		require.NoError(t, err)

		th.env.SetTime(inc.NextRun().Add(time.Second))
		require.NoError(t, th.executeSchedules())
		th.waitForSuccessfulScheduledJob(t, inc.ScheduleID())
	}
	for range 3 {
		executeIncremental()
	}

	var jobID jobspb.JobID
	require.NoError(
		t,
		th.sqlDB.DB.QueryRowContext(
			ctx,
			`SELECT job_id FROM [SHOW JOBS] WHERE description ILIKE 'COMPACT%' AND job_type = 'BACKUP'`,
		).Scan(&jobID),
	)

	th.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
	jobutils.WaitForJobToPause(t, th.sqlDB, jobID)
	// Force another compaction to be attempted and ensure that it is blocked from
	// running.
	executeIncremental()

	var numCompactions int
	require.NoError(
		t,
		th.sqlDB.DB.QueryRowContext(
			ctx,
			`SELECT count(*) FROM [SHOW JOBS] WHERE description ILIKE 'COMPACT%' AND job_type = 'BACKUP'`,
		).Scan(&numCompactions),
	)
	require.Equal(t, 1, numCompactions)

	// Allow the first compaction job to complete and cause another compaction to
	// be attempted, which should not be blocked.
	th.sqlDB.Exec(t, "RESUME JOB $1", jobID)
	jobutils.WaitForJobToSucceed(t, th.sqlDB, jobID)
	th.sqlDB.Exec(
		t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''",
	)
	executeIncremental()
	require.NoError(
		t,
		th.sqlDB.DB.QueryRowContext(
			ctx,
			`SELECT count(*) FROM [SHOW JOBS] WHERE description ILIKE 'COMPACT%' AND job_type = 'BACKUP'`,
		).Scan(&numCompactions),
	)
	require.Equal(t, 2, numCompactions)
}

func TestBackupCompactionExecLocality(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "too slow")

	args := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestDoesNotWorkWithSecondaryTenantsButWeDontKnowWhyYet(142798),
		},
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {
				ExternalIODir: "/west0",
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "tier", Value: "0"},
					{Key: "region", Value: "west"},
				}},
			},
			1: {
				ExternalIODir: "/west1",
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "tier", Value: "1"},
					{Key: "region", Value: "west"},
				}},
			},
			2: {
				ExternalIODir: "/east0",
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "tier", Value: "0"},
					{Key: "region", Value: "east"},
				}},
			},
			3: {
				ExternalIODir: "/east1",
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "tier", Value: "1"},
					{Key: "region", Value: "east"},
				}},
			},
		},
	}
	const numAccounts = 1000
	tc, db, _, cleanup := backupRestoreTestSetupWithParams(t, 4, numAccounts, InitManualReplication, args)
	defer cleanup()

	west1Node, east0Node := sqlutils.MakeSQLRunner(tc.Conns[1]), sqlutils.MakeSQLRunner(tc.Conns[2])

	const targets = "DATABASE data"

	// initBackupChain will create an identical chain of backups in all four node directories.
	initBackupChain := func(subCollection string) (hlc.Timestamp, hlc.Timestamp) {
		start := getTime()
		for i := 1; i <= 4; i++ {
			db.Exec(t, fullBackupQuery(targets,
				[]string{fmt.Sprintf("nodelocal://%d/%s", i, subCollection)},
				start, noOpts))
		}
		db.Exec(t, "UPDATE data.bank SET balance = 200")
		for i := 1; i <= 4; i++ {
			db.Exec(t, incBackupQuery(targets,
				[]string{fmt.Sprintf("nodelocal://%d/%s", i, subCollection)},
				noAOST, noOpts))
		}
		db.Exec(t, "UPDATE data.bank SET balance = 201")
		for i := 1; i <= 4; i++ {
			db.Exec(t, incBackupQuery(targets,
				[]string{fmt.Sprintf("nodelocal://%d/%s", i, subCollection)},
				noAOST, noOpts))
		}
		db.Exec(t, "UPDATE data.bank SET balance = 202")
		end := getTime()
		for i := 1; i <= 4; i++ {
			db.Exec(t, incBackupQuery(targets,
				[]string{fmt.Sprintf("nodelocal://%d/%s", i, subCollection)},
				end, noOpts))
		}
		return start, end
	}
	const numInitialBackups = 4

	t.Run("pin-tier", func(t *testing.T) {
		ensureLeaseholder(t, db)
		start, end := initBackupChain("pin-tier")

		// Note that using "nodelocal://0", in combination with setting a unique `ExternalIODir` for
		// each node, allows us to see which nodes participated in processing the job.
		// Processor nodes will translate "nodelocal://0" to their respective directories,
		// meaning only the directories of nodes which participated in processing will be modified.
		processorCollection := []string{"nodelocal://0/pin-tier"}

		// Run and wait for an execution locality filtered compaction job.
		jobutils.WaitForJobToSucceed(
			t, db,
			triggerCompaction(
				t, east0Node,
				fullBackupQuery(targets, processorCollection, start,
					"WITH EXECUTION LOCALITY = 'tier=0'"),
				getLatestFullDir(t, db, processorCollection[0]),
				start, end,
			),
		)

		numWest0 := countBackups(t, db, []string{"nodelocal://1/pin-tier"})
		numWest1 := countBackups(t, db, []string{"nodelocal://2/pin-tier"})
		numEast0 := countBackups(t, db, []string{"nodelocal://3/pin-tier"})
		numEast1 := countBackups(t, db, []string{"nodelocal://4/pin-tier"})

		// Validate that at least one node matching the locality filter processed the compaction,
		// and that all nodes which don't match the locality filter did not.
		require.True(t, numEast0 == numInitialBackups+1 || numWest0 == numInitialBackups+1)
		require.Equal(t, numWest1, numInitialBackups)
		require.Equal(t, numEast1, numInitialBackups)
	})

	t.Run("pin-region", func(t *testing.T) {
		ensureLeaseholder(t, db)
		start, end := initBackupChain("pin-region")
		processorCollection := []string{"nodelocal://0/pin-region"}

		jobutils.WaitForJobToSucceed(
			t, db,
			triggerCompaction(
				t, east0Node,
				fullBackupQuery(targets, processorCollection, start,
					"WITH EXECUTION LOCALITY = 'region=east'"),
				getLatestFullDir(t, db, processorCollection[0]),
				start, end,
			),
		)

		numWest0 := countBackups(t, db, []string{"nodelocal://1/pin-region"})
		numWest1 := countBackups(t, db, []string{"nodelocal://2/pin-region"})
		numEast0 := countBackups(t, db, []string{"nodelocal://3/pin-region"})
		numEast1 := countBackups(t, db, []string{"nodelocal://4/pin-region"})

		require.True(t, numEast0 == numInitialBackups+1 || numEast1 == numInitialBackups+1)
		require.Equal(t, numWest0, numInitialBackups)
		require.Equal(t, numWest1, numInitialBackups)
	})

	t.Run("pin-single", func(t *testing.T) {
		ensureLeaseholder(t, db)
		start, end := initBackupChain("pin-single")
		processorCollection := []string{"nodelocal://0/pin-single"}

		jobutils.WaitForJobToSucceed(
			t, db,
			triggerCompaction(
				t, west1Node,
				fullBackupQuery(targets, processorCollection, start,
					"WITH EXECUTION LOCALITY = 'tier=1,region=west'"),
				getLatestFullDir(t, db, processorCollection[0]),
				start, end,
			),
		)

		numWest0 := countBackups(t, db, []string{"nodelocal://1/pin-single"})
		numWest1 := countBackups(t, db, []string{"nodelocal://2/pin-single"})
		numEast0 := countBackups(t, db, []string{"nodelocal://3/pin-single"})
		numEast1 := countBackups(t, db, []string{"nodelocal://4/pin-single"})

		// Validate that the expected node processed the compaction, and the rest did not.
		require.Equal(t, numWest1, numInitialBackups+1)
		require.Equal(t, numWest0, numInitialBackups)
		require.Equal(t, numEast0, numInitialBackups)
		require.Equal(t, numEast1, numInitialBackups)

		// Validate that the expected node's directory restores correctly using the expected number of backups.
		//
		// Note that with this test structure, we can only do this restore validation in cases where
		// we pin the processing to a single node. Using the "nodelocal://0" trick to see which nodes
		// participated in processing means that, in cases where multiple nodes participate in processing,
		// the compaction will be split across multiple directories, and thus cannot be restored from correctly.
		validateCompactedBackupForTables(t, db,
			[]string{"nodelocal://2/pin-single"},
			[]string{"bank"}, start, end, noOpts, noOpts,
			2)
	})

	t.Run("validate-coordinator", func(t *testing.T) {
		// The previous subtests only validate that the processor nodes adhere to the locality filter.
		// This test ensures that the coordinator node also matches the locality filter.
		ensureLeaseholder(t, db)
		start, end := initBackupChain("validate-coordinator")
		processorCollection := []string{"nodelocal://0/validate-coordinator"}

		compactionJobID := triggerCompaction(
			t, db,
			fullBackupQuery(targets, processorCollection, start,
				"WITH EXECUTION LOCALITY = 'tier=1,region=west'"),
			getLatestFullDir(t, db, processorCollection[0]),
			start, end,
		)
		jobutils.WaitForJobToSucceed(t, db, compactionJobID)

		var instanceId int32
		db.QueryRow(t, fmt.Sprintf(
			"select system.jobs.claim_instance_id from system.jobs where system.jobs.id = %d",
			compactionJobID)).Scan(&instanceId)

		require.Equal(t, int32(tc.Servers[1].SQLInstanceID()), instanceId)
	})
}

func TestBackupCompactionLocAware(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDuress(t, "too slow")

	var hookFn func(processorLocality roachpb.Locality, fileLocality string) error
	knobs := base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{
			BackupRestoreTestingKnobs: &sql.BackupRestoreTestingKnobs{
				OnCompactionFileAccess: &hookFn,
			},
		},
	}
	args := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestDoesNotWorkWithSecondaryTenantsButWeDontKnowWhyYet(142798),
		},
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "west"},
					{Key: "az", Value: "az1"},
					{Key: "dc", Value: "dc1"},
				}},
				Knobs: knobs,
			},
			1: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "east"},
					{Key: "az", Value: "az1"},
					{Key: "dc", Value: "dc2"},
				}},
				Knobs: knobs,
			},
			2: {
				Locality: roachpb.Locality{Tiers: []roachpb.Tier{
					{Key: "region", Value: "east"},
					{Key: "az", Value: "az2"},
					{Key: "dc", Value: "dc3"},
				}},
				Knobs: knobs,
			},
		},
	}

	const numAccounts = 1000
	_, db, _, cleanupFn := backupRestoreTestSetupWithParams(t, 3, numAccounts, NoInitManipulation, args)
	defer cleanupFn()

	getLatestFullDir := func(collectionURI []string) string {
		t.Helper()
		var backupPath string
		db.QueryRow(
			t,
			fmt.Sprintf("SHOW BACKUPS IN (%s)", stringifyCollectionURI(collectionURI)),
		).Scan(&backupPath)
		return backupPath
	}

	const targets = "DATABASE data"
	const numInitialBackups = 4
	initBackupChain := func(uris []string, opts string) (hlc.Timestamp, hlc.Timestamp) {
		start := getTime()
		db.Exec(t, fullBackupQuery(targets, uris, start, opts))
		db.Exec(t, "UPDATE data.bank SET balance = 200")
		db.Exec(t, incBackupQuery(targets, uris, noAOST, opts))
		db.Exec(t, "UPDATE data.bank SET balance = 201")
		db.Exec(t, incBackupQuery(targets, uris, noAOST, opts))
		db.Exec(t, "UPDATE data.bank SET balance = 202")
		end := getTime()
		db.Exec(t, incBackupQuery(targets, uris, end, opts))
		return start, end
	}

	validateCounts := func(uris []string, shouldCompact ...string) {
		rows := db.Query(t, fmt.Sprintf(`
			WITH with_dir AS (
	  			SELECT *, regexp_replace(path, '/data/[^/]+\.sst$', '') AS dir
	  			FROM [SHOW BACKUP FILES FROM LATEST IN (%s)]
			)
			SELECT count(DISTINCT dir), locality
			FROM with_dir
			GROUP BY  locality`,
			stringifyCollectionURI(uris),
		))
		for rows.Next() {
			var count int
			var locality string
			require.NoError(t, rows.Scan(&count, &locality))

			if slices.Contains(shouldCompact, locality) {
				require.Equal(t, numInitialBackups+1, count)
			} else {
				t.Fatalf("invalid locality in file counts: %s", locality)
			}
		}
		require.NoError(t, rows.Err())
		require.Equal(t, numInitialBackups+1, countBackups(t, db, uris))
	}

	// Make RestoreSpanEntry smaller than it typically is,
	// so that we have many entries to divy up among workers.
	db.Exec(t, "SET CLUSTER SETTING backup.restore_span.target_size = '1KB'")
	db.Exec(t, "SET CLUSTER SETTING backup.restore_span.max_file_count = 2")

	// Test the common/recommended usage, where all nodes match a locality-specific URI.
	t.Run("fully-matching", func(t *testing.T) {
		ensureLeaseholder(t, db)

		testSubDir := t.Name()
		uris := []string{
			localFoo + "/" + testSubDir + "/1?COCKROACH_LOCALITY=" + url.QueryEscape("default"),
			localFoo + "/" + testSubDir + "/2?COCKROACH_LOCALITY=" + url.QueryEscape("dc=dc1"),
			localFoo + "/" + testSubDir + "/3?COCKROACH_LOCALITY=" + url.QueryEscape("dc=dc2"),
			localFoo + "/" + testSubDir + "/4?COCKROACH_LOCALITY=" + url.QueryEscape("dc=dc3"),
		}

		hookFn = func(processorLocality roachpb.Locality, fileLocality string) error {
			// All data should be in one of the dc=dcn URIs.
			if fileLocality == "" || fileLocality == "default" {
				return errors.New("unexpected default data")
			}

			processorDC, ok := processorLocality.Find("dc")
			if !ok {
				return errors.Newf(
					"processor has no dc locality but is processing file from %s",
					fileLocality,
				)
			}

			expectedFileLocality := "dc=" + processorDC
			if fileLocality != expectedFileLocality {
				return errors.Newf(
					"processor in %s attempted to read file from %s",
					expectedFileLocality, fileLocality,
				)
			}

			return nil
		}

		start, end := initBackupChain(uris, noOpts)
		fullBackupPath := getLatestFullDir(uris)
		jobutils.WaitForJobToSucceed(
			t, db,
			triggerCompaction(
				t, db,
				incBackupQuery(targets, uris, end, noOpts),
				fullBackupPath,
				start, end,
			),
		)

		// Validate that each locality-specific URI recieved an additional backup,
		// and that the default URI did not.
		validateCounts(uris, "dc=dc1", "dc=dc2", "dc=dc3")

		// Validate that the backup chain restores correctly using the expected number of backups.
		validateCompactedBackupForTables(t, db, uris, []string{"bank"},
			start, end, noOpts, noOpts, 2)
	})

	// Validate that data in the default URI is handled by non-matching nodes.
	t.Run("some-default", func(t *testing.T) {
		ensureLeaseholder(t, db)

		testSubDir := t.Name()
		uris := []string{
			localFoo + "/" + testSubDir + "/1?COCKROACH_LOCALITY=" + url.QueryEscape("default"),
			localFoo + "/" + testSubDir + "/2?COCKROACH_LOCALITY=" + url.QueryEscape("dc=dc1"),
			localFoo + "/" + testSubDir + "/3?COCKROACH_LOCALITY=" + url.QueryEscape("dc=dc2"),
		}

		hookFn = func(processorLocality roachpb.Locality, fileLocality string) error {
			processorDC, ok := processorLocality.Find("dc")
			if !ok {
				return errors.Newf(
					"processor has no dc locality but is processing file from %s",
					fileLocality,
				)
			}

			// Files from default location should be processed by non-matching node (dc=dc3).
			if fileLocality == "" || fileLocality == "default" {
				if processorDC != "dc3" {
					return errors.Newf(
						"file from default uri is incorrectly being processed by a node with locality: %+v",
						processorLocality,
					)
				}
				return nil
			}

			// The rest of the files should be processed by their respective matching node.
			expectedFileLocality := "dc=" + processorDC
			if fileLocality != expectedFileLocality {
				return errors.Newf(
					"processor in %s attempted to read file from %s",
					expectedFileLocality, fileLocality,
				)
			}

			return nil
		}

		start, end := initBackupChain(uris, noOpts)
		fullBackupPath := getLatestFullDir(uris)
		jobutils.WaitForJobToSucceed(
			t, db,
			triggerCompaction(
				t, db,
				incBackupQuery(targets, uris, end, noOpts),
				fullBackupPath,
				start, end,
			),
		)

		validateCounts(uris, "default", "dc=dc1", "dc=dc2")
		validateCompactedBackupForTables(t, db, uris, []string{"bank"},
			start, end, noOpts, noOpts, 2)
	})

	// Test that workers send compacted data to the most specific matching URI.
	t.Run("most-specific", func(t *testing.T) {
		ensureLeaseholder(t, db)

		testSubDir := t.Name()
		uris := []string{
			localFoo + "/" + testSubDir + "/1?COCKROACH_LOCALITY=" + url.QueryEscape("default"),
			localFoo + "/" + testSubDir + "/2?COCKROACH_LOCALITY=" + url.QueryEscape("region=east"),
			localFoo + "/" + testSubDir + "/3?COCKROACH_LOCALITY=" + url.QueryEscape("az=az1"),
			localFoo + "/" + testSubDir + "/4?COCKROACH_LOCALITY=" + url.QueryEscape("az=az2"),
		}

		hookFn = func(processorLocality roachpb.Locality, fileLocality string) error {
			if fileLocality == "" || fileLocality == "default" {
				return errors.New("unexpected default data")
			}
			if fileLocality == "region=east" {
				return errors.New("unexpected east region data")
			}

			processorAZ, ok := processorLocality.Find("az")
			if !ok {
				return errors.Newf(
					"processor has no az locality but is processing file from %s",
					fileLocality,
				)
			}

			expectedFileLocality := "az=" + processorAZ
			if fileLocality != expectedFileLocality {
				return errors.Newf(
					"processor in %s attempted to read file from %s",
					expectedFileLocality, fileLocality,
				)
			}

			return nil
		}

		start, end := initBackupChain(uris, noOpts)
		fullBackupPath := getLatestFullDir(uris)
		jobutils.WaitForJobToSucceed(
			t, db,
			triggerCompaction(
				t, db,
				incBackupQuery(targets, uris, end, noOpts),
				fullBackupPath,
				start, end,
			),
		)

		validateCounts(uris, "az=az1", "az=az2")
		validateCompactedBackupForTables(t, db, uris, []string{"bank"},
			start, end, noOpts, noOpts, 2)
	})

	// This sub test does a dry run of locality-aware compaction with `STRICT STORAGE LOCALITY` set.
	// Note that this test only performs the expected/recommended happy path. This is because, for
	// testing the actual edge cases of this option, we would need to alter the topology of the cluster
	// between backup time and compaction time, which is difficult and slow with a test cluster.
	//
	// This option is largely just a directive for us to error in situations where we would otherwise
	// fall back to even assignment. This error occurs in buildLocalitySets, and thus the edge cases
	// surrounding this option are tested in TestBuildLocalitySetsStrict.
	t.Run("dry-run-strict", func(t *testing.T) {
		ensureLeaseholder(t, db)

		testSubDir := t.Name()
		uris := []string{
			localFoo + "/" + testSubDir + "/1?COCKROACH_LOCALITY=" + url.QueryEscape("default"),
			localFoo + "/" + testSubDir + "/2?COCKROACH_LOCALITY=" + url.QueryEscape("dc=dc1"),
			localFoo + "/" + testSubDir + "/3?COCKROACH_LOCALITY=" + url.QueryEscape("dc=dc2"),
			localFoo + "/" + testSubDir + "/4?COCKROACH_LOCALITY=" + url.QueryEscape("dc=dc3"),
		}

		hookFn = func(processorLocality roachpb.Locality, fileLocality string) error {
			if fileLocality == "" || fileLocality == "default" {
				return errors.New("unexpected default data")
			}

			processorDC, ok := processorLocality.Find("dc")
			if !ok {
				return errors.Newf(
					"processor has no dc locality but is processing file from %s",
					fileLocality,
				)
			}

			expectedFileLocality := "dc=" + processorDC
			if fileLocality != expectedFileLocality {
				return errors.Newf(
					"processor in %s attempted to read file from %s",
					expectedFileLocality, fileLocality,
				)
			}

			return nil
		}

		const strictOpts = "WITH STRICT STORAGE LOCALITY"
		start, end := initBackupChain(uris, strictOpts)
		fullBackupPath := getLatestFullDir(uris)
		jobutils.WaitForJobToSucceed(
			t, db,
			triggerCompaction(
				t, db,
				incBackupQuery(targets, uris, end, strictOpts),
				fullBackupPath,
				start, end,
			),
		)

		validateCounts(uris, "dc=dc1", "dc=dc2", "dc=dc3")
		validateCompactedBackupForTables(t, db, uris, []string{"bank"},
			start, end, noOpts, noOpts, 2)
	})
}

func TestBackupCompactionUnsupportedOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	backupCompactionThreshold.Override(ctx, &st.SV, 3)
	execCfg := sql.ExecutorConfig{
		Settings: st,
	}
	testcases := []struct {
		name    string
		details jobspb.BackupDetails
		error   string
	}{
		{
			"scheduled backups only",
			jobspb.BackupDetails{
				ScheduleID: 0,
			},
			"only scheduled backups can be compacted",
		},
		{
			"tenant specific backups not supported",
			jobspb.BackupDetails{
				ScheduleID:        1,
				SpecificTenantIds: []roachpb.TenantID{roachpb.MustMakeTenantID(12)},
			},
			"backups of tenants not supported for compaction",
		},
		{
			"backups including tenants not supported",
			jobspb.BackupDetails{
				ScheduleID:                 1,
				IncludeAllSecondaryTenants: true,
			},
			"backups of tenants not supported for compaction",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			tc.details.StartTime = hlc.Timestamp{WallTime: 100} // Nonzero start time
			jobID, err := maybeStartCompactionJob(
				ctx, &execCfg, username.RootUserName(), tc.details,
			)
			require.Zero(t, jobID)
			require.ErrorContains(t, err, tc.error)
		})
	}
}

func TestCompactionCheckpointing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The backup needs to be large enough that checkpoints will be created, so
	// we pick a large number of accounts.
	const numAccounts = 1000
	var manifestNumFiles atomic.Int32
	manifestNumFiles.Store(-1) // -1 means we haven't seen the manifests yet.
	_, db, _, cleanup := backupRestoreTestSetupWithParams(
		t, singleNode, numAccounts, InitManualReplication, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					BackupRestore: &sql.BackupRestoreTestingKnobs{
						AfterLoadingCompactionManifestOnResume: func(m *backuppb.BackupManifest) {
							manifestNumFiles.Store(int32(len(m.Files)))
						},
					},
				},
			},
		},
	)
	defer cleanup()

	collectionURI := []string{"nodelocal://1/backup"}

	writeQueries := func() {
		db.Exec(t, "UPDATE data.bank SET balance = balance + 1")
	}
	db.Exec(t, "SET CLUSTER SETTING bulkio.backup.checkpoint_interval = '10ms'")
	start := getTime()
	backupStmt := fullBackupQuery(fullCluster, collectionURI, start, noOpts)
	db.Exec(t, backupStmt)
	writeQueries()
	db.Exec(t, incBackupQuery(fullCluster, collectionURI, noAOST, noOpts))
	writeQueries()
	end := getTime()
	db.Exec(t, incBackupQuery(fullCluster, collectionURI, end, noOpts))

	var backupPath string
	db.QueryRow(
		t,
		fmt.Sprintf("SHOW BACKUPS IN (%s)", stringifyCollectionURI(collectionURI)),
	).Scan(&backupPath)

	db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'backup_compaction.after.write_checkpoint'")
	jobID := triggerCompaction(t, db, backupStmt, backupPath, start, end)

	// Ensure that the very first manifest when the job is initially started has
	// no files.
	testutils.SucceedsSoon(t, func() error {
		if manifestNumFiles.Load() < 0 {
			return fmt.Errorf("waiting for manifest to be loaded")
		}
		return nil
	})
	require.Equal(t, 0, int(manifestNumFiles.Load()), "expected no files in manifest")

	// Wait for the job to hit the pausepoint.
	jobutils.WaitForJobToPause(t, db, jobID)
	// Don't bother pausing on other checkpoints.
	db.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = ''")
	db.Exec(t, "RESUME JOB $1", jobID)
	jobutils.WaitForJobToRun(t, db, jobID)

	// Now that the job has been paused and resumed after previously hitting a
	// checkpoint, the initial manifest at the start of the job should have
	// some files from before the pause.
	testutils.SucceedsSoon(t, func() error {
		if manifestNumFiles.Load() <= 0 {
			return fmt.Errorf("waiting for manifest to be loaded")
		}
		return nil
	})
	require.Greater(t, int(manifestNumFiles.Load()), 0, "expected non-zero number of files in manifest")

	jobutils.WaitForJobToSucceed(t, db, jobID)
	validateCompactedBackupForTables(t, db, collectionURI, []string{"bank"}, start, end, noOpts, noOpts, 2)
}

func TestBackupCompactionProgressTracking(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.TestingSetProgressThresholds()()

	_, db, _, cleanup := backupRestoreTestSetup(
		t, singleNode, 1000 /* numAccounts */, InitManualReplication,
	)
	defer cleanup()

	mutate := func() {
		db.Exec(t, "UPDATE data.bank SET balance = balance + 1")
	}
	collectionURI := []string{"nodelocal://1/backup"}
	start := getTime()
	backupStmt := fullBackupQuery(fullCluster, collectionURI, start, noOpts)
	db.Exec(t, backupStmt)
	mutate()
	db.Exec(t, incBackupQuery(fullCluster, collectionURI, noAOST, noOpts))
	mutate()
	end := getTime()
	db.Exec(t, incBackupQuery(fullCluster, collectionURI, end, noOpts))

	var backupPath string
	db.QueryRow(
		t,
		fmt.Sprintf("SHOW BACKUPS IN (%s)", stringifyCollectionURI(collectionURI)),
	).Scan(&backupPath)

	db.Exec(t, "SET CLUSTER SETTING backup.restore_span.target_size = '1KB'")
	db.Exec(t, "SET CLUSTER SETTING backup.restore_span.max_file_count = 2")
	jobID := triggerCompaction(t, db, backupStmt, backupPath, start, end)
	jobutils.WaitForJobToSucceed(t, db, jobID)

	// Validate that fraction_completed was updated correctly.
	rows := db.Query(t,
		"SELECT fraction FROM system.job_progress_history WHERE job_id = $1 ORDER BY written",
		jobID,
	)
	var frac float64
	var updates int
	for rows.Next() {
		updates++
		oldFrac := frac
		require.NoError(t, rows.Scan(&frac))
		require.GreaterOrEqual(t, frac, oldFrac)
	}
	require.NoError(t, rows.Err())
	require.Greater(t, updates, 2)
	require.Equal(t, 1.0, frac)
}

func TestToggleCompactionForRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc, db, _, cleanup := backupRestoreTestSetup(
		t, singleNode, 2 /* numAccounts */, InitManualReplication,
	)
	defer cleanup()

	collectionURI := []string{"nodelocal://1/backup"}
	start := getTime()
	backupStmt := fullBackupQuery(fullCluster, collectionURI, start, noOpts)
	db.Exec(t, backupStmt)
	db.Exec(t, incBackupQuery(fullCluster, collectionURI, noAOST, noOpts))
	end := getTime()
	db.Exec(t, incBackupQuery(fullCluster, collectionURI, end, noOpts))
	var backupPath string
	db.QueryRow(t, "SHOW BACKUPS IN 'nodelocal://1/backup'").Scan(&backupPath)
	compactionID := triggerCompaction(t, db, backupStmt, backupPath, start, end)
	waitForSuccessfulJob(t, tc, compactionID)

	var compRestoreID, classicRestoreID jobspb.JobID
	var unused any
	db.QueryRow(
		t, restoreQuery(t, "DATABASE data", collectionURI, noAOST, "WITH new_db_name = 'data1'"),
	).Scan(&compRestoreID, &unused, &unused, &unused)
	db.Exec(t, "SET CLUSTER SETTING restore.compacted_backups.enabled = false")
	db.QueryRow(
		t, restoreQuery(t, "DATABASE data", collectionURI, noAOST, "WITH new_db_name = 'data2'"),
	).Scan(&classicRestoreID, &unused, &unused, &unused)

	require.Equal(t, 2, getNumBackupsInRestore(t, db, compRestoreID))
	require.Equal(t, 3, getNumBackupsInRestore(t, db, classicRestoreID))
}

func TestCheckCompactionManifestFields(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// NOTE: Whenever a new field is added to the backup manifest, it will need to be
	// added to the corresponding lists below.
	inherited := []string{
		"EndTime",
		"Spans",
		"CompleteDbs",
		"NodeID",
		"ClusterID",
		"ClusterVersion",
		"FormatVersion",
		"BuildInfo",
		"DescriptorCoverage",
		"StatisticsFilenames",
		"ElidedPrefix",
		"LocalityKVs",
	}
	overridden := []string{
		"ID",
		"StartTime",
		"IntroducedSpans",
		"IsCompacted",
		"MVCCFilter",
	}
	emptied := []string{
		"Dir",
		"DescriptorChanges",
		"Files",
		"PartitionDescriptorFilenames",
		"EntryCounts",
	}
	// Ignored fields are fields that we do not check because either:
	// 1. They are no longer used
	// 2. There is a case-specific reason for not checking them
	// 3. Compaction does not support these fields and there are safeguards
	// against compaction running against such backups. The test is simpler if we
	// ignore these fields.
	ignored := []string{
		"TenantsDeprecated",
		"DeprecatedStatistics",
		// HasExternalManifestSSTs is a unique case because it will be set to true
		// for all manifests, except when creating a new manifest, it is always set
		// to false until the manifest has been split up and written.
		"HasExternalManifestSSTs",
		// Descriptors is ignored because an actual backup manifest loaded from
		// storage will have this field emptied due to using slim manifests whereas
		// createCompactedManifest will have filled this out.
		"Descriptors",
		"Tenants",
		"RevisionStartTime",
	}

	statisticsFilenames := make(map[descpb.ID]string)
	statisticsFilenames[1] = "foo"
	lastBackup := backuppb.BackupManifest{
		StartTime:         hlc.Timestamp{WallTime: 2},
		EndTime:           hlc.Timestamp{WallTime: 3},
		RevisionStartTime: hlc.Timestamp{WallTime: 2},
		Spans: []roachpb.Span{
			{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
			{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
			{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
		},
		IntroducedSpans: []roachpb.Span{
			{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
		},
		DescriptorChanges: []backuppb.BackupManifest_DescriptorRevision{
			{},
		},
		Files: []backuppb.BackupManifest_File{{}},
		Descriptors: []descpb.Descriptor{
			{
				Union: &descpb.Descriptor_Table{
					Table: &descpb.TableDescriptor{
						ID:   descpb.ID(1),
						Name: "table",
					},
				},
			},
		},
		CompleteDbs: []descpb.ID{1},
		EntryCounts: roachpb.RowCount{
			Rows: 1,
		},
		Dir: cloudpb.ExternalStorage{
			Provider: cloudpb.ExternalStorageProvider_external,
		},
		FormatVersion: 1,
		ClusterID:     uuid.MakeV4(),
		NodeID:        1,
		BuildInfo: build.Info{
			Tag: "v1.0.0",
		},
		ClusterVersion:               roachpb.Version{Major: 1},
		ID:                           uuid.MakeV4(),
		StatisticsFilenames:          statisticsFilenames,
		DescriptorCoverage:           tree.AllDescriptors,
		ElidedPrefix:                 execinfrapb.ElidePrefix_TenantAndTable,
		MVCCFilter:                   backuppb.MVCCFilter_All,
		IsCompacted:                  false,
		PartitionDescriptorFilenames: []string{"BACKUP_PART_1_tier=value"},
		LocalityKVs:                  []string{"tier=value"},
	}
	lastBackupStruct := structs.New(lastBackup)

	// Test that all fields in a backup manifest have been explicitly specified in
	// this test. This ensures that if the backup manifest fields are ever
	// updated, this test forces the developer to ensure the manifest creation for
	// compacted backups is correct.
	specifiedFields := make([]string, 0, len(ignored)+len(inherited)+len(overridden)+len(emptied))
	specifiedFields = append(specifiedFields, ignored...)
	specifiedFields = append(specifiedFields, inherited...)
	specifiedFields = append(specifiedFields, overridden...)
	specifiedFields = append(specifiedFields, emptied...)
	for _, field := range lastBackupStruct.Names() {
		if !slices.Contains(specifiedFields, field) {
			t.Fatalf("field %s not specified in test", field)
		}
	}

	// Ensuring that fields that are supposed to be inherited/emptied contain
	// non-zero values in the original backup manifest. This avoids false
	// positives such as the test reporting a field was properly emptied but only
	// because the original backup already had it empty.
	checkNotEmpty := make([]string, 0, len(inherited)+len(emptied))
	checkNotEmpty = append(checkNotEmpty, inherited...)
	checkNotEmpty = append(checkNotEmpty, emptied...)
	for _, field := range checkNotEmpty {
		require.Falsef(
			t, lastBackupStruct.Field(field).IsZero(),
			"field %s should not be empty to avoid false positives", field,
		)
	}

	compactChain := compactionChain{
		backupChain: []backuppb.BackupManifest{
			// Create some additional backups such that IntroducedSpans for the
			// compacted backup should be different from the last backup.
			{
				EndTime: hlc.Timestamp{WallTime: 1},
				Spans: []roachpb.Span{
					{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
				},
			},
			{
				StartTime: hlc.Timestamp{WallTime: 1},
				EndTime:   hlc.Timestamp{WallTime: 2},
				Spans: []roachpb.Span{
					{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
				},
				IntroducedSpans: []roachpb.Span{
					{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
				},
			},
			lastBackup,
		},
		start:    hlc.Timestamp{WallTime: 1},
		end:      hlc.Timestamp{WallTime: 3},
		startIdx: 1,
		endIdx:   3,
	}

	compactManifest, err := compactChain.createCompactionManifest(
		context.Background(), jobspb.BackupDetails{},
	)
	require.NoError(t, err)
	compactManifestStruct := structs.New(compactManifest)

	for _, field := range inherited {
		require.Equalf(
			t, lastBackupStruct.Field(field).Value(), compactManifestStruct.Field(field).Value(),
			"field %s should be inherited", field,
		)
	}
	for _, field := range overridden {
		require.NotEqualf(
			t, lastBackupStruct.Field(field).Value(), compactManifestStruct.Field(field).Value(),
			"field %s should be overridden", field,
		)
	}
	for _, field := range emptied {
		require.Truef(t, compactManifestStruct.Field(field).IsZero(), "field %s should be empty", field)
	}
}

// validateCompactedBackupForTables validates that a compacted backup
// with the specified start and end time (in nanoseconds) exists and restores
// from that backup. It checks that the tables specified have the same contents
// before and after the restore. It also checks that the restore process used
// the number of backups specified in numBackups.
// opts should be a string starting with "WITH "
func validateCompactedBackupForTables(
	t *testing.T,
	db *sqlutils.SQLRunner,
	collectionURI []string,
	tables []string,
	start, end hlc.Timestamp,
	restoreOpts string,
	showOpts string,
	numBackups int,
) {
	t.Helper()
	ensureBackupExists(t, db, collectionURI, start, end, showOpts)
	rows := make(map[string][][]string)
	for _, table := range tables {
		rows[table] = db.QueryStr(t, "SELECT * FROM "+table+" ORDER BY PRIMARY KEY "+table)
	}
	tablesList := strings.Join(tables, ", ")
	db.Exec(t, "DROP TABLE "+tablesList)
	row := db.QueryRow(t, restoreQuery(t, "TABLE "+tablesList, collectionURI, noAOST, restoreOpts))
	var restoreJobID jobspb.JobID
	var discard *any
	row.Scan(&restoreJobID, &discard, &discard, &discard)
	for table, originalRows := range rows {
		restoredRows := db.QueryStr(t, "SELECT * FROM "+table+" ORDER BY PRIMARY KEY "+table)
		require.Equal(t, originalRows, restoredRows, "table %s", table)
	}

	require.Equal(t, numBackups, getNumBackupsInRestore(t, db, restoreJobID))
}

// getNumBackupsInRestore returns the number of backups used in the restore
func getNumBackupsInRestore(t *testing.T, db *sqlutils.SQLRunner, jobID jobspb.JobID) int {
	t.Helper()
	var detailsStr string
	db.QueryRow(t, `SELECT crdb_internal.pb_to_json(
		'cockroach.sql.jobs.jobspb.Payload',
		value
	)::JSONB->>'restore'
	FROM system.job_info
	WHERE job_id = $1
	AND info_key = 'legacy_payload';
	`, jobID).Scan(&detailsStr)
	var details jobspb.RestoreDetails
	require.NoError(t, json.Unmarshal([]byte(detailsStr), &details))
	return len(details.URIs)
}

// ensureBackupExists ensures that a backup exists that spans the given start and end times.
// opts should be a string starting with "WITH "
func ensureBackupExists(
	t *testing.T,
	db *sqlutils.SQLRunner,
	collectionURI []string,
	start, end hlc.Timestamp,
	opts string,
) {
	t.Helper()
	// Convert times to millisecond epoch. We compare millisecond epoch instead of
	// nanosecond epoch because the backup time is stored in milliseconds, but timeutil.Now()
	// will return a nanosecond-precise epoch.
	rows := db.Query(
		t,
		fmt.Sprintf(`SELECT * FROM (
				SELECT DISTINCT
				COALESCE(start_time::DECIMAL * 1e6, 0) as start_time,
				COALESCE(end_time::DECIMAL * 1e6, 0) as end_time
				FROM [%s]
			) WHERE start_time = '%s'::DECIMAL / 1e3 AND end_time = '%s'::DECIMAL / 1e3
			`,
			showBackupQuery(collectionURI, opts), start.AsOfSystemTime(), end.AsOfSystemTime(),
		),
	)
	defer rows.Close()
	require.True(
		t, rows.Next(), "missing backup with start time %d and end time %d",
		start.AsOfSystemTime(), end.AsOfSystemTime(),
	)
}

func getTime() hlc.Timestamp {
	// Due to fun precision differences between timeutil.Now() on Unix vs MacOS
	// and the rounding of times to the nearest millisecond by `SHOW BACKUP`, it
	// is simpler to just ensure the precision to be milliseconds here and avoid
	// the precision issues entirely.
	return hlc.Timestamp{WallTime: timeutil.Now().UnixNano() / 1e3 * 1e3}
}

// Some named variables to improve test readability.
const fullCluster = ""

var noAOST = hlc.Timestamp{}
var noOpts = ""

// fullBackupQuery creates a `BACKUP INTO` statement.
// opts should be a string starting with "WITH "
func fullBackupQuery(
	targets string, collectionURI []string, aost hlc.Timestamp, opts string,
) string {
	uri := stringifyCollectionURI(collectionURI)
	return fmt.Sprintf(
		"BACKUP %s INTO (%s) %s %s",
		targets, uri, aostExpr(aost), opts,
	)
}

// incBackupQuery creates a `BACKUP INTO LATEST` statement against the latest
// full backup in the collection.
// opts should be a string starting with "WITH "
func incBackupQuery(
	targets string, collectionURI []string, aost hlc.Timestamp, opts string,
) string {
	uri := stringifyCollectionURI(collectionURI)
	return fmt.Sprintf(
		"BACKUP %s INTO LATEST IN (%s) %s %s",
		targets, uri, aostExpr(aost), opts,
	)
}

// triggerCompaction creates and executes a `crdb_internal.backup_compaction` statement
// and returns the job id of the backup compaction.
func triggerCompaction(
	t *testing.T,
	db *sqlutils.SQLRunner,
	backupStmt string,
	fullPath string,
	start, end hlc.Timestamp,
) jobspb.JobID {
	t.Helper()

	backupStmt = strings.ReplaceAll(backupStmt, "'", "''")
	backupStmt = fmt.Sprintf(
		`SELECT crdb_internal.backup_compaction(
			0, '%s', '%s', '%s'::DECIMAL, '%s'::DECIMAL
		)`,
		backupStmt, fullPath, start.AsOfSystemTime(), end.AsOfSystemTime(),
	)

	var jobID jobspb.JobID
	db.QueryRow(t, backupStmt).Scan(&jobID)
	return jobID
}

// restoreQuery creates a `RESTORE` statement that restores from the latest
// backup chain in the collection.
// opts should be a string starting with "WITH "
func restoreQuery(
	t *testing.T, targets string, collectionURI []string, aost hlc.Timestamp, opts string,
) string {
	t.Helper()
	uri := stringifyCollectionURI(collectionURI)
	return fmt.Sprintf(
		"RESTORE %s FROM LATEST IN (%s) %s %s",
		targets, uri, aostExpr(aost), opts,
	)
}

// showBackupQuery creates a `SHOW BACKUP` statement for the latest backup in
// the collection.
// opts should be a string starting with "WITH "
func showBackupQuery(collectionURI []string, opts string) string {
	uri := stringifyCollectionURI(collectionURI)
	return fmt.Sprintf(
		`SHOW BACKUP FROM LATEST IN (%s) %s`, uri, opts,
	)
}

// stringifyCollectionURI converts a comma separated list of collection URIs
// into properly quoted strings for use in SQL statements.
func stringifyCollectionURI(collectionURI []string) string {
	return strings.Join(
		util.Map(collectionURI, func(uri string) string {
			return fmt.Sprintf("'%s'", uri)
		},
		), ", ",
	)
}

func aostExpr(aost hlc.Timestamp) string {
	if aost.IsEmpty() {
		return ""
	}
	return fmt.Sprintf("AS OF SYSTEM TIME '%s'", aost.AsOfSystemTime())
}

func getDescUri(t *testing.T, db *sqlutils.SQLRunner, jobId jobspb.JobID) string {
	var desc string
	db.QueryRow(t, fmt.Sprintf("SELECT system.jobs.description from system.jobs WHERE system.jobs.id = %d", jobId)).Scan(&desc)
	inStart := strings.Index(desc, "IN ")
	uriStart := inStart + strings.Index(desc[inStart:], "'") + 1
	uriEnd := uriStart + strings.Index(desc[uriStart:], "'")
	return desc[uriStart:uriEnd]
}
func countBackups(t *testing.T, db *sqlutils.SQLRunner, uris []string) int {
	uri := stringifyCollectionURI(uris)
	var count int
	db.QueryRow(
		t, fmt.Sprintf(
			"SELECT count(DISTINCT (start_time, end_time)) FROM [SHOW BACKUP FROM LATEST IN (%s)]",
			uri,
		),
	).Scan(&count)
	return count
}
