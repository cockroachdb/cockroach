// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	gosql "database/sql"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
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

// This test verifies that restore cleanup does not fail due to dropped
// temporary system tables as described in #148088.
func TestFailAfterCleanupSystemTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterSize := 1
	_, sqlDB, _, cleanupFn := backuptestutils.StartBackupRestoreTestCluster(t, clusterSize)
	defer cleanupFn()

	// Must set cluster setting before backup to ensure the setting is preserved.
	sqlDB.Exec(
		t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'restore.after_cleanup_temp_system_tables'",
	)
	sqlDB.Exec(t, "BACKUP INTO 'nodelocal://1/backup'")

	var jobID jobspb.JobID
	sqlDB.QueryRow(t, "RESTORE FROM LATEST IN 'nodelocal://1/backup' WITH detached").Scan(&jobID)
	sqlDB.Exec(t, "USE system")
	jobutils.WaitForJobToPause(t, sqlDB, jobID)

	sqlDB.Exec(t, "CANCEL JOB $1", jobID)
	jobutils.WaitForJobToCancel(t, sqlDB, jobID)
}

func TestRestoreRetryFastFails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "test is flaky without extending duration of retry policy, takes too long")
	skip.UnderDeadlock(t, "test is flaky under deadlock detector, takes too long")

	// Max duration needs to be long enough such that the restore job
	// runtime does not exceed the max duration of the retry policy, or
	// else very few attempts will be made.
	maxDuration := 500 * time.Millisecond
	if skip.DevStress() {
		// Under stress, the restore will take longer to complete, so we need to
		// increase max duration accordingly.
		maxDuration = 1500 * time.Millisecond
	}
	const numAccounts = 10

	// This will run a restore job and fail it repeatedly at the start of the
	// restore until we've exhausted the maximum attempts before fast failing. It
	// will allow progress to be made, and then start failing the restore at the
	// end of the job to until it either fast fails or exhausts the retry policy.
	// It tracks the number of attempts made by the restore job and returns it.
	runRestoreAndTrackAttempts := func(
		t *testing.T, progThreshold float32, endStatus jobs.State,
	) int {
		mu := struct {
			syncutil.Mutex
			attemptCount int
		}{}
		// waitForProgress is a channel that will be closed whenever we detect that
		// the restore job has made progress.
		waitForProgress := make(chan struct{})

		testKnobs := &sql.BackupRestoreTestingKnobs{
			RestoreDistSQLRetryPolicy: &retry.Options{
				InitialBackoff: time.Microsecond,
				Multiplier:     2,
				MaxBackoff:     100 * time.Millisecond,
				MaxDuration:    maxDuration,
			},
			RestoreRetryProgressThreshold: progThreshold,
			RunBeforeRestoreFlow: func() error {
				mu.Lock()
				defer mu.Unlock()
				mu.attemptCount++

				if mu.attemptCount <= maxRestoreRetryFastFail {
					// Have not consumed all retries before a fast fail.
					return syscall.ECONNRESET
				}

				return nil
			},
			RunAfterRestoreFlow: func() error {
				// Wait for progress to persist, then continue sending retryable errors
				<-waitForProgress
				return syscall.ECONNRESET
			},
		}
		var params base.TestClusterArgs
		params.ServerArgs.Knobs.BackupRestore = testKnobs

		_, sqlDB, _, cleanupFn := backuptestutils.StartBackupRestoreTestCluster(
			t, singleNode, backuptestutils.WithParams(params), backuptestutils.WithBank(numAccounts),
		)
		defer cleanupFn()

		sqlDB.Exec(t, "BACKUP DATABASE data INTO 'nodelocal://1/backup'")
		var restoreJobID jobspb.JobID
		sqlDB.QueryRow(
			t,
			`RESTORE DATABASE data FROM LATEST IN 'nodelocal://1/backup'
			WITH detached, new_db_name = 'restored_data'`,
		).Scan(&restoreJobID)

		testutils.SucceedsSoon(t, func() error {
			jobProgress := jobutils.GetJobProgress(t, sqlDB, restoreJobID)
			if len(jobProgress.GetRestore().Checkpoint) == 0 {
				return errors.Newf("frontier has not advanced yet")
			}
			return nil
		})
		close(waitForProgress)
		jobutils.WaitForJobToHaveStatus(t, sqlDB, restoreJobID, endStatus)
		mu.Lock()
		defer mu.Unlock()
		return mu.attemptCount
	}

	// This is the total number of attempts that should occur assuming we fast
	// fail, accounting for the fact that the above flow will reset the retry loop
	// once time.
	var expFastFailAttempts = maxRestoreRetryFastFail*2 + 2

	t.Run("retry policy times out when enough progress is made", func(t *testing.T) {
		attempts := runRestoreAndTrackAttempts(t, 0 /* progThreshold */, jobs.StatePaused)
		// If progress is made, then the restore job should make more attempts than
		// the fast fail path.
		require.Greater(t, attempts, expFastFailAttempts)
	})

	t.Run("retry policy fast fails if insufficient progress is made", func(t *testing.T) {
		// Set an impossibly high threshold so that the restore job never
		// sufficiently makes enough progress to avoid fast failing.
		attempts := runRestoreAndTrackAttempts(t, 1.5 /* progThreshold */, jobs.StateFailed)
		// Since we do allow progress to be made, we expect the restore job to reset
		// the retry loop and then fast fail.
		require.Equal(t, expFastFailAttempts, attempts)
	})
}

func TestRestoreJobMessages(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	mu := struct {
		syncutil.Mutex
		retryCount int
	}{}
	testKnobs := &sql.BackupRestoreTestingKnobs{
		RestoreDistSQLRetryPolicy: &retry.Options{
			InitialBackoff: time.Microsecond,
			Multiplier:     1,
			MaxBackoff:     time.Microsecond,
			// We want enough messages to be logged so that we can verify the count,
			// so we set MaxDuration long enough so that it doesn't get inadvertently
			// triggered.
			MaxDuration: 5 * time.Minute,
		},
		RunAfterRestoreFlow: func() error {
			mu.Lock()
			defer mu.Unlock()
			mu.retryCount++
			return syscall.ECONNRESET
		},
	}
	var params base.TestClusterArgs
	params.ServerArgs.Knobs.BackupRestore = testKnobs

	const numAccounts = 2
	_, sqlDB, _, cleanupFn := backuptestutils.StartBackupRestoreTestCluster(
		t, singleNode, backuptestutils.WithParams(params), backuptestutils.WithBank(numAccounts),
	)
	defer cleanupFn()

	sqlDB.Exec(t, "SET CLUSTER SETTING restore.retry_log_rate = '1000ms'")
	sqlDB.Exec(t, "BACKUP DATABASE data INTO 'nodelocal://1/backup'")

	var restoreJobID jobspb.JobID
	sqlDB.QueryRow(
		t, `RESTORE DATABASE data FROM LATEST IN 'nodelocal://1/backup'
					WITH detached, new_db_name = 'restored_data'`,
	).Scan(&restoreJobID)

	// We need to cancel the restore job or else it will block the test from
	// completing on Engflow.
	defer sqlDB.QueryRow(t, "CANCEL JOB $1", restoreJobID)

	testutils.SucceedsSoon(t, func() error {
		var numErrMessages int
		sqlDB.QueryRow(
			t, `SELECT count(*) FROM system.job_message WHERE job_id = $1 AND kind = $2`,
			restoreJobID, "error",
		).Scan(&numErrMessages)
		if numErrMessages < 2 {
			return errors.Newf("waiting for at least 2 retries to be logged")
		}
		mu.Lock()
		defer mu.Unlock()
		// Since we throttle the frequency of error messages, we expect there to be
		// more retries than the number of error messages logged.
		require.Greater(t, mu.retryCount, numErrMessages)
		return nil
	})
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

func TestRestoreRetryRevert(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	droppedDescs := make(chan struct{})
	jobPaused := make(chan struct{})
	testKnobs := &sql.BackupRestoreTestingKnobs{
		AfterRevertRestoreDropDescriptors: func() error {
			close(droppedDescs)
			<-jobPaused
			return nil
		},
	}
	var params base.TestClusterArgs
	params.ServerArgs.Knobs.BackupRestore = testKnobs

	_, sqlDB, _, cleanupFn := backuptestutils.StartBackupRestoreTestCluster(
		t, singleNode, backuptestutils.WithParams(params),
	)
	defer cleanupFn()

	// We create a variety of descriptors to ensure that missing descriptors
	// during drop do not break revert.
	sqlDB.Exec(t, "CREATE DATABASE foo")
	sqlDB.Exec(t, "USE foo")
	sqlDB.Exec(t, "CREATE OR REPLACE FUNCTION bar(a INT) RETURNS INT AS 'SELECT a*a' LANGUAGE SQL;")
	sqlDB.Exec(t, "CREATE TYPE baz AS ENUM ('a', 'b', 'c')")
	sqlDB.Exec(t, "CREATE TABLE qux (x INT)")
	sqlDB.Exec(t, "BACKUP DATABASE foo INTO 'nodelocal://1/backup'")

	// We need restore to publish descriptors so that they will be cleaned up
	// during restore.
	sqlDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'restore.after_publishing_descriptors'")

	var restoreID jobspb.JobID
	sqlDB.QueryRow(
		t, "RESTORE DATABASE foo FROM LATEST IN 'nodelocal://1/backup' WITH detached, new_db_name='foo_restored'",
	).Scan(&restoreID)

	jobutils.WaitForJobToPause(t, sqlDB, restoreID)

	sqlDB.Exec(t, "CANCEL JOB $1", restoreID)
	<-droppedDescs
	sqlDB.Exec(t, "PAUSE JOB $1", restoreID)
	jobutils.WaitForJobToPause(t, sqlDB, restoreID)
	close(jobPaused)
	testKnobs.AfterRevertRestoreDropDescriptors = nil

	sqlDB.Exec(t, "RESUME JOB $1", restoreID)
	jobutils.WaitForJobToCancel(t, sqlDB, restoreID)
}

func TestRestoreRevisionHistoryWithCompactions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.setOverrideAsOfClauseKnob(t)
	th.env.SetTime(time.Date(2025, 12, 11, 1, 0, 0, 0, time.UTC))

	th.sqlDB.Exec(t, "SET CLUSTER SETTING backup.compaction.threshold = 4")
	th.sqlDB.Exec(t, "SET CLUSTER SETTING backup.compaction.window_size = 3")

	// This test will take a backup of a table with revision history enabled. To
	// check the correctness of a restore, we check the amount of rows in the
	// table at different restore points. We will assume that at each point in
	// time t, the number of rows in the table is t. We take backups at the
	// following times:
	//
	// t=0: full backup is taken
	// t=2: inc backup is taken
	// t=4: inc backup is taken
	// t=6: inc backup is taken, compaction occurs here, merging t=[2, 6]
	// t=8: inc backup is taken
	th.sqlDB.Exec(t, "CREATE TABLE t (x INT)")
	th.sqlDB.Exec(t, "CREATE DATABASE restored")

	collURI := "nodelocal://1/backup"
	schedules, err := th.createBackupSchedule(
		t,
		"CREATE SCHEDULE FOR BACKUP TABLE t INTO $1 WITH revision_history RECURRING '@hourly'", collURI,
	)
	require.NoError(t, err)
	require.Equal(t, 2, len(schedules))

	full, inc := schedules[0], schedules[1]
	if full.IsPaused() {
		full, inc = inc, full
	}

	getLastBackupTime := func(t *testing.T) string {
		t.Helper()
		var lastBackupTime string
		th.sqlDB.QueryRow(
			t,
			"SELECT end_time FROM [SHOW BACKUP FROM LATEST IN $1] ORDER BY end_time DESC LIMIT 1",
			collURI,
		).Scan(&lastBackupTime)
		require.NoError(t, err)
		return lastBackupTime
	}

	// Create backups as according to the aforementioned schedule.
	times := make([]string, 9)
	th.env.SetTime(full.NextRun().Add(time.Second))
	require.NoError(t, th.executeSchedules())
	th.waitForSuccessfulScheduledJob(t, full.ScheduleID())
	times[0] = getLastBackupTime(t)
	for i := 1; i < 9; i++ {
		th.sqlDB.Exec(t, "INSERT INTO t VALUES ($1)", i)
		if i%2 == 1 {
			var now string
			th.sqlDB.QueryRow(t, "SELECT now()").Scan(&now)
			times[i] = now
		} else {
			inc, err = jobs.ScheduledJobDB(th.internalDB()).
				Load(context.Background(), th.env, inc.ScheduleID())
			require.NoError(t, err)
			th.env.SetTime(inc.NextRun().Add(time.Second))
			require.NoError(t, th.executeSchedules())
			th.waitForSuccessfulScheduledJobCount(t, inc.ScheduleID(), i/2)
			times[i] = getLastBackupTime(t)
		}
	}
	// Wait for the compaction job to complete.
	var compactionJobID jobspb.JobID
	th.sqlDB.QueryRow(
		t,
		"SELECT job_id FROM [SHOW JOBS] WHERE description ILIKE 'COMPACT%' AND job_type = 'BACKUP'",
	).Scan(&compactionJobID)
	jobutils.WaitForJobToSucceed(t, th.sqlDB, compactionJobID)

	countRestoredRows := func(t *testing.T) int {
		t.Helper()
		var rowCount int
		th.sqlDB.QueryRow(t, "SELECT count(*) FROM restored.t").Scan(&rowCount)
		return rowCount
	}

	t.Run("restore to time before compaction", func(t *testing.T) {
		defer th.sqlDB.Exec(t, "DROP TABLE IF EXISTS restored.t")

		for i := 1; i < 6; i++ {
			th.sqlDB.Exec(
				t,
				fmt.Sprintf(
					"RESTORE TABLE t FROM LATEST IN $1 AS OF SYSTEM TIME '%s' WITH into_db='restored'", times[i],
				),
				collURI,
			)

			restoreType := "regular"
			if i%2 == 1 {
				restoreType = "revision-history"
			}
			require.Equal(
				t, i, countRestoredRows(t),
				"%s restore to time %d resulted in unexpected row count", restoreType, i,
			)
			th.sqlDB.Exec(t, "DROP TABLE restored.t")
		}
	})

	t.Run("restore to exact compaction time", func(t *testing.T) {
		defer th.sqlDB.Exec(t, "DROP TABLE IF EXISTS restored.t")

		var restoreJobID jobspb.JobID
		var unused any
		th.sqlDB.QueryRow(
			t,
			fmt.Sprintf(
				"RESTORE TABLE t FROM LATEST IN $1 AS OF SYSTEM TIME '%s' WITH into_db='restored'", times[6],
			),
			collURI,
		).Scan(&restoreJobID, &unused, &unused, &unused)
		require.NoError(t, err)

		require.Equal(t, 6, countRestoredRows(t))
		require.Equal(t, 2, getNumBackupsInRestore(t, th.sqlDB, restoreJobID))
	})

	t.Run("restore to time after compaction", func(t *testing.T) {
		defer th.sqlDB.Exec(t, "DROP TABLE IF EXISTS restored.t")

		for i := 7; i < 9; i++ {
			var restoreJobID jobspb.JobID
			var unused any
			th.sqlDB.QueryRow(
				t,
				fmt.Sprintf(
					"RESTORE TABLE t FROM LATEST IN $1 AS OF SYSTEM TIME '%s' WITH into_db='restored'", times[i],
				),
				collURI,
			).Scan(&restoreJobID, &unused, &unused, &unused)
			require.NoError(t, err)

			restoreType := "regular"
			if i%2 == 1 {
				restoreType = "revision-history"
			}

			require.Equal(
				t, i, countRestoredRows(t),
				"%s restore to time %d resulted in unexpected row count", restoreType, i,
			)

			// Both regular and revision-history restores to a point after compaction
			// should use the compacted backup in its chain.
			require.Equal(
				t, 3, getNumBackupsInRestore(t, th.sqlDB, restoreJobID),
				"unexpected number of backups used when %s restoring to t=%d", restoreType, i,
			)
			th.sqlDB.Exec(t, "DROP TABLE restored.t")
		}
	})
}

func TestRestorePausepointSkipsRetries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tmpDir := t.TempDir()
	defer nodelocal.ReplaceNodeLocalForTesting(tmpDir)()

	mu := struct {
		syncutil.Mutex
		attemptCount int
	}{}
	testKnobs := &sql.BackupRestoreTestingKnobs{
		RunBeforeRestoreFlow: func() error {
			mu.Lock()
			defer mu.Unlock()
			mu.attemptCount++
			return nil
		},
	}
	var params base.TestClusterArgs
	params.ServerArgs.Knobs.BackupRestore = testKnobs

	const numAccounts = 10
	_, sqlDB, _, cleanupFn := backuptestutils.StartBackupRestoreTestCluster(
		t, singleNode, backuptestutils.WithParams(params), backuptestutils.WithBank(numAccounts),
	)
	defer cleanupFn()

	sqlDB.Exec(t, "SET CLUSTER SETTING jobs.debug.pausepoints = 'restore.before_link'")
	sqlDB.Exec(t, "BACKUP DATABASE data INTO 'nodelocal://1/backup'")

	sqlDB.ExpectErr(
		t,
		"pause point",
		`RESTORE DATABASE data FROM LATEST IN 'nodelocal://1/backup'
		WITH experimental deferred copy, new_db_name = 'restored'`,
	)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(
		t, 1, mu.attemptCount, "expected only 1 restore attempt since pausepoint should skip retries",
	)
}
