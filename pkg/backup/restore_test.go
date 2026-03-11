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

	// TODO(at): refactor test to work with FastRestore
	// currently uses newTestHelper which bypasses our special test setup.
	backuptestutils.DisableFastRestoreForTest(t)

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

	// The restore.before_link pausepoint is only checked in the non-dist-flow
	// online restore path (sendAddRemoteSSTs).
	sqlDB.Exec(t, "SET CLUSTER SETTING backup.restore.online_use_dist_flow.enabled = false")
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

func TestRestoreWithGrants(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	_, db, _, cleanup := backuptestutils.StartBackupRestoreTestCluster(t, 1)
	defer cleanup()

	tests := []struct {
		name              string
		setupStmts        []string
		grantStmts        []string
		backupStmt        string
		dropBeforeRestore []string
		restoreStmt       string
		ownerChecks       []ownerCheck
		privilegeChecks   []privilegeCheck
		expectError       string // if non-empty, expect this error message
	}{
		// Database ownership tests
		{
			name: "owner-preserved-database",
			setupStmts: []string{
				"CREATE DATABASE testdb",
				"CREATE USER testowner",
			},
			grantStmts: []string{
				"ALTER DATABASE testdb OWNER TO testowner",
			},
			backupStmt:  "BACKUP DATABASE testdb INTO 'nodelocal://1/test-owner-db'",
			restoreStmt: "RESTORE DATABASE testdb FROM LATEST IN 'nodelocal://1/test-owner-db' WITH GRANTS, new_db_name = newtestdb",
			ownerChecks: []ownerCheck{
				{objectType: "database", objectName: "newtestdb", expectedOwner: "testowner"},
			},
		},
		{
			name: "owner-not-preserved",
			setupStmts: []string{
				"CREATE DATABASE testdb2",
				"CREATE USER testowner2",
			},
			grantStmts: []string{
				"ALTER DATABASE testdb2 OWNER TO testowner2",
			},
			backupStmt: "BACKUP DATABASE testdb2 INTO 'nodelocal://1/test-owner-dropped'",
			dropBeforeRestore: []string{
				"DROP DATABASE testdb2",
				"DROP USER testowner2",
			},
			restoreStmt: "RESTORE DATABASE testdb2 FROM LATEST IN 'nodelocal://1/test-owner-dropped' WITH GRANTS",
			ownerChecks: []ownerCheck{
				{objectType: "database", objectName: "testdb2", expectedOwner: "root"},
			},
		},

		// Grant option tests
		{
			name: "grant-option-on-select",
			setupStmts: []string{
				"CREATE DATABASE grantoptdb",
				"CREATE TABLE grantoptdb.t (id INT PRIMARY KEY)",
				"CREATE USER user1",
			},
			grantStmts: []string{
				"GRANT SELECT ON TABLE grantoptdb.t TO user1 WITH GRANT OPTION",
				"GRANT INSERT ON TABLE grantoptdb.t TO user1",
			},
			backupStmt:  "BACKUP DATABASE grantoptdb INTO 'nodelocal://1/test-grant-option'",
			restoreStmt: "RESTORE DATABASE grantoptdb FROM LATEST IN 'nodelocal://1/test-grant-option' WITH GRANTS, new_db_name = newgrantoptdb",
			privilegeChecks: []privilegeCheck{
				hasGrantablePriv("user1", "TABLE", "newgrantoptdb.t", "SELECT"),
				hasPriv("user1", "TABLE", "newgrantoptdb.t", "INSERT"),
			},
		},

		// Multi-privilege tests
		{
			name: "multiple-table-privileges",
			setupStmts: []string{
				"CREATE DATABASE multiprivdb",
				"CREATE TABLE multiprivdb.t (id INT PRIMARY KEY)",
				"CREATE USER user3",
			},
			grantStmts: []string{
				"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE multiprivdb.t TO user3",
			},
			backupStmt:      "BACKUP DATABASE multiprivdb INTO 'nodelocal://1/test-multi-priv'",
			restoreStmt:     "RESTORE TABLE multiprivdb.t FROM LATEST IN 'nodelocal://1/test-multi-priv' WITH GRANTS, into_db = testdb",
			privilegeChecks: hasPrivs("user3", "TABLE", "testdb.t", "SELECT", "INSERT", "UPDATE", "DELETE"),
		},

		// Object type privilege tests
		{
			name: "multi-desc-types",
			setupStmts: []string{
				"CREATE DATABASE descdb",
				"CREATE SCHEMA descdb.testschema",
				"CREATE TYPE descdb.myenum AS ENUM ('a', 'b', 'c')",
				"CREATE USER user5",
			},
			grantStmts: []string{
				"GRANT CREATE ON DATABASE descdb TO user5",
				"GRANT CREATE ON SCHEMA descdb.testschema TO user5",
				"GRANT USAGE ON TYPE descdb.myenum TO user5",
			},
			backupStmt:  "BACKUP DATABASE descdb INTO 'nodelocal://1/test-multi-desc'",
			restoreStmt: "RESTORE DATABASE descdb FROM LATEST IN 'nodelocal://1/test-multi-desc' WITH GRANTS, new_db_name = newdescdb",
			privilegeChecks: []privilegeCheck{
				hasPriv("user5", "DATABASE", "newdescdb", "CREATE"),
				hasPriv("user5", "SCHEMA", "newdescdb.testschema", "CREATE"),
				hasPriv("user5", "TYPE", "newdescdb.myenum", "USAGE"),
			},
		},

		// Multi-user scenarios
		{
			name: "multiple-users",
			setupStmts: []string{
				"CREATE DATABASE multiuserdb",
				"CREATE USER alice",
				"CREATE USER bob",
				"CREATE USER charlie",
			},
			grantStmts: []string{
				"GRANT CREATE ON DATABASE multiuserdb TO alice",
				"GRANT CONNECT ON DATABASE multiuserdb TO bob",
				"GRANT CREATE ON DATABASE multiuserdb TO charlie",
			},
			backupStmt: "BACKUP DATABASE multiuserdb INTO 'nodelocal://1/test-multi-user'",
			dropBeforeRestore: []string{
				"DROP DATABASE multiuserdb",
				"DROP USER bob",
			},
			restoreStmt: "RESTORE DATABASE multiuserdb FROM LATEST IN 'nodelocal://1/test-multi-user' WITH GRANTS",
			privilegeChecks: []privilegeCheck{
				hasPriv("alice", "DATABASE", "multiuserdb", "CREATE"),
				lacksPriv("bob", "DATABASE", "multiuserdb", "CONNECT"),
				hasPriv("charlie", "DATABASE", "multiuserdb", "CREATE"),
			},
		},

		// Edge cases and error conditions
		{
			name: "without-grants-option",
			setupStmts: []string{
				"CREATE DATABASE nograntdb",
				"CREATE USER user7",
			},
			grantStmts: []string{
				"GRANT CREATE ON DATABASE nograntdb TO user7",
			},
			backupStmt:  "BACKUP DATABASE nograntdb INTO 'nodelocal://1/test-no-grants'",
			restoreStmt: "RESTORE DATABASE nograntdb FROM LATEST IN 'nodelocal://1/test-no-grants' WITH new_db_name = newnograntdb",
			privilegeChecks: []privilegeCheck{
				lacksPriv("user7", "DATABASE", "newnograntdb", "CREATE"),
			},
		},
		{
			name: "cluster-restore-rejected",
			setupStmts: []string{
				"CREATE DATABASE clusterdb",
			},
			backupStmt:  "BACKUP INTO 'nodelocal://1/test-cluster'",
			restoreStmt: "RESTORE FROM LATEST IN 'nodelocal://1/test-cluster' WITH GRANTS",
			expectError: "only supported for database and table level restores",
		},
		{
			name: "system-users-restore-rejected",
			setupStmts: []string{
				"CREATE DATABASE systemusersdb",
			},
			backupStmt:  "BACKUP INTO 'nodelocal://1/test-system-users'",
			restoreStmt: "RESTORE SYSTEM USERS FROM LATEST IN 'nodelocal://1/test-system-users' WITH GRANTS",
			expectError: "does not support the WITH GRANTS option",
		},

		// Table-level operations
		{
			name: "table-level-backup",
			setupStmts: []string{
				"CREATE DATABASE tablebackupdb",
				"CREATE TABLE tablebackupdb.t1 (id INT PRIMARY KEY)",
				"CREATE USER user8",
			},
			grantStmts: []string{
				"GRANT SELECT, INSERT ON TABLE tablebackupdb.t1 TO user8",
			},
			backupStmt: "BACKUP TABLE tablebackupdb.t1 INTO 'nodelocal://1/test-table-backup'",
			dropBeforeRestore: []string{
				"DROP TABLE tablebackupdb.t1",
			},
			restoreStmt:     "RESTORE TABLE tablebackupdb.t1 FROM LATEST IN 'nodelocal://1/test-table-backup' WITH GRANTS",
			privilegeChecks: hasPrivs("user8", "TABLE", "tablebackupdb.t1", "SELECT", "INSERT"),
		},

		// Complex scenarios
		{
			name: "nested-objects",
			setupStmts: []string{
				"CREATE DATABASE nesteddb",
				"CREATE SCHEMA nesteddb.myschema",
				"CREATE TABLE nesteddb.myschema.t1 (id INT PRIMARY KEY)",
				"CREATE USER user9",
			},
			grantStmts: []string{
				"GRANT CREATE ON DATABASE nesteddb TO user9",
				"GRANT USAGE ON SCHEMA nesteddb.myschema TO user9",
				"GRANT SELECT ON TABLE nesteddb.myschema.t1 TO user9",
			},
			backupStmt:  "BACKUP DATABASE nesteddb INTO 'nodelocal://1/test-nested'",
			restoreStmt: "RESTORE DATABASE nesteddb FROM LATEST IN 'nodelocal://1/test-nested' WITH GRANTS, new_db_name = newnesteddb",
			privilegeChecks: []privilegeCheck{
				hasPriv("user9", "DATABASE", "newnesteddb", "CREATE"),
				hasPriv("user9", "SCHEMA", "newnesteddb.myschema", "USAGE"),
				hasPriv("user9", "TABLE", "newnesteddb.myschema.t1", "SELECT"),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Setup.
			for _, stmt := range tc.setupStmts {
				db.Exec(t, stmt)
			}
			for _, stmt := range tc.grantStmts {
				db.Exec(t, stmt)
			}

			// Backup.
			db.Exec(t, tc.backupStmt)

			// Drop before restore.
			for _, stmt := range tc.dropBeforeRestore {
				db.Exec(t, stmt)
			}

			// Restore.
			if tc.expectError != "" {
				db.ExpectErr(t, tc.expectError, tc.restoreStmt)
				return
			}
			db.Exec(t, tc.restoreStmt)

			// Verify ownership.
			for _, check := range tc.ownerChecks {
				verifyOwner(t, db, check)
			}

			// Verify privileges.
			for _, check := range tc.privilegeChecks {
				verifyPrivilege(t, db, check)
			}
		})
	}
}

// hasPriv creates a privilege check expecting a non-grantable privilege to be present.
func hasPriv(grantee, objectType, objectName, privilege string) privilegeCheck {
	return privilegeCheck{
		grantee:         grantee,
		objectType:      objectType,
		objectName:      objectName,
		privilege:       privilege,
		expectPresent:   true,
		expectGrantable: false,
	}
}

func hasPrivs(grantee, objectType, objectName string, privileges ...string) []privilegeCheck {
	checks := make([]privilegeCheck, len(privileges))
	for i, priv := range privileges {
		checks[i] = hasPriv(grantee, objectType, objectName, priv)
	}
	return checks
}

// hasGrantablePriv creates a privilege check expecting a grantable privilege to be present.
func hasGrantablePriv(grantee, objectType, objectName, privilege string) privilegeCheck {
	return privilegeCheck{
		grantee:         grantee,
		objectType:      objectType,
		objectName:      objectName,
		privilege:       privilege,
		expectPresent:   true,
		expectGrantable: true,
	}
}

// lacksPriv creates a privilege check expecting a privilege to be absent.
func lacksPriv(grantee, objectType, objectName, privilege string) privilegeCheck {
	return privilegeCheck{
		grantee:       grantee,
		objectType:    objectType,
		objectName:    objectName,
		privilege:     privilege,
		expectPresent: false,
	}
}

// ownerCheck specifies an ownership verification.
type ownerCheck struct {
	objectType    string // "database", "table", "schema", "type"
	objectName    string
	expectedOwner string
}

// privilegeCheck specifies a privilege verification.
type privilegeCheck struct {
	grantee         string
	objectType      string // "DATABASE", "TABLE", "SCHEMA", "TYPE"
	objectName      string
	privilege       string // "SELECT", "INSERT", "CREATE", etc.
	expectPresent   bool
	expectGrantable bool // only checked if expectPresent is true
}

// verifyOwner checks that an object has the expected owner.
func verifyOwner(t *testing.T, db *sqlutils.SQLRunner, check ownerCheck) {
	t.Helper()
	var query string
	switch check.objectType {
	case "database":
		query = fmt.Sprintf("SELECT owner FROM [SHOW DATABASES] WHERE database_name = '%s'", check.objectName)
	case "table":
		// objectName format is "database.table" or "database.schema.table"
		query = fmt.Sprintf("SELECT owner FROM [SHOW TABLES] WHERE table_name = '%s'", check.objectName)
	case "schema":
		// objectName format is "database.schema"
		query = fmt.Sprintf("SELECT owner FROM [SHOW SCHEMAS] WHERE schema_name = '%s'", check.objectName)
	default:
		t.Fatalf("unknown object type: %s", check.objectType)
	}

	var owner string
	db.QueryRow(t, query).Scan(&owner)
	require.Equal(t, check.expectedOwner, owner, "owner mismatch for %s %s", check.objectType, check.objectName)
}

// verifyPrivilege checks that a privilege is present or absent as expected.
func verifyPrivilege(t *testing.T, db *sqlutils.SQLRunner, check privilegeCheck) {
	t.Helper()
	query := fmt.Sprintf(
		"SELECT is_grantable FROM [SHOW GRANTS ON %s %s] WHERE grantee = '%s' AND privilege_type = '%s'",
		check.objectType, check.objectName, check.grantee, check.privilege)

	rows := db.Query(t, query)
	defer rows.Close()

	require.Equal(t, check.expectPresent, rows.Next())

	if check.expectPresent {
		var isGrantable bool
		require.NoError(t, rows.Scan(&isGrantable))
		require.Equal(t, check.expectGrantable, isGrantable)
	}
}
