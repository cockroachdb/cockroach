// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/base"
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
	maxDuration := 100 * time.Millisecond
	if skip.Stress() {
		// Under stress, the restore will take longer to complete, so we need to
		// increase max duration accordingly.
		maxDuration = time.Second
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
	// allowSuccess is a channel that will be closed when we want to allow the
	// restore job to succeed.
	allowSuccess := make(chan struct{})

	testKnobs := &sql.BackupRestoreTestingKnobs{
		RestoreDistSQLRetryPolicy: &retry.Options{
			InitialBackoff: time.Microsecond,
			Multiplier:     1,
			MaxBackoff:     time.Microsecond,
			// We will be allowing the restore job to succeed after a few job messages
			// are logged, so we just need MaxDuration to be long enough that it won't
			// be hit.
			MaxDuration: 5 * time.Minute,
		},
		RunBeforeRestoreFlow: func() error {
			mu.Lock()
			defer mu.Unlock()

			if mu.retryCount < maxRestoreRetryFastFail {
				// Have not consumed all retries before a fast fail.
				mu.retryCount++
				return syscall.ECONNRESET
			}

			return nil
		},
		RunAfterRestoreFlow: func() error {
			mu.Lock()
			defer mu.Unlock()

			// We need enough attempts ensure that the restore job will set the
			// running status of the restore job.
			if mu.retryCount < declareStuckAttemptThreshold+maxRestoreRetryFastFail {
				mu.retryCount++
				return syscall.ECONNRESET
			}

			select {
			case <-allowSuccess:
				return nil
			default:
				mu.retryCount++
				return syscall.ECONNRESET
			}
		},
	}
	var params base.TestClusterArgs
	params.ServerArgs.Knobs.BackupRestore = testKnobs

	const numAccounts = 10
	_, sqlDB, _, cleanupFn := backuptestutils.StartBackupRestoreTestCluster(
		t, singleNode, backuptestutils.WithParams(params), backuptestutils.WithBank(numAccounts),
	)
	defer cleanupFn()

	sqlDB.Exec(t, "SET CLUSTER SETTING restore.retry_log_rate = '100ms'")
	sqlDB.Exec(t, "BACKUP DATABASE data INTO 'nodelocal://1/backup'")
	var restoreJobID jobspb.JobID
	sqlDB.QueryRow(
		t,
		`RESTORE DATABASE data FROM LATEST IN 'nodelocal://1/backup'
		WITH detached, new_db_name = 'restored_data'`,
	).Scan(&restoreJobID)

	// Wait for the restore job to update its running status to indicate it is
	// retrying, and then allow it to fail for a bit longer so that we can collect
	// job messages.
	testutils.SucceedsSoon(t, func() error {
		status := jobutils.GetJobStatusMessage(t, sqlDB, restoreJobID)
		if status != "retrying due to recurring errors" {
			return errors.New("no status message found")
		}
		return nil
	})
	time.AfterFunc(500*time.Millisecond, func() {
		close(allowSuccess)
	})
	jobutils.WaitForJobToHaveStatus(t, sqlDB, restoreJobID, jobs.StateSucceeded)

	var numErrMessages int
	sqlDB.QueryRow(
		t, `SELECT count(*) FROM system.job_message WHERE job_id = $1 AND kind = $2`,
		restoreJobID, "error",
	).Scan(&numErrMessages)
	t.Logf("number of error messages logged: %d", numErrMessages)
	require.Greater(t, numErrMessages, 1)
	// Depending on if the test is run under stress or not, we may log more
	// messages, so we just check that we log fewer than 40 messages. If there
	// were no throttling, there would be significantly more messages logged, so
	// this is sufficient.
	require.Less(t, numErrMessages, 40)

	finalStatusMsg := jobutils.GetJobStatusMessage(t, sqlDB, restoreJobID)
	require.Empty(t, finalStatusMsg, "status message should be cleared on job completion")
}
