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

func TestDynamicRestoreRetryPolicySwitching(t *testing.T) {
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
		maxDuration = 500 * time.Millisecond
	}
	maxRetries := 3
	const numNodes = 1
	const numAccounts = 10

	// This will run a restore job and fail it repeatedly at the start of the
	// restore until the initial retry policy is exhausted. After that, it will
	// allow progress to be made, and then start failing the restore at the end of
	// the job to exhaust the retry policy again (regardless of whether it
	// switched or not). It tracks the number of attempts made by the restore job
	// and returns it.
	runRestoreAndTrackAttempts := func(
		t *testing.T, policySwitchThreshold float32, endStatus jobs.State,
	) int {
		mu := struct {
			syncutil.Mutex
			attemptCount int
		}{}
		// waitForProgress is a channel that will be closed whenever we detect that
		// the restore job has made progress.
		waitForProgress := make(chan struct{})

		testKnobs := &sql.BackupRestoreTestingKnobs{
			InitialRestoreDistSQLRetryPolicy: &retry.Options{
				InitialBackoff: time.Microsecond,
				Multiplier:     2,
				MaxBackoff:     2 * time.Microsecond,
				MaxRetries:     maxRetries,
			},
			SecondaryRestoreDistSQLRetryPolicy: &retry.Options{
				InitialBackoff: 2 * time.Microsecond,
				Multiplier:     2,
				MaxBackoff:     100 * time.Millisecond,
				MaxDuration:    maxDuration,
			},
			RestoreRetryPolicySwitchThreshold: policySwitchThreshold,
			RunBeforeRestoreFlow: func() error {
				mu.Lock()
				defer mu.Unlock()
				mu.attemptCount++

				if mu.attemptCount <= maxRetries {
					// Have not consumed all retries on initial policy.
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
			t, numNodes, backuptestutils.WithParams(params), backuptestutils.WithBank(numAccounts),
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

	// If we were to stay with the initial retry policy the entire time, this
	// would be the expected number of attempts made by the restore job.
	var expTotalAttemptsFirstPolicy = maxRetries*2 + 2

	t.Run("retry policy switches when enough progress is made", func(t *testing.T) {
		attempts := runRestoreAndTrackAttempts(t, 0.01 /* policySwitchThreshold */, jobs.StatePaused)
		// If we successfully switched retry policies, the job should have performed
		// more attempts than	permitted by the initial retry policy. We multiply by 2
		// to ensure that this isn't caused by the reset of the initial retry policy.
		require.Greater(t, attempts, expTotalAttemptsFirstPolicy)
	})

	t.Run("retry policy does not switch if insufficient progress is made", func(t *testing.T) {
		// Set an impossibly high threshold so that the restore job never
		// sufficiently makes enough progress to switch retry policies.
		attempts := runRestoreAndTrackAttempts(t, 1.5 /* policySwitchThreshold */, jobs.StateFailed)
		// Since we do allow progress to be made, we expect the restore job to reset
		// the retry loop, but retry policy will stay the same.
		require.Equal(t, expTotalAttemptsFirstPolicy, attempts)
	})
}
