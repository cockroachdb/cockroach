// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestRestoreCheckpointing checks that all completed spans are
// skipped over when creating the slice for makeSimpleImportSpans.
func TestRestoreCheckpointing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer jobs.TestingSetProgressThresholds()()

	var allowResponse chan struct{}
	params := base.TestClusterArgs{}
	knobs := base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{
			BackupRestoreTestingKnobs: &sql.BackupRestoreTestingKnobs{
				RunAfterProcessingRestoreSpanEntry: func(_ context.Context) {
					<-allowResponse
				},
			},
		},
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
	}
	testServerArgs := base.TestServerArgs{DisableDefaultTestTenant: true}
	params.ServerArgs = testServerArgs
	params.ServerArgs.Knobs = knobs

	ctx := context.Background()
	_, sqlDB, dir, cleanupFn := backupRestoreTestSetupWithParams(t, multiNode, 1,
		InitManualReplication, params)
	conn := sqlDB.DB.(*gosql.DB)
	defer cleanupFn()

	sqlDB.Exec(t, `CREATE DATABASE r1`)
	for char := 'a'; char <= 'g'; char++ {
		tableName := "r1." + string(char)
		sqlDB.Exec(t, fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY, s STRING)`, tableName))
		sqlDB.Exec(t, fmt.Sprintf(`INSERT INTO %s VALUES (1, 'x'),(2,'y')`, tableName))
	}
	sqlDB.Exec(t, `BACKUP DATABASE r1 TO 'nodelocal://0/test-root'`)

	restoreQuery := `RESTORE DATABASE r1 FROM 'nodelocal://0/test-root' WITH detached, new_db_name=r2`

	backupTableID := sqlutils.QueryTableID(t, conn, "r1", "public", "a")

	var jobID jobspb.JobID
	// The do function on a restore stops more progress from being persisted to the job record
	// after some progress is made.
	do := func(query string, check inProgressChecker) {
		t.Logf("checking query %q", query)

		var totalExpectedResponses int
		if strings.Contains(query, "RESTORE") {
			// We expect restore to process each file in the backup individually.
			// SST files are written per-range in the backup. So we expect the
			// restore to process #(ranges) that made up the original table.
			totalExpectedResponses = 7
		} else {
			t.Fatal("expected query to be either a backup or restore")
		}
		jobDone := make(chan error)
		allowResponse = make(chan struct{}, totalExpectedResponses)

		go func() {
			_, err := conn.Exec(query)
			jobDone <- err
		}()

		// Allow one of the total expected responses to proceed.
		for i := 0; i < 1; i++ {
			allowResponse <- struct{}{}
		}

		err := retry.ForDuration(testutils.DefaultSucceedsSoonDuration, func() error {
			return check(ctx, inProgressState{
				DB:            conn,
				backupTableID: backupTableID,
				dir:           dir,
				name:          "foo",
			})
		})

		// Close the channel to allow all remaining responses to proceed. We do this
		// even if the above retry.ForDuration failed, otherwise the test will hang
		// forever.
		close(allowResponse)

		if err := <-jobDone; err != nil {
			t.Fatalf("%q: %+v", query, err)
		}

		if err != nil {
			t.Log(err)
		}
	}

	progressQuery := `select crdb_internal.pb_to_json('cockroach.sql.jobs.jobspb.Progress', progress) as progress from system.jobs where id=$1`

	var progressMessage string

	checkFraction := func(ctx context.Context, ip inProgressState) error {
		latestJobID, err := ip.latestJobID()
		if err != nil {
			return err
		}
		var fractionCompleted float32
		if err := ip.QueryRow(
			`SELECT fraction_completed FROM crdb_internal.jobs WHERE job_id = $1`,
			latestJobID,
		).Scan(&fractionCompleted); err != nil {
			return err
		}
		err = ip.QueryRow(progressQuery, latestJobID).Scan(&progressMessage)
		if err != nil {
			return err
		}
		t.Logf(progressMessage)
		if fractionCompleted < 0.01 || fractionCompleted > 0.99 {
			return errors.Errorf(
				"expected progress to be in range [0.01, 0.99] but got %f",
				fractionCompleted,
			)
		}
		return nil
	}

	do(restoreQuery, checkFraction)

	sqlDB.QueryRow(t, `SELECT job_id FROM crdb_internal.jobs ORDER BY created DESC LIMIT 1`).Scan(&jobID)
	jobProgress := jobutils.GetJobProgress(t, sqlDB, jobID)
	require.NotNil(t, jobProgress)
	require.NotEmpty(t, jobProgress.GetRestore().CompletedSpans)
	require.Equal(t, jobProgress.GetRestore().CompletedSpans[0].Timestamp, hlc.Timestamp{WallTime: 1})

	sqlDB.Exec(t, `PAUSE JOB $1`, jobID)
	jobutils.WaitForJobToPause(t, sqlDB, jobID)
	sqlDB.Exec(t, `RESUME JOB $1`, jobID)
	jobutils.WaitForJobToSucceed(t, sqlDB, jobID)
	jobProgress = jobutils.GetJobProgress(t, sqlDB, jobID)
	require.NotNil(t, jobProgress)
	require.Equal(t, 7, len(jobProgress.GetRestore().CompletedSpans))
}
