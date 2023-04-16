// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl" // register cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestJobsExecutionDetails tests that a job's execution details are retrieved
// and rendered correctly.
func TestJobsExecutionDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer ccl.TestingEnableEnterprise()()

	// Timeout the test in a few minutes if it hasn't succeeded.
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Minute*2)
	defer cancel()

	params, _ := tests.CreateTestServerParams()
	params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()
	canContinue := make(chan struct{})
	params.Knobs.SQLExecutor = &sql.ExecutorTestingKnobs{
		AfterStorePlanDiagram: func() {
			<-canContinue
		},
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(sqlDB)

	runner.Exec(t, `CREATE TABLE t (id INT)`)
	runner.Exec(t, `INSERT INTO t SELECT generate_series(1, 100)`)
	var backupJobID int
	runner.QueryRow(t, `BACKUP TABLE t INTO 'userfile:///foo' WITH detached`).Scan(&backupJobID)
	// Wait till the job has written a DSP diagram.
	jobsprofiler.TestingCheckForPlanDiagram(ctx, t, s.InternalDB().(isql.DB), jobspb.JobID(backupJobID))
	canContinue <- struct{}{}
	jobutils.WaitForJobToSucceed(t, runner, jobspb.JobID(backupJobID))

	runner.Exec(t, `CREATE DATABASE t`)
	var restoreJobID int
	runner.QueryRow(t, `RESTORE TABLE t FROM LATEST IN 'userfile:///foo' WITH detached, into_db = t`).Scan(&restoreJobID)
	// Wait till the job has written a DSP diagram.
	jobsprofiler.TestingCheckForPlanDiagram(ctx, t, s.InternalDB().(isql.DB), jobspb.JobID(restoreJobID))
	canContinue <- struct{}{}
	jobutils.WaitForJobToSucceed(t, runner, jobspb.JobID(restoreJobID))

	var count int
	t.Run("SHOW JOB", func(t *testing.T) {
		runner.QueryRow(t, `SELECT count(*) FROM [SHOW JOB $1 WITH EXECUTION DETAILS] WHERE plan_diagram IS NOT NULL`, backupJobID).Scan(&count)
		require.NotZero(t, count)
		runner.QueryRow(t, `SELECT count(*) FROM [SHOW JOB $1 WITH EXECUTION DETAILS] WHERE plan_diagram IS NOT NULL`, restoreJobID).Scan(&count)
		require.NotZero(t, count)
	})

	t.Run("SHOW JOBS", func(t *testing.T) {
		runner.CheckQueryResults(t, `SELECT count(*) FROM [SHOW JOBS WITH EXECUTION DETAILS] WHERE plan_diagram IS NOT NULL`, [][]string{{"2"}})
		runner.CheckQueryResults(t, fmt.Sprintf(`SELECT count(*) FROM [SHOW JOBS SELECT %d WITH EXECUTION DETAILS] WHERE plan_diagram IS NOT NULL`,
			backupJobID), [][]string{{"1"}})
	})
}
