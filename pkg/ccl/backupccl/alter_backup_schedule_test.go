// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupccl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// alterSchedulesTestHelper starts a server, and arranges for job scheduling daemon to
// use jobstest.JobSchedulerTestEnv.
// This helper also arranges for the manual override of scheduling logic
// via executeSchedules callback.
type execAlterSchedulesFn = func(ctx context.Context, maxSchedules int64) error
type alterSchedulesTestHelper struct {
	iodir            string
	server           serverutils.TestServerInterface
	env              *jobstest.JobSchedulerTestEnv
	cfg              *scheduledjobs.JobExecutionConfig
	sqlDB            *sqlutils.SQLRunner
	executeSchedules func() error
}

// newAlterSchedulesTestHelper creates and initializes appropriate state for a test,
// returning alterSchedulesTestHelper as well as a cleanup function.
func newAlterSchedulesTestHelper(t *testing.T) (*alterSchedulesTestHelper, func()) {
	dir, dirCleanupFn := testutils.TempDir(t)

	th := &alterSchedulesTestHelper{
		env: jobstest.NewJobSchedulerTestEnv(
			jobstest.UseSystemTables, timeutil.Now(), tree.ScheduledBackupExecutor),
		iodir: dir,
	}

	knobs := &jobs.TestingKnobs{
		JobSchedulerEnv: th.env,
		TakeOverJobsScheduling: func(fn execAlterSchedulesFn) {
			th.executeSchedules = func() error {
				defer th.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
				return fn(context.Background(), allSchedules)
			}
		},
		CaptureJobExecutionConfig: func(config *scheduledjobs.JobExecutionConfig) {
			th.cfg = config
		},
		IntervalOverrides: jobs.NewTestingKnobsWithShortIntervals().IntervalOverrides,
	}

	args := base.TestServerArgs{
		ExternalIODir: dir,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: knobs,
		},
	}
	s, db, _ := serverutils.StartServer(t, args)
	require.NotNil(t, th.cfg)
	th.sqlDB = sqlutils.MakeSQLRunner(db)
	th.server = s
	th.sqlDB.Exec(t, `SET CLUSTER SETTING bulkio.backup.merge_file_buffer_size = '1MiB'`)
	sysDB := sqlutils.MakeSQLRunner(s.SystemLayer().SQLConn(t))
	sysDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`) // speeds up test

	return th, func() {
		dirCleanupFn()
		s.Stopper().Stop(context.Background())
	}
}

func TestAlterBackupScheduleEmitsSummary(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newAlterSchedulesTestHelper(t)
	defer cleanup()

	th.sqlDB.Exec(t, `
CREATE DATABASE mydb;
USE mydb;

CREATE TABLE t1(a int);
INSERT INTO t1 values (1), (10), (100);
`)

	rows := th.sqlDB.Query(t,
		`CREATE SCHEDULE FOR BACKUP t1 INTO 'nodelocal://1/backup/alter-schedule' RECURRING '@daily';`)
	require.NoError(t, rows.Err())

	var scheduleID int64
	var unusedStr string
	var unusedTS *time.Time
	rowCount := 0
	for rows.Next() {
		// We just need to retrieve one of the schedule IDs, don't care whether
		// it's the incremental or full.
		require.NoError(t, rows.Scan(&scheduleID, &unusedStr, &unusedStr, &unusedTS, &unusedStr, &unusedStr))
		rowCount++
	}
	require.Equal(t, 2, rowCount)

	var status, schedule, backupStmt string
	var statuses, schedules, backupStmts []string
	rows = th.sqlDB.Query(t,
		fmt.Sprintf(`ALTER BACKUP SCHEDULE %d SET FULL BACKUP '@weekly';`, scheduleID))
	require.NoError(t, rows.Err())
	for rows.Next() {
		require.NoError(t, rows.Scan(&scheduleID, &unusedStr, &status, &unusedTS, &schedule, &backupStmt))
		statuses = append(statuses, status)
		schedules = append(schedules, schedule)
		backupStmts = append(backupStmts, backupStmt)
	}

	// Incremental should be emitted first.
	require.Equal(t, []string{"PAUSED: Waiting for initial backup to complete", "ACTIVE"}, statuses)
	require.Equal(t, []string{"@daily", "@weekly"}, schedules)
	require.Equal(t, []string{
		"BACKUP TABLE mydb.public.t1 INTO LATEST IN 'nodelocal://1/backup/alter-schedule' WITH OPTIONS (detached)",
		"BACKUP TABLE mydb.public.t1 INTO 'nodelocal://1/backup/alter-schedule' WITH OPTIONS (detached)",
	},
		backupStmts)

}
