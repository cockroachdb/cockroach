// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupccl

import (
	"context"
	"fmt"
	"strconv"
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
func newAlterSchedulesTestHelper(
	t *testing.T, beforeExec func(),
) (*alterSchedulesTestHelper, func()) {
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
				if beforeExec != nil {
					beforeExec()
				}
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

	// block execution while we mess with the schedule.
	ch := make(chan struct{})
	beforeExec := func() {
		<-ch
	}
	defer close(ch)

	th, cleanup := newAlterSchedulesTestHelper(t, beforeExec)
	defer cleanup()

	th.sqlDB.Exec(t, `
CREATE DATABASE mydb;
USE mydb;

CREATE TABLE t1(a int);
INSERT INTO t1 values (1), (10), (100);
`)

	rows := th.sqlDB.QueryStr(t,
		`CREATE SCHEDULE FOR BACKUP t1 INTO 'nodelocal://1/backup/alter-schedule' RECURRING '@daily';`)
	require.Len(t, rows, 2)
	scheduleID, err := strconv.Atoi(rows[0][0])
	require.NoError(t, err)

	rows = th.sqlDB.QueryStr(t,
		fmt.Sprintf(`ALTER BACKUP SCHEDULE %d SET FULL BACKUP '@weekly';`, scheduleID))

	// Incremental should be emitted first.
	require.Equal(t, []string{
		"PAUSED: Waiting for initial backup to complete",
		"@daily",
		"BACKUP TABLE mydb.public.t1 INTO LATEST IN 'nodelocal://1/backup/alter-schedule' WITH OPTIONS (detached)",
	}, []string{rows[0][2], rows[0][4], rows[0][5]})
	require.Equal(t, []string{
		"ACTIVE",
		"@weekly",
		"BACKUP TABLE mydb.public.t1 INTO 'nodelocal://1/backup/alter-schedule' WITH OPTIONS (detached)",
	}, []string{rows[1][2], rows[1][4], rows[1][5]})

	trim := func(s string) string {
		l := len(`2005-06-07 08:09:10`)
		if len(s) > l {
			return s[:l]
		}
		return s
	}

	th.env.AdvanceTime(time.Second)

	rows = th.sqlDB.QueryStr(t, fmt.Sprintf(`ALTER BACKUP SCHEDULE %d EXECUTE FULL IMMEDIATELY;`, scheduleID))
	require.Equal(t, trim(th.env.Now().String()), trim(rows[1][3]))

	// The paused inc schedule -- paused while it waits for the full -- can't be
	// triggered while it is paused.
	th.sqlDB.ExpectErr(t, "cannot execute a paused schedule",
		fmt.Sprintf(`ALTER BACKUP SCHEDULE %d EXECUTE IMMEDIATELY;`, scheduleID))

	th.sqlDB.Exec(t, `RESUME SCHEDULE $1`, scheduleID)

	rows = th.sqlDB.QueryStr(t, fmt.Sprintf(`ALTER BACKUP SCHEDULE %d EXECUTE IMMEDIATELY;`, scheduleID))
	require.Equal(t, trim(th.env.Now().String()), trim(rows[0][3]))
}

func TestAlterBackupScheduleWithSQLSpecialCharacters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newAlterSchedulesTestHelper(t, nil)
	defer cleanup()

	// Characters that require quoting as specified in mustQuoteMap in
	// sql/lexbase/encode.go.
	uri := "nodelocal://1/backup/alter ,s{hedu}e"

	createCmd := fmt.Sprintf(
		"CREATE SCHEDULE FOR BACKUP INTO '%s' WITH"+
			" incremental_location = '%s' RECURRING '@hourly' FULL BACKUP '@daily';",
		uri, uri,
	)
	rows := th.sqlDB.QueryStr(t, createCmd)
	require.Len(t, rows, 2)
	incID, err := strconv.Atoi(rows[0][0])
	require.NoError(t, err)
	fullID, err := strconv.Atoi(rows[1][0])
	require.NoError(t, err)

	alterCmd := fmt.Sprintf(
		"ALTER BACKUP SCHEDULE %d SET INTO '%s', "+
			"SET RECURRING '@daily', SET FULL BACKUP '@weekly';",
		fullID, uri,
	)
	th.sqlDB.Exec(t, alterCmd)

	_, incRecurrence := scheduleStatusAndRecurrence(t, th, incID)
	_, fullRecurrence := scheduleStatusAndRecurrence(t, th, fullID)
	require.Equal(t, "@daily", incRecurrence)
	require.Equal(t, "@weekly", fullRecurrence)
}

func scheduleStatusAndRecurrence(
	t *testing.T, th *alterSchedulesTestHelper, id int,
) (status string, recurrence string) {
	t.Helper()
	th.sqlDB.
		QueryRow(t, `SELECT schedule_status, recurrence FROM [SHOW SCHEDULES] WHERE id=$1`, id).
		Scan(&status, &recurrence)
	return status, recurrence
}
