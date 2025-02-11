// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package persistedsqlstats_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type testHelper struct {
	server           serverutils.TestServerInterface
	sqlDB            *sqlutils.SQLRunner
	env              *jobstest.JobSchedulerTestEnv
	cfg              *scheduledjobs.JobExecutionConfig
	executeSchedules func() error
}

func (h *testHelper) waitForSuccessfulScheduledJob(t *testing.T, sj *jobs.ScheduledJob) {
	query := fmt.Sprintf(`
SELECT id
FROM %s
WHERE
  status=$1
  AND created_by_type=$2
  AND created_by_id=$3
`, h.env.SystemJobsTableName())

	testutils.SucceedsSoon(t, func() error {
		// Force the job created by the schedule to actually run.
		h.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
		var unused int64
		return h.sqlDB.DB.QueryRowContext(context.Background(),
			query, jobs.StateSucceeded, jobs.CreatedByScheduledJobs, sj.ScheduleID()).Scan(&unused)
	})
}

func newTestHelper(
	t *testing.T, sqlStatsKnobs *sqlstats.TestingKnobs,
) (helper *testHelper, cleanup func()) {
	helper = &testHelper{
		env: jobstest.NewJobSchedulerTestEnv(
			jobstest.UseSystemTables, timeutil.Now(), tree.ScheduledSQLStatsCompactionExecutor),
	}

	knobs := jobs.NewTestingKnobsWithShortIntervals()
	knobs.JobSchedulerEnv = helper.env
	knobs.TakeOverJobsScheduling = func(fn func(ctx context.Context, maxSchedules int64) error) {
		helper.executeSchedules = func() error {
			defer helper.server.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
			// maxSchedules = 0 means there's no limit.
			return fn(context.Background(), 0 /* maxSchedules */)
		}
	}
	knobs.CaptureJobExecutionConfig = func(config *scheduledjobs.JobExecutionConfig) {
		helper.cfg = config
	}

	var params base.TestServerArgs
	params.Knobs.JobsTestingKnobs = knobs
	params.Knobs.SQLStatsKnobs = sqlStatsKnobs
	srv, db, _ := serverutils.StartServer(t, params)
	require.NotNil(t, helper.cfg)

	helper.sqlDB = sqlutils.MakeSQLRunner(db)
	helper.server = srv

	return helper, func() {
		srv.Stopper().Stop(context.Background())
	}
}

func verifySQLStatsCompactionScheduleCreatedOnStartup(t *testing.T, helper *testHelper) {
	var compactionScheduleCount int
	row := helper.sqlDB.QueryRow(t, `SELECT count(*) FROM system.scheduled_jobs WHERE schedule_name = 'sql-stats-compaction'`)
	row.Scan(&compactionScheduleCount)
	require.Equal(t, 1 /* expected */, compactionScheduleCount)
}

func getSQLStatsCompactionSchedule(t *testing.T, helper *testHelper) *jobs.ScheduledJob {
	var scheduleID jobspb.ScheduleID
	helper.sqlDB.
		QueryRow(t, `SELECT schedule_id FROM system.scheduled_jobs WHERE schedule_name = 'sql-stats-compaction'`).
		Scan(&scheduleID)
	schedules := jobs.ScheduledJobDB(helper.server.InternalDB().(isql.DB))
	sj, err := schedules.Load(context.Background(), helper.env, scheduleID)
	require.NoError(t, err)
	require.NotNil(t, sj)
	return sj
}

func TestScheduledSQLStatsCompaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var tm atomic.Value
	tm.Store(timeutil.Now().Add(-2 * time.Hour))
	knobs := &sqlstats.TestingKnobs{
		StubTimeNow: func() time.Time {
			return tm.Load().(time.Time)
		},
	}

	helper, helperCleanup := newTestHelper(t, knobs)
	defer helperCleanup()

	// We run some queries then flush so that we ensure that are some stats in
	// the system table.
	helper.sqlDB.Exec(t, "SELECT 1; SELECT 1, 1")
	helper.server.ApplicationLayer().SQLServer().(*sql.Server).GetSQLStatsProvider().MaybeFlush(ctx, helper.server.AppStopper())
	helper.sqlDB.Exec(t, "SET CLUSTER SETTING sql.stats.persisted_rows.max = 1")

	stmtStatsCnt, txnStatsCnt := getPersistedStatsEntry(t, helper.sqlDB)
	require.True(t, stmtStatsCnt >= 2,
		"expecting at least 2 persisted stmt fingerprints, but found: %d", stmtStatsCnt)
	require.True(t, txnStatsCnt >= 2,
		"expecting at least 2 persisted txn fingerprints, but found: %d", txnStatsCnt)

	verifySQLStatsCompactionScheduleCreatedOnStartup(t, helper)
	schedule := getSQLStatsCompactionSchedule(t, helper)
	require.Equal(t, string(jobs.StatePending), schedule.ScheduleStatus())

	tm.Store(timeutil.Now())

	// Force the schedule to execute.
	helper.env.SetTime(schedule.NextRun().Add(time.Minute))
	require.NoError(t, helper.executeSchedules())
	helper.waitForSuccessfulScheduledJob(t, schedule)

	// Read the system.scheduled_job table again.
	schedule = getSQLStatsCompactionSchedule(t, helper)
	require.Equal(t, string(jobs.StateSucceeded), schedule.ScheduleStatus())

	stmtStatsCntPostCompact, txnStatsCntPostCompact := getPersistedStatsEntry(t, helper.sqlDB)
	require.Less(t, stmtStatsCntPostCompact, stmtStatsCnt,
		"expecting persisted stmt fingerprints count to be less than %d, but found: %d", stmtStatsCnt, stmtStatsCntPostCompact)
	require.Less(t, txnStatsCntPostCompact, txnStatsCnt,
		"expecting persisted txn fingerprints count to be less than %d, but found: %d", txnStatsCnt, txnStatsCntPostCompact)
}

func TestSQLStatsScheduleOperations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "test is too slow to run under race")

	ctx := context.Background()
	helper, helperCleanup := newTestHelper(t, &sqlstats.TestingKnobs{JobMonitorUpdateCheckInterval: time.Second})
	defer helperCleanup()

	schedID := getSQLStatsCompactionSchedule(t, helper).ScheduleID()

	t.Run("schedule_cannot_be_dropped", func(t *testing.T) {
		_, err := helper.sqlDB.DB.ExecContext(ctx, "DROP SCHEDULE $1", schedID)
		require.True(t,
			strings.Contains(err.Error(), persistedsqlstats.ErrScheduleUndroppable.Error()),
			"expected to found ErrScheduleUndroppable, but found %+v", err)
	})

	t.Run("warn_schedule_paused", func(t *testing.T) {
		helper.sqlDB.Exec(t, "PAUSE SCHEDULE $1", schedID)
		defer helper.sqlDB.Exec(t, "RESUME SCHEDULE $1", schedID)

		helper.sqlDB.CheckQueryResults(
			t,
			fmt.Sprintf("SELECT schedule_status FROM [SHOW SCHEDULE %d]", schedID),
			[][]string{{"PAUSED"}},
		)

		// Reload schedule from DB.
		sj := getSQLStatsCompactionSchedule(t, helper)
		err := persistedsqlstats.CheckScheduleAnomaly(sj)
		require.True(t, errors.Is(err, persistedsqlstats.ErrSchedulePaused),
			"expected ErrSchedulePaused, but found %+v", err)
	})

	t.Run("warn_schedule_long_run_interval", func(t *testing.T) {
		t.Run("via cluster setting", func(t *testing.T) {
			// Craft an expression that next repeats next month.
			expr := fmt.Sprintf("59 23 24 %d ?", timeutil.Now().AddDate(0, 1, 0).Month())
			helper.sqlDB.Exec(t, "SET CLUSTER SETTING sql.stats.cleanup.recurrence = $1", expr)

			var err error
			testutils.SucceedsSoon(t, func() error {
				// Reload schedule from DB.
				sj := getSQLStatsCompactionSchedule(t, helper)
				err = persistedsqlstats.CheckScheduleAnomaly(sj)
				if err == nil {
					return errors.Newf("retry: next_run=%s, schedule_expr=%s", sj.NextRun(), sj.ScheduleExpr())
				}
				require.Equal(t, expr, sj.ScheduleExpr())
				return nil
			})

			require.True(t, errors.Is(
				errors.Unwrap(err), persistedsqlstats.ErrScheduleIntervalTooLong),
				"expected ErrScheduleIntervalTooLong, but found %+v", err)

			helper.sqlDB.Exec(t, "RESET CLUSTER SETTING sql.stats.cleanup.recurrence")
			helper.sqlDB.CheckQueryResultsRetry(t,
				`SHOW CLUSTER SETTING sql.stats.cleanup.recurrence`,
				[][]string{{"@hourly"}},
			)

			helper.sqlDB.CheckQueryResultsRetry(t,
				fmt.Sprintf(`
SELECT schedule_expr
FROM system.scheduled_jobs WHERE schedule_id = %d`, schedID),
				[][]string{{"@hourly"}},
			)
		})

		t.Run("via directly updating system table", func(t *testing.T) {
			sj := getSQLStatsCompactionSchedule(t, helper)
			rowsAffected, err := helper.sqlDB.Exec(t, `
			 UPDATE system.scheduled_jobs
			 SET (schedule_expr, next_run) = ('@weekly', $1)
			 WHERE schedule_id = $2`, timeutil.Now().Add(time.Hour*24*7), sj.ScheduleID()).RowsAffected()
			require.NoError(t, err)
			require.Equal(t, int64(1) /* expected */, rowsAffected)

			// Sanity check.
			helper.sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`
SELECT schedule_expr
FROM system.scheduled_jobs WHERE schedule_id = %d`, sj.ScheduleID()),
				[][]string{{"@weekly"}},
			)

			sj = getSQLStatsCompactionSchedule(t, helper)
			require.Equal(t, "@weekly", sj.ScheduleExpr())
			err = persistedsqlstats.CheckScheduleAnomaly(sj)
			require.NotNil(t, err)
			require.True(t, errors.Is(
				errors.Unwrap(err), persistedsqlstats.ErrScheduleIntervalTooLong),
				"expected ErrScheduleIntervalTooLong, but found %+v", err)
		})
	})
}
