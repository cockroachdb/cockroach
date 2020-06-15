// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
	"github.com/gorhill/cronexpr"
	"github.com/stretchr/testify/require"
)

func addFakeJob(t *testing.T, h *testHelper, id int64, status Status, txn *kv.Txn) {
	payload := []byte("fake payload")
	n, err := h.ex.ExecEx(context.Background(), "fake-job", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		fmt.Sprintf(
			"INSERT INTO %s (created_by_type, created_by_id, status, payload) VALUES ($1, $2, $3, $4)",
			h.env.SystemJobsTableName()),
		createdByName, id, status, payload,
	)
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func TestJobSchedulerReschedulesRunning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h, cleanup := newTestHelper(t)
	defer cleanup()

	ctx := context.Background()

	for _, wait := range []jobspb.ScheduleDetails_WaitBehavior{
		jobspb.ScheduleDetails_WAIT,
		jobspb.ScheduleDetails_SKIP,
	} {
		t.Run(wait.String(), func(t *testing.T) {
			// Create job with the target wait behavior.
			j := h.newScheduledJob(t, "j", "j sql")
			j.SetScheduleDetails(jobspb.ScheduleDetails{Wait: wait})
			require.NoError(t, j.SetSchedule("@hourly"))

			require.NoError(t,
				h.kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					require.NoError(t, j.Create(ctx, h.ex, txn))

					// Lets add few fake runs for this schedule, including terminal and
					// non terminal states.
					for _, status := range []Status{
						StatusRunning, StatusFailed, StatusCanceled, StatusSucceeded, StatusPaused} {
						addFakeJob(t, h, j.ScheduleID(), status, txn)
					}
					return nil
				}))

			// Verify the job has expected nextRun time.
			expectedRunTime := cronexpr.MustParse("@hourly").Next(h.env.Now())
			loaded := h.loadJob(t, j.ScheduleID())
			require.Equal(t, expectedRunTime, loaded.NextRun())

			// Advance time past the expected start time.
			h.env.SetTime(expectedRunTime.Add(time.Second))

			// The job should not run -- it should be rescheduled `recheckJobAfter` time in the
			// future.
			s := newJobScheduler(h.env, h.ex)
			require.NoError(t, s.executeSchedules(ctx, allSchedules, nil))

			if wait == jobspb.ScheduleDetails_WAIT {
				expectedRunTime = h.env.Now().Add(recheckRunningAfter)
			} else {
				expectedRunTime = cronexpr.MustParse("@hourly").Next(h.env.Now())
			}
			loaded = h.loadJob(t, j.ScheduleID())
			require.Equal(t, expectedRunTime, loaded.NextRun())
		})
	}
}

func TestJobSchedulerExecutesAndSchedulesNextRun(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h, cleanup := newTestHelper(t)
	defer cleanup()

	ctx := context.Background()

	// Create job that waits for the previous runs to finish.
	j := h.newScheduledJob(t, "j", "SELECT 42 AS meaning_of_life;")
	require.NoError(t, j.SetSchedule("@hourly"))

	require.NoError(t,
		h.kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			require.NoError(t, j.Create(ctx, h.ex, txn))
			return nil
		}))

	// Verify the job has expected nextRun time.
	expectedRunTime := cronexpr.MustParse("@hourly").Next(h.env.Now())
	loaded := h.loadJob(t, j.ScheduleID())
	require.Equal(t, expectedRunTime, loaded.NextRun())

	// Advance time past the expected start time.
	h.env.SetTime(expectedRunTime.Add(time.Second))

	// Execute the job and verify it has the next run scheduled.
	s := newJobScheduler(h.env, h.ex)
	require.NoError(t, s.executeSchedules(ctx, allSchedules, nil))

	expectedRunTime = cronexpr.MustParse("@hourly").Next(h.env.Now())
	loaded = h.loadJob(t, j.ScheduleID())
	require.Equal(t, expectedRunTime, loaded.NextRun())
}

func TestJobSchedulerDaemonInitialScanDelay(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for i := 0; i < 100; i++ {
		require.Greater(t, int64(getInitialScanDelay()), int64(time.Minute))
	}
}

func getScopedSettings() (*settings.Values, func()) {
	sv := &settings.Values{}
	sv.Init(nil)
	return sv, settings.TestingSaveRegistry()
}

func TestJobSchedulerDaemonGetWaitPeriod(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sv, cleanup := getScopedSettings()
	defer cleanup()

	schedulerEnabledSetting.Override(sv, false)

	// When disabled, we wait 5 minutes before rechecking.
	require.EqualValues(t, 5*time.Minute, getWaitPeriod(sv))
	schedulerEnabledSetting.Override(sv, true)

	// When pace is too low, we use something more reasonable.
	schedulerPaceSetting.Override(sv, time.Nanosecond)
	require.EqualValues(t, minPacePeriod, getWaitPeriod(sv))

	// Otherwise, we use user specified setting.
	pace := 42 * time.Second
	schedulerPaceSetting.Override(sv, pace)
	require.EqualValues(t, pace, getWaitPeriod(sv))
}

type recordScheduleExecutor struct {
	executed []int64
}

func (n *recordScheduleExecutor) ExecuteJob(
	_ context.Context, schedule *ScheduledJob, _ *kv.Txn,
) error {
	n.executed = append(n.executed, schedule.ScheduleID())
	return nil
}

func (n *recordScheduleExecutor) NotifyJobTermination(
	_ context.Context, _ *JobMetadata, _ *ScheduledJob, _ *kv.Txn,
) error {
	return nil
}

var _ ScheduledJobExecutor = &recordScheduleExecutor{}

func scanImmediately() func() {
	oldScanDelay := getInitialScanDelay
	getInitialScanDelay = func() time.Duration { return 0 }
	return func() { getInitialScanDelay = oldScanDelay }
}

func TestJobSchedulerCanBeDisabledWhileSleeping(t *testing.T) {
	defer leaktest.AfterTest(t)()

	h, cleanup := newTestHelper(t)
	defer cleanup()
	ctx := context.Background()

	sv, cleanup := getScopedSettings()
	defer cleanup()
	schedulerEnabledSetting.Override(sv, true)

	// Register executor which keeps track of schedules it executes.
	const executorName = "record-execute"
	neverExecute := &recordScheduleExecutor{}
	defer registerScopedScheduledJobExecutor(executorName, neverExecute)()

	// Disable initial scan delay.
	defer scanImmediately()()

	// Override getWaitPeriod to use small delay.
	defer func(f func(_ *settings.Values) time.Duration) {
		getWaitPeriod = f
	}(getWaitPeriod)

	stopper := stop.NewStopper()
	getWaitPeriodCalled := make(chan struct{})

	getWaitPeriod = func(sv *settings.Values) time.Duration {
		// Disable daemon
		schedulerEnabledSetting.Override(sv, false)

		// Before we return, create a job which should not be executed
		// (since the daemon is disabled).  We use our special executor
		// to verify this.
		schedule := h.newScheduledJobForExecutor("test_job", executorName, nil)
		schedule.SetNextRun(h.env.Now())
		require.NoError(t, schedule.Create(ctx, h.ex, nil))

		// Advance time so that daemon picks up test_job.
		h.env.AdvanceTime(time.Second)

		// Notify main thread and return some small delay for daemon to sleep.
		select {
		case getWaitPeriodCalled <- struct{}{}:
		case <-stopper.ShouldStop():
		}

		return 10 * time.Millisecond
	}

	// Run the daemon.
	StartJobSchedulerDaemon(ctx, stopper, sv, h.env, h.kvDB, h.ex)

	// Wait for daemon to run it's scan loop few times.
	for i := 0; i < 5; i++ {
		<-getWaitPeriodCalled
	}

	// Stop the daemon.  If we attempt to execute our 'test_job', the test will fails.
	stopper.Stop(ctx)
	// Verify we never executed any jobs due to disabled daemon.
	require.Equal(t, 0, len(neverExecute.executed))
}

// We expect the first 2 jobs to be executed.
type expectedRun struct {
	id      int64
	nextRun interface{} // Interface to support nullable nextRun
}

func expectScheduledRuns(t *testing.T, h *testHelper, expected ...expectedRun) {
	query := fmt.Sprintf("SELECT schedule_id, next_run FROM %s", h.env.scheduledJobsTableName)

	testutils.SucceedsSoon(t, func() error {
		rows := h.sqlDB.Query(t, query)
		var res []expectedRun
		for rows.Next() {
			var s expectedRun
			require.NoError(t, rows.Scan(&s.id, &s.nextRun))
			res = append(res, s)
		}

		if reflect.DeepEqual(expected, res) {
			return nil
		}

		return errors.Newf("still waiting for matching jobs: res=%+v expected=%+v", res, expected)
	})
}

func overridePaceSetting(d time.Duration) func() {
	oldPace := getWaitPeriod
	getWaitPeriod = func(_ *settings.Values) time.Duration {
		return d
	}
	return func() {
		getWaitPeriod = oldPace
	}
}

func TestJobSchedulerDaemonProcessesJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h, cleanup := newTestHelper(t)
	defer cleanup()

	ctx := context.Background()

	// Create few, one-off schedules.
	const numJobs = 5
	scheduleRunTime := h.env.Now().Add(time.Hour)
	var scheduleIDs []int64
	for i := 0; i < numJobs; i++ {
		schedule := h.newScheduledJob(t, "test_job", "SELECT 42")
		schedule.SetNextRun(scheduleRunTime)
		require.NoError(t, schedule.Create(ctx, h.ex, nil))
		scheduleIDs = append(scheduleIDs, schedule.ScheduleID())
	}

	// Sort by schedule ID.
	sort.Slice(scheduleIDs, func(i, j int) bool { return scheduleIDs[i] < scheduleIDs[j] })

	// Make daemon run fast.
	defer overridePaceSetting(10 * time.Millisecond)()
	defer scanImmediately()()

	stopper := stop.NewStopper()
	sv, cleanup := getScopedSettings()
	defer cleanup()
	StartJobSchedulerDaemon(ctx, stopper, sv, h.env, h.kvDB, h.ex)

	// Advance our fake time 1 hour forward (plus a bit)
	h.env.AdvanceTime(time.Hour + time.Second)

	expectScheduledRuns(t, h,
		expectedRun{scheduleIDs[0], nil},
		expectedRun{scheduleIDs[1], nil},
		expectedRun{scheduleIDs[2], nil},
		expectedRun{scheduleIDs[3], nil},
		expectedRun{scheduleIDs[4], nil},
	)

	stopper.Stop(ctx)
}

func TestJobSchedulerDaemonHonorsMaxJobsLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h, cleanup := newTestHelper(t)
	defer cleanup()

	ctx := context.Background()

	// Create few, one-off schedules.
	const numJobs = 5
	scheduleRunTime := h.env.Now().Add(time.Hour)
	var scheduleIDs []int64
	for i := 0; i < numJobs; i++ {
		schedule := h.newScheduledJob(t, "test_job", "SELECT 42")
		schedule.SetNextRun(scheduleRunTime)
		require.NoError(t, schedule.Create(ctx, h.ex, nil))
		scheduleIDs = append(scheduleIDs, schedule.ScheduleID())
	}

	// Sort by schedule ID.
	sort.Slice(scheduleIDs, func(i, j int) bool { return scheduleIDs[i] < scheduleIDs[j] })

	// Make daemon execute initial scan immediately, but block subsequent scans.
	defer scanImmediately()()
	defer overridePaceSetting(time.Hour)()

	// Advance our fake time 1 hour forward (plus a bit) so that the daemon finds matching jobs.
	h.env.AdvanceTime(time.Hour + time.Second)

	stopper := stop.NewStopper()
	sv, cleanup := getScopedSettings()
	defer cleanup()
	schedulerMaxJobsPerIterationSetting.Override(sv, 2)
	StartJobSchedulerDaemon(ctx, stopper, sv, h.env, h.kvDB, h.ex)

	// Note: time is stored in the table with microsecond precision.
	expectScheduledRuns(t, h,
		expectedRun{scheduleIDs[0], nil},
		expectedRun{scheduleIDs[1], nil},
		expectedRun{scheduleIDs[2], scheduleRunTime.Round(time.Microsecond)},
		expectedRun{scheduleIDs[3], scheduleRunTime.Round(time.Microsecond)},
		expectedRun{scheduleIDs[4], scheduleRunTime.Round(time.Microsecond)},
	)

	stopper.Stop(ctx)
}

func TestJobSchedulerDaemonUsesSystemTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer settings.TestingSaveRegistry()()

	// Make daemon run quickly.
	defer scanImmediately()()
	defer overridePaceSetting(10 * time.Millisecond)()
	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(db)
	runner.Exec(t, "CREATE TABLE defaultdb.foo(a int)")

	// Create a one off job which writes some values into 'foo' table.
	schedule := NewScheduledJob(ProdJobSchedulerEnv)
	schedule.SetScheduleName("test schedule")
	schedule.SetNextRun(timeutil.Now())
	any, err := types.MarshalAny(
		&jobspb.SqlStatementExecutionArg{Statement: "INSERT INTO defaultdb.foo VALUES (1), (2), (3)"})
	require.NoError(t, err)
	schedule.SetExecutionDetails(InlineExecutorName, jobspb.ExecutionArguments{Args: any})
	require.NoError(t, schedule.Create(
		ctx, s.InternalExecutor().(sqlutil.InternalExecutor), nil))

	// Verify the schedule ran.
	testutils.SucceedsSoon(t, func() error {
		var count int
		if err := db.QueryRow(
			"SELECT count(*) FROM defaultdb.foo").Scan(&count); err != nil || count != 3 {
			return errors.Newf("expected 3 rows, got %d (err=%+v)", count, err)
		}
		return nil
	})
}
