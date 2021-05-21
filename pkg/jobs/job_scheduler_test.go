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
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
	"github.com/gorhill/cronexpr"
	"github.com/stretchr/testify/require"
)

func TestJobSchedulerReschedulesRunning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
				h.cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					require.NoError(t, j.Create(ctx, h.cfg.InternalExecutor, txn))

					// Lets add few fake runs for this schedule, including terminal and
					// non terminal states.
					for _, status := range []Status{
						StatusRunning, StatusFailed, StatusCanceled, StatusSucceeded, StatusPaused} {
						_ = addFakeJob(t, h, j.ScheduleID(), status, txn)
					}
					return nil
				}))

			// Verify the job has expected nextRun time.
			expectedRunTime := cronexpr.MustParse("@hourly").Next(h.env.Now())
			loaded := h.loadSchedule(t, j.ScheduleID())
			require.Equal(t, expectedRunTime, loaded.NextRun())

			// Advance time past the expected start time.
			h.env.SetTime(expectedRunTime.Add(time.Second))

			// The job should not run -- it should be rescheduled `recheckJobAfter` time in the
			// future.
			s := newJobScheduler(h.cfg, h.env, metric.NewRegistry())
			require.NoError(t,
				h.cfg.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
					return s.executeSchedules(ctx, allSchedules, txn)
				}))

			if wait == jobspb.ScheduleDetails_WAIT {
				expectedRunTime = h.env.Now().Add(recheckRunningAfter)
			} else {
				expectedRunTime = cronexpr.MustParse("@hourly").Next(h.env.Now())
			}
			loaded = h.loadSchedule(t, j.ScheduleID())
			require.Equal(t, expectedRunTime, loaded.NextRun())
		})
	}
}

func TestJobSchedulerExecutesAfterTerminal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h, cleanup := newTestHelper(t)
	defer cleanup()

	ctx := context.Background()

	// If all of the previous runs are in a terminal state, the waiting policy
	// should not matter, so ensure that the behavior is the same for them all.
	for _, wait := range []jobspb.ScheduleDetails_WaitBehavior{
		jobspb.ScheduleDetails_WAIT,
		jobspb.ScheduleDetails_SKIP,
	} {
		t.Run(wait.String(), func(t *testing.T) {
			// Create job that waits for the previous runs to finish.
			j := h.newScheduledJob(t, "j", "SELECT 42 AS meaning_of_life;")
			j.SetScheduleDetails(jobspb.ScheduleDetails{Wait: wait})
			require.NoError(t, j.SetSchedule("@hourly"))

			require.NoError(t,
				h.cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					require.NoError(t, j.Create(ctx, h.cfg.InternalExecutor, txn))

					// Let's add few fake runs for this schedule which are in every
					// terminal state.
					for _, status := range []Status{StatusFailed, StatusCanceled, StatusSucceeded} {
						_ = addFakeJob(t, h, j.ScheduleID(), status, txn)
					}
					return nil
				}))

			// Verify the job has expected nextRun time.
			expectedRunTime := cronexpr.MustParse("@hourly").Next(h.env.Now())
			loaded := h.loadSchedule(t, j.ScheduleID())
			require.Equal(t, expectedRunTime, loaded.NextRun())

			// Advance time past the expected start time.
			h.env.SetTime(expectedRunTime.Add(time.Second))

			// Execute the job and verify it has the next run scheduled.
			s := newJobScheduler(h.cfg, h.env, metric.NewRegistry())
			require.NoError(t,
				h.cfg.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
					return s.executeSchedules(ctx, allSchedules, txn)
				}))

			expectedRunTime = cronexpr.MustParse("@hourly").Next(h.env.Now())
			loaded = h.loadSchedule(t, j.ScheduleID())
			require.Equal(t, expectedRunTime, loaded.NextRun())
		})
	}
}

func TestJobSchedulerExecutesAndSchedulesNextRun(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	h, cleanup := newTestHelper(t)
	defer cleanup()

	ctx := context.Background()

	// Create job that waits for the previous runs to finish.
	j := h.newScheduledJob(t, "j", "SELECT 42 AS meaning_of_life;")
	require.NoError(t, j.SetSchedule("@hourly"))

	require.NoError(t,
		h.cfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			require.NoError(t, j.Create(ctx, h.cfg.InternalExecutor, txn))
			return nil
		}))

	// Verify the job has expected nextRun time.
	expectedRunTime := cronexpr.MustParse("@hourly").Next(h.env.Now())
	loaded := h.loadSchedule(t, j.ScheduleID())
	require.Equal(t, expectedRunTime, loaded.NextRun())

	// Advance time past the expected start time.
	h.env.SetTime(expectedRunTime.Add(time.Second))

	// Execute the job and verify it has the next run scheduled.
	s := newJobScheduler(h.cfg, h.env, metric.NewRegistry())
	require.NoError(t,
		h.cfg.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
			return s.executeSchedules(ctx, allSchedules, txn)
		}))

	expectedRunTime = cronexpr.MustParse("@hourly").Next(h.env.Now())
	loaded = h.loadSchedule(t, j.ScheduleID())
	require.Equal(t, expectedRunTime, loaded.NextRun())
}

func TestJobSchedulerDaemonInitialScanDelay(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for i := 0; i < 100; i++ {
		require.Greater(t, int64(getInitialScanDelay(nil)), int64(time.Minute))
	}
}

func getScopedSettings() (*settings.Values, func()) {
	sv := &settings.Values{}
	sv.Init(context.Background(), nil)
	return sv, settings.TestingSaveRegistry()
}

func TestJobSchedulerDaemonGetWaitPeriod(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	sv, cleanup := getScopedSettings()
	defer cleanup()

	schedulerEnabledSetting.Override(ctx, sv, false)

	// When disabled, we wait 5 minutes before rechecking.
	require.EqualValues(t, 5*time.Minute, getWaitPeriod(sv, nil))
	schedulerEnabledSetting.Override(ctx, sv, true)

	// When pace is too low, we use something more reasonable.
	schedulerPaceSetting.Override(ctx, sv, time.Nanosecond)
	require.EqualValues(t, minPacePeriod, getWaitPeriod(sv, nil))

	// Otherwise, we use user specified setting.
	pace := 42 * time.Second
	schedulerPaceSetting.Override(ctx, sv, pace)
	require.EqualValues(t, pace, getWaitPeriod(sv, nil))
}

type recordScheduleExecutor struct {
	executed []int64
}

func (n *recordScheduleExecutor) ExecuteJob(
	_ context.Context,
	_ *scheduledjobs.JobExecutionConfig,
	_ scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
	_ *kv.Txn,
) error {
	n.executed = append(n.executed, schedule.ScheduleID())
	return nil
}

func (n *recordScheduleExecutor) NotifyJobTermination(
	ctx context.Context,
	jobID jobspb.JobID,
	jobStatus Status,
	_ jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
	ex sqlutil.InternalExecutor,
	txn *kv.Txn,
) error {
	return nil
}

func (n *recordScheduleExecutor) Metrics() metric.Struct {
	return nil
}

var _ ScheduledJobExecutor = &recordScheduleExecutor{}

func fastDaemonKnobs(scanDelay func() time.Duration) *TestingKnobs {
	return &TestingKnobs{
		SchedulerDaemonInitialScanDelay: func() time.Duration { return 0 },
		SchedulerDaemonScanDelay:        scanDelay,
	}
}

func TestJobSchedulerCanBeDisabledWhileSleeping(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	h, cleanup := newTestHelper(t)
	defer cleanup()
	ctx := context.Background()

	// Register executor which keeps track of schedules it executes.
	const executorName = "record-execute"
	neverExecute := &recordScheduleExecutor{}
	defer registerScopedScheduledJobExecutor(executorName, neverExecute)()

	stopper := stop.NewStopper()
	getWaitPeriodCalled := make(chan struct{})

	knobs := fastDaemonKnobs(func() time.Duration {
		// Disable daemon
		schedulerEnabledSetting.Override(ctx, &h.cfg.Settings.SV, false)

		// Before we return, create a job which should not be executed
		// (since the daemon is disabled).  We use our special executor
		// to verify this.
		schedule := h.newScheduledJobForExecutor("test_job", executorName, nil)
		schedule.SetNextRun(h.env.Now())
		require.NoError(t, schedule.Create(ctx, h.cfg.InternalExecutor, nil))

		// Advance time so that daemon picks up test_job.
		h.env.AdvanceTime(time.Second)

		// Notify main thread and return some small delay for daemon to sleep.
		select {
		case getWaitPeriodCalled <- struct{}{}:
		case <-stopper.ShouldQuiesce():
		}

		return 10 * time.Millisecond
	})

	h.cfg.TestingKnobs = knobs
	daemon := newJobScheduler(h.cfg, h.env, metric.NewRegistry())
	daemon.runDaemon(ctx, stopper)

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
	query := fmt.Sprintf("SELECT schedule_id, next_run FROM %s", h.env.ScheduledJobsTableName())

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

func overridePaceSetting(d time.Duration) func() time.Duration {
	return func() time.Duration { return d }
}

func TestJobSchedulerDaemonProcessesJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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
		require.NoError(t, schedule.Create(ctx, h.cfg.InternalExecutor, nil))
		scheduleIDs = append(scheduleIDs, schedule.ScheduleID())
	}

	// Sort by schedule ID.
	sort.Slice(scheduleIDs, func(i, j int) bool { return scheduleIDs[i] < scheduleIDs[j] })

	// Make daemon run fast.
	h.cfg.TestingKnobs = fastDaemonKnobs(overridePaceSetting(10 * time.Millisecond))

	// Start daemon.
	stopper := stop.NewStopper()
	daemon := newJobScheduler(h.cfg, h.env, metric.NewRegistry())
	daemon.runDaemon(ctx, stopper)

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
	defer log.Scope(t).Close(t)
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
		require.NoError(t, schedule.Create(ctx, h.cfg.InternalExecutor, nil))
		scheduleIDs = append(scheduleIDs, schedule.ScheduleID())
	}

	// Sort by schedule ID.
	sort.Slice(scheduleIDs, func(i, j int) bool { return scheduleIDs[i] < scheduleIDs[j] })

	// Advance our fake time 1 hour forward (plus a bit) so that the daemon finds matching jobs.
	h.env.AdvanceTime(time.Hour + time.Second)
	const jobsPerIteration = 2
	schedulerMaxJobsPerIterationSetting.Override(ctx, &h.cfg.Settings.SV, jobsPerIteration)

	// Make daemon execute initial scan immediately, but block subsequent scans.
	h.cfg.TestingKnobs = fastDaemonKnobs(overridePaceSetting(time.Hour))

	// Start daemon.
	stopper := stop.NewStopper()
	daemon := newJobScheduler(h.cfg, h.env, metric.NewRegistry())
	daemon.runDaemon(ctx, stopper)

	testutils.SucceedsSoon(t, func() error {
		if ready := daemon.metrics.ReadyToRun.Value(); numJobs != ready {
			return errors.Errorf("waiting for metric %d = %d", ready, numJobs)
		}
		return nil
	})
	stopper.Stop(ctx)
}

// returnErrorExecutor counts the number of times it is
// called, and always returns an error.
type returnErrorExecutor struct {
	numCalls int
}

func (e *returnErrorExecutor) ExecuteJob(
	_ context.Context,
	_ *scheduledjobs.JobExecutionConfig,
	_ scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
	_ *kv.Txn,
) error {
	e.numCalls++
	return errors.Newf("error for schedule %d", schedule.ScheduleID())
}

func (e *returnErrorExecutor) NotifyJobTermination(
	_ context.Context,
	_ jobspb.JobID,
	_ Status,
	_ jobspb.Details,
	_ scheduledjobs.JobSchedulerEnv,
	_ *ScheduledJob,
	_ sqlutil.InternalExecutor,
	_ *kv.Txn,
) error {
	return nil
}

func (e *returnErrorExecutor) Metrics() metric.Struct {
	return nil
}

var _ ScheduledJobExecutor = &returnErrorExecutor{}

func TestJobSchedulerToleratesBadSchedules(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	h, cleanup := newTestHelper(t)
	defer cleanup()

	ctx := context.Background()

	const executorName = "return_error"
	ex := &returnErrorExecutor{}
	defer registerScopedScheduledJobExecutor(executorName, ex)()

	// Create few one-off schedules.
	const numJobs = 5
	scheduleRunTime := h.env.Now().Add(time.Hour)
	for i := 0; i < numJobs; i++ {
		s := h.newScheduledJobForExecutor("schedule", executorName, nil)
		s.SetNextRun(scheduleRunTime)
		require.NoError(t, s.Create(ctx, h.cfg.InternalExecutor, nil))
	}
	h.env.SetTime(scheduleRunTime.Add(time.Second))
	daemon := newJobScheduler(h.cfg, h.env, metric.NewRegistry())
	require.NoError(t,
		h.cfg.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
			return daemon.executeSchedules(ctx, numJobs, txn)
		}))
	require.Equal(t, numJobs, ex.numCalls)
}

func TestJobSchedulerRetriesFailed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	h, cleanup := newTestHelper(t)
	defer cleanup()

	ctx := context.Background()

	const executorName = "return_error"
	ex := &returnErrorExecutor{}
	defer registerScopedScheduledJobExecutor(executorName, ex)()

	daemon := newJobScheduler(h.cfg, h.env, metric.NewRegistry())

	schedule := h.newScheduledJobForExecutor("schedule", executorName, nil)
	require.NoError(t, schedule.Create(ctx, h.cfg.InternalExecutor, nil))

	startTime := h.env.Now()
	execTime := startTime.Add(time.Hour).Add(time.Second)

	cron := cronexpr.MustParse("@hourly")

	for _, tc := range []struct {
		onError jobspb.ScheduleDetails_ErrorHandlingBehavior
		nextRun time.Time
	}{
		{jobspb.ScheduleDetails_PAUSE_SCHED, time.Time{}},
		{jobspb.ScheduleDetails_RETRY_SOON, execTime.Add(retryFailedJobAfter).Round(time.Microsecond)},
		{jobspb.ScheduleDetails_RETRY_SCHED, cron.Next(execTime).Round(time.Microsecond)},
	} {
		t.Run(tc.onError.String(), func(t *testing.T) {
			h.env.SetTime(startTime)
			schedule.SetScheduleDetails(jobspb.ScheduleDetails{OnError: tc.onError})
			require.NoError(t, schedule.SetSchedule("@hourly"))
			require.NoError(t, schedule.Update(ctx, h.cfg.InternalExecutor, nil))

			h.env.SetTime(execTime)
			require.NoError(t,
				h.cfg.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
					return daemon.executeSchedules(ctx, 1, txn)
				}))

			loaded := h.loadSchedule(t, schedule.ScheduleID())
			require.EqualValues(t, tc.nextRun, loaded.NextRun())
		})
	}
}

func TestJobSchedulerDaemonUsesSystemTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer settings.TestingSaveRegistry()()

	// Make daemon run quickly.
	knobs := &TestingKnobs{
		SchedulerDaemonInitialScanDelay: func() time.Duration { return 0 },
		SchedulerDaemonScanDelay:        overridePaceSetting(10 * time.Microsecond),
	}

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t,
		base.TestServerArgs{
			Knobs: base.TestingKnobs{JobsTestingKnobs: knobs},
		})
	defer s.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(db)
	runner.Exec(t, "CREATE TABLE defaultdb.foo(a int)")

	// Create a one off job which writes some values into 'foo' table.
	schedule := NewScheduledJob(scheduledjobs.ProdJobSchedulerEnv)
	schedule.SetScheduleLabel("test schedule")
	schedule.SetOwner(security.TestUserName())
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

type txnConflictExecutor struct {
	beforeUpdate, proceed chan struct{}
}

func (e *txnConflictExecutor) ExecuteJob(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
	txn *kv.Txn,
) error {
	// Read number of rows -- this count will be used when updating
	// a single row in the table.
	row, err := cfg.InternalExecutor.QueryRow(
		ctx, "txn-executor", txn, "SELECT count(*) FROM defaultdb.foo")
	if err != nil {
		return err
	}
	cnt := int(tree.MustBeDInt(row[0]))
	if e.beforeUpdate != nil {
		// Wait to be signaled.
		e.beforeUpdate <- struct{}{}
		<-e.proceed
		e.beforeUpdate = nil
		e.proceed = nil
	}

	// Try updating.
	_, err = cfg.InternalExecutor.Exec(
		ctx, "txn-executor", txn, "UPDATE defaultdb.foo SET b=b+$1 WHERE a=1", cnt)
	return err
}

func (e *txnConflictExecutor) NotifyJobTermination(
	ctx context.Context,
	jobID jobspb.JobID,
	jobStatus Status,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
	ex sqlutil.InternalExecutor,
	txn *kv.Txn,
) error {
	return nil
}

func (e *txnConflictExecutor) Metrics() metric.Struct {
	return nil
}

var _ ScheduledJobExecutor = (*txnConflictExecutor)(nil)

func TestTransientTxnErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	h, cleanup := newTestHelper(t)
	defer cleanup()
	ctx := context.Background()

	h.sqlDB.Exec(t, `
CREATE TABLE defaultdb.foo(a int primary key, b int);
INSERT INTO defaultdb.foo VALUES(1, 1)
`)

	const execName = "test-executor"
	ex := &txnConflictExecutor{
		beforeUpdate: make(chan struct{}),
		proceed:      make(chan struct{}),
	}
	defer registerScopedScheduledJobExecutor(execName, ex)()

	// Setup schedule with our test executor.
	schedule := NewScheduledJob(h.env)
	schedule.SetScheduleLabel("test schedule")
	schedule.SetOwner(security.TestUserName())
	nextRun := h.env.Now().Add(time.Hour)
	schedule.SetNextRun(nextRun)
	schedule.SetExecutionDetails(execName, jobspb.ExecutionArguments{})
	require.NoError(t, schedule.Create(
		ctx, h.cfg.InternalExecutor, nil))

	// Execute schedule on another thread.
	g := ctxgroup.WithContext(context.Background())
	g.Go(func() error {
		return h.cfg.DB.Txn(context.Background(),
			func(ctx context.Context, txn *kv.Txn) error {
				err := h.execSchedules(ctx, allSchedules, txn)
				return err
			},
		)
	})

	require.NoError(t,
		h.cfg.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
			// Let schedule start running, and wait for it to be ready to update.
			h.env.SetTime(nextRun.Add(time.Second))
			<-ex.beforeUpdate

			// Before we let schedule proceed, update the number of rows in the table.
			// This should cause transaction in schedule to restart, but we don't
			// expect to see any errors in the schedule status.
			if _, err := h.cfg.InternalExecutor.Exec(ctx, "update-a", txn,
				`UPDATE defaultdb.foo SET b=3 WHERE a=1`); err != nil {
				return err
			}
			if _, err := h.cfg.InternalExecutor.Exec(ctx, "add-row", txn,
				`INSERT INTO defaultdb.foo VALUES (123, 123)`); err != nil {
				return err
			}
			ex.proceed <- struct{}{}
			return nil
		}))

	err := g.Wait()
	require.True(t, errors.HasType(err, &savePointError{}))

	// Reload schedule -- verify it doesn't have any errors in its status.
	updated := h.loadSchedule(t, schedule.ScheduleID())
	require.Equal(t, "", updated.ScheduleStatus())
}
