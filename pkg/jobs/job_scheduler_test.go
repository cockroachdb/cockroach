// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
	cron "github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
)

func cronMustParse(t *testing.T, s string) cron.Schedule {
	e, err := cron.ParseStandard(s)
	require.NoError(t, err)
	return e
}

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
			details := j.ScheduleDetails()
			details.Wait = wait
			j.SetScheduleDetails(*details)
			require.NoError(t, j.SetScheduleAndNextRun("@hourly"))

			require.NoError(t,
				h.cfg.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
					schedules := ScheduledJobTxn(txn)
					require.NoError(t, schedules.Create(ctx, j))

					// Lets add few fake runs for this schedule, including terminal and
					// non terminal states.
					for _, state := range []State{
						StateRunning, StateFailed, StateCanceled, StateSucceeded, StatePaused} {
						_ = addFakeJob(t, h, j.ScheduleID(), state, txn)
					}
					return nil
				}))

			// Verify the job has expected nextRun time.
			expectedRunTime := cronMustParse(t, "@hourly").Next(h.env.Now())
			loaded := h.loadSchedule(t, j.ScheduleID())
			require.Equal(t, expectedRunTime, loaded.NextRun())

			// Advance time past the expected start time.
			h.env.SetTime(expectedRunTime.Add(time.Second))

			// The job should not run -- it should be rescheduled `recheckJobAfter` time in the
			// future.
			s := newJobScheduler(h.cfg, h.env, metric.NewRegistry())
			require.NoError(t, s.executeSchedules(ctx, allSchedules))

			if wait == jobspb.ScheduleDetails_WAIT {
				expectedRunTime = h.env.Now().Add(recheckRunningAfter)
			} else {
				expectedRunTime = cronMustParse(t, "@hourly").Next(h.env.Now())
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
			j.SetScheduleDetails(jobstest.AddDummyScheduleDetails(jobspb.ScheduleDetails{Wait: wait}))
			require.NoError(t, j.SetScheduleAndNextRun("@hourly"))

			require.NoError(t,
				h.cfg.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
					schedules := ScheduledJobTxn(txn)
					require.NoError(t, schedules.Create(ctx, j))

					// Let's add few fake runs for this schedule which are in every
					// terminal state.
					for _, state := range []State{StateFailed, StateCanceled, StateSucceeded} {
						_ = addFakeJob(t, h, j.ScheduleID(), state, txn)
					}
					return nil
				}))

			// Verify the job has expected nextRun time.
			expectedRunTime := cronMustParse(t, "@hourly").Next(h.env.Now())
			loaded := h.loadSchedule(t, j.ScheduleID())
			require.Equal(t, expectedRunTime, loaded.NextRun())

			// Advance time past the expected start time.
			h.env.SetTime(expectedRunTime.Add(time.Second))

			// Execute the job and verify it has the next run scheduled.
			s := newJobScheduler(h.cfg, h.env, metric.NewRegistry())
			require.NoError(t, s.executeSchedules(ctx, allSchedules))

			expectedRunTime = cronMustParse(t, "@hourly").Next(h.env.Now())
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
	require.NoError(t, j.SetScheduleAndNextRun("@hourly"))

	require.NoError(t,
		h.cfg.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			schedules := ScheduledJobTxn(txn)
			require.NoError(t, schedules.Create(ctx, j))
			return nil
		}))

	// Verify the job has expected nextRun time.
	expectedRunTime := cronMustParse(t, "@hourly").Next(h.env.Now())
	loaded := h.loadSchedule(t, j.ScheduleID())
	require.Equal(t, expectedRunTime, loaded.NextRun())

	// Advance time past the expected start time.
	h.env.SetTime(expectedRunTime.Add(time.Second))

	// Execute the job and verify it has the next run scheduled.
	s := newJobScheduler(h.cfg, h.env, metric.NewRegistry())
	require.NoError(t, s.executeSchedules(ctx, allSchedules))

	expectedRunTime = cronMustParse(t, "@hourly").Next(h.env.Now())
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

func getScopedSettings() *settings.Values {
	sv := &settings.Values{}
	sv.Init(context.Background(), nil)
	return sv
}

func TestJobSchedulerDaemonGetWaitPeriod(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	sv := getScopedSettings()

	noJitter := func(d time.Duration) time.Duration { return d }

	schedulerEnabledSetting.Override(ctx, sv, false)

	// When disabled, we wait 5 minutes before rechecking.
	require.EqualValues(t, 5*time.Minute, getWaitPeriod(ctx, sv, noJitter, nil))
	schedulerEnabledSetting.Override(ctx, sv, true)

	// When pace is too low, we use something more reasonable.
	schedulerPaceSetting.Override(ctx, sv, time.Nanosecond)
	require.EqualValues(t, minPacePeriod, getWaitPeriod(ctx, sv, noJitter, nil))

	// Otherwise, we use user specified setting.
	pace := 42 * time.Second
	schedulerPaceSetting.Override(ctx, sv, pace)
	require.EqualValues(t, pace, getWaitPeriod(ctx, sv, noJitter, nil))
}

type recordScheduleExecutor struct {
	executed []jobspb.ScheduleID
}

func (n *recordScheduleExecutor) ExecuteJob(
	ctx context.Context,
	txn isql.Txn,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
) error {
	n.executed = append(n.executed, schedule.ScheduleID())
	return nil
}

func (n *recordScheduleExecutor) NotifyJobTermination(
	ctx context.Context,
	txn isql.Txn,
	jobID jobspb.JobID,
	jobState State,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
) error {
	return nil
}

func (n *recordScheduleExecutor) Metrics() metric.Struct {
	return nil
}

func (n *recordScheduleExecutor) GetCreateScheduleStatement(
	ctx context.Context, txn isql.Txn, env scheduledjobs.JobSchedulerEnv, sj *ScheduledJob,
) (string, error) {
	return "", errors.AssertionFailedf("unimplemented method: 'GetCreateScheduleStatement'")
}

var _ ScheduledJobExecutor = &recordScheduleExecutor{}

func fastDaemonKnobs(scanDelay func() time.Duration) *TestingKnobs {
	knobs := NewTestingKnobsWithShortIntervals()
	knobs.SchedulerDaemonInitialScanDelay = func() time.Duration { return 0 }
	knobs.SchedulerDaemonScanDelay = scanDelay
	return knobs
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

	stopper := stop.NewStopper(stop.WithTracer(h.server.TracerI().(*tracing.Tracer)))
	getWaitPeriodCalled := make(chan struct{})

	knobs := fastDaemonKnobs(func() time.Duration {
		// Disable daemon
		schedulerEnabledSetting.Override(ctx, &h.cfg.Settings.SV, false)

		// Before we return, create a job which should not be executed
		// (since the daemon is disabled).  We use our special executor
		// to verify this.
		schedule := h.newScheduledJobForExecutor("test_job", executorName, nil)
		schedule.SetNextRun(h.env.Now())
		schedules := ScheduledJobDB(h.cfg.DB)
		require.NoError(t, schedules.Create(ctx, schedule))

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
	id      jobspb.ScheduleID
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
	var scheduleIDs []jobspb.ScheduleID
	schedules := ScheduledJobDB(h.cfg.DB)
	for i := 0; i < numJobs; i++ {
		schedule := h.newScheduledJob(t, "test_job", "SELECT 42")
		schedule.SetNextRun(scheduleRunTime)
		require.NoError(t, schedules.Create(ctx, schedule))
		scheduleIDs = append(scheduleIDs, schedule.ScheduleID())
	}

	// Sort by schedule ID.
	sort.Slice(scheduleIDs, func(i, j int) bool { return scheduleIDs[i] < scheduleIDs[j] })

	// Make daemon run fast.
	h.cfg.TestingKnobs = fastDaemonKnobs(overridePaceSetting(10 * time.Millisecond))

	// Start daemon.
	stopper := stop.NewStopper(stop.WithTracer(h.server.TracerI().(*tracing.Tracer)))
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
	var scheduleIDs []jobspb.ScheduleID
	schedules := ScheduledJobDB(h.cfg.DB)
	for i := 0; i < numJobs; i++ {
		schedule := h.newScheduledJob(t, "test_job", "SELECT 42")
		schedule.SetNextRun(scheduleRunTime)
		require.NoError(t, schedules.Create(ctx, schedule))
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
	stopper := stop.NewStopper(stop.WithTracer(h.server.TracerI().(*tracing.Tracer)))
	daemon := newJobScheduler(h.cfg, h.env, metric.NewRegistry())
	daemon.runDaemon(ctx, stopper)

	readyToRunStmt := fmt.Sprintf(
		"SELECT count(*) FROM %s WHERE next_run < %s",
		h.env.ScheduledJobsTableName(), h.env.NowExpr())
	testutils.SucceedsSoon(t, func() error {
		var ready int
		h.sqlDB.QueryRow(t, readyToRunStmt).Scan(&ready)
		if ready != numJobs-jobsPerIteration {
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
	ctx context.Context,
	txn isql.Txn,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
) error {
	e.numCalls++
	return errors.Newf("error for schedule %d", schedule.ScheduleID())
}

func (e *returnErrorExecutor) NotifyJobTermination(
	ctx context.Context,
	txn isql.Txn,
	jobID jobspb.JobID,
	jobState State,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
) error {
	return nil
}

func (e *returnErrorExecutor) Metrics() metric.Struct {
	return nil
}

func (e *returnErrorExecutor) GetCreateScheduleStatement(
	ctx context.Context, txn isql.Txn, env scheduledjobs.JobSchedulerEnv, sj *ScheduledJob,
) (string, error) {
	return "", errors.AssertionFailedf("unimplemented method: 'GetCreateScheduleStatement'")
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
	schedules := ScheduledJobDB(h.cfg.DB)
	for i := 0; i < numJobs; i++ {
		s := h.newScheduledJobForExecutor("schedule", executorName, nil)
		s.SetNextRun(scheduleRunTime)
		require.NoError(t, schedules.Create(ctx, s))
	}
	h.env.SetTime(scheduleRunTime.Add(time.Second))
	daemon := newJobScheduler(h.cfg, h.env, metric.NewRegistry())
	require.NoError(t, daemon.executeSchedules(ctx, numJobs))

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
	schedules := ScheduledJobDB(h.cfg.DB)
	require.NoError(t, schedules.Create(ctx, schedule))

	startTime := h.env.Now()
	execTime := startTime.Add(time.Hour).Add(time.Second)

	cron := cronMustParse(t, "@hourly")

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
			schedule.SetScheduleDetails(jobstest.AddDummyScheduleDetails(jobspb.ScheduleDetails{OnError: tc.onError}))
			require.NoError(t, schedule.SetScheduleAndNextRun("@hourly"))
			require.NoError(t, schedules.Update(ctx, schedule))

			h.env.SetTime(execTime)
			require.NoError(t, daemon.executeSchedules(ctx, 1))

			loaded := h.loadSchedule(t, schedule.ScheduleID())
			require.EqualValues(t, tc.nextRun, loaded.NextRun())
		})
	}
}

func TestJobSchedulerDaemonUsesSystemTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	schedules := ScheduledJobDB(s.InternalDB().(isql.DB))
	runner := sqlutils.MakeSQLRunner(db)
	runner.Exec(t, "CREATE TABLE defaultdb.foo(a int)")

	// Create a one off job which writes some values into 'foo' table.
	schedule := NewScheduledJob(scheduledjobs.ProdJobSchedulerEnv)
	schedule.SetScheduleLabel("test schedule")
	schedule.SetOwner(username.TestUserName())
	schedule.SetNextRun(timeutil.Now())
	schedule.SetScheduleDetails(jobstest.AddDummyScheduleDetails(jobspb.ScheduleDetails{}))
	any, err := types.MarshalAny(
		&jobspb.SqlStatementExecutionArg{Statement: "INSERT INTO defaultdb.foo VALUES (1), (2), (3)"})
	require.NoError(t, err)
	schedule.SetExecutionDetails(InlineExecutorName, jobspb.ExecutionArguments{Args: any})
	require.NoError(t, schedules.Create(ctx, schedule))

	// Verify the schedule ran.
	testutils.SucceedsSoon(t, func() error {
		var count int
		if err := db.QueryRow(
			"SELECT count(*) FROM defaultdb.foo").Scan(&count); err != nil || count != 3 {
			return errors.Newf("expected 3 rows, got %d (err=%+v)", count, err) // nolint:errwrap
		}
		return nil
	})
}

type txnConflictExecutor struct {
	beforeUpdate, proceed chan struct{}
}

func (e *txnConflictExecutor) ExecuteJob(
	ctx context.Context,
	txn isql.Txn,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
) error {
	// Read number of rows -- this count will be used when updating
	// a single row in the table.
	row, err := txn.QueryRow(
		ctx, "txn-executor", txn.KV(), "SELECT count(*) FROM defaultdb.foo")
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
	_, err = txn.Exec(
		ctx, "txn-executor", txn.KV(), "UPDATE defaultdb.foo SET b=b+$1 WHERE a=1", cnt)
	return err
}

func (e *txnConflictExecutor) NotifyJobTermination(
	ctx context.Context,
	txn isql.Txn,
	jobID jobspb.JobID,
	jobStatus State,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
) error {
	return nil
}

func (e *txnConflictExecutor) Metrics() metric.Struct {
	return nil
}

func (e *txnConflictExecutor) GetCreateScheduleStatement(
	ctx context.Context, txn isql.Txn, env scheduledjobs.JobSchedulerEnv, sj *ScheduledJob,
) (string, error) {
	return "", errors.AssertionFailedf("unimplemented method: 'GetCreateScheduleStatement'")
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

	schedules := ScheduledJobDB(h.cfg.DB)
	// Setup schedule with our test executor.
	schedule := NewScheduledJob(h.env)
	schedule.SetScheduleLabel("test schedule")
	schedule.SetOwner(username.TestUserName())
	nextRun := h.env.Now().Add(time.Hour)
	schedule.SetNextRun(nextRun)
	schedule.SetExecutionDetails(execName, jobspb.ExecutionArguments{})
	schedule.SetScheduleDetails(jobstest.AddDummyScheduleDetails(jobspb.ScheduleDetails{}))
	require.NoError(t, schedules.Create(ctx, schedule))

	// Execute schedule on another thread.
	g := ctxgroup.WithContext(context.Background())
	ready := make(chan struct{})
	g.GoCtx(func(ctx context.Context) error {
		<-ready
		return h.execSchedules(ctx, allSchedules)
	})

	require.NoError(t,
		h.cfg.DB.Txn(context.Background(), func(ctx context.Context, txn isql.Txn) error {
			// Let schedule start running, and wait for it to be ready to update.
			h.env.SetTime(nextRun.Add(time.Second))
			close(ready)
			<-ex.beforeUpdate

			// Before we let schedule proceed, update the number of rows in the table.
			// This should cause transaction in schedule to restart, but we don't
			// expect to see any errors in the schedule status.
			if _, err := txn.Exec(ctx, "update-a", txn.KV(),
				`UPDATE defaultdb.foo SET b=3 WHERE a=1`); err != nil {
				return err
			}
			if _, err := txn.Exec(ctx, "add-row", txn.KV(),
				`INSERT INTO defaultdb.foo VALUES (123, 123)`); err != nil {
				return err
			}
			ex.proceed <- struct{}{}
			return nil
		}))

	require.NoError(t, g.Wait())

	// Reload schedule -- verify it doesn't have any errors in its status.
	updated := h.loadSchedule(t, schedule.ScheduleID())
	require.Equal(t, "", updated.ScheduleStatus())
}

type blockUntilCancelledExecutor struct {
	once          sync.Once
	started, done chan struct{}
}

var _ ScheduledJobExecutor = (*blockUntilCancelledExecutor)(nil)

func (e *blockUntilCancelledExecutor) ExecuteJob(
	ctx context.Context,
	txn isql.Txn,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
) error {
	done := func() {}
	e.once.Do(func() {
		close(e.started)
		done = func() { close(e.done) }
	})
	defer done()
	<-ctx.Done()
	return ctx.Err()
}

func (e *blockUntilCancelledExecutor) NotifyJobTermination(
	ctx context.Context,
	txn isql.Txn,
	jobID jobspb.JobID,
	jobState State,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	schedule *ScheduledJob,
) error {
	return nil
}

func (e *blockUntilCancelledExecutor) Metrics() metric.Struct {
	return nil
}

func (e *blockUntilCancelledExecutor) GetCreateScheduleStatement(
	ctx context.Context, txn isql.Txn, env scheduledjobs.JobSchedulerEnv, sj *ScheduledJob,
) (string, error) {
	return "", errors.AssertionFailedf("unexpected GetCreateScheduleStatement call")
}

func readWithTimeout(t *testing.T, ch chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout")
	}
}

func TestDisablingSchedulerCancelsSchedules(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const executorName = "block-until-cancelled-executor"
	ex := &blockUntilCancelledExecutor{
		started: make(chan struct{}),
		done:    make(chan struct{}),
	}
	defer registerScopedScheduledJobExecutor(executorName, ex)()

	knobs := base.TestingKnobs{
		JobsTestingKnobs: fastDaemonKnobs(overridePaceSetting(10 * time.Millisecond)),
	}
	ts := serverutils.StartServerOnly(t, base.TestServerArgs{Knobs: knobs})
	defer ts.Stopper().Stop(context.Background())

	schedules := ScheduledJobDB(ts.InternalDB().(isql.DB))

	// Create schedule which blocks until its context cancelled due to disabled scheduler.
	// We only need to create one schedule.  This is because
	// scheduler executes its batch of schedules sequentially, and so, creating more
	// than one doesn't change anything since we block.
	schedule := NewScheduledJob(scheduledjobs.ProdJobSchedulerEnv)
	schedule.SetScheduleLabel("test schedule")
	schedule.SetOwner(username.TestUserName())
	schedule.SetNextRun(timeutil.Now())
	schedule.SetExecutionDetails(executorName, jobspb.ExecutionArguments{})
	schedule.SetScheduleDetails(jobstest.AddDummyScheduleDetails(jobspb.ScheduleDetails{}))
	require.NoError(t, schedules.Create(context.Background(), schedule))

	readWithTimeout(t, ex.started)
	// Disable scheduler and verify all running schedules were cancelled.
	schedulerEnabledSetting.Override(context.Background(), &ts.ClusterSettings().SV, false)
	readWithTimeout(t, ex.done)
}

func TestSchedulePlanningRespectsTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const executorName = "block-until-cancelled-executor"
	ex := &blockUntilCancelledExecutor{
		started: make(chan struct{}),
		done:    make(chan struct{}),
	}
	defer registerScopedScheduledJobExecutor(executorName, ex)()

	knobs := base.TestingKnobs{
		JobsTestingKnobs: fastDaemonKnobs(overridePaceSetting(10 * time.Millisecond)),
	}
	ts := serverutils.StartServerOnly(t, base.TestServerArgs{Knobs: knobs})
	defer ts.Stopper().Stop(context.Background())
	schedules := ScheduledJobDB(ts.InternalDB().(isql.DB))

	// timeout must be long enough to work when running under stress.
	schedulerScheduleExecutionTimeout.Override(
		context.Background(), &ts.ClusterSettings().SV, 100*time.Millisecond)
	// Create schedule which blocks until its context cancelled due to timeout.
	// We only need to create one schedule.  This is because
	// scheduler executes its batch of schedules sequentially, and so, creating more
	// than one doesn't change anything since we block.
	schedule := NewScheduledJob(scheduledjobs.ProdJobSchedulerEnv)
	schedule.SetScheduleLabel("test schedule")
	schedule.SetOwner(username.TestUserName())
	schedule.SetNextRun(timeutil.Now())
	schedule.SetExecutionDetails(executorName, jobspb.ExecutionArguments{})
	schedule.SetScheduleDetails(jobstest.AddDummyScheduleDetails(jobspb.ScheduleDetails{}))
	require.NoError(t, schedules.Create(context.Background(), schedule))

	readWithTimeout(t, ex.started)
	readWithTimeout(t, ex.done)
}
