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

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/fmtsafe/testdata/src/github.com/cockroachdb/errors"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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
			h.env.now = expectedRunTime.Add(time.Second)

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
	h.env.now = expectedRunTime.Add(time.Second)

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

func TestJobSchedulerDaemonGetWaitPeriod(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sv := settings.Values{}
	schedulerEnabledSetting.Override(&sv, false)

	// When disabled, we wait 5 minutes before rechecking.
	require.True(t, 5*time.Minute == getWaitPeriod(&sv))
	schedulerEnabledSetting.Override(&sv, true)

	// When pace is too low, we use something more reasonable.
	schedulerPaceSetting.Override(&sv, time.Nanosecond)
	require.True(t, minPacePeriod == getWaitPeriod(&sv))

	// Otherwise, we use user specified setting.
	pace := 42 * time.Second
	schedulerPaceSetting.Override(&sv, pace)
	require.True(t, pace == getWaitPeriod(&sv))
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
	defer func(f func(_ *settings.Values) time.Duration) {
		getWaitPeriod = f
	}(getWaitPeriod)

	getWaitPeriod = func(_ *settings.Values) time.Duration {
		return 10 * time.Millisecond
	}
	defer func(f func() time.Duration) {
		getInitialScanDelay = f
	}(getInitialScanDelay)
	getInitialScanDelay = func() time.Duration { return 0 }

	stopper := stop.NewStopper()
	sv := settings.Values{}
	StartJobSchedulerDaemon(ctx, stopper, &sv, h.env, h.kvDB, h.ex)

	// Advance our fake time 1 hour forward (plus a bit)
	h.env.now = h.env.now.Add(time.Hour + time.Second)

	expected := [][]string{
		{fmt.Sprintf("%d", scheduleIDs[0]), "NULL"},
		{fmt.Sprintf("%d", scheduleIDs[1]), "NULL"},
		{fmt.Sprintf("%d", scheduleIDs[2]), "NULL"},
		{fmt.Sprintf("%d", scheduleIDs[3]), "NULL"},
		{fmt.Sprintf("%d", scheduleIDs[4]), "NULL"},
	}
	query := fmt.Sprintf("SELECT schedule_id, next_run FROM %s", h.env.scheduledJobsTableName)

	testutils.SucceedsSoon(t, func() error {
		results := h.sqlDB.QueryStr(t, query)
		if reflect.DeepEqual(expected, results) {
			return nil
		}
		return errors.Newf("still waiting for matching jobs: res=%+v", results)
	})

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
	scheduleRunTimeStr := scheduleRunTime.Format(timestampTZWithTZLayout)
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
	defer func(f func(_ *settings.Values) time.Duration) {
		getWaitPeriod = f
	}(getWaitPeriod)

	getWaitPeriod = func(_ *settings.Values) time.Duration {
		return time.Hour
	}

	defer func(f func() time.Duration) {
		getInitialScanDelay = f
	}(getInitialScanDelay)
	getInitialScanDelay = func() time.Duration { return 0 }

	// Advance our fake time 1 hour forward (plus a bit) so that the daemon finds matching jobs.
	h.env.now = h.env.now.Add(time.Hour + time.Second)

	stopper := stop.NewStopper()
	sv := settings.Values{}
	schedulerMaxJobsPerIterationSetting.Override(&sv, 2)
	StartJobSchedulerDaemon(ctx, stopper, &sv, h.env, h.kvDB, h.ex)

	// We expect the first 2 jobs to be executed.
	expected := [][]string{
		{fmt.Sprintf("%d", scheduleIDs[0]), "NULL"},
		{fmt.Sprintf("%d", scheduleIDs[1]), "NULL"},
		{fmt.Sprintf("%d", scheduleIDs[2]), scheduleRunTimeStr},
		{fmt.Sprintf("%d", scheduleIDs[3]), scheduleRunTimeStr},
		{fmt.Sprintf("%d", scheduleIDs[4]), scheduleRunTimeStr},
	}
	query := fmt.Sprintf("SELECT schedule_id, next_run FROM %s", h.env.scheduledJobsTableName)

	testutils.SucceedsSoon(t, func() error {
		results := h.sqlDB.QueryStr(t, query)
		if reflect.DeepEqual(expected, results) {
			return nil
		}
		return errors.Newf("still waiting for matching jobs: res=%+v expected=%+v", results, expected)
	})

	stopper.Stop(ctx)
}
