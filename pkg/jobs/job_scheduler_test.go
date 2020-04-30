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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/gorhill/cronexpr"
	"github.com/stretchr/testify/require"
)

func addFakeJob(t *testing.T, h *testHelper, id int64, status Status, txn *kv.Txn) {
	payload := []byte("fake payload")
	n, err := h.ex.ExecEx(context.Background(), "fake-job", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		fmt.Sprintf("INSERT INTO %s (schedule_id, status, payload) VALUES ($1, $2, $3)",
			h.env.SystemJobsTableName()),
		id, status, payload,
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
			// Create job that with target wait behavior.
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
			expectedRunTime := cronexpr.MustParse("@hourly").Next(h.env.now)
			loaded := h.loadJob(t, j.ScheduleID())
			require.Equal(t, expectedRunTime, loaded.NextRun())

			// Advance time past the expected start time.
			h.env.now = expectedRunTime.Add(time.Second)

			// The job should not run -- it should be rescheduled `recheckJobAfter` time in the
			// future.
			c := newJobScheduler(h.env, h.kvDB, h.ex)
			require.NoError(t, c.findJobs(ctx, nil))

			if wait == jobspb.ScheduleDetails_WAIT {
				expectedRunTime = h.env.now.Add(recheckRunningAfter)
			} else {
				expectedRunTime = cronexpr.MustParse("@hourly").Next(h.env.now)
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
	expectedRunTime := cronexpr.MustParse("@hourly").Next(h.env.now)
	loaded := h.loadJob(t, j.ScheduleID())
	require.Equal(t, expectedRunTime, loaded.NextRun())

	// Advance time past the expected start time.
	h.env.now = expectedRunTime.Add(time.Second)

	// Execute the job and verify it has the next run scheduled.
	c := newJobScheduler(h.env, h.kvDB, h.ex)
	require.NoError(t, c.findJobs(ctx, nil))

	expectedRunTime = cronexpr.MustParse("@hourly").Next(h.env.now)
	loaded = h.loadJob(t, j.ScheduleID())
	require.Equal(t, expectedRunTime, loaded.NextRun())
}
