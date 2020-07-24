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
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestScheduleControl(t *testing.T) {
	defer leaktest.AfterTest(t)()
	th, cleanup := newTestHelper(t)
	defer cleanup()

	// Inject our test environment into schedule control execution via testing knobs.
	th.cfg.TestingKnobs.(*TestingKnobs).JobSchedulerEnv = th.env

	makeWith := func(with ...string) []string {
		return append(with, "")
	}

	t.Run("non-existent", func(t *testing.T) {
		for _, tc := range []struct {
			command string
			with    []string
		}{
			{"PAUSE SCHEDULE 123", makeWith("CANCEL", "PAUSE")},
			{"PAUSE SCHEDULES SELECT 123", makeWith("CANCEL", "PAUSE")},
			{"RESUME SCHEDULE 123", makeWith("RESUME")},
			{"RESUME SCHEDULES SELECT schedule_id FROM system.scheduled_jobs", makeWith("RESUME")},
			{"DROP SCHEDULE 123", makeWith("CANCEL")},
			{"DROP SCHEDULES SELECT schedule_id FROM system.scheduled_jobs", makeWith("CANCEL")},
		} {
			for _, with := range tc.with {
				var withClause string
				if with != "" {
					withClause = fmt.Sprintf("WITH %s JOBS", with)
				}
				t.Run(fmt.Sprintf("%s-%s", tc.command, withClause), func(t *testing.T) {
					th.sqlDB.ExecRowsAffected(t, 0, fmt.Sprintf("%s %s", tc.command, withClause))
				})
			}
		}
	})

	ctx := context.Background()

	t.Run("cannot-resume-one-off-schedule", func(t *testing.T) {
		schedule := th.newScheduledJob(t, "test schedule", "select 42")
		require.NoError(t, schedule.Create(ctx, th.cfg.InternalExecutor, nil))

		th.sqlDB.ExpectErr(t, "cannot set next run for schedule",
			"RESUME SCHEDULE $1", schedule.ScheduleID())
		th.sqlDB.ExpectErr(t, "cannot set next run for schedule",
			"RESUME SCHEDULES SELECT $1 WITH RESUME JOBS", schedule.ScheduleID())
	})

	type testCase struct {
		name         string
		numSchedules int
		jobStatuses  []Status
	}

	t.Run("pause-schedule", func(t *testing.T) {
		for _, tc := range []testCase{
			{
				name:         "simple-pause",
				numSchedules: 1,
			},
			// {
			// 	name:         "pause-many",
			// 	numSchedules: 3,
			// 	jobStatuses:  []Status{StatusRunning, StatusRunning, StatusReverting, StatusCanceled, StatusPaused},
			// },
		} {
			t.Run(tc.name, func(t *testing.T) {
				var scheduleIDs []int64
				var expectedJobStatuses [][]string
				var numRunningJobs int

				for i := 0; i < tc.numSchedules; i++ {
					schedule := th.newScheduledJob(t, fmt.Sprintf("%s-%d", tc.name, i), "sql")
					require.NoError(t, schedule.SetSchedule("@daily"))
					require.NoError(t, schedule.Create(ctx, th.cfg.InternalExecutor, nil))

					// Create jobs for each schedule.
					for _, status := range tc.jobStatuses {
						id := addFakeJob(t, th, schedule.ScheduleID(), status, nil)
						expectedStatus := status
						if status == StatusRunning {
							numRunningJobs++
							expectedStatus = StatusPaused
						}

						expectedJobStatuses = append(
							expectedJobStatuses, []string{fmt.Sprintf("%d", id), string(expectedStatus)})
					}
					scheduleIDs = append(scheduleIDs, schedule.ScheduleID())
				}

				sort.Slice(expectedJobStatuses, func(i, j int) bool {
					return expectedJobStatuses[i][0] < expectedJobStatuses[j][0]
				})

				th.sqlDB.Exec(
					t,
					fmt.Sprintf(
						"PAUSE SCHEDULES SELECT schedule_id FROM %s WHERE schedule_name LIKE '%s-%%'",
						th.env.ScheduledJobsTableName(), tc.name),
				)
			})

		}
	})
}
