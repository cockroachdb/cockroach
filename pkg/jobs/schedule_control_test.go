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

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestScheduleControl(t *testing.T) {
	defer leaktest.AfterTest(t)()
	th, cleanup := newTestHelper(t)
	defer cleanup()

	// Inject our test environment into schedule control execution via testing knobs.
	th.cfg.TestingKnobs.(*TestingKnobs).JobSchedulerEnv = th.env

	t.Run("non-existent", func(t *testing.T) {
		for _, command := range []string{
			"PAUSE SCHEDULE 123",
			"PAUSE SCHEDULES SELECT 123",
			"RESUME SCHEDULE 123",
			"RESUME SCHEDULES SELECT schedule_id FROM system.scheduled_jobs",
			"DROP SCHEDULE 123",
			"DROP SCHEDULES SELECT schedule_id FROM system.scheduled_jobs",
		} {
			t.Run(command, func(t *testing.T) {
				th.sqlDB.ExecRowsAffected(t, 0, command)
			})
		}
	})

	ctx := context.Background()

	var recurringNever string

	makeSchedule := func(name string, cron string) int64 {
		schedule := th.newScheduledJob(t, name, "sql")
		if cron != "" {
			require.NoError(t, schedule.SetSchedule(cron))
		}
		require.NoError(t, schedule.Create(ctx, th.cfg.InternalExecutor, nil))
		return schedule.ScheduleID()
	}

	t.Run("pause-one-schedule", func(t *testing.T) {
		scheduleID := makeSchedule("one-schedule", "@daily")
		th.sqlDB.Exec(t, "PAUSE SCHEDULE $1", scheduleID)
		require.True(t, th.loadSchedule(t, scheduleID).IsPaused())
	})

	t.Run("pause-one-off-schedule", func(t *testing.T) {
		scheduleID := makeSchedule("one-schedule", recurringNever)
		th.sqlDB.Exec(t, "PAUSE SCHEDULE $1", scheduleID)
		require.True(t, th.loadSchedule(t, scheduleID).IsPaused())
	})

	t.Run("cannot-resume-one-off-schedule", func(t *testing.T) {
		schedule := th.newScheduledJob(t, "test schedule", "select 42")
		require.NoError(t, schedule.Create(ctx, th.cfg.InternalExecutor, nil))

		th.sqlDB.ExpectErr(t, "cannot set next run for schedule",
			"RESUME SCHEDULE $1", schedule.ScheduleID())
	})

	t.Run("pause-and-resume-one-schedule", func(t *testing.T) {
		scheduleID := makeSchedule("one-schedule", "@daily")
		th.sqlDB.Exec(t, "PAUSE SCHEDULE $1", scheduleID)
		require.True(t, th.loadSchedule(t, scheduleID).IsPaused())
		th.sqlDB.Exec(t, "RESUME SCHEDULE $1", scheduleID)

		schedule := th.loadSchedule(t, scheduleID)
		require.False(t, schedule.IsPaused())
	})

	t.Run("pause-resume-and-drop-many-schedules", func(t *testing.T) {
		var scheduleIDs []int64
		for i := 0; i < 10; i++ {
			scheduleIDs = append(
				scheduleIDs,
				makeSchedule(fmt.Sprintf("pause-resume-many-%d", i), "@daily"),
			)
		}

		querySchedules := "SELECT schedule_id FROM " + th.env.ScheduledJobsTableName() +
			" WHERE schedule_name LIKE 'pause-resume-many-%'"

		th.sqlDB.Exec(t, "PAUSE SCHEDULES "+querySchedules)

		for _, scheduleID := range scheduleIDs {
			require.True(t, th.loadSchedule(t, scheduleID).IsPaused())
		}

		th.sqlDB.Exec(t, "RESUME SCHEDULES "+querySchedules)

		for _, scheduleID := range scheduleIDs {
			require.False(t, th.loadSchedule(t, scheduleID).IsPaused())
		}

		th.sqlDB.Exec(t, "DROP SCHEDULES "+querySchedules)
		require.Equal(t, 0, len(th.sqlDB.QueryStr(t, querySchedules)))
	})
}
