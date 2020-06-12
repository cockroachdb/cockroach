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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestCreateScheduledJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h, cleanup := newTestHelper(t)
	defer cleanup()

	j := h.newScheduledJob(t, "test_job", "test sql")
	require.NoError(t, j.SetSchedule("@daily"))
	require.NoError(t, j.Create(context.Background(), h.ex, nil))
	require.True(t, j.ScheduleID() > 0)
}

func TestCreatePausedScheduledJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h, cleanup := newTestHelper(t)
	defer cleanup()

	j := h.newScheduledJob(t, "test_job", "test sql")
	require.NoError(t, j.SetSchedule("@daily"))
	j.Pause("this job is paused")
	require.NoError(t, j.Create(context.Background(), h.ex, nil))
	require.True(t, j.ScheduleID() > 0)
	require.True(t, j.NextRun().Equal(time.Time{}))
}

func TestSetsSchedule(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h, cleanup := newTestHelper(t)
	defer cleanup()

	j := h.newScheduledJob(t, "test_job", "test sql")

	// Set job schedule to run "@daily" -- i.e. at midnight.
	require.NoError(t, j.SetSchedule("@daily"))

	// The job is expected to run at midnight the next day.
	// We want to ensure nextRun correctly persisted in the cron table.
	expectedNextRun := h.env.Now().Truncate(24 * time.Hour).Add(24 * time.Hour)

	require.NoError(t, j.Create(context.Background(), h.ex, nil))

	loaded := h.loadJob(t, j.ScheduleID())
	require.Equal(t, j.ScheduleID(), loaded.ScheduleID())
	require.Equal(t, "@daily", loaded.rec.ScheduleExpr)
	require.True(t, loaded.NextRun().Equal(expectedNextRun))
}

func TestCreateOneOffJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h, cleanup := newTestHelper(t)
	defer cleanup()

	j := h.newScheduledJob(t, "test_job", "test sql")
	j.SetNextRun(timeutil.Now())

	require.NoError(t, j.Create(context.Background(), h.ex, nil))
	require.True(t, j.ScheduleID() > 0)

	loaded := h.loadJob(t, j.ScheduleID())
	require.Equal(t, j.NextRun().Round(time.Microsecond), loaded.NextRun())
	require.Equal(t, "", loaded.rec.ScheduleExpr)
}

func TestPauseUnpauseJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h, cleanup := newTestHelper(t)
	defer cleanup()

	ctx := context.Background()
	j := h.newScheduledJob(t, "test_job", "test sql")
	require.NoError(t, j.SetSchedule("@daily"))
	require.NoError(t, j.Create(ctx, h.ex, nil))

	// Pause and save.
	j.Pause("just because")
	require.NoError(t, j.Update(ctx, h.ex, nil))

	// Verify job is paused
	loaded := h.loadJob(t, j.ScheduleID())
	// Paused jobs have next run time set to NULL
	require.True(t, loaded.IsPaused())
	require.Equal(t, 1, len(loaded.rec.ScheduleChanges.Changes))
	require.Equal(t, "just because", loaded.rec.ScheduleChanges.Changes[0].Reason)

	// Un-pausing the job resets next run time.
	require.NoError(t, j.Unpause("we are back"))
	require.NoError(t, j.Update(ctx, h.ex, nil))

	// Verify job is no longer paused
	loaded = h.loadJob(t, j.ScheduleID())
	// Running schedules have nextRun set to non-null value
	require.False(t, loaded.IsPaused())
	require.False(t, loaded.NextRun().Equal(time.Time{}))
	require.Equal(t, 2, len(loaded.rec.ScheduleChanges.Changes))
	require.Equal(t, "just because", loaded.rec.ScheduleChanges.Changes[0].Reason)
	require.Equal(t, "we are back", loaded.rec.ScheduleChanges.Changes[1].Reason)
}
