// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestCreateScheduledJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	h, cleanup := newTestHelper(t)
	defer cleanup()

	schedules := ScheduledJobDB(h.cfg.DB)
	j := h.newScheduledJob(t, "test_job", "test sql")
	require.NoError(t, j.SetScheduleAndNextRun("@daily"))
	require.NoError(t, schedules.Create(context.Background(), j))
	require.True(t, j.ScheduleID() > 0)
}

func TestCreatePausedScheduledJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	h, cleanup := newTestHelper(t)
	defer cleanup()

	j := h.newScheduledJob(t, "test_job", "test sql")
	require.NoError(t, j.SetScheduleAndNextRun("@daily"))
	schedules := ScheduledJobDB(h.cfg.DB)
	j.Pause()
	require.NoError(t, schedules.Create(context.Background(), j))
	require.True(t, j.ScheduleID() > 0)
	require.True(t, j.NextRun().Equal(time.Time{}))
}

func TestSetsSchedule(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	h, cleanup := newTestHelper(t)
	defer cleanup()

	j := h.newScheduledJob(t, "test_job", "test sql")

	// Set job schedule to run "@daily" -- i.e. at midnight.
	require.NoError(t, j.SetScheduleAndNextRun("@daily"))

	// The job is expected to run at midnight the next day.
	// We want to ensure nextRun correctly persisted in the cron table.
	expectedNextRun := h.env.Now().Truncate(24 * time.Hour).Add(24 * time.Hour)
	schedules := ScheduledJobDB(h.cfg.DB)
	require.NoError(t, schedules.Create(context.Background(), j))

	loaded := h.loadSchedule(t, j.ScheduleID())
	require.Equal(t, j.ScheduleID(), loaded.ScheduleID())
	require.Equal(t, "@daily", loaded.rec.ScheduleExpr)
	require.True(t, loaded.NextRun().Equal(expectedNextRun))
}

func TestCreateOneOffJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	h, cleanup := newTestHelper(t)
	defer cleanup()

	j := h.newScheduledJob(t, "test_job", "test sql")
	j.SetNextRun(timeutil.Now())

	schedules := ScheduledJobDB(h.cfg.DB)
	require.NoError(t, schedules.Create(context.Background(), j))
	require.True(t, j.ScheduleID() > 0)

	loaded := h.loadSchedule(t, j.ScheduleID())
	require.Equal(t, j.NextRun().Round(time.Microsecond), loaded.NextRun())
	require.Equal(t, "", loaded.rec.ScheduleExpr)
}

func TestPauseUnpauseJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	h, cleanup := newTestHelper(t)
	defer cleanup()

	schedules := ScheduledJobDB(h.cfg.DB)
	ctx := context.Background()
	j := h.newScheduledJob(t, "test_job", "test sql")
	require.NoError(t, j.SetScheduleAndNextRun("@daily"))
	require.NoError(t, schedules.Create(ctx, j))

	// Pause and save.
	j.Pause()
	require.NoError(t, schedules.Update(ctx, j))

	// Verify job is paused
	loaded := h.loadSchedule(t, j.ScheduleID())
	// Paused jobs have next run time set to NULL
	require.True(t, loaded.IsPaused())

	// Un-pausing the job resets next run time.
	require.NoError(t, j.ScheduleNextRun())
	require.NoError(t, schedules.Update(ctx, j))

	// Verify job is no longer paused
	loaded = h.loadSchedule(t, j.ScheduleID())
	// Running schedules have nextRun set to non-null value
	require.False(t, loaded.IsPaused())
	require.False(t, loaded.NextRun().Equal(time.Time{}))
}

// TestScheduleMustHaveClusterVersionAndID tests that the ClusterID and version
// in the cluster details can be accessed. Further it tests that a schedule
// cannot be created without these fields.
func TestScheduleMustHaveClusterVersionAndID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	h, cleanup := newTestHelper(t)
	defer cleanup()

	sj := h.newScheduledJob(t, "test_job", "test sql")
	schedules := ScheduledJobDB(h.cfg.DB)

	// Fail without a Cluster version
	sj.SetScheduleDetails(jobspb.ScheduleDetails{ClusterID: jobstest.DummyClusterID})
	require.ErrorContains(t, schedules.Create(ctx, sj), "scheduled job created without a cluster version")

	// Fail without a Cluster ID
	sj.SetScheduleDetails(jobspb.ScheduleDetails{
		CreationClusterVersion: jobstest.DummyClusterVersion,
		ClusterID:              uuid.UUID{},
	})
	require.ErrorContains(t, schedules.Create(ctx, sj), "scheduled job created without a cluster ID")

	// Succeed with both.
	sj.SetScheduleDetails(jobstest.AddDummyScheduleDetails(jobspb.ScheduleDetails{}))
	require.NoError(t, schedules.Create(ctx, sj))
	loaded := h.loadSchedule(t, sj.ScheduleID())
	require.EqualValues(t, jobstest.DummyClusterID, loaded.ScheduleDetails().ClusterID)
	require.EqualValues(t, jobstest.DummyClusterVersion, loaded.ScheduleDetails().CreationClusterVersion)
}

func TestInitFromDatums(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var sj ScheduledJob

	testCases := []struct {
		Datums []tree.Datum
		Cols   []colinfo.ResultColumn
		Err    string
	}{
		{
			Datums: []tree.Datum{},
			Cols:   []colinfo.ResultColumn{},
			Err:    "no fields initialized",
		},
		{
			Datums: []tree.Datum{
				tree.DNull,
				tree.DNull,
			},
			Cols: []colinfo.ResultColumn{
				{Name: "schedule_id"},
				{Name: "schedule_name"},
			},
			Err: "no fields initialized",
		},
		{
			Datums: []tree.Datum{
				tree.MakeDBool(false),
			},
			Cols: []colinfo.ResultColumn{
				{Name: "schedule_name"},
			},
			Err: `expected "schedule_name" to be *tree.DString got *tree.DBool`,
		},
		{
			Datums: []tree.Datum{
				tree.NewDBytes(tree.DBytes("\x00")),
			},
			Cols: []colinfo.ResultColumn{
				{Name: "execution_args"},
			},
			Err: `proto: ExecutionArguments: illegal tag 0 (wire type 0)`,
		},
	}

	for _, tc := range testCases {
		require.EqualError(t, sj.InitFromDatums(tc.Datums, tc.Cols), tc.Err)
	}

	mustMarshal := func(pb protoutil.Message) *tree.DBytes {
		b, err := protoutil.Marshal(pb)
		require.NoError(t, err)
		return tree.NewDBytes(tree.DBytes(b))
	}

	now := timeutil.Now()

	require.NoError(t, sj.InitFromDatums([]tree.Datum{
		tree.NewDInt(1),
		tree.NewDString("the label"),
		tree.NewDString("cyrilfiggis"),
		tree.MustMakeDTimestampTZ(now, time.Second),
		mustMarshal(&jobspb.ScheduleState{
			Status: "some status",
		}),
		tree.NewDString("this is a cron tab"),
		mustMarshal(&jobspb.ScheduleDetails{
			Wait: jobspb.ScheduleDetails_NO_WAIT,
		}),
		tree.NewDString("executor type"),
		mustMarshal(&jobspb.ExecutionArguments{}),
		tree.DNull,
	}, []colinfo.ResultColumn{
		{Name: "schedule_id"},
		{Name: "schedule_name"},
		{Name: "owner"},
		{Name: "next_run"},
		{Name: "schedule_state"},
		{Name: "schedule_expr"},
		{Name: "schedule_details"},
		{Name: "executor_type"},
		{Name: "execution_args"},
		{Name: "ignored_column"},
	}))

	require.Equal(t, scheduledJobRecord{
		ScheduleID:    1,
		ScheduleLabel: "the label",
		Owner:         username.MakeSQLUsernameFromPreNormalizedString("cyrilfiggis"),
		NextRun:       now.Round(time.Second),
		ScheduleState: jobspb.ScheduleState{Status: "some status"},
		ScheduleExpr:  "this is a cron tab",
		ScheduleDetails: jobspb.ScheduleDetails{
			Wait: jobspb.ScheduleDetails_NO_WAIT,
		},
		ExecutorType:  "executor type",
		ExecutionArgs: jobspb.ExecutionArguments{},
	}, sj.rec)
	// Assert that scheduledTime was initialized appropriately.
	require.Equal(t, now.Round(time.Second), sj.ScheduledRunTime())
}
