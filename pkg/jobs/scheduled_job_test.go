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

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/gorhill/cronexpr"
	"github.com/stretchr/testify/require"
)

func TestCreateCronJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h, cleanup := newTestHelper(t)
	defer cleanup()

	j := h.newScheduledJob("test_job", "test sql")
	j.schedExpr.Set("@daily")
	require.NoError(t,
		h.kvDB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) (err error) {
			return j.Create(ctx, h.ex, txn)
		}))
	require.True(t, j.schedID.Value() > 0)
}

func TestSetsSchedule(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h, cleanup := newTestHelper(t)
	defer cleanup()

	j := h.newScheduledJob("test_job", "test sql")

	// Set job schedule to run "@daily" -- i.e. at midnight.
	require.NoError(t, j.SetSchedule("@daily"))

	// The job is expected to run at midnight the next day.
	// We want to ensure nextRun correctly persisted in the cron table.
	expectedNextRun := h.env.now.Truncate(24 * time.Hour).Add(24 * time.Hour)

	require.NoError(t,
		h.kvDB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) (err error) {
			return j.Create(ctx, h.ex, txn)
		}))

	loaded := h.loadJob(t, j.schedID.Value())
	require.Equal(t, j.schedID, loaded.schedID)
	require.Equal(t, "@daily", loaded.schedExpr.Value())
	require.Equal(t, expectedNextRun, loaded.nextRun.Value())
}

func TestCreateOneOffJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h, cleanup := newTestHelper(t)
	defer cleanup()

	j := h.newScheduledJob("test_job", "test sql")
	j.nextRun.Set(timeutil.Now())

	require.NoError(t,
		h.kvDB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) (err error) {
			return j.Create(ctx, h.ex, txn)
		}))
	require.True(t, j.schedID.Value() > 0)

	loaded := h.loadJob(t, j.schedID.Value())
	require.Equal(t, loaded.nextRun.Value(), j.nextRun.Value())
	require.Equal(t, "", loaded.schedExpr.Value())
}

func TestPauseUnpauseJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h, cleanup := newTestHelper(t)
	defer cleanup()

	j := h.newScheduledJob("test_job", "test sql")
	j.schedExpr.Set("@daily")
	require.NoError(t,
		h.kvDB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) (err error) {
			return j.Create(ctx, h.ex, txn)
		}))

	// Pause and save.
	j.Pause("just because")
	require.NoError(t,
		h.kvDB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) (err error) {
			return j.Update(ctx, h.ex, txn)
		}))

	// Verify job is paused
	loaded := h.loadJob(t, j.schedID.Value())
	// Paused jobs have next run time set to NULL
	require.Equal(t, time.Time{}, loaded.nextRun.Value())
	ci := loaded.changeInfo.Mutable()
	require.Equal(t, 1, len(ci.Changes))
	require.Equal(t, "just because", ci.Changes[0].Reason)

	// Un-pausing the job resets next run time.
	require.NoError(t, j.Unpause("we are back"))
	require.NoError(t,
		h.kvDB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) (err error) {
			return j.Update(ctx, h.ex, txn)
		}))

	// Verify job is no longer paused
	loaded = h.loadJob(t, j.schedID.Value())
	// Paused jobs have next run time set to NULL
	require.NotEqual(t, time.Time{}, loaded.nextRun.Value())
	ci = loaded.changeInfo.Mutable()
	require.Equal(t, 2, len(ci.Changes))
	require.Equal(t, "just because", ci.Changes[0].Reason)
	require.Equal(t, "we are back", ci.Changes[1].Reason)
}

func TestFailedJobsHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h, cleanup := newTestHelper(t)
	defer cleanup()

	var tests = []struct {
		onerror jobspb.ScheduleDetails_ErrorHandlingBehavior
		nextRun time.Time
	}{
		{
			onerror: jobspb.ScheduleDetails_RETRY_SCHED,
			nextRun: cronexpr.MustParse("@daily").Next(h.env.Now()),
		},
		{
			onerror: jobspb.ScheduleDetails_RETRY_SOON,
			nextRun: h.env.Now().Add(retryFailedJobAfter),
		},
		{
			onerror: jobspb.ScheduleDetails_PAUSE_SCHED,
			nextRun: time.Time{}.UTC(),
		},
	}

	for _, test := range tests {
		t.Run(test.onerror.String(), func(t *testing.T) {
			j := h.newScheduledJob("test_job", "test sql")
			require.NoError(t, j.SetSchedule("@daily"))
			j.execSpec.Mutable().OnError = test.onerror

			ctx := context.Background()
			require.NoError(t,
				h.kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
					return j.Create(ctx, h.ex, txn)
				}))

			// Pretend we failed running; we expect job to be rescheduled.
			md := &JobMetadata{
				ID:      123,
				Status:  "failed",
				Payload: &jobspb.Payload{ScheduleID: j.schedID.Value()},
			}

			require.NoError(t,
				h.kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
					return NotifyJobCompletion(ctx, h.env, md, h.ex, txn)
				}))

			// Verify nextRun updated
			loaded := h.loadJob(t, j.schedID.Value())
			require.Equal(t, test.nextRun, loaded.nextRun.Value())
		})
	}
}
