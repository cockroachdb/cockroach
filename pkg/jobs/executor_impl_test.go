// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestInlineExecutorFailedJobsHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	argsFn := func(args *base.TestServerArgs) {
		args.Knobs.JobsTestingKnobs = NewTestingKnobsWithShortIntervals()
	}

	h, cleanup := newTestHelperWithServerArgs(t, argsFn)
	defer cleanup()

	var tests = []struct {
		onError         jobspb.ScheduleDetails_ErrorHandlingBehavior
		expectedNextRun time.Time
	}{
		{
			onError:         jobspb.ScheduleDetails_RETRY_SCHED,
			expectedNextRun: cronMustParse(t, "@daily").Next(h.env.Now()).Round(time.Microsecond),
		},
		{
			onError:         jobspb.ScheduleDetails_RETRY_SOON,
			expectedNextRun: h.env.Now().Add(retryFailedJobAfter).Round(time.Microsecond),
		},
		{
			onError:         jobspb.ScheduleDetails_PAUSE_SCHED,
			expectedNextRun: time.Time{}.UTC(),
		},
	}

	for _, test := range tests {
		t.Run(test.onError.String(), func(t *testing.T) {
			j := h.newScheduledJob(t, "test_job", "test sql")
			j.rec.ExecutorType = InlineExecutorName

			require.NoError(t, j.SetScheduleAndNextRun("@daily"))
			j.SetScheduleDetails(jobstest.AddDummyScheduleDetails(jobspb.ScheduleDetails{OnError: test.onError}))

			ctx := context.Background()
			require.NoError(t, ScheduledJobDB(h.cfg.DB).Create(ctx, j))

			// Pretend we failed running; we expect job to be rescheduled.
			require.NoError(t, h.cfg.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				return NotifyJobTermination(ctx, txn, h.env, 123, StateFailed, nil, j.ScheduleID())
			}))
			// Verify nextRun updated
			loaded := h.loadSchedule(t, j.ScheduleID())
			require.Equal(t, test.expectedNextRun, loaded.NextRun())
		})
	}
}
