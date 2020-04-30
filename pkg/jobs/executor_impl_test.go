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
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/gorhill/cronexpr"
	"github.com/stretchr/testify/require"
)

func TestInlineExecutorFailedJobsHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h, cleanup := newTestHelper(t)
	defer cleanup()

	var tests = []struct {
		onerror jobspb.ScheduleDetails_ErrorHandlingBehavior
		nextRun time.Time
	}{
		{
			onerror: jobspb.ScheduleDetails_RETRY_SCHED,
			nextRun: cronexpr.MustParse("@daily").Next(h.env.Now()).Round(time.Microsecond),
		},
		{
			onerror: jobspb.ScheduleDetails_RETRY_SOON,
			nextRun: h.env.Now().Add(retryFailedJobAfter).Round(time.Microsecond),
		},
		{
			onerror: jobspb.ScheduleDetails_PAUSE_SCHED,
			nextRun: time.Time{}.UTC(),
		},
	}

	for _, test := range tests {
		t.Run(test.onerror.String(), func(t *testing.T) {
			j := h.newScheduledJob(t, "test_job", "test sql")
			j.rec.ExecutorType = InlineExecutorName

			require.NoError(t, j.SetSchedule("@daily"))
			j.SetScheduleDetails(jobspb.ScheduleDetails{OnError: test.onerror})

			ctx := context.Background()
			require.NoError(t, j.Create(ctx, h.ex, nil))

			// Pretend we failed running; we expect job to be rescheduled.
			md := &JobMetadata{
				ID:      123,
				Status:  "failed",
				Payload: &jobspb.Payload{ScheduleID: j.ScheduleID()},
			}

			require.NoError(t, NotifyJobTermination(ctx, h.env, md, h.ex, nil))

			// Verify nextRun updated
			loaded := h.loadJob(t, j.ScheduleID())
			require.Equal(t, test.nextRun, loaded.NextRun())
		})
	}
}
