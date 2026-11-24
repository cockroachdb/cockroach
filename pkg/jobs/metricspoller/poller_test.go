// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metricspoller_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestMetricsPollerExitZerosMetrics verifies that when the metrics poller job
// exits (e.g., due to node drain), it zeros out all the metrics it maintains.
// This ensures that only the node actively running the poller exports non-zero
// values for poller-maintained metrics.
func TestMetricsPollerExitZerosMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Set up server with fast polling interval.
	args := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
		Settings: cluster.MakeTestingClusterSettings(),
	}
	jobs.PollJobsMetricsInterval.Override(ctx, &args.Settings.SV, 10*time.Millisecond)

	s := serverutils.StartServerOnly(t, args)
	defer s.Stopper().Stop(ctx)

	reg := s.JobRegistry().(*jobs.Registry)

	// Register a fake resumer for import jobs that just blocks until cancelled.
	defer jobs.TestingRegisterConstructor(jobspb.TypeImport, func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
		return fakeResumer{
			onResume: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
		}
	}, jobs.UsesTenantCostControl)()

	// Create a job and pause it.
	record := jobs.Record{
		Details:  jobspb.ImportDetails{},
		Progress: jobspb.ImportProgress{},
		Username: username.TestUserName(),
	}
	job, err := jobs.TestingCreateAndStartJob(ctx, reg, s.InternalDB().(isql.DB), record)
	require.NoError(t, err)

	// Pause the job.
	require.NoError(t, reg.PauseRequested(ctx, nil, job.ID(), "testing"))

	// Wait for the CurrentlyPaused metric to become non-zero (poller has run
	// and the job is paused).
	testutils.SucceedsSoon(t, func() error {
		paused := reg.MetricsStruct().JobMetrics[jobspb.TypeImport].CurrentlyPaused.Value()
		if paused == 0 {
			return errors.New("CurrentlyPaused metric still zero")
		}
		return nil
	})

	// Verify the metric is non-zero before pausing the poller to make it exit.
	pausedBefore := reg.MetricsStruct().JobMetrics[jobspb.TypeImport].CurrentlyPaused.Value()
	require.Equal(t, int64(1), pausedBefore, "expected 1 paused job")

	require.NoError(t, reg.PauseRequested(ctx, nil, jobs.JobMetricsPollerJobID, "pause to trigger exit cleanup fn"))

	// Wait for the CurrentlyPaused metric to become zero (exit cleanup ran).
	testutils.SucceedsSoon(t, func() error {
		paused := reg.MetricsStruct().JobMetrics[jobspb.TypeImport].CurrentlyPaused.Value()
		if paused != 0 {
			return errors.Newf("CurrentlyPaused metric not yet zero: %d", paused)
		}
		return nil
	})
}

type fakeResumer struct {
	onResume func(ctx context.Context) error
}

func (f fakeResumer) Resume(ctx context.Context, execCtx interface{}) error {
	return f.onResume(ctx)
}

func (f fakeResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}, jobErr error) error {
	return nil
}

func (f fakeResumer) CollectProfile(ctx context.Context, execCtx interface{}) error {
	return nil
}
