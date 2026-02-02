// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmreader

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var _ jobs.Resumer = (*updateJob)(nil)

type updateJob struct {
	job *jobs.Job
	st  *cluster.Settings
}

func (u *updateJob) Resume(ctx context.Context, execCtx interface{}) error {
	log.Dev.Infof(ctx, "starting cluster metrics update job")

	// This job is a forever running background job, and it is always safe to
	// terminate the SQL pod whenever the job is running, so mark it as idle.
	u.job.MarkIdle(true)

	var timer timeutil.Timer
	defer timer.Stop()

	timer.Reset(0)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			timer.Reset(5 * time.Minute)
		}
	}

}

func (u *updateJob) OnFailOrCancel(ctx context.Context, execCtx interface{}, jobErr error) error {
	return nil
}

func (u *updateJob) CollectProfile(ctx context.Context, execCtx interface{}) error {
	return nil
}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeClusterMetricsUpdater,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &updateJob{job: job, st: settings}

		},
		jobs.DisablesTenantCostControl,
	)
}
