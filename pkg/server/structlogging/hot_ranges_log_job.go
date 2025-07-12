// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package structlogging

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type hotRangesLoggingJob struct {
	job      *jobs.Job
	settings *cluster.Settings
}

// hot_ranges_log_job.go adds the required functions to satisfy
// the jobs.Scheduler interface for the hot ranges logging job.
// This is only required for app tenants in a multi-tenant deployment
// as the app tenants have no notion of "local" ranges, and therefore
// require a fanout to be performed to collect the hot ranges.
// It's run as a job, as since fanout is required, only one node
// needs to run it at any given time, as opposed to the every
// node task behavior otherwise.
func (j *hotRangesLoggingJob) Resume(ctx context.Context, execCtxI interface{}) error {
	// This job is a forever running background job, and it is always safe to
	// terminate the SQL pod whenever the job is running, so mark it as idle.
	j.job.MarkIdle(true)

	jobExec := execCtxI.(sql.JobExecContext)
	execCfg := jobExec.ExecCfg()

	// Do not run this job for the system tenant.
	if execCfg.Codec.ForSystemTenant() {
		return nil
	}

	logger := &hotRangesLogger{
		sServer:     execCfg.TenantStatusServer,
		st:          j.settings,
		multiTenant: true,
		lastLogged:  timeutil.Now(),
	}
	logger.start(ctx, execCfg.Stopper)

	// Signal to the job system to pick this job back up.
	return jobs.MarkAsRetryJobError(errors.New("failing hot ranges job so that it is restarted"))
}

func (j *hotRangesLoggingJob) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	if jobs.HasErrJobCanceled(jobErr) {
		err := errors.NewAssertionErrorWithWrappedErrf(
			jobErr, "hot range logging job is not cancelable",
		)
		log.Errorf(ctx, "%v", err)
	}
	return nil
}

func (j *hotRangesLoggingJob) CollectProfile(ctx context.Context, execCtx interface{}) error {
	return nil
}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeHotRangesLogger,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &hotRangesLoggingJob{
				job:      job,
				settings: settings,
			}
		},
		jobs.DisablesTenantCostControl,
	)
}
