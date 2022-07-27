package keyvisjob

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type resumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = (*resumer)(nil)

func Initialize() {
	jobs.RegisterConstructor(jobspb.TypeKeyVisualizer,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &resumer{job: job}
		},
		jobs.UsesTenantCostControl,
	)
}

// Resume implements the jobs.Resumer interface.
func (r *resumer) Resume(ctx context.Context, execCtxI interface{}) (jobErr error) {
	defer func() {
		// This job retries internally, any error here should fail the entire job
		// (at which point we expect the keyvismanager.Manager to create a new one).
		// Between the job's internal retries and the keyvismanager.Manager, we don't
		// need/want to rely on the job system's internal backoff (where retry
		// durations aren't configurable on a per-job basis).
		jobErr = jobs.MarkAsPermanentJobError(jobErr)
	}()

	execCtx := execCtxI.(sql.JobExecContext)
	if !execCtx.ExecCfg().NodeInfo.LogicalClusterID().Equal(r.job.Payload().CreationClusterID) {
		// When restoring a cluster, we don't want to end up with two instances of
		// the singleton reconciliation job.
		log.Infof(ctx, "duplicate restored job (source-cluster-id=%s, dest-cluster-id=%s); exiting",
			r.job.Payload().CreationClusterID, execCtx.ExecCfg().NodeInfo.LogicalClusterID())
		return nil
	}

	c := execCtx.SpanStatsConsumer()

	// The reconciliation job is a forever running background job. It's always
	// safe to wind the SQL pod down whenever it's running -- something we
	// indicate through the job's idle status.
	r.job.MarkIdle(true)

	// If the Job's NumRuns is greater than 1, reset it to 0 so that future
	// resumptions are not delayed by the job system.
	//
	// Note that we are doing this before the possible error return below. If
	// there is a problem starting the span stats consumer, this job will
	// aggressively restart at the job system level with no backoff.
	if err := r.job.Update(ctx, nil, func(_ *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		if md.RunStats != nil && md.RunStats.NumRuns > 1 {
			ju.UpdateRunStats(1, md.RunStats.LastRun)
		}
		return nil
	}); err != nil {
		log.Warningf(ctx, "failed to reset key visualizer job run stats: %v", err)
	}


	// TODO(zachlite): use a retry
	if err := c.DecideBoundaries(ctx); err != nil {
		log.Warningf(ctx, "decide boundaries failed with...%v", err)
		return err
	}
	if err := c.FetchStats(ctx); err != nil {
		log.Warningf(ctx, "fetch stats failed with...%v", err)
		return err
	}
	if err := c.DeleteOldestSamples(ctx); err != nil {
		log.Warningf(ctx, "delete oldest samples failed with...%v", err)
		return err
	}

	return nil
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (r *resumer) OnFailOrCancel(ctx context.Context, _ interface{}, _ error) error {
	if jobs.HasErrJobCanceled(errors.DecodeError(ctx, *r.job.Payload().FinalResumeError)) {
		return errors.AssertionFailedf("key visualizer job cannot be canceled")
	}
	return nil
}

