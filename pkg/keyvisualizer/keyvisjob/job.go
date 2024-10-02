// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keyvisjob

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvissettings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type resumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = (*resumer)(nil)

// Resume implements the jobs.Resumer interface.
// The key visualizer job runs as a forever-running background job,
// and runs the SpanStatsConsumer according to keyvissettings.SampleInterval.
// The key visualizer job should never end up in a state where the registry refuses
// to adopt or run it. Because the job is non-cancellable, errors returned
// here will be considered retryable.
func (r *resumer) Resume(ctx context.Context, execCtxI interface{}) (jobErr error) {
	execCtx := execCtxI.(sql.JobExecContext)
	consumer := execCtx.SpanStatsConsumer()
	stopper := execCtx.ExecCfg().DistSQLSrv.Stopper

	runConsumer := func() error {
		r.job.MarkIdle(false)
		defer r.job.MarkIdle(true)

		// TODO(zachlite): wrap this in a retry for better fault-tolerance
		if err := consumer.UpdateBoundaries(ctx); err != nil {
			return errors.Wrap(err, "update boundaries failed")
		}
		if err := consumer.GetSamples(ctx); err != nil {
			return errors.Wrap(err, "get samples failed")
		}
		if err := consumer.DeleteExpiredSamples(ctx); err != nil {
			return errors.Wrap(err, "delete oldest samples failed")
		}
		return nil
	}

	settingValues := &execCtx.ExecCfg().Settings.SV

	for {
		sampleInterval := keyvissettings.SampleInterval.Get(settingValues)
		select {
		case <-time.After(sampleInterval):
			if keyvissettings.Enabled.Get(settingValues) {
				if err := runConsumer(); err != nil {
					// Errors returned from the SpanStatsConsumer should be considered transient.
					// This job should be retried.
					return err
				}
			}
		case <-ctx.Done():
			return nil
		case <-stopper.ShouldQuiesce():
			return nil
		}
	}
}

// OnFailOrCancel implements the jobs.Resumer interface.
// No action needs to be taken on our part. There's no state to clean up.
func (r *resumer) OnFailOrCancel(ctx context.Context, _ interface{}, jobErr error) error {
	if jobs.HasErrJobCanceled(jobErr) {
		err := errors.NewAssertionErrorWithWrappedErrf(jobErr,
			"key visualizer job is not cancelable")
		log.Infof(ctx, "%v", err)
	}
	return nil
}

// CollectProfile implements the jobs.Resumer interface.
func (r *resumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeKeyVisualizer,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &resumer{job: job}
		},
		jobs.DisablesTenantCostControl,
	)
}
