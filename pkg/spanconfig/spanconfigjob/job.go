// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigjob

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type resumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = (*resumer)(nil)

var reconciliationJobCheckpointInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"spanconfig.reconciliation_job.checkpoint_interval",
	"the frequency at which the span config reconciliation job checkpoints itself",
	5*time.Second,
	settings.NonNegativeDuration,
)

// Resume implements the jobs.Resumer interface.
func (r *resumer) Resume(ctx context.Context, execCtxI interface{}) (jobErr error) {
	defer func() {
		// This job retries internally, any error here should fail the entire job
		// (at which point we expect the spanconfig.Manager to create a new one).
		// Between the job's internal retries and the spanconfig.Manager, we don't
		// need/want to rely on the job system's internal backoff (where retry
		// durations aren't configurable on a per-job basis).
		jobErr = jobs.MarkAsPermanentJobError(jobErr)
	}()

	execCtx := execCtxI.(sql.JobExecContext)
	rc := execCtx.SpanConfigReconciler()
	stopper := execCtx.ExecCfg().DistSQLSrv.Stopper

	// The reconciliation job is a forever running background job. It's always
	// safe to wind the SQL pod down whenever it's running -- something we
	// indicate through the job's idle status.
	r.job.MarkIdle(true)

	// Start the protected timestamp reconciler. This will periodically poll the
	// protected timestamp table to cleanup stale records. We take advantage of
	// the fact that there can only be one instance of the spanconfig.Resumer
	// running in a cluster, and so consequently the reconciler is only started on
	// the coordinator node.
	ptsRCContext, cancel := stopper.WithCancelOnQuiesce(ctx)
	defer cancel()
	if err := execCtx.ExecCfg().ProtectedTimestampProvider.StartReconciler(
		ptsRCContext, execCtx.ExecCfg().DistSQLSrv.Stopper); err != nil {
		return errors.Wrap(err, "could not start protected timestamp reconciliation")
	}

	// TODO(irfansharif): #73086 bubbles up retryable errors from the
	// reconciler/underlying watcher in the (very) unlikely event that it's
	// unable to generate incremental updates from the given timestamp (things
	// could've been GC-ed from underneath us). For such errors, instead of
	// failing this entire job, we should simply retry the reconciliation
	// process here. Not doing so is still fine, the spanconfig.Manager starts
	// the job all over again after some time, it's just that the checks for
	// failed jobs happen infrequently.

	// TODO(irfansharif): We're still not using a persisted checkpoint, both
	// here when starting at the empty timestamp and down in the reconciler
	// (does the full reconciliation at every start). Once we do, it'll be
	// possible that the checkpoint timestamp provided is too stale -- for a
	// suspended tenant perhaps data was GC-ed from underneath it. When we
	// bubble up said error, we want to retry at this level with an empty
	// timestamp.

	settingValues := &execCtx.ExecCfg().Settings.SV
	persistCheckpoints := util.Every(reconciliationJobCheckpointInterval.Get(settingValues))
	reconciliationJobCheckpointInterval.SetOnChange(settingValues, func(ctx context.Context) {
		persistCheckpoints = util.Every(reconciliationJobCheckpointInterval.Get(settingValues))
	})

	shouldPersistCheckpoint := true
	shouldSkipRetry := false
	var onCheckpointInterceptor func() error

	if knobs := execCtx.ExecCfg().SpanConfigTestingKnobs; knobs != nil {
		if knobs.JobDisablePersistingCheckpoints {
			shouldPersistCheckpoint = false
		}
		shouldSkipRetry = knobs.JobDisableInternalRetry
		onCheckpointInterceptor = knobs.JobOnCheckpointInterceptor
	}

	retryOpts := retry.Options{
		InitialBackoff: 5 * time.Second,
		MaxBackoff:     30 * time.Second,
		Multiplier:     1.3,
		MaxRetries:     40, // ~20 mins
	}

	const aWhile = 5 * time.Minute // arbitrary but much longer than a retry
	for retrier := retry.StartWithCtx(ctx, retryOpts); retrier.Next(); {
		started := timeutil.Now()
		if err := rc.Reconcile(ctx, hlc.Timestamp{}, func() error {
			if onCheckpointInterceptor != nil {
				if err := onCheckpointInterceptor(); err != nil {
					return err
				}
			}

			if !shouldPersistCheckpoint {
				return nil
			}
			if !persistCheckpoints.ShouldProcess(timeutil.Now()) {
				return nil
			}

			if timeutil.Since(started) > aWhile {
				retrier.Reset()
			}

			return r.job.SetProgress(ctx, nil, jobspb.AutoSpanConfigReconciliationProgress{
				Checkpoint: rc.Checkpoint(),
			})
		}); err != nil {
			log.Errorf(ctx, "reconciler failed with %v, retrying...", err)
			if shouldSkipRetry {
				break
			}
			continue
		}
		return nil // we're done here (the stopper was stopped, Reconcile exited cleanly)
	}

	return errors.Newf("reconciliation unsuccessful, failing job")
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (r *resumer) OnFailOrCancel(ctx context.Context, _ interface{}) error {
	if jobs.HasErrJobCanceled(errors.DecodeError(ctx, *r.job.Payload().FinalResumeError)) {
		return errors.AssertionFailedf("span config reconciliation job cannot be canceled")
	}
	return nil
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeAutoSpanConfigReconciliation,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &resumer{job: job}
		})
}
