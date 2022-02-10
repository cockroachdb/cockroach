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
func (r *resumer) Resume(ctx context.Context, execCtxI interface{}) error {
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
	if knobs := execCtx.ExecCfg().SpanConfigTestingKnobs; knobs != nil && knobs.JobDisablePersistingCheckpoints {
		shouldPersistCheckpoint = false
	}
	if err := rc.Reconcile(ctx, hlc.Timestamp{}, func() error {
		if !shouldPersistCheckpoint {
			return nil
		}
		if !persistCheckpoints.ShouldProcess(timeutil.Now()) {
			return nil
		}

		return r.job.SetProgress(ctx, nil, jobspb.AutoSpanConfigReconciliationProgress{
			Checkpoint: rc.Checkpoint(),
		})
	}); err != nil {
		return err
	}

	return nil
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (r *resumer) OnFailOrCancel(context.Context, interface{}) error {
	return errors.AssertionFailedf("span config reconciliation job can never fail or be canceled")
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeAutoSpanConfigReconciliation,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &resumer{job: job}
		})
}
