// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keyvisjob

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvissettings"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type resumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = (*resumer)(nil)

// Resume implements the jobs.Resumer interface.
func (r *resumer) Resume(ctx context.Context, execCtxI interface{}) (jobErr error) {
	defer func() {
		// An error here should fail the entire job. We expect the keyvismanager.Manager
		// to create a new one. We don't need/want to rely on the job system's
		// internal backoff (where retry durations aren't configurable on a per-job basis).
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

	consumer := execCtx.SpanStatsConsumer()
	stopper := execCtx.ExecCfg().DistSQLSrv.Stopper

	// The key visualizer job is a forever running background job. It's always
	// safe to wind the SQL pod down whenever it's running -- something we
	// indicate through the job's idle status.
	r.job.MarkIdle(true)

	// If the Job's NumRuns is greater than 1, reset it to 0 so that future
	// resumptions are not delayed by the job system.
	//
	// Note that we are doing this before the possible error return below. If
	// there is a problem running the span stats consumer, this job will
	// aggressively restart at the job system level with no backoff.
	if err := r.job.Update(ctx, nil, func(_ *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		if md.RunStats != nil && md.RunStats.NumRuns > 1 {
			ju.UpdateRunStats(1, md.RunStats.LastRun)
		}
		return nil
	}); err != nil {
		log.Warningf(ctx, "failed to reset key visualizer job run stats: %v", err)
	}

	ctxDoneChan := ctx.Done()

	runConsumer := func() error {
		// TODO(zachlite): wrap this in a retry for better fault-tolerance
		if err := consumer.DecideBoundaries(ctx); err != nil {
			log.Warningf(ctx, "decide boundaries failed with...%v", err)
			return err
		}
		if err := consumer.FetchStats(ctx); err != nil {
			log.Warningf(ctx, "fetch stats failed with...%v", err)
			return err
		}
		if err := consumer.DeleteOldestSamples(ctx); err != nil {
			log.Warningf(ctx, "delete oldest samples failed with...%v", err)
			return err
		}
		return nil
	}

	settingValues := &execCtx.ExecCfg().Settings.SV

	sampleIntervalMu := struct {
		syncutil.Mutex
		interval time.Duration
	}{}

	sampleIntervalMu.interval = keyvissettings.SampleInterval.Get(settingValues)

	keyvissettings.SampleInterval.SetOnChange(settingValues, func(ctx context.Context) {
		sampleIntervalMu.Lock()
		defer sampleIntervalMu.Unlock()
		sampleIntervalMu.interval = keyvissettings.SampleInterval.Get(settingValues)
	})

	for {
		sampleIntervalMu.Lock()
		interval := sampleIntervalMu.interval
		sampleIntervalMu.Unlock()

		select {
		case <-time.After(interval):
			if err := runConsumer(); err != nil {
				return err // SpanStatsConsumer unsuccessful, failing job.
			}
		case <-ctxDoneChan:
			return nil

		case <-stopper.ShouldQuiesce():
			return nil
		}
	}
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (r *resumer) OnFailOrCancel(ctx context.Context, _ interface{}, jobErr error) error {
	if jobs.HasErrJobCanceled(jobErr) {
		log.Infof(ctx, "key visualizer job canceled")
	} else {
		log.Errorf(ctx, "key visualizer job failed")
	}
	return nil
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeKeyVisualizer,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &resumer{job: job}
		},
		jobs.UsesTenantCostControl,
	)
}
