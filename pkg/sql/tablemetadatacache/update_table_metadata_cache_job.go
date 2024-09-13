// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tablemetadatacache

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// updateJobExecFn specifies the function that is run on each iteration of the
// table metadata update job. It can be overriden in tests.
var updateJobExecFn func(context.Context, isql.Executor) error = updateTableMetadataCache

type tableMetadataUpdateJobResumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = (*tableMetadataUpdateJobResumer)(nil)

// Resume is part of the jobs.Resumer interface.
func (j *tableMetadataUpdateJobResumer) Resume(ctx context.Context, execCtxI interface{}) error {
	j.job.MarkIdle(true)

	execCtx := execCtxI.(sql.JobExecContext)
	metrics := execCtx.ExecCfg().JobRegistry.MetricsStruct().
		JobSpecificMetrics[jobspb.TypeUpdateTableMetadataCache].(TableMetadataUpdateJobMetrics)
	var onJobStartKnob, onJobCompleteKnob, onJobReady func()
	if execCtx.ExecCfg().TableMetadataKnobs != nil {
		onJobStartKnob = execCtx.ExecCfg().TableMetadataKnobs.OnJobStart
		onJobCompleteKnob = execCtx.ExecCfg().TableMetadataKnobs.OnJobComplete
		onJobReady = execCtx.ExecCfg().TableMetadataKnobs.OnJobReady
	}
	// We must reset the job's num runs to 0 so that it doesn't get
	// delayed by the job system's exponential backoff strategy.
	if err := j.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		if md.RunStats != nil && md.RunStats.NumRuns > 0 {
			ju.UpdateRunStats(0, md.RunStats.LastRun)
		}
		return nil
	}); err != nil {
		log.Errorf(ctx, "%s", err.Error())
	}

	// Channel used to signal the job should run.
	signalCh := execCtx.ExecCfg().SQLStatusServer.GetUpdateTableMetadataCacheSignal()

	settings := execCtx.ExecCfg().Settings
	// Register callbacks to signal the job to reset the timer when timer related settings change.
	scheduleSettingsCh := make(chan struct{})
	tableMetadataCacheAutoUpdatesEnabled.SetOnChange(&settings.SV, func(_ context.Context) {
		select {
		case scheduleSettingsCh <- struct{}{}:
		default:
		}
	})
	DataValidDurationSetting.SetOnChange(&settings.SV, func(_ context.Context) {
		select {
		case scheduleSettingsCh <- struct{}{}:
		default:
		}
	})

	var timer timeutil.Timer
	if onJobReady != nil {
		onJobReady()
	}
	for {
		if tableMetadataCacheAutoUpdatesEnabled.Get(&settings.SV) {
			timer.Reset(DataValidDurationSetting.Get(&settings.SV))
		}
		select {
		case <-scheduleSettingsCh:
			timer.Stop()
			continue
		case <-timer.C:
			timer.Read = true
			log.Info(ctx, "running table metadata update job after data cache expiration")
		case <-signalCh:
			log.Info(ctx, "running table metadata update job via grpc signal")
		case <-ctx.Done():
			return ctx.Err()
		}

		if onJobStartKnob != nil {
			onJobStartKnob()
		}
		// Run table metadata update job.
		metrics.NumRuns.Inc(1)
		j.markAsRunning(ctx)
		if err := updateJobExecFn(ctx, execCtx.ExecCfg().InternalDB.Executor()); err != nil {
			log.Errorf(ctx, "error running table metadata update job: %s", err)
		}
		j.markAsCompleted(ctx)
		if onJobCompleteKnob != nil {
			onJobCompleteKnob()
		}
	}
}

// markAsRunning updates the last_start_time and status fields in the job's progress
// details and writes the job progress as a JSON string to the running status.
func (j *tableMetadataUpdateJobResumer) markAsRunning(ctx context.Context) {
	if err := j.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		progress := md.Progress
		details := progress.Details.(*jobspb.Progress_TableMetadataCache).TableMetadataCache
		now := timeutil.Now()
		progress.RunningStatus = fmt.Sprintf("Job started at %s", now)
		details.LastStartTime = &now
		details.Status = jobspb.UpdateTableMetadataCacheProgress_RUNNING
		ju.UpdateProgress(progress)
		return nil
	}); err != nil {
		log.Errorf(ctx, "%s", err.Error())
	}
}

// markAsCompleted updates the last_completed_time and status fields in the job's progress
// details and writes the job progress as a JSON string to the running status.
func (j *tableMetadataUpdateJobResumer) markAsCompleted(ctx context.Context) {
	if err := j.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		progress := md.Progress
		details := progress.Details.(*jobspb.Progress_TableMetadataCache).TableMetadataCache
		now := timeutil.Now()
		progress.RunningStatus = fmt.Sprintf("Job completed at %s", now)
		details.LastCompletedTime = &now
		details.Status = jobspb.UpdateTableMetadataCacheProgress_NOT_RUNNING
		ju.UpdateProgress(progress)
		return nil
	}); err != nil {
		log.Errorf(ctx, "%s", err.Error())
	}
}

// updateTableMetadataCache performs a full update of system.table_metadata by collecting
// metadata from the system.namespace, system.descriptor tables and table span stats RPC.
func updateTableMetadataCache(ctx context.Context, ie isql.Executor) error {
	updater := newTableMetadataUpdater(ie)
	if _, err := updater.pruneCache(ctx); err != nil {
		log.Errorf(ctx, "failed to prune table metadata cache: %s", err.Error())
	}

	// We'll use the updated ret val in a follow-up to update metrics and
	// fractional job progress.
	_, err := updater.updateCache(ctx)
	return err
}

// OnFailOrCancel implements jobs.Resumer.
func (j *tableMetadataUpdateJobResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	if jobs.HasErrJobCanceled(jobErr) {
		err := errors.NewAssertionErrorWithWrappedErrf(
			jobErr, "update table metadata cache job is not cancelable",
		)
		log.Errorf(ctx, "%v", err)
	}
	return nil
}

// CollectProfile implements jobs.Resumer.
func (j *tableMetadataUpdateJobResumer) CollectProfile(
	ctx context.Context, execCtx interface{},
) error {
	return nil
}

type TableMetadataUpdateJobMetrics struct {
	NumRuns *metric.Counter
}

func (m TableMetadataUpdateJobMetrics) MetricStruct() {}

func newTableMetadataUpdateJobMetrics() metric.Struct {
	return TableMetadataUpdateJobMetrics{
		NumRuns: metric.NewCounter(metric.Metadata{
			Name:        "obs.tablemetadata.update_job.runs",
			Help:        "The total number of runs of the update table metadata job.",
			Measurement: "Executions",
			Unit:        metric.Unit_COUNT,
			MetricType:  io_prometheus_client.MetricType_COUNTER,
		}),
	}
}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeUpdateTableMetadataCache,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &tableMetadataUpdateJobResumer{job: job}
		},
		jobs.DisablesTenantCostControl,
		jobs.WithJobMetrics(newTableMetadataUpdateJobMetrics()),
	)
}
