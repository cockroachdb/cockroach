// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tablemetadatacache

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	tablemetadatacacheutil "github.com/cockroachdb/cockroach/pkg/sql/tablemetadatacache/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

const (
	// batchesPerProgressUpdate is used to determine how many batches
	// should be processed before updating the job progress
	batchesPerProgressUpdate = 10
)

type tableMetadataUpdateJobResumer struct {
	job     *jobs.Job
	metrics *TableMetadataUpdateJobMetrics
}

var _ jobs.Resumer = (*tableMetadataUpdateJobResumer)(nil)

// Resume is part of the jobs.Resumer interface.
func (j *tableMetadataUpdateJobResumer) Resume(ctx context.Context, execCtxI interface{}) error {
	// This job is a forever running background job, and it is always safe to
	// terminate the SQL pod whenever the job is running, so mark it as idle.
	j.job.MarkIdle(true)

	execCtx := execCtxI.(sql.JobExecContext)
	metrics := execCtx.ExecCfg().JobRegistry.MetricsStruct().
		JobSpecificMetrics[jobspb.TypeUpdateTableMetadataCache].(TableMetadataUpdateJobMetrics)
	j.metrics = &metrics
	testKnobs := execCtx.ExecCfg().TableMetadataKnobs
	var updater tablemetadatacacheutil.ITableMetadataUpdater
	var onJobStartKnob, onJobCompleteKnob, onJobReady func()
	if testKnobs != nil {
		onJobStartKnob = testKnobs.OnJobStart
		onJobCompleteKnob = testKnobs.OnJobComplete
		onJobReady = testKnobs.OnJobReady
		if testKnobs.TableMetadataUpdater != nil {
			updater = testKnobs.TableMetadataUpdater
		}
	}

	if updater == nil {
		updater = newTableMetadataUpdater(
			j.updateProgress,
			&metrics,
			execCtx.ExecCfg().TenantStatusServer,
			execCtx.ExecCfg().InternalDB.Executor(),
			timeutil.DefaultTimeSource{},
			updateJobBatchSizeSetting.Get(&execCtx.ExecCfg().Settings.SV),
			testKnobs)
	}

	// Channel used to signal the job should run.
	signalCh := execCtx.ExecCfg().SQLStatusServer.GetUpdateTableMetadataCacheSignal()

	settings := execCtx.ExecCfg().Settings
	// Register callbacks to signal the job to reset the timer when timer related settings change.
	scheduleSettingsCh := make(chan struct{})
	AutomaticCacheUpdatesEnabledSetting.SetOnChange(&settings.SV, func(_ context.Context) {
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
		if AutomaticCacheUpdatesEnabledSetting.Get(&settings.SV) {
			timer.Reset(DataValidDurationSetting.Get(&settings.SV))
		}
		select {
		case <-scheduleSettingsCh:
			log.Info(ctx, "table metadata job settings updated, stopping timer.")
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

		j.markAsRunning(ctx)
		err := updater.RunUpdater(ctx)
		if err != nil {
			log.Errorf(ctx, "error running table metadata update job: %s", err)
			j.metrics.Errors.Inc(1)
		}
		j.markAsCompleted(ctx)
		if onJobCompleteKnob != nil {
			onJobCompleteKnob()
		}
	}
}

func (j *tableMetadataUpdateJobResumer) updateProgress(ctx context.Context, progress float32) {
	if err := j.job.NoTxn().FractionProgressed(ctx, jobs.FractionUpdater(progress)); err != nil {
		log.Errorf(ctx, "Error updating table metadata log progress. error: %s", err.Error())
	}
}

// markAsRunning updates the last_start_time and status fields in the job's progress
// details and writes the job progress as a JSON string to the running status.
func (j *tableMetadataUpdateJobResumer) markAsRunning(ctx context.Context) {
	if err := j.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		progress := md.Progress
		details := progress.Details.(*jobspb.Progress_TableMetadataCache).TableMetadataCache
		now := timeutil.Now()
		progress.StatusMessage = fmt.Sprintf("Job started at %s", now)
		details.LastStartTime = &now
		details.Status = jobspb.UpdateTableMetadataCacheProgress_RUNNING
		progress.Progress = &jobspb.Progress_FractionCompleted{
			FractionCompleted: 0,
		}
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
		progress.StatusMessage = fmt.Sprintf("Job completed at %s", now)
		details.LastCompletedTime = &now
		details.Status = jobspb.UpdateTableMetadataCacheProgress_NOT_RUNNING
		progress.Progress = &jobspb.Progress_FractionCompleted{
			FractionCompleted: 1.0,
		}
		ju.UpdateProgress(progress)
		return nil
	}); err != nil {
		log.Errorf(ctx, "%s", err.Error())
	}
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
	NumRuns       *metric.Counter
	UpdatedTables *metric.Counter
	Errors        *metric.Counter
	Duration      metric.IHistogram
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
		UpdatedTables: metric.NewCounter(metric.Metadata{
			Name:        "obs.tablemetadata.update_job.table_updates",
			Help:        "The total number of rows that have been updated in system.table_metadata",
			Measurement: "Rows Updated",
			Unit:        metric.Unit_COUNT,
			MetricType:  io_prometheus_client.MetricType_COUNTER,
		}),
		Errors: metric.NewCounter(metric.Metadata{
			Name:        "obs.tablemetadata.update_job.errors",
			Help:        "The total number of errors that have been emitted from the update table metadata job.",
			Measurement: "Errors",
			Unit:        metric.Unit_COUNT,
			MetricType:  io_prometheus_client.MetricType_COUNTER,
		}),
		Duration: metric.NewHistogram(metric.HistogramOptions{
			Metadata: metric.Metadata{
				Name:        "obs.tablemetadata.update_job.duration",
				Help:        "Time spent running the update table metadata job.",
				Measurement: "Duration",
				Unit:        metric.Unit_NANOSECONDS},
			Duration:     base.DefaultHistogramWindowInterval(),
			BucketConfig: metric.LongRunning60mLatencyBuckets,
			Mode:         metric.HistogramModePrometheus,
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
