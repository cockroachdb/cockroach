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
	"math"

	"github.com/cockroachdb/cockroach/pkg/base"
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

const (
	// batchesPerProgressUpdate is used to determine how many batches
	// should be processed before updating the job progress
	batchesPerProgressUpdate = 10
)

// updateJobExecFn specifies the function that is run on each iteration of the
// table metadata update job. It can be overriden in tests.
var updateJobExecFn func(context.Context, isql.Executor, *tableMetadataUpdateJobResumer) (int, error) = updateTableMetadataCache

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
		j.metrics.NumRuns.Inc(1)
		sw := timeutil.NewStopWatch()
		sw.Start()
		j.markAsRunning(ctx)
		rowsUpdated, err := updateJobExecFn(ctx, execCtx.ExecCfg().InternalDB.Executor(), j)
		if err != nil {
			log.Errorf(ctx, "error running table metadata update job: %s", err)
			j.metrics.Errors.Inc(1)
		}
		j.markAsCompleted(ctx)
		if onJobCompleteKnob != nil {
			onJobCompleteKnob()
		}
		sw.Stop()
		j.metrics.Duration.RecordValue(sw.Elapsed().Nanoseconds())
		j.metrics.UpdatedTables.Inc(int64(rowsUpdated))
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
		progress.RunningStatus = fmt.Sprintf("Job started at %s", now)
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
		progress.RunningStatus = fmt.Sprintf("Job completed at %s", now)
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

// updateTableMetadataCache performs a full update of system.table_metadata by collecting
// metadata from the system.namespace, system.descriptor tables and table span stats RPC.
func updateTableMetadataCache(
	ctx context.Context, ie isql.Executor, resumer *tableMetadataUpdateJobResumer,
) (int, error) {
	updater := newTableMetadataUpdater(ie)
	if _, err := updater.pruneCache(ctx); err != nil {
		log.Errorf(ctx, "failed to prune table metadata cache: %s", err.Error())
	}

	resumer.updateProgress(ctx, .01)
	return updater.updateCache(ctx, resumer.onBatchUpdate(), func(ctx context.Context, e error) {
		if resumer.metrics != nil {
			resumer.metrics.Errors.Inc(1)
		}
	})
}

// onBatchUpdate returns an onBatchUpdateCallback func that updates the progress of the job.
// The call to updateProgress doesn't happen on every invocation, and only happens every nth
// invocation, where n is defined by batchesPerProgressUpdate. This is done because each
// batch update is expected to execute quickly, and updating progress at a high velocity
// doesn't seem worth it.
func (j *tableMetadataUpdateJobResumer) onBatchUpdate() onBatchUpdateCallback {
	batchNum := 0
	return func(ctx context.Context, totalRowsToUpdate int, rowsUpdated int) {
		batchNum++
		estimatedBatches := int(math.Ceil(float64(totalRowsToUpdate) / float64(tableBatchSize)))
		if batchNum == estimatedBatches {
			j.updateProgress(ctx, .99)
		} else if batchNum%batchesPerProgressUpdate == 0 && estimatedBatches > 0 {
			progress := float32(rowsUpdated) / float32(totalRowsToUpdate)
			j.updateProgress(ctx, progress)
		}
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
			BucketConfig: metric.IOLatencyBuckets,
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
