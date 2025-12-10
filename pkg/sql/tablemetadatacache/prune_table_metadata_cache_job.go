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
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

type tableMetadataPruneJobResumer struct {
	job     *jobs.Job
	metrics *TableMetadataPruneJobMetrics
}

var _ jobs.Resumer = (*tableMetadataPruneJobResumer)(nil)

// Resume is part of the jobs.Resumer interface.
func (j *tableMetadataPruneJobResumer) Resume(ctx context.Context, execCtxI interface{}) error {
	// This job is a forever running background job, and it is always safe to
	// terminate the SQL pod whenever the job is running, so mark it as idle.
	j.job.MarkIdle(true)

	execCtx := execCtxI.(sql.JobExecContext)
	metrics := execCtx.ExecCfg().JobRegistry.MetricsStruct().
		JobSpecificMetrics[jobspb.TypePruneTableMetadataCache].(TableMetadataPruneJobMetrics)
	j.metrics = &metrics

	settings := execCtx.ExecCfg().Settings
	ie := execCtx.ExecCfg().InternalDB.Executor()

	// Channel used to signal the job should run via gRPC.
	signalCh := execCtx.ExecCfg().SQLStatusServer.GetUpdateTableMetadataCacheSignal()

	// Register callbacks to signal the job to reset the timer when timer related settings change.
	scheduleSettingsCh := make(chan struct{})
	AutomaticCacheUpdatesEnabledSetting.SetOnChange(&settings.SV, func(_ context.Context) {
		select {
		case scheduleSettingsCh <- struct{}{}:
		default:
		}
	})
	PruneIntervalSetting.SetOnChange(&settings.SV, func(_ context.Context) {
		select {
		case scheduleSettingsCh <- struct{}{}:
		default:
		}
	})

	var timer timeutil.Timer
	for {
		if AutomaticCacheUpdatesEnabledSetting.Get(&settings.SV) {
			timer.Reset(PruneIntervalSetting.Get(&settings.SV))
		}
		select {
		case <-scheduleSettingsCh:
			log.Dev.Info(ctx, "table metadata prune job settings updated, resetting timer.")
			timer.Stop()
			continue
		case <-timer.C:
			log.Dev.Info(ctx, "running table metadata prune job after interval expiration")
		case <-signalCh:
			log.Dev.Info(ctx, "running table metadata prune job via grpc signal")
		case <-ctx.Done():
			return ctx.Err()
		}

		j.markAsRunning(ctx)
		err := j.runPrune(ctx, ie)
		if err != nil {
			log.Dev.Errorf(ctx, "error running table metadata prune job: %s", err)
			j.metrics.Errors.Inc(1)
		}
		j.markAsCompleted(ctx)
	}
}

// runPrune performs the actual pruning of the table metadata cache.
func (j *tableMetadataPruneJobResumer) runPrune(ctx context.Context, ie isql.Executor) error {
	j.metrics.NumRuns.Inc(1)
	sw := timeutil.NewStopWatch()
	sw.Start()
	defer func() {
		sw.Stop()
		j.metrics.Duration.RecordValue(sw.Elapsed().Nanoseconds())
	}()

	removed, err := pruneTableMetadataCache(ctx, ie)
	if err != nil {
		return err
	}
	log.Dev.Infof(ctx, "pruned %d rows from table metadata cache", removed)
	return nil
}

// pruneTableMetadataCache deletes entries in the system.table_metadata that are not
// present in system.namespace, using batched deletions with a batch size
// of pruneBatchSize.
func pruneTableMetadataCache(ctx context.Context, ie isql.Executor) (removed int, err error) {
	for {
		rowsAffected, err := ie.ExecEx(
			ctx,
			"prune-table-metadata",
			nil, // txn
			sessiondata.NodeUserWithBulkLowPriSessionDataOverride, `
DELETE FROM system.table_metadata
WHERE table_id IN (
  SELECT table_id
  FROM system.table_metadata
  WHERE table_id NOT IN (
    SELECT id FROM system.namespace
  )
  LIMIT $1
)
RETURNING table_id`, pruneBatchSize)

		if err != nil {
			return 0, err
		}

		if rowsAffected == 0 {
			// No more rows to delete
			break
		}

		removed += rowsAffected
	}

	return removed, nil
}

// markAsRunning updates the last_start_time and status fields in the job's progress
// details and writes the job progress as a JSON string to the running status.
func (j *tableMetadataPruneJobResumer) markAsRunning(ctx context.Context) {
	if err := j.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		progress := md.Progress
		details := progress.Details.(*jobspb.Progress_PruneTableMetadataCache).PruneTableMetadataCache
		now := timeutil.Now()
		progress.StatusMessage = fmt.Sprintf("Job started at %s", now)
		details.LastStartTime = &now
		details.Status = jobspb.PruneTableMetadataCacheProgress_RUNNING
		progress.Progress = &jobspb.Progress_FractionCompleted{
			FractionCompleted: 0,
		}
		ju.UpdateProgress(progress)
		return nil
	}); err != nil {
		log.Dev.Errorf(ctx, "%s", err.Error())
	}
}

// markAsCompleted updates the last_completed_time and status fields in the job's progress
// details and writes the job progress as a JSON string to the running status.
func (j *tableMetadataPruneJobResumer) markAsCompleted(ctx context.Context) {
	if err := j.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		progress := md.Progress
		details := progress.Details.(*jobspb.Progress_PruneTableMetadataCache).PruneTableMetadataCache
		now := timeutil.Now()
		progress.StatusMessage = fmt.Sprintf("Job completed at %s", now)
		details.LastCompletedTime = &now
		details.Status = jobspb.PruneTableMetadataCacheProgress_NOT_RUNNING
		progress.Progress = &jobspb.Progress_FractionCompleted{
			FractionCompleted: 1.0,
		}
		ju.UpdateProgress(progress)
		return nil
	}); err != nil {
		log.Dev.Errorf(ctx, "%s", err.Error())
	}
}

// OnFailOrCancel implements jobs.Resumer.
func (j *tableMetadataPruneJobResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	if jobs.HasErrJobCanceled(jobErr) {
		err := errors.NewAssertionErrorWithWrappedErrf(
			jobErr, "prune table metadata cache job is not cancelable",
		)
		log.Dev.Errorf(ctx, "%v", err)
	}
	return nil
}

// CollectProfile implements jobs.Resumer.
func (j *tableMetadataPruneJobResumer) CollectProfile(
	ctx context.Context, execCtx interface{},
) error {
	return nil
}

// TableMetadataPruneJobMetrics contains metrics for the prune table metadata cache job.
type TableMetadataPruneJobMetrics struct {
	NumRuns  *metric.Counter
	Errors   *metric.Counter
	Duration metric.IHistogram
}

func (m TableMetadataPruneJobMetrics) MetricStruct() {}

func newTableMetadataPruneJobMetrics() metric.Struct {
	return TableMetadataPruneJobMetrics{
		NumRuns: metric.NewCounter(metric.Metadata{
			Name:        "obs.tablemetadata.prune_job.runs",
			Help:        "The total number of runs of the prune table metadata job.",
			Measurement: "Executions",
			Unit:        metric.Unit_COUNT,
			MetricType:  io_prometheus_client.MetricType_COUNTER,
		}),
		Errors: metric.NewCounter(metric.Metadata{
			Name:        "obs.tablemetadata.prune_job.errors",
			Help:        "The total number of errors that have been emitted from the prune table metadata job.",
			Measurement: "Errors",
			Unit:        metric.Unit_COUNT,
			MetricType:  io_prometheus_client.MetricType_COUNTER,
		}),
		Duration: metric.NewHistogram(metric.HistogramOptions{
			Metadata: metric.Metadata{
				Name:        "obs.tablemetadata.prune_job.duration",
				Help:        "Time spent running the prune table metadata job.",
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
		jobspb.TypePruneTableMetadataCache,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &tableMetadataPruneJobResumer{job: job}
		},
		jobs.DisablesTenantCostControl,
		jobs.WithJobMetrics(newTableMetadataPruneJobMetrics()),
	)
}
