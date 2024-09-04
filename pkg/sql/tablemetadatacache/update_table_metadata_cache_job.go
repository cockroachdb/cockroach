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

// MockJobExecFn sets the function that is run on each iteration of the
// table metadata update job. It is not thread-safe and should only be used in
// tests prior to starting the cluster.
func MockJobExecFn(fn func(context.Context, isql.Executor) error) {
	if fn == nil {
		fn = updateTableMetadataCache
	}
	updateJobExecFn = fn
}

type tableMetadataUpdateJobResumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = (*tableMetadataUpdateJobResumer)(nil)

// Resume is part of the jobs.Resumer interface.
func (j *tableMetadataUpdateJobResumer) Resume(ctx context.Context, execCtxI interface{}) error {
	log.Infof(ctx, "starting table metadata update job")
	j.job.MarkIdle(true)

	execCtx := execCtxI.(sql.JobExecContext)
	metrics := execCtx.ExecCfg().JobRegistry.MetricsStruct().
		JobSpecificMetrics[jobspb.TypeUpdateTableMetadataCache].(TableMetadataUpdateJobMetrics)

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

	// Register callbacks to signal the job to reset the timer when timer related settings change.
	scheduleSettingsCh := make(chan struct{})
	tableMetadataCacheAutoUpdatesEnabled.SetOnChange(&execCtx.ExecCfg().Settings.SV, func(_ context.Context) {
		select {
		case scheduleSettingsCh <- struct{}{}:
		default:
		}
	})
	tableMetadataCacheValidDuration.SetOnChange(&execCtx.ExecCfg().Settings.SV, func(_ context.Context) {
		select {
		case scheduleSettingsCh <- struct{}{}:
		default:
		}
	})

	var timer timeutil.Timer
	timer.Stop()

	for {
		if tableMetadataCacheAutoUpdatesEnabled.Get(&execCtx.ExecCfg().Settings.SV) {
			timer.Reset(tableMetadataCacheValidDuration.Get(&execCtx.ExecCfg().Settings.SV))
		}
		select {
		case <-scheduleSettingsCh:
			// Restart the loop to recompute the timer.
			timer.Stop()
			continue
		case <-timer.C:
			timer.Read = true
		case <-signalCh:
		case <-ctx.Done():
			return ctx.Err()
		}

		// Run table metadata update job.
		log.Infof(ctx, "running table metadata update job")
		metrics.NumRuns.Inc(1)
		j.updateLastRunTime(ctx)
		if err := updateJobExecFn(ctx, execCtx.ExecCfg().InternalDB.Executor()); err != nil {
			log.Errorf(ctx, "error running table metadata update job: %s", err)
		}
	}
}

// updateLastRunTime updates the last_run_time field in the job's progress
// details and writes the job progress as a JSON string to the running status.
func (j *tableMetadataUpdateJobResumer) updateLastRunTime(ctx context.Context) {
	if err := j.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		lrt := timeutil.Now()
		ju.UpdateProgress(&jobspb.Progress{
			RunningStatus: fmt.Sprintf("last metadata update at %s", lrt),
			Details: &jobspb.Progress_TableMetadataCache{
				TableMetadataCache: &jobspb.UpdateTableMetadataCacheProgress{LastRunTime: lrt},
			},
		})
		return nil
	}); err != nil {
		log.Errorf(ctx, "%s", err.Error())
	}
}

func updateTableMetadataCache(ctx context.Context, ie isql.Executor) error {
	return nil
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
			Name:        "tablemetadatacache.update_job.runs",
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
