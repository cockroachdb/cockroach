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

	stopper := execCtx.ExecCfg().Stopper
	// Channel used to signal the job should run.

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

	signalCh := make(chan struct{})
	execCtx.ExecCfg().SQLStatusServer.SetUpdateTableMetadataCachSignal(signalCh)

	for {
		select {
		case <-signalCh:
			log.Infof(ctx, "running table metadata update job")
			metrics.NumRuns.Inc(1)
			if err := j.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
				if md.RunStats != nil {
					ju.UpdateRunStats(md.RunStats.NumRuns, timeutil.Now())
				}
				return nil
			}); err != nil {
				log.Errorf(ctx, "%s", err.Error())
			}

			// TODO(xinhaoz): implement the actual table metadata update logic.

		case <-ctx.Done():
			return ctx.Err()
		case <-stopper.ShouldQuiesce():
			return nil
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
	NumRuns *metric.Counter
}

func (m TableMetadataUpdateJobMetrics) MetricStruct() {}

func newTableMetadataUpdateJobMetrics() metric.Struct {
	return TableMetadataUpdateJobMetrics{
		NumRuns: metric.NewCounter(metric.Metadata{
			Name:        "jobs.update_table_metadata.runs",
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
