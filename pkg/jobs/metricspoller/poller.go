// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metricspoller

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/logtags"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// metricsPoller is a singleton job whose purpose is to poll various metrics
// periodically.  These metrics are meant to be cluster wide metrics -- for
// example, number of jobs currently paused in the cluster.  While such metrics
// could be implemented locally by each node, doing so would result in the
// metric being inflated by the number of nodes.  That's not ideal, and that's
// what the purpose of this job is: namely, to provide a convenient way to query
// various aspects of cluster state, and make that state available via correctly
// counted metrics.

type metricsPoller struct {
	job *jobs.Job
}

var _ jobs.Resumer = &metricsPoller{}

// OnFailOrCancel is a part of the Resumer interface.
func (mp *metricsPoller) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	return nil
}

// CollectProfile is a part of the Resumer interface.
func (mp *metricsPoller) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

// Resume is part of the Resumer interface.
func (mp *metricsPoller) Resume(ctx context.Context, execCtx interface{}) error {
	// The metrics polling job is a forever running background job. It's always
	// safe to wind the SQL pod down whenever it's running, something we
	// indicate through the job's idle status.
	mp.job.MarkIdle(true)

	exec := execCtx.(sql.JobExecContext)
	metrics := exec.ExecCfg().JobRegistry.MetricsStruct().JobSpecificMetrics[jobspb.TypePollJobsStats].(pollerMetrics)

	var t timeutil.Timer
	defer t.Stop()

	runTask := func(name string, task func(ctx context.Context, execCtx sql.JobExecContext) error) error {
		return task(logtags.AddTag(ctx, "task", name), exec)
	}

	for {
		t.Reset(jobs.PollJobsMetricsInterval.Get(&exec.ExecCfg().Settings.SV))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			t.Read = true
			for name, task := range metricPollerTasks {
				if err := runTask(name, task); err != nil {
					log.Errorf(ctx, "Periodic stats collector task %s completed with error %s", name, err)
					metrics.NumErrors.Inc(1)
				}
			}
		}
	}
}

type pollerMetrics struct {
	NumErrors *metric.Counter
}

// metricsPollerTasks lists the list of tasks performed on each iteration
// of metrics poller.
var metricPollerTasks = map[string]func(ctx context.Context, execCtx sql.JobExecContext) error{
	"paused-jobs": updatePausedMetrics,
	"manage-pts":  manageProtectedTimestamps,
	"resolved-ts": updateTSMetrics,
}

func (m pollerMetrics) MetricStruct() {}

func newPollerMetrics() metric.Struct {
	return pollerMetrics{
		NumErrors: metric.NewCounter(metric.Metadata{
			Name:        "jobs.metrics.task_failed",
			Help:        "Number of metrics poller tasks that failed",
			Measurement: "errors",
			Unit:        metric.Unit_COUNT,
			MetricType:  io_prometheus_client.MetricType_COUNTER,
		}),
	}
}

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &metricsPoller{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypePollJobsStats, createResumerFn,
		jobs.DisablesTenantCostControl, jobs.WithJobMetrics(newPollerMetrics()))
}
