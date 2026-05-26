// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schematelemetry

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schematelemetry/schematelemetrycontroller"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
)

type schemaTelemetryExecutor struct {
	metrics schemaTelemetryJobMetrics
}

var _ jobs.ScheduledJobController = (*schemaTelemetryExecutor)(nil)
var _ jobs.ScheduledJobExecutor = (*schemaTelemetryExecutor)(nil)

type schemaTelemetryJobMetrics struct {
	*jobs.ExecutorMetrics
}

var _ metric.Struct = &schemaTelemetryJobMetrics{}

// MetricStruct is part of the metric.Struct interface.
func (m *schemaTelemetryJobMetrics) MetricStruct() {}

// OnDrop is part of the jobs.ScheduledJobController interface.
func (s schemaTelemetryExecutor) OnDrop(
	ctx context.Context,
	scheduleControllerEnv scheduledjobs.ScheduleControllerEnv,
	env scheduledjobs.JobSchedulerEnv,
	schedule *jobs.ScheduledJob,
	txn isql.Txn,
	descsCol *descs.Collection,
) (int, error) {
	return 0, errScheduleUndroppable
}

var errScheduleUndroppable = errors.New("SQL schema telemetry schedule cannot be dropped")

// ExecuteJob is part of the jobs.ScheduledJobExecutor interface.
func (s schemaTelemetryExecutor) ExecuteJob(
	ctx context.Context,
	txn isql.Txn,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
) (err error) {
	defer func() {
		if err == nil {
			s.metrics.NumStarted.Inc(1)
		} else {
			s.metrics.NumFailed.Inc(1)
		}
	}()
	p, cleanup := cfg.PlanHookMaker(ctx, "invoke-schema-telemetry", txn.KV(), username.NodeUserName())
	defer cleanup()
	jr := p.(sql.PlanHookState).ExecCfg().JobRegistry
	r := schematelemetrycontroller.CreateSchemaTelemetryJobRecord(jobs.CreatedByScheduledJobs, int64(sj.ScheduleID()))
	_, err = jr.CreateAdoptableJobWithTxn(ctx, r, jr.MakeJobID(), txn)
	return err
}

// NotifyJobTermination is part of the jobs.ScheduledJobExecutor interface.
func (s schemaTelemetryExecutor) NotifyJobTermination(
	ctx context.Context,
	txn isql.Txn,
	jobID jobspb.JobID,
	jobStatus jobs.State,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
) error {
	switch jobStatus {
	case jobs.StateFailed:
		jobs.DefaultHandleFailedRun(sj, "SQL schema telemetry job failed")
		s.metrics.NumFailed.Inc(1)
		return nil
	case jobs.StateSucceeded:
		s.metrics.NumSucceeded.Inc(1)
	}
	sj.SetScheduleStatus(string(jobStatus))
	return nil
}

// Metrics is part of the jobs.ScheduledJobExecutor interface.
func (s schemaTelemetryExecutor) Metrics() metric.Struct {
	return &s.metrics
}

// GetCreateScheduleStatement is part of the jobs.ScheduledJobExecutor interface.
func (s schemaTelemetryExecutor) GetCreateScheduleStatement(
	ctx context.Context, txn isql.Txn, env scheduledjobs.JobSchedulerEnv, sj *jobs.ScheduledJob,
) (string, error) {
	// This schedule cannot be created manually.
	return "", nil
}

func init() {
	jobs.RegisterScheduledJobExecutorFactory(
		tree.ScheduledSchemaTelemetryExecutor.InternalName(),
		func() (jobs.ScheduledJobExecutor, error) {
			m := jobs.MakeExecutorMetrics(tree.ScheduledSchemaTelemetryExecutor.InternalName())
			return &schemaTelemetryExecutor{
				metrics: schemaTelemetryJobMetrics{
					ExecutorMetrics: &m,
				},
			}, nil
		},
	)
}
