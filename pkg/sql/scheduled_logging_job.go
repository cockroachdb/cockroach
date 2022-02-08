// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/scheduledloggingjobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterLogResumerAndScheduleExecutor(
		scheduledloggingjobs.CaptureIndexUsageStatsResumeFunc,
		jobspb.TypeCaptureIndexUsageStats,
		tree.CaptureIndexUsageStatsExecutor,
		jobspb.CaptureIndexUsageStatsDetails{},
		jobspb.CaptureIndexUsageStatsProgress{},
	)
}

// RegisterLogResumerAndScheduleExecutor registers the constructor and factory
// function for the job's resumer and executor respectively.
func RegisterLogResumerAndScheduleExecutor(
	resumeFunc func(ctx context.Context, executor sqlutil.InternalExecutor) ([]eventpb.EventPayload, error),
	jobType jobspb.Type,
	executorType tree.ScheduledJobExecutorType,
	jobDetails jobspb.Details,
	jobProgress jobspb.ProgressDetails,
) {
	// Note: registering a constructor on a job type that already has a
	// constructor will overwrite it.
	jobs.RegisterConstructor(jobType, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &loggingResumer{
			job:        job,
			st:         settings,
			resumeFunc: resumeFunc,
		}
	})

	// Note: registering an executor factory on an executor name that already has
	// an executor factory will cause a panic.
	jobs.RegisterScheduledJobExecutorFactory(
		executorType.InternalName(),
		func() (jobs.ScheduledJobExecutor, error) {
			m := jobs.MakeExecutorMetrics(executorType.InternalName())
			return &loggingExecutor{
				metrics: loggingMetrics{
					ExecutorMetrics: &m,
				},
				jobType:     jobType,
				jobDetails:  jobDetails,
				jobProgress: jobProgress,
			}, nil
		})
}

type loggingResumer struct {
	job        *jobs.Job
	st         *cluster.Settings
	sj         *jobs.ScheduledJob
	resumeFunc func(ctx context.Context, ie sqlutil.InternalExecutor) ([]eventpb.EventPayload, error)
}

var _ jobs.Resumer = &loggingResumer{}

func (r *loggingResumer) Resume(ctx context.Context, execCtx interface{}) error {
	jobExecCtx := execCtx.(JobExecContext)
	events, err := r.resumeFunc(ctx, jobExecCtx.ExecCfg().InternalExecutor)
	if err != nil {
		return err
	}
	for _, event := range events {
		log.StructuredEvent(ctx, event)
	}
	return nil
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (r *loggingResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(JobExecContext)
	execCfg := p.ExecCfg()
	ie := execCfg.InternalExecutor
	return r.maybeNotifyJobTerminated(ctx, ie, execCfg, jobs.StatusFailed)
}

// maybeNotifyJobTerminated will notify the job termination
// (with termination status).
func (r *loggingResumer) maybeNotifyJobTerminated(
	ctx context.Context, ie sqlutil.InternalExecutor, exec *ExecutorConfig, status jobs.Status,
) error {
	log.Infof(ctx, "logging job with id %d terminated with status = %s", r.job.ID(), status)
	if r.sj != nil {
		env := scheduledjobs.ProdJobSchedulerEnv
		if knobs, ok := exec.DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs); ok {
			if knobs.JobSchedulerEnv != nil {
				env = knobs.JobSchedulerEnv
			}
		}
		if err := jobs.NotifyJobTermination(
			ctx, env, r.job.ID(), status, r.job.Details(), r.sj.ScheduleID(),
			ie, nil /* txn */); err != nil {
			return err
		}

		return nil
	}
	return nil
}

type loggingMetrics struct {
	*jobs.ExecutorMetrics
}

var _ metric.Struct = &loggingMetrics{}

// MetricStruct implements metric.Struct interface.
func (m loggingMetrics) MetricStruct() {}

// loggingExecutor is executed by scheduledjob subsystem
// to launch loggingResumer through the job subsystem.
type loggingExecutor struct {
	metrics     loggingMetrics
	jobType     jobspb.Type
	jobDetails  jobspb.Details
	jobProgress jobspb.ProgressDetails
}

// Pass linting (unused type).
var _ jobs.ScheduledJobExecutor = &loggingExecutor{}

// Metrics implements jobs.ScheduledJobExecutor interface.
func (e *loggingExecutor) Metrics() metric.Struct {
	return e.metrics
}

// ExecuteJob implements the jobs.ScheduledJobExecutor interface.
func (e *loggingExecutor) ExecuteJob(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	if err := e.createLoggingJob(ctx, cfg, sj, txn); err != nil {
		e.metrics.NumFailed.Inc(1)
	}
	e.metrics.NumStarted.Inc(1)
	return nil
}

func (e *loggingExecutor) createLoggingJob(
	ctx context.Context, cfg *scheduledjobs.JobExecutionConfig, sj *jobs.ScheduledJob, txn *kv.Txn,
) error {
	p, cleanup := cfg.PlanHookMaker("invoke-logging-job", txn, security.NodeUserName())
	defer cleanup()

	jobRegistry := p.(*planner).ExecCfg().JobRegistry
	ie := cfg.InternalExecutor
	createdByInfo := &jobs.CreatedByInfo{
		ID:   sj.ScheduleID(),
		Name: jobs.CreatedByScheduledJobs,
	}

	err := checkExistingLoggingJob(ctx, ie, txn, e.jobType)
	if err != nil {
		return err
	}

	record := jobs.Record{
		Description: "Logging Job: " + e.jobType.String(),
		Username:    security.NodeUserName(),
		Details:     e.jobDetails,
		Progress:    e.jobProgress,
		CreatedBy:   createdByInfo,
	}

	jobID := jobRegistry.MakeJobID()
	if _, err := jobRegistry.CreateAdoptableJobWithTxn(ctx, record, jobID, txn); err != nil {
		return err
	}
	return nil
}

// NotifyJobTermination implements the jobs.ScheduledJobExecutor interface.
func (e *loggingExecutor) NotifyJobTermination(
	ctx context.Context,
	jobID jobspb.JobID,
	jobStatus jobs.Status,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	ex sqlutil.InternalExecutor,
	txn *kv.Txn,
) error {
	if jobStatus == jobs.StatusFailed {
		jobs.DefaultHandleFailedRun(sj, "logging job %d failed", jobID)
		e.metrics.NumFailed.Inc(1)
		return nil
	}

	if jobStatus == jobs.StatusSucceeded {
		e.metrics.NumSucceeded.Inc(1)
	}

	sj.SetScheduleStatus(string(jobStatus))
	return nil
}

// GetCreateScheduleStatement implements the jobs.ScheduledJobExecutor interface.
func (e *loggingExecutor) GetCreateScheduleStatement(
	ctx context.Context,
	env scheduledjobs.JobSchedulerEnv,
	txn *kv.Txn,
	sj *jobs.ScheduledJob,
	ex sqlutil.InternalExecutor,
) (string, error) {
	// TODO(thomas): get create schedule statement
	return "", nil
}

// checkExistingLoggingJob checks for existing logging job of the same type.
// If an existing logging job of the same type is found, an error is returned,
// preventing a duplicate job from being created.
func checkExistingLoggingJob(
	ctx context.Context, ie sqlutil.InternalExecutor, txn *kv.Txn, jobType jobspb.Type,
) error {
	jobID := jobspb.InvalidJobID
	exists, err := jobs.RunningJobExists(ctx, jobID, ie, txn, func(payload *jobspb.Payload) bool {
		return payload.Type() == jobType
	})

	if err == nil && exists {
		err = errors.Newf("another logging job of the same type '%s' is running\n", jobType.String())
	}
	return err
}
