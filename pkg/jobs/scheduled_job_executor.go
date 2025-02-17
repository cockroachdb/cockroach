// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// ScheduledJobExecutor is an interface describing execution of the scheduled job.
type ScheduledJobExecutor interface {
	// Executes scheduled job;  Implementation may use provided transaction.
	// Modifications to the ScheduledJob object will be persisted.
	ExecuteJob(
		ctx context.Context,
		txn isql.Txn,
		cfg *scheduledjobs.JobExecutionConfig,
		env scheduledjobs.JobSchedulerEnv,
		schedule *ScheduledJob,
	) error

	// NotifyJobTermination notifies that the system.job started by the
	// ScheduledJob completed. Implementation may use provided transaction to
	// perform any additional mutations. Modifications to the ScheduledJob
	// object will be persisted.
	NotifyJobTermination(
		ctx context.Context,
		txn isql.Txn,
		jobID jobspb.JobID,
		jobState State,
		details jobspb.Details,
		env scheduledjobs.JobSchedulerEnv,
		schedule *ScheduledJob,
	) error

	// Metrics returns optional metric.Struct object for this executor.
	Metrics() metric.Struct

	// GetCreateScheduleStatement returns a `CREATE SCHEDULE` statement that is
	// functionally equivalent to the statement that led to the creation of
	// the passed in `schedule`.
	GetCreateScheduleStatement(
		ctx context.Context,
		txn isql.Txn,
		env scheduledjobs.JobSchedulerEnv,
		sj *ScheduledJob,
	) (string, error)
}

// ScheduledJobController is an interface describing hooks that will execute
// when controlling a scheduled job.
type ScheduledJobController interface {
	// OnDrop runs before the passed in `schedule` is dropped as part of a `DROP
	// SCHEDULE` query. OnDrop may drop the schedule's dependent schedules and will
	// return the number of additional schedules it drops.
	OnDrop(
		ctx context.Context,
		scheduleControllerEnv scheduledjobs.ScheduleControllerEnv,
		env scheduledjobs.JobSchedulerEnv,
		schedule *ScheduledJob,
		txn isql.Txn,
		descsCol *descs.Collection,
	) (int, error)
}

// ScheduledJobExecutorFactory is a callback to create a ScheduledJobExecutor.
type ScheduledJobExecutorFactory = func() (ScheduledJobExecutor, error)

var executorRegistry struct {
	syncutil.Mutex
	factories map[string]ScheduledJobExecutorFactory
	executors map[string]ScheduledJobExecutor
}

// RegisterScheduledJobExecutorFactory registers callback for creating ScheduledJobExecutor
// with the specified name.
func RegisterScheduledJobExecutorFactory(name string, factory ScheduledJobExecutorFactory) {
	executorRegistry.Lock()
	defer executorRegistry.Unlock()
	if executorRegistry.factories == nil {
		executorRegistry.factories = make(map[string]ScheduledJobExecutorFactory)
	}

	if _, ok := executorRegistry.factories[name]; ok {
		panic("executor " + name + " already registered")
	}
	executorRegistry.factories[name] = factory
}

// newScheduledJobExecutor creates new instance of ScheduledJobExecutor.
func newScheduledJobExecutorLocked(name string) (ScheduledJobExecutor, error) {
	if factory, ok := executorRegistry.factories[name]; ok {
		return factory()
	}
	return nil, errors.Newf("executor %q is not registered", name)
}

// GetScheduledJobExecutor returns a singleton instance of
// ScheduledJobExecutor and a flag indicating if that instance was just created.
func GetScheduledJobExecutor(name string) (ScheduledJobExecutor, error) {
	executorRegistry.Lock()
	defer executorRegistry.Unlock()
	return getScheduledJobExecutorLocked(name)
}

func getScheduledJobExecutorLocked(name string) (ScheduledJobExecutor, error) {
	if executorRegistry.executors == nil {
		executorRegistry.executors = make(map[string]ScheduledJobExecutor)
	}
	if ex, ok := executorRegistry.executors[name]; ok {
		return ex, nil
	}
	ex, err := newScheduledJobExecutorLocked(name)
	if err != nil {
		return nil, err
	}
	executorRegistry.executors[name] = ex
	return ex, nil
}

// RegisterExecutorsMetrics registered the metrics updated by each executor.
func RegisterExecutorsMetrics(registry *metric.Registry) error {
	executorRegistry.Lock()
	defer executorRegistry.Unlock()

	for executorType := range executorRegistry.factories {
		ex, err := getScheduledJobExecutorLocked(executorType)
		if err != nil {
			return err
		}
		if m := ex.Metrics(); m != nil {
			registry.AddMetricStruct(m)
		}
	}

	return nil
}

// DefaultHandleFailedRun is a default implementation for handling failed run
// (either system.job failure, or perhaps error processing the schedule itself).
func DefaultHandleFailedRun(schedule *ScheduledJob, fmtOrMsg string, args ...interface{}) {
	switch schedule.ScheduleDetails().OnError {
	case jobspb.ScheduleDetails_RETRY_SOON:
		schedule.SetScheduleStatusf("retrying: "+fmtOrMsg, args...)
		schedule.SetNextRun(schedule.env.Now().Add(retryFailedJobAfter)) // TODO(yevgeniy): backoff
	case jobspb.ScheduleDetails_PAUSE_SCHED:
		schedule.Pause()
		schedule.SetScheduleStatusf("schedule paused: "+fmtOrMsg, args...)
	case jobspb.ScheduleDetails_RETRY_SCHED:
		schedule.SetScheduleStatusf("reschedule: "+fmtOrMsg, args...)
	}
}

// NotifyJobTermination is invoked when the job triggered by specified schedule
// completes
//
// The 'txn' transaction argument is the transaction the job will use to update its
// state (e.g. status, etc).  If any changes need to be made to the scheduled job record,
// those changes are applied to the same transaction -- that is, they are applied atomically
// with the job status changes.
func NotifyJobTermination(
	ctx context.Context,
	txn isql.Txn,
	env scheduledjobs.JobSchedulerEnv,
	jobID jobspb.JobID,
	jobStatus State,
	jobDetails jobspb.Details,
	scheduleID jobspb.ScheduleID,
) error {
	if env == nil {
		env = scheduledjobs.ProdJobSchedulerEnv
	}
	schedules := ScheduledJobTxn(txn)
	schedule, err := schedules.Load(ctx, env, scheduleID)
	if err != nil {
		return err
	}
	executor, err := GetScheduledJobExecutor(schedule.ExecutorType())
	if err != nil {
		return err
	}

	// Delegate handling of the job termination to the executor.
	err = executor.NotifyJobTermination(ctx, txn, jobID, jobStatus, jobDetails, env, schedule)
	if err != nil {
		return err
	}

	// Update this schedule in case executor made changes to it.
	return schedules.Update(ctx, schedule)
}
