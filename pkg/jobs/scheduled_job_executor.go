// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
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
		cfg *scheduledjobs.JobExecutionConfig,
		env scheduledjobs.JobSchedulerEnv,
		schedule *ScheduledJob,
		txn *kv.Txn,
	) error

	// Notifies that the system.job started by the ScheduledJob completed.
	// Implementation may use provided transaction to perform any additional mutations.
	// Modifications to the ScheduledJob object will be persisted.
	NotifyJobTermination(
		ctx context.Context,
		jobID jobspb.JobID,
		jobStatus Status,
		details jobspb.Details,
		env scheduledjobs.JobSchedulerEnv,
		schedule *ScheduledJob,
		ex sqlutil.InternalExecutor,
		txn *kv.Txn,
	) error

	// Metrics returns optional metric.Struct object for this executor.
	Metrics() metric.Struct
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
func GetScheduledJobExecutor(name string) (ScheduledJobExecutor, bool, error) {
	executorRegistry.Lock()
	defer executorRegistry.Unlock()
	if executorRegistry.executors == nil {
		executorRegistry.executors = make(map[string]ScheduledJobExecutor)
	}
	if ex, ok := executorRegistry.executors[name]; ok {
		return ex, false, nil
	}
	ex, err := newScheduledJobExecutorLocked(name)
	if err != nil {
		return nil, false, err
	}
	executorRegistry.executors[name] = ex
	return ex, true, nil
}

// DefaultHandleFailedRun is a default implementation for handling failed run
// (either system.job failure, or perhaps error processing the schedule itself).
func DefaultHandleFailedRun(schedule *ScheduledJob, fmtOrMsg string, args ...interface{}) {
	switch schedule.ScheduleDetails().OnError {
	case jobspb.ScheduleDetails_RETRY_SOON:
		schedule.SetScheduleStatus("retrying: "+fmtOrMsg, args...)
		schedule.SetNextRun(schedule.env.Now().Add(retryFailedJobAfter)) // TODO(yevgeniy): backoff
	case jobspb.ScheduleDetails_PAUSE_SCHED:
		schedule.Pause()
		schedule.SetScheduleStatus("schedule paused: "+fmtOrMsg, args...)
	case jobspb.ScheduleDetails_RETRY_SCHED:
		schedule.SetScheduleStatus("reschedule: "+fmtOrMsg, args...)
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
	env scheduledjobs.JobSchedulerEnv,
	jobID jobspb.JobID,
	jobStatus Status,
	jobDetails jobspb.Details,
	scheduleID int64,
	ex sqlutil.InternalExecutor,
	txn *kv.Txn,
) error {
	if env == nil {
		env = scheduledjobs.ProdJobSchedulerEnv
	}

	schedule, err := LoadScheduledJob(ctx, env, scheduleID, ex, txn)
	if err != nil {
		return err
	}
	executor, _, err := GetScheduledJobExecutor(schedule.ExecutorType())
	if err != nil {
		return err
	}

	// Delegate handling of the job termination to the executor.
	err = executor.NotifyJobTermination(ctx, jobID, jobStatus, jobDetails, env, schedule, ex, txn)
	if err != nil {
		return err
	}

	// Update this schedule in case executor made changes to it.
	return schedule.Update(ctx, ex, txn)
}
