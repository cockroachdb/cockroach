// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package autoconfig

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/acprovider"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/autoconfigpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// autoConfigRunner runs the job that accepts new auto configuration
// payloads and sequences the creation of individual jobs to execute
// them.
// The auto config payloads are run by the taskRunner defined in
// auto_config_task.go.
//
// Refer to the package-level documentation for more details.
type autoConfigRunner struct {
	job *jobs.Job
}

var _ jobs.Resumer = (*autoConfigRunner)(nil)

// OnFailOrCancel is a part of the Resumer interface.
func (r *autoConfigRunner) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	return nil
}

// Resume is part of the Resumer interface.
func (r *autoConfigRunner) Resume(ctx context.Context, execCtx interface{}) error {
	// The auto config runner is a forever running background job.
	// It's always safe to wind the SQL pod down whenever it's
	// running, something we indicate through the job's idle
	// status.
	r.job.MarkIdle(true)

	exec := execCtx.(sql.JobExecContext)
	execCfg := exec.ExecCfg()

	// Note: even though the job registry cancels all running job upon
	// shutdown, we find some tests fail unless this job also does its
	// own wait.
	shutdownCh := execCfg.RPCContext.Stopper.ShouldQuiesce()

	// Provider gives us tasks to run.
	provider := execCfg.AutoConfigProvider
	if provider == nil {
		panic(errors.AssertionFailedf("programming error: missing provider"))
	}
	log.Infof(ctx, "using provider with type %T", provider)

	// wait is the channel that indicates there are new tasks to run.
	waitForMoreTasks := provider.RegisterTasksChannel()

	// taskQueue will contain the tasks remaining to run.
	var taskQueue []autoconfigpb.Task

	for {
		if len(taskQueue) == 0 {
			// No tasks to create. Just wait until some tasks are delivered.
			log.Infof(ctx, "waiting for more tasks...")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-shutdownCh:
				return context.Canceled

			case <-waitForMoreTasks:
				taskQueue = provider.GetTasks()
				continue
			}
		}

		r.job.MarkIdle(false)
		var waitSome bool
		var err error
		waitSome, taskQueue, err = maybeRunNextTask(ctx, provider, execCfg, taskQueue)
		if err != nil {
			log.Warningf(ctx, "error processing auto config: %v", err)
		}
		r.job.MarkIdle(true)
		if err != nil || waitSome {
			// Wait a bit before retrying.
			select {
			case <-time.After(5 * time.Second):
				continue
			case <-ctx.Done():
				return ctx.Err()
			case <-shutdownCh:
				return context.Canceled
			}
		}
	}
}

// maybeRunNextTask checks which task was last executed, ratchets the
// task queue beyond the last completed tasks, and attempts to start
// the first next task in the queue.
func maybeRunNextTask(
	ctx context.Context,
	provider acprovider.Provider,
	execCfg *sql.ExecutorConfig,
	taskQueue []autoconfigpb.Task,
) (waitSome bool, remainingTaskQueue []autoconfigpb.Task, err error) {
	log.Infof(ctx, "got %d tasks to create", len(taskQueue))

	// First of all, wait on any task jobs, if any.
	//
	// This is an optimization: the logic below would work without this step.
	// We do this to avoid waiting too much after a task has completed
	// (the logic below waits for a random amount of time in case of conflict).
	if err = maybeWaitForCurrentTaskJob(ctx, execCfg); err != nil {
		return true, taskQueue, err
	}

	// Now on to the actual work.
	//
	// Transactionally, we are going to extract the last known
	// completed task; ratchet our queue forward to that point; assert
	// there is no already-started task beyond it; then create our
	// job.

	// The job ID we'll use.
	jobID := execCfg.JobRegistry.MakeJobID()
	log.Infof(ctx, "allocated job ID %d for next task (of %d)", jobID, len(taskQueue))
	var nextTaskID uint64

	err = execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) (resErr error) {
		// Is there any other started task already?
		otherTaskID, _, err := getCurrentlyStartedTaskID(ctx, txn, execCfg)
		if err != nil {
			return err
		}
		if otherTaskID != 0 {
			// What likely happened is that another node caught up with us,
			// observed the job completion for the previous task and already
			// created the start marker and job for the next one.
			log.Infof(ctx, "found start marker for task %d, unable to start another task", otherTaskID)
			// We will simply retry, with some jittered delay.
			waitSome = true
			return nil
		}

		// Find the latest completed task.
		lastTaskID, err := getLastCompletedTaskID(ctx, txn, execCfg)
		if err != nil {
			return err
		}

		// If we've seen a completed task, ratchet the task list forward.
		if lastTaskID > 0 {
			log.Infof(ctx, "found last completed task %d", lastTaskID)
			provider.ReportLastKnownCompletedTaskID(lastTaskID)
			taskQueue = ratchetTaskQueue(ctx, taskQueue, lastTaskID)
			// Ratchet the task queue forward.
			if len(taskQueue) == 0 {
				// Nothing remaining to do!
				return nil
			}
		}

		// nextTask is the head of the queue: the first next task that
		// hasn't been executed yet.
		nextTask := taskQueue[0]
		// Can we even start this task? It may have a min version condition.
		if !execCfg.Settings.Version.ActiveVersion(ctx).IsActiveVersion(nextTask.MinVersion) {
			log.Infof(ctx, "next task %d has min version requirement %v while cluster is at version %v; waiting...", nextTask.TaskID, nextTask.MinVersion, execCfg.Settings.Version.ActiveVersion(ctx))
			waitSome = true
			return nil
		}

		// Enable the log event at the end of the surrounding function.
		nextTaskID = nextTask.TaskID

		// Now we can create the start marker for our next task.
		//
		// We report the job ID in the value field to help with
		// observability and to enable the call to
		// maybeWaitForCurrentTaskJob(), which is an optimization. Storing
		// the job ID is not strictly required for sequencing the tasks.
		if err := writeStartMarker(ctx, txn, execCfg, jobID, nextTaskID); err != nil {
			return errors.Wrapf(err, "unable to write start marker for task %d", nextTaskID)
		}

		// Finally, create the job.
		if err := createTaskJob(ctx, txn, execCfg, jobID, nextTask); err != nil {
			return errors.Wrapf(err, "unable to create job %d for task %d", jobID, nextTaskID)
		}
		return nil
	})

	if err == nil && nextTaskID != 0 {
		log.Infof(ctx, "created job %d for task %d", jobID, nextTaskID)
	}

	return waitSome, taskQueue, err
}

// maybeWaitForCurrentTaskJob checks whether there is a current task
// running (there's a start marker for one); if there is, it waits for
// its job to complete.
func maybeWaitForCurrentTaskJob(ctx context.Context, execCfg *sql.ExecutorConfig) error {
	var prevJobID jobspb.JobID
	var prevTaskID uint64

	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		var err error
		prevTaskID, prevJobID, err = getCurrentlyStartedTaskID(ctx, txn, execCfg)
		return err
	}); err != nil {
		return errors.Wrap(err, "checking latest task job")
	}

	if prevJobID != 0 {
		// We have a job already. Just wait for it.
		log.Infof(ctx, "waiting for task %d, job %d to complete", prevTaskID, prevJobID)
		if err := execCfg.JobRegistry.Run(ctx, []jobspb.JobID{prevJobID}); err != nil {
			// Job fail errors here are not hard errors; it may be that
			// the job was simply cancelled by the user.
			log.Infof(ctx, "previous task job error: %v", err)
		}
	}
	return nil
}

// rastchedTaskQueue skips all tasks in the queue with an ID smaller
// or equal to that of the last known completed task.
func ratchetTaskQueue(
	ctx context.Context, taskQueue []autoconfigpb.Task, lastCompletedTaskID uint64,
) []autoconfigpb.Task {
	for len(taskQueue) > 0 {
		if taskQueue[0].TaskID > lastCompletedTaskID {
			break
		}
		log.Infof(ctx, "skipping task %d that is already completed", taskQueue[0].TaskID)
		taskQueue = taskQueue[1:]
	}
	return taskQueue
}

func init() {
	// Note: we disable tenant cost control because auto-config is used
	// by operators and should thus not incur costs (or performance
	// penalties) to tenants.
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &autoConfigRunner{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeAutoConfigRunner, createResumerFn, jobs.DisablesTenantCostControl)
}
