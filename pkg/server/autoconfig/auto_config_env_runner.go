// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package autoconfig

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/acprovider"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/autoconfigpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// createEnvRunnerJob creates a job to execute tasks for the given
// environment.
//
// Refer to the package-level documentation for more details.
func createEnvRunnerJob(
	ctx context.Context,
	txn isql.Txn,
	registry *jobs.Registry,
	jobID jobspb.JobID,
	envID EnvironmentID,
) error {
	jobRecord := jobs.Record{
		Description: "runs configuration tasks",
		Username:    username.NodeUserName(),
		Details:     jobspb.AutoConfigEnvRunnerDetails{EnvID: envID},
		Progress:    jobspb.AutoConfigEnvRunnerProgress{},
		CreatedBy: &jobs.CreatedByInfo{
			Name: makeEnvRunnerJobCreatedKey(envID),
		},
		NonCancelable: true,
	}

	_, err := registry.CreateJobWithTxn(ctx, jobRecord, jobID, txn)
	return err
}

func makeEnvRunnerJobCreatedKey(envID EnvironmentID) string {
	const autoConfigEnvRunnerCreatedName = "auto-config-env-runner"
	return fmt.Sprintf("%s:%s", autoConfigEnvRunnerCreatedName, envID)
}

// envRunner is the runner for one task environment.
type envRunner struct {
	envID EnvironmentID
	job   *jobs.Job
}

var _ jobs.Resumer = (*envRunner)(nil)

// OnFailOrCancel is a part of the Resumer interface.
func (r *envRunner) OnFailOrCancel(ctx context.Context, execCtx interface{}, jobErr error) error {
	return nil
}

// CollectProfile is a part of the Resumer interface.
func (r *envRunner) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

// Resume is part of the Resumer interface.
func (r *envRunner) Resume(ctx context.Context, execCtx interface{}) error {
	ctx = logtags.AddTag(ctx, "taskenv", r.envID)

	// The auto config runner is a forever running background job.
	// It's always safe to wind the SQL pod down whenever it's
	// running, something we indicate through the job's idle
	// status.
	r.job.MarkIdle(true)

	exec := execCtx.(sql.JobExecContext)
	execCfg := exec.ExecCfg()

	// Provider gives us tasks to run.
	provider := getProvider(ctx, execCfg)

	for {
		log.Infof(ctx, "waiting for more tasks...")
		task, err := provider.Peek(ctx, r.envID)
		if err != nil {
			if errors.Is(err, acprovider.ErrNoMoreTasks) {
				// No more tasks to process. Just stop the runner.
				return nil
			}
			return err
		}

		r.job.MarkIdle(false)
		var waitSome bool
		waitSome, err = r.maybeRunNextTask(ctx, provider, execCfg, task)
		if err != nil {
			log.Warningf(ctx, "error processing auto config: %v", err)
		}
		r.job.MarkIdle(true)
		if err != nil || waitSome {
			// Wait a bit before retrying.
			select {
			// TODO(knz): Maybe make this delay configurable.
			case <-time.After(5 * time.Second):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (r *envRunner) maybeRunNextTask(
	ctx context.Context,
	provider acprovider.Provider,
	execCfg *sql.ExecutorConfig,
	nextTask autoconfigpb.Task,
) (waitSome bool, err error) {
	// Wait on any task jobs, if any.
	//
	// This is an optimization: the logic below would work without this step.
	// We do this to avoid waiting too much after a task has completed
	// (the logic below waits for a random amount of time in case of conflict).
	if err = r.maybeWaitForCurrentTaskJob(ctx, execCfg); err != nil {
		return true, err
	}

	// The job ID we'll use.
	jobID := execCfg.JobRegistry.MakeJobID()
	log.Infof(ctx, "allocated job ID %d for next task candidate %d", jobID, nextTask.TaskID)
	var nextTaskID TaskID

	err = execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) (resErr error) {
		// Re-check if there any other started task already.
		otherTaskID, _, err := getCurrentlyStartedTaskID(ctx, txn, r.envID)
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
		lastTaskID, err := getLastCompletedTaskID(ctx, txn, r.envID)
		if err != nil {
			return err
		}

		// Did another node catch up with us?
		if lastTaskID >= nextTask.TaskID {
			// Yes. Just tell the provider what we found and retry
			// the peek.
			return provider.Pop(ctx, r.envID, lastTaskID)
		}

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
		if err := writeStartMarker(ctx, txn,
			InfoKeyTaskRef{Environment: r.envID, Task: nextTaskID}, jobID); err != nil {
			return errors.Wrapf(err, "unable to write start marker for task %d", nextTaskID)
		}

		// Finally, create the job.
		if err := createTaskJob(ctx, txn, execCfg.JobRegistry, jobID, r.envID, nextTask); err != nil {
			return errors.Wrapf(err, "unable to create job %d for task %d", jobID, nextTaskID)
		}
		return nil
	})

	if err == nil && nextTaskID != 0 {
		log.Infof(ctx, "created job %d for task %d", jobID, nextTaskID)
	}

	return waitSome, err
}

// maybeWaitForCurrentTaskJob checks whether there is a current task
// running (there's a start marker for one); if there is, it waits for
// its job to complete.
func (r *envRunner) maybeWaitForCurrentTaskJob(
	ctx context.Context, execCfg *sql.ExecutorConfig,
) error {
	var prevJobID jobspb.JobID
	var prevTaskID TaskID

	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		var err error
		prevTaskID, prevJobID, err = getCurrentlyStartedTaskID(ctx, txn, r.envID)
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

func init() {
	// Note: we disable tenant cost control because auto-config is used
	// by operators and should thus not incur costs (or performance
	// penalties) to tenants.
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		jd := job.Details()
		details := jd.(jobspb.AutoConfigEnvRunnerDetails)
		return &envRunner{envID: details.EnvID, job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeAutoConfigEnvRunner, createResumerFn, jobs.DisablesTenantCostControl)
}
