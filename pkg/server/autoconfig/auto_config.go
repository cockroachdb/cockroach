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
	"bytes"
	"context"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/autoconfigpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

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
	jobRegistry := execCfg.JobRegistry

	// Provider gives us tasks to run.
	provider := execCfg.AutoConfigProvider
	if provider == nil {
		// Misconfiguration!
		log.Errorf(ctx, "programming error: no provider set")
		<-ctx.Done() // don't busy-loop.
		return nil
	}
	log.Infof(ctx, "found provider with type %T", provider)

	wait := provider.RegisterTasksChannel()

	var taskQueue []autoconfigpb.Task

	for {
		if len(taskQueue) == 0 {
			// No tasks to create. Just wait until some tasks are delivered.
			log.Infof(ctx, "waiting for more tasks...")
			select {
			case <-ctx.Done():
				return ctx.Err()

			case <-wait:
				taskQueue = provider.GetTasks()
				continue
			}
		}

		// We have some tasks to create.
		log.Infof(ctx, "found %d tasks to create", len(taskQueue))

		// First of all, wait on any task jobs, if any.
		// This is an optimization: the logic below would work without this step.
		// We do this to avoid waiting too much after a task has completed
		// (the logic below waits for a random amount of time in case of conflict).
		var prevJobID jobspb.JobID
		if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			row, err := txn.QueryRowEx(ctx, "get-task-start-info", txn.KV(), sessiondata.NodeUserSessionDataOverride,
				"SELECT info_key, value FROM system.job_info WHERE job_id = $1 AND info_key >= $2 ORDER BY info_key DESC, written ASC LIMIT 1",
				jobs.AutoConfigRunnerJobID, []byte(InfoKeyStartPrefix))
			if err != nil {
				return errors.Wrap(err, "finding last task start marker")
			}
			if row != nil {
				// There's a started task. Retrieve is job ID from the start marker value.
				infoKey := []byte(tree.MustBeDBytes(row[0]))
				prevTaskID, err := DecodeTaskID([]byte(InfoKeyStartPrefix), infoKey)
				if err != nil {
					log.Warningf(ctx, "while decoding marker: %v", err)
				} else {
					valueBytes := tree.MustBeDBytes(row[1])
					jid, err := strconv.ParseInt(string(valueBytes), 10, 64)
					if err != nil {
						log.Warningf(ctx, "while decoding value for start marker for task %d: %v", prevTaskID, err)
					} else {
						prevJobID = jobspb.JobID(jid)
						log.Infof(ctx, "found active job %d for task %d", prevJobID, prevTaskID)
					}
				}
			}
			return nil
		}); err != nil {
			log.Warningf(ctx, "checking latest task job: %v", err)
			// Just retry later.
			select {
			case <-time.After(5 * time.Second):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if prevJobID != 0 {
			// We have a job already. Just wait for it.
			log.Infof(ctx, "waiting for job %d to complete", prevJobID)
			if err := jobRegistry.Run(ctx, []jobspb.JobID{prevJobID}); err != nil {
				// Job fail errors here are not hard errors.
				log.Infof(ctx, "previous task job error: %v", err)
			}
		}

		// Now on to the actual work.
		//
		// Transactionally, we are going to extract the last known
		// completed task; ratchet our queue forward to that point; assert
		// there is no already-started task beyond it; then create our
		// job.

		// The job ID we'll use.
		jobID := jobRegistry.MakeJobID()
		log.Infof(ctx, "attempting to create job %d for next task of %d", jobID, len(taskQueue))

		forceWait := false
		err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) (resErr error) {
			defer func() {
				log.Infof(ctx, "encountered error: %v", resErr)
			}()

			// Find the latest completed task.
			row, err := txn.QueryRowEx(ctx, "get-last-task-completion-info", txn.KV(), sessiondata.NodeUserSessionDataOverride,
				"SELECT info_key FROM system.job_info WHERE job_id = $1 AND info_key >= $2 AND info_key < $3 ORDER BY info_key DESC, written ASC LIMIT 1",
				jobs.AutoConfigRunnerJobID, []byte(InfoKeyCompletionPrefix), []byte(InfoKeyStartPrefix))
			if err != nil {
				return errors.Wrap(err, "finding latest task completion marker")
			}
			var lastTaskID uint64
			if row != nil {
				infoKey := []byte(tree.MustBeDBytes(row[0]))
				lastTaskID, err = DecodeTaskID([]byte(InfoKeyCompletionPrefix), infoKey)
				if err != nil {
					return errors.Wrap(err, "decoding info key")
				}
				log.Infof(ctx, "found last completed task %d", lastTaskID)
			}

			// Ratchet the task queue forward.
			for len(taskQueue) > 0 {
				if taskQueue[0].TaskID > lastTaskID {
					break
				}
				log.Infof(ctx, "skipping task %d that is already completed", taskQueue[0].TaskID)
				taskQueue = taskQueue[1:]
			}
			if len(taskQueue) == 0 {
				// Nothing remaining to do!
				return nil
			}

			nextTask := taskQueue[0]
			nextTaskStartMarker := MakeTaskInfoKey([]byte(InfoKeyStartPrefix), nextTask.TaskID)

			// Do we have a start marker for the next task already?
			// (Another server has caught up with us.)
			row, err = txn.QueryRowEx(ctx, "get-task-start-info", txn.KV(), sessiondata.NodeUserSessionDataOverride,
				"SELECT info_key FROM system.job_info WHERE job_id = $1 AND info_key >= $2 ORDER BY info_key DESC, written ASC LIMIT 1",
				jobs.AutoConfigRunnerJobID, nextTaskStartMarker)
			if err != nil {
				return errors.Wrap(err, "finding next task start marker")
			}
			if row != nil {
				// A task for this or another ID was found. This is a conflict: another
				// server already created a next task marker.
				infoKey := []byte(tree.MustBeDBytes(row[0]))
				otherTaskID, err := DecodeTaskID([]byte(InfoKeyStartPrefix), infoKey)
				if err != nil {
					log.Warningf(ctx, "while decoding marker: %v", err)
				}
				log.Infof(ctx, "cannot start task %d, found start marker for task %d", nextTask.TaskID, otherTaskID)
				// We will simply retry.
				forceWait = true
				return nil
			}

			// Now we can create the start marker for our next task.
			//
			// Although we don't use the value field, we report the job ID in there to help with observability.
			if err := jobRegistry.WriteJobInfo(ctx,
				jobs.AutoConfigRunnerJobID,
				nextTaskStartMarker,                           /* infoKey */
				[]byte(strconv.FormatUint(uint64(jobID), 10)), /* value */
				txn,
			); err != nil {
				return errors.Wrapf(err, "unable to write start marker for task %d", nextTask.TaskID)
			}

			// Finally, create the job.
			jobRecord := jobs.Record{
				Description:   nextTask.Description,
				Username:      username.NodeUserName(),
				Statements:    []string{"(...auto config...)"},
				Details:       jobspb.AutoConfigTaskDetails{Task: nextTask},
				Progress:      jobspb.AutoConfigTaskProgress{},
				RunningStatus: "executing",
				CreatedBy: &jobs.CreatedByInfo{
					Name: AutoConfigTaskCreatedName,
					ID:   int64(nextTask.TaskID),
				},
			}

			_, err = jobRegistry.CreateJobWithTxn(ctx, jobRecord, jobID, txn)
			if err != nil {
				return errors.Wrapf(err, "unable to create job %d for task %d: %v", jobID, nextTask.TaskID)
			}
			log.Infof(ctx, "created job %d for task %d", jobID, nextTask.TaskID)
			return nil
		})
		if err != nil {
			log.Warningf(ctx, "error processing auto config: %v", err)
		}
		if err != nil || forceWait {
			// Wait a bit before retrying.
			select {
			case <-time.After(5 * time.Second):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return nil
}

// AutoConfigTaskCreatedName is the value in created_by_name for jobs
// created by the auto config runner.
const AutoConfigTaskCreatedName = "auto-config-task"

// InfoKeyCompletionPrefix is the prefix of the key inserted in job_info
// when a task has completed.
const InfoKeyCompletionPrefix = "completed-"

// InfoKeyStartPrefix is the prefix of the key inserted in job_info
// when a task has started.
const InfoKeyStartPrefix = "started-"

// DecodeTaskID returns the task ID for the given infoKey.
func DecodeTaskID(prefix []byte, infoKey []byte) (uint64, error) {
	if !bytes.HasPrefix(infoKey, prefix) {
		return 0, errors.AssertionFailedf("programming error: prefix %q missing: %q", string(prefix), string(infoKey))
	}
	infoKey = infoKey[len(prefix):]
	_, v, err := encoding.DecodeUvarintAscending(infoKey)
	return v, err
}

// MakeTaskInfoKey creates a job_info infoKey for the given task.
func MakeTaskInfoKey(prefix []byte, taskID uint64) []byte {
	infoKey := make([]byte, 0, len(prefix)+10)
	infoKey = append(infoKey, prefix...)
	infoKey = encoding.EncodeUvarintAscending(infoKey, taskID)
	return infoKey
}

type taskRunner struct {
	job *jobs.Job
}

// OnFailOrCancel is a part of the Resumer interface.
func (r *taskRunner) OnFailOrCancel(ctx context.Context, execCtx interface{}, jobErr error) error {
	exec := execCtx.(sql.JobExecContext)
	execCfg := exec.ExecCfg()
	jobRegistry := execCfg.JobRegistry

	details := r.job.Details()
	task := details.(jobspb.AutoConfigTaskDetails).Task
	log.Infof(ctx, "execution failed for task %d: %v", task.TaskID, jobErr)

	startInfoKey := MakeTaskInfoKey([]byte(InfoKeyStartPrefix), task.TaskID)
	completionInfoKey := MakeTaskInfoKey([]byte(InfoKeyCompletionPrefix), task.TaskID)
	return execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := txn.ExecEx(ctx, "delete-task-start-info", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			`DELETE FROM system.job_info WHERE job_id = $1 AND info_key = $2`,
			jobs.AutoConfigRunnerJobID, startInfoKey)
		if err != nil {
			return err
		}
		return jobRegistry.WriteJobInfo(ctx,
			jobs.AutoConfigRunnerJobID,
			completionInfoKey,
			[]byte("task error"),
			txn)
	})
}

// Resume is part of the Resumer interface.
func (r *taskRunner) Resume(ctx context.Context, execCtx interface{}) error {
	exec := execCtx.(sql.JobExecContext)
	execCfg := exec.ExecCfg()
	jobRegistry := execCfg.JobRegistry

	details := r.job.Details()
	task := details.(jobspb.AutoConfigTaskDetails).Task
	log.Infof(ctx, "starting execution for task %d", task.TaskID)

	startInfoKey := MakeTaskInfoKey([]byte(InfoKeyStartPrefix), task.TaskID)
	completionInfoKey := MakeTaskInfoKey([]byte(InfoKeyCompletionPrefix), task.TaskID)
	return execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := txn.ExecEx(ctx, "delete-task-start-info", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			`DELETE FROM system.job_info WHERE job_id = $1 AND info_key = $2`,
			jobs.AutoConfigRunnerJobID, startInfoKey)
		if err != nil {
			return err
		}
		return jobRegistry.WriteJobInfo(ctx,
			jobs.AutoConfigRunnerJobID,
			completionInfoKey,
			[]byte("task success"),
			txn)
	})

}

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &autoConfigRunner{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeAutoConfigRunner, createResumerFn, jobs.DisablesTenantCostControl)

	createResumerFn = func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &taskRunner{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeAutoConfigTask, createResumerFn, jobs.DisablesTenantCostControl)
}
