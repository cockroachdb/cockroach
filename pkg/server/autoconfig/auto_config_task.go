// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package autoconfig

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/autoconfigpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// createTaskJob creates a job to execute the given task.
//
// Refer to the package-level documentation for more details.
func createTaskJob(
	ctx context.Context,
	txn isql.Txn,
	registry *jobs.Registry,
	jobID jobspb.JobID,
	envID EnvironmentID,
	task autoconfigpb.Task,
) error {
	jobRecord := jobs.Record{
		Description: fmt.Sprintf("configuration task: %s", task.Description),
		Username:    username.NodeUserName(),
		Details:     jobspb.AutoConfigTaskDetails{EnvID: envID, Task: task},
		Progress:    jobspb.AutoConfigTaskProgress{},
		CreatedBy: &jobs.CreatedByInfo{
			Name: fmt.Sprintf("%s:%s", AutoConfigTaskCreatedName, envID),
			ID:   int64(task.TaskID),
		},
	}

	_, err := registry.CreateJobWithTxn(ctx, jobRecord, jobID, txn)
	return err
}

// AutoConfigTaskCreatedName is the value in created_by_name for jobs
// created by the auto config runner.
const AutoConfigTaskCreatedName = "auto-config-task"

// taskRunner is the runner for one task.
type taskRunner struct {
	envID EnvironmentID
	task  autoconfigpb.Task
}

var _ jobs.Resumer = (*taskRunner)(nil)

// OnFailOrCancel is a part of the Resumer interface.
func (r *taskRunner) OnFailOrCancel(ctx context.Context, execCtx interface{}, jobErr error) error {
	ctx = logtags.AddTag(ctx, "taskenv", r.envID)
	ctx = logtags.AddTag(ctx, "task", r.task.TaskID)
	log.Infof(ctx, "task execution failed: %v", jobErr)

	exec := execCtx.(sql.JobExecContext)
	execCfg := exec.ExecCfg()
	provider := getProvider(ctx, execCfg)
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return markTaskComplete(ctx, txn,
			InfoKeyTaskRef{Environment: r.envID, Task: r.task.TaskID},
			[]byte("task error"))
	}); err != nil {
		return err
	}

	// Tell the provider we're done so the task is not served again to
	// the runner.
	if err := provider.Pop(ctx, r.envID, r.task.TaskID); err != nil {
		// Failing to Pop is not a hard job error: the runner also knows
		// how to pop upon finding unexpected completion markers.
		log.Warningf(ctx, "error popping the task off the queue: %v", err)
	}
	return nil
}

// CollectProfile implements the jobs.Resumer interface.
func (r *taskRunner) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

// Resume is part of the Resumer interface.
func (r *taskRunner) Resume(ctx context.Context, execCtx interface{}) error {
	ctx = logtags.AddTag(ctx, "taskenv", r.envID)
	ctx = logtags.AddTag(ctx, "task", r.task.TaskID)
	log.Infof(ctx, "starting execution")

	exec := execCtx.(sql.JobExecContext)
	execCfg := exec.ExecCfg()
	provider := getProvider(ctx, execCfg)

	switch payload := r.task.GetPayload().(type) {
	case *autoconfigpb.Task_SimpleSQL:
		if err := execSimpleSQL(ctx, execCfg, r.envID, r.task.TaskID, payload.SimpleSQL); err != nil {
			return err
		}

	default:
		return errors.AssertionFailedf("unknown task payload type: %T", payload)
	}

	// Tell the provider we're done so the task is not served again to
	// the runner.
	if err := provider.Pop(ctx, r.envID, r.task.TaskID); err != nil {
		// Failing to Pop is not a hard job error: the runner also knows
		// how to pop upon finding unexpected completion markers.
		log.Warningf(ctx, "error popping the task off the queue: %v", err)
	}
	return nil
}

// execSimpleSQL executs a SQL payload a a single combined transaction
// that also includes the removal of the start marker and the creation
// of the completion marker.
func execSimpleSQL(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	envID EnvironmentID,
	taskID TaskID,
	sqlPayload *autoconfigpb.SimpleSQL,
) error {
	// Which SQL identity to use.
	sqlUsername := username.RootUserName()
	if sqlPayload.UsernameProto != "" {
		sqlUsername = sqlPayload.UsernameProto.Decode()
	}
	execOverride := sessiondata.InternalExecutorOverride{
		User: sqlUsername,
	}
	// First execute all the non-transactional, idempotent SQL statements.
	if len(sqlPayload.NonTransactionalStatements) > 0 {
		exec := execCfg.InternalDB.Executor()
		for _, stmt := range sqlPayload.NonTransactionalStatements {
			log.Infof(ctx, "attempting execution of non-txn task statement:\n%s", stmt)
			_, err := exec.ExecEx(ctx, "exec-task-statement", nil, /* txn */
				execOverride, stmt)
			if err != nil {
				return err
			}
		}
		log.Infof(ctx, "finished executing non-txn task statements")
	}

	// Now execute all the transactional, potentially not idempotent
	// statements.
	//
	// We use DescsTxn here because the tasks may contain DDL statements
	// and we want to ensure that each txn will wait for the leases from
	// previous txns.
	return execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		for _, stmt := range sqlPayload.TransactionalStatements {
			log.Infof(ctx, "attempting execution of task statement:\n%s", stmt)
			_, err := txn.ExecEx(ctx, "exec-task-statement",
				txn.KV(),
				execOverride,
				stmt)
			if err != nil {
				return err
			}
		}
		log.Infof(ctx, "finished executing txn statements")
		return markTaskComplete(ctx, txn,
			InfoKeyTaskRef{Environment: envID, Task: taskID},
			[]byte("task success"))
	})
}

func init() {
	// Note: we disable tenant cost control because auto-config is used
	// by operators and should thus not incur costs (or performance
	// penalties) to tenants.
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		jd := job.Details()
		details := jd.(jobspb.AutoConfigTaskDetails)
		return &taskRunner{envID: details.EnvID, task: details.Task}
	}
	jobs.RegisterConstructor(jobspb.TypeAutoConfigTask, createResumerFn, jobs.DisablesTenantCostControl)
}
