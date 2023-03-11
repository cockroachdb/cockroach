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

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/autoconfigpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// createTaskJob creates a job to execute the given task.
//
// Refer to the package-level documentation for more details.
func createTaskJob(
	ctx context.Context,
	txn isql.Txn,
	execCfg *sql.ExecutorConfig,
	jobID jobspb.JobID,
	task autoconfigpb.Task,
) error {
	jobRecord := jobs.Record{
		Description:   task.Description,
		Username:      username.NodeUserName(),
		Statements:    []string{"(...auto config...)"},
		Details:       jobspb.AutoConfigTaskDetails{Task: task},
		Progress:      jobspb.AutoConfigTaskProgress{},
		RunningStatus: "executing",
		CreatedBy: &jobs.CreatedByInfo{
			Name: AutoConfigTaskCreatedName,
			ID:   int64(task.TaskID),
		},
	}

	_, err := execCfg.JobRegistry.CreateJobWithTxn(ctx, jobRecord, jobID, txn)
	return err
}

// AutoConfigTaskCreatedName is the value in created_by_name for jobs
// created by the auto config runner.
const AutoConfigTaskCreatedName = "auto-config-task"

type taskRunner struct {
	job *jobs.Job
}

var _ jobs.Resumer = (*taskRunner)(nil)

// OnFailOrCancel is a part of the Resumer interface.
func (r *taskRunner) OnFailOrCancel(ctx context.Context, execCtx interface{}, jobErr error) error {
	exec := execCtx.(sql.JobExecContext)
	execCfg := exec.ExecCfg()

	details := r.job.Details()
	task := details.(jobspb.AutoConfigTaskDetails).Task
	log.Infof(ctx, "execution failed for task %d: %v", task.TaskID, jobErr)

	return execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return markTaskComplete(ctx, txn, execCfg, task.TaskID, []byte("task error"))
	})
}

// Resume is part of the Resumer interface.
func (r *taskRunner) Resume(ctx context.Context, execCtx interface{}) error {
	exec := execCtx.(sql.JobExecContext)
	execCfg := exec.ExecCfg()

	details := r.job.Details()
	task := details.(jobspb.AutoConfigTaskDetails).Task
	log.Infof(ctx, "starting execution for task %d", task.TaskID)

	switch payload := task.GetPayload().(type) {
	case *autoconfigpb.Task_SimpleSQL:
		return execSimpleSQL(ctx, execCfg, task.TaskID, payload.SimpleSQL)

	default:
		return errors.AssertionFailedf("unknown task payload type: %T", payload)
	}
}

// execSimpleSQL executs a SQL payload a a single combined transaction
// that also includes the removal of the start marker and the creation
// of the completion marker.
func execSimpleSQL(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	taskID uint64,
	sqlPayload *autoconfigpb.SimpleSQL,
) error {
	return execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		sqlUsername := username.RootUserName()
		if sqlPayload.UsernameProto != "" {
			sqlUsername = sqlPayload.UsernameProto.Decode()
		}
		execOverride := sessiondata.InternalExecutorOverride{
			User: sqlUsername,
		}
		for _, stmt := range sqlPayload.Statements {
			log.Infof(ctx, "attempting execution of task statement:\n%s", stmt)
			_, err := txn.ExecEx(ctx, "exec-task-statement",
				txn.KV(),
				execOverride,
				stmt)
			if err != nil {
				return err
			}
		}
		log.Infof(ctx, "finished executing SQL statements for task %d", taskID)
		return markTaskComplete(ctx, txn, execCfg, taskID, []byte("task success"))
	})
}

func init() {
	// Note: we disable tenant cost control because auto-config is used
	// by operators and should thus not incur costs (or performance
	// penalties) to tenants.
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &taskRunner{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeAutoConfigTask, createResumerFn, jobs.DisablesTenantCostControl)
}
