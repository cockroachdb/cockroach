// Copyright 2020 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

type controlSchedulesNode struct {
	rows    planNode
	command tree.ScheduleCommand
	numRows int
}

func collectTelemetry(command tree.ScheduleCommand) {
	switch command {
	case tree.PauseSchedule:
		telemetry.Inc(sqltelemetry.ScheduledBackupControlCounter("pause"))
	case tree.ResumeSchedule:
		telemetry.Inc(sqltelemetry.ScheduledBackupControlCounter("resume"))
	case tree.DropSchedule:
		telemetry.Inc(sqltelemetry.ScheduledBackupControlCounter("drop"))
	}
}

// FastPathResults implements the planNodeFastPath interface.
func (n *controlSchedulesNode) FastPathResults() (int, bool) {
	return n.numRows, true
}

// JobSchedulerEnv returns JobSchedulerEnv.
func JobSchedulerEnv(execCfg *ExecutorConfig) scheduledjobs.JobSchedulerEnv {
	if knobs, ok := execCfg.DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs); ok {
		if knobs.JobSchedulerEnv != nil {
			return knobs.JobSchedulerEnv
		}
	}
	return scheduledjobs.ProdJobSchedulerEnv
}

// loadSchedule loads schedule information.
func loadSchedule(params runParams, scheduleID tree.Datum) (*jobs.ScheduledJob, error) {
	env := JobSchedulerEnv(params.ExecCfg())
	schedule := jobs.NewScheduledJob(env)

	// Load schedule expression.  This is needed for resume command, but we
	// also use this query to check for the schedule existence.
	datums, cols, err := params.ExecCfg().InternalExecutor.QueryRowExWithCols(
		params.ctx,
		"load-schedule",
		params.EvalContext().Txn, sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		fmt.Sprintf(
			"SELECT schedule_id, next_run, schedule_expr, executor_type, execution_args FROM %s WHERE schedule_id = $1",
			env.ScheduledJobsTableName(),
		),
		scheduleID)
	if err != nil {
		return nil, err
	}

	// Not an error if schedule does not exist.
	if datums == nil {
		return nil, nil
	}

	if err := schedule.InitFromDatums(datums, cols); err != nil {
		return nil, err
	}
	return schedule, nil
}

// updateSchedule executes update for the schedule.
func updateSchedule(params runParams, schedule *jobs.ScheduledJob) error {
	return schedule.Update(
		params.ctx,
		params.ExecCfg().InternalExecutor,
		params.EvalContext().Txn,
	)
}

// DeleteSchedule deletes specified schedule.
func DeleteSchedule(
	ctx context.Context, execCfg *ExecutorConfig, txn *kv.Txn, scheduleID int64,
) error {
	env := JobSchedulerEnv(execCfg)
	_, err := execCfg.InternalExecutor.ExecEx(
		ctx,
		"delete-schedule",
		txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		fmt.Sprintf(
			"DELETE FROM %s WHERE schedule_id = $1",
			env.ScheduledJobsTableName(),
		),
		scheduleID,
	)
	return err
}

// startExec implements planNode interface.
func (n *controlSchedulesNode) startExec(params runParams) error {
	for {
		ok, err := n.rows.Next(params)
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		schedule, err := loadSchedule(params, n.rows.Values()[0])
		if err != nil {
			return err
		}

		if schedule == nil {
			continue // not an error if schedule does not exist
		}

		switch n.command {
		case tree.PauseSchedule:
			schedule.Pause()
			err = updateSchedule(params, schedule)
		case tree.ResumeSchedule:
			// Only schedule the next run time on PAUSED schedules, since ACTIVE schedules may
			// have a custom next run time set by first_run.
			if schedule.IsPaused() {
				err = schedule.ScheduleNextRun()
				if err == nil {
					err = updateSchedule(params, schedule)
				}
			}
		case tree.DropSchedule:
			var ex jobs.ScheduledJobExecutor
			ex, err = jobs.GetScheduledJobExecutor(schedule.ExecutorType())
			if err != nil {
				return errors.Wrap(err, "failed to get scheduled job executor during drop")
			}
			if controller, ok := ex.(jobs.ScheduledJobController); ok {
				scheduleControllerEnv := scheduledjobs.MakeProdScheduleControllerEnv(
					params.ExecCfg().ProtectedTimestampProvider, params.ExecCfg().InternalExecutor)
				if err := controller.OnDrop(params.ctx, scheduleControllerEnv,
					scheduledjobs.ProdJobSchedulerEnv, schedule,
					params.extendedEvalCtx.Txn); err != nil {
					return errors.Wrap(err, "failed to run OnDrop")
				}
			}
			err = DeleteSchedule(params.ctx, params.ExecCfg(), params.p.txn, schedule.ScheduleID())
		default:
			err = errors.AssertionFailedf("unhandled command %s", n.command)
		}
		collectTelemetry(n.command)

		if err != nil {
			return err
		}
		n.numRows++
	}

	return nil
}

// Next implements planNode interface.
func (*controlSchedulesNode) Next(runParams) (bool, error) { return false, nil }

// Values implements planNode interface.
func (*controlSchedulesNode) Values() tree.Datums { return nil }

// Close implements planNode interface.
func (n *controlSchedulesNode) Close(ctx context.Context) {
	n.rows.Close(ctx)
}
