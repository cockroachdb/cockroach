// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/errors"
)

type controlSchedulesNode struct {
	singleInputPlanNode
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
func JobSchedulerEnv(knobs *jobs.TestingKnobs) scheduledjobs.JobSchedulerEnv {
	if knobs != nil && knobs.JobSchedulerEnv != nil {
		return knobs.JobSchedulerEnv
	}
	return scheduledjobs.ProdJobSchedulerEnv
}

// loadSchedule loads schedule information as the node user.
func loadSchedule(params runParams, scheduleID tree.Datum) (*jobs.ScheduledJob, error) {
	env := JobSchedulerEnv(params.ExecCfg().JobsKnobs())
	schedule := jobs.NewScheduledJob(env)

	// Load schedule expression.  This is needed for resume command, but we
	// also use this query to check for the schedule existence.
	//
	// Run the query as the node user since we perform our own privilege checks
	// before using the returned schedule.
	datums, cols, err := params.p.InternalSQLTxn().QueryRowExWithCols(
		params.ctx,
		"load-schedule",
		params.p.Txn(), sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(
			"SELECT schedule_id, next_run, schedule_expr, executor_type, execution_args, owner FROM %s WHERE schedule_id = $1",
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

// DeleteSchedule deletes specified schedule.
func DeleteSchedule(
	ctx context.Context, execCfg *ExecutorConfig, txn isql.Txn, scheduleID jobspb.ScheduleID,
) error {
	env := JobSchedulerEnv(execCfg.JobsKnobs())
	_, err := txn.ExecEx(
		ctx,
		"delete-schedule",
		txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
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
		ok, err := n.input.Next(params)
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		schedule, err := loadSchedule(params, n.input.Values()[0])
		if err != nil {
			return err
		}

		if schedule == nil {
			continue // not an error if schedule does not exist
		}

		// Check that the user has privileges or is the owner of the schedules being altered.
		hasPriv, err := params.p.HasPrivilege(
			params.ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER, params.p.User(),
		)
		if err != nil {
			return err
		}
		isOwner := schedule.Owner() == params.p.User()
		if !hasPriv && !isOwner {
			return pgerror.Newf(pgcode.InsufficientPrivilege, "must have %s privilege or be owner of the "+
				"schedule %d to %s it", privilege.REPAIRCLUSTER, schedule.ScheduleID(), n.command.String())
		}

		switch n.command {
		case tree.PauseSchedule:
			schedule.Pause()
			err = jobs.ScheduledJobTxn(params.p.InternalSQLTxn()).
				Update(params.ctx, schedule)
		case tree.ResumeSchedule:
			// Only schedule the next run time on PAUSED schedules, since ACTIVE schedules may
			// have a custom next run time set by first_run.
			if schedule.IsPaused() {
				if err = schedule.ScheduleNextRun(); err == nil {
					err = jobs.ScheduledJobTxn(params.p.InternalSQLTxn()).
						Update(params.ctx, schedule)
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
					params.ExecCfg().ProtectedTimestampProvider.WithTxn(params.p.InternalSQLTxn()),
				)
				additionalDroppedSchedules, err := controller.OnDrop(
					params.ctx,
					scheduleControllerEnv,
					scheduledjobs.ProdJobSchedulerEnv,
					schedule,
					params.p.InternalSQLTxn(),
					params.p.Descriptors(),
				)
				if err != nil {
					return errors.Wrap(err, "failed to run OnDrop")
				}
				n.numRows += additionalDroppedSchedules
			}
			err = DeleteSchedule(
				params.ctx, params.ExecCfg(), params.p.InternalSQLTxn(),
				schedule.ScheduleID(),
			)
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
	n.input.Close(ctx)
}
