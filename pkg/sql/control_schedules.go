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
	nonReusablePlanNode
	rowsAffectedOutputHelper
	command tree.ScheduleCommand
}

func collectTelemetry(command tree.ScheduleCommand) {
	switch command {
	case tree.PauseSchedule:
		telemetry.Inc(sqltelemetry.ScheduledBackupControlCounter("pause"))
	case tree.ResumeSchedule:
		telemetry.Inc(sqltelemetry.ScheduledBackupControlCounter("resume"))
	case tree.DropSchedule:
		telemetry.Inc(sqltelemetry.ScheduledBackupControlCounter("drop"))
	case tree.ExecuteSchedule:
		telemetry.Inc(sqltelemetry.ScheduledBackupControlCounter("execute"))
	}
}

// loadSchedule loads schedule information as the node user.
func loadSchedule(params runParams, scheduleID tree.Datum) (*jobs.ScheduledJob, error) {
	env := jobs.JobSchedulerEnv(params.ExecCfg().JobsKnobs())
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
	env := jobs.JobSchedulerEnv(execCfg.JobsKnobs())
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
		case tree.ExecuteSchedule:
			if schedule.ExecutorType() == tree.ScheduledBackupExecutor.InternalName() {
				err = errors.WithHintf(
					pgerror.Newf(
						pgcode.FeatureNotSupported,
						"EXECUTE SCHEDULE is not supported for this schedule type",
					),
					"use ALTER BACKUP SCHEDULE ... EXECUTE IMMEDIATELY",
				)
				break
			}
			// Execute schedule will run the schedule immediately. It does this by
			// setting the next run to now. The job scheduler daemon will pick it up
			// and execute it.
			if schedule.IsPaused() {
				err = errors.Newf("cannot execute a paused schedule; use RESUME SCHEDULE instead")
			} else {
				env := jobs.JobSchedulerEnv(params.ExecCfg().JobsKnobs())
				schedule.SetNextRun(env.Now())
				err = jobs.ScheduledJobTxn(params.p.InternalSQLTxn()).
					Update(params.ctx, schedule)
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
				n.addAffectedRows(additionalDroppedSchedules)
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
		n.incAffectedRows()
	}

	return nil
}

// Next implements the planNode interface.
func (n *controlSchedulesNode) Next(_ runParams) (bool, error) {
	return n.next(), nil
}

// Values implements the planNode interface.
func (n *controlSchedulesNode) Values() tree.Datums {
	return n.values()
}

// Close implements planNode interface.
func (n *controlSchedulesNode) Close(ctx context.Context) {
	n.input.Close(ctx)
}
