// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

const alterBackupScheduleOp = "ALTER BACKUP SCHEDULE"

type scheduleDetails struct {
	fullJob  *jobs.ScheduledJob
	fullArgs *backuppb.ScheduledBackupExecutionArgs
	incJob   *jobs.ScheduledJob
	incArgs  *backuppb.ScheduledBackupExecutionArgs
}

func loadSchedules(
	ctx context.Context, p sql.PlanHookState, eval *alterBackupScheduleEval,
) (scheduleDetails, error) {
	scheduleID := eval.scheduleID
	s := scheduleDetails{}
	if scheduleID == 0 {
		return s, errors.Newf("Schedule ID expected, none found")
	}

	execCfg := p.ExecCfg()
	env := sql.JobSchedulerEnv(execCfg)
	schedule, err := jobs.LoadScheduledJob(ctx, env, int64(scheduleID), execCfg.InternalExecutor, p.Txn())
	if err != nil {
		return s, err
	}

	args := &backuppb.ScheduledBackupExecutionArgs{}
	if err := pbtypes.UnmarshalAny(schedule.ExecutionArgs().Args, args); err != nil {
		return s, errors.Wrap(err, "un-marshaling args")
	}

	var dependentSchedule *jobs.ScheduledJob
	var dependentArgs *backuppb.ScheduledBackupExecutionArgs

	if args.DependentScheduleID != 0 {
		dependentSchedule, err = jobs.LoadScheduledJob(ctx, env, args.DependentScheduleID, execCfg.InternalExecutor, p.Txn())
		if err != nil {
			return s, err
		}
		dependentArgs = &backuppb.ScheduledBackupExecutionArgs{}
		if err := pbtypes.UnmarshalAny(dependentSchedule.ExecutionArgs().Args, dependentArgs); err != nil {
			return s, errors.Wrap(err, "un-marshaling args")
		}
	}

	if args.BackupType == backuppb.ScheduledBackupExecutionArgs_FULL {
		s.fullJob, s.fullArgs = schedule, args
		s.incJob, s.incArgs = dependentSchedule, dependentArgs
	} else {
		s.fullJob, s.fullArgs = dependentSchedule, dependentArgs
		s.incJob, s.incArgs = schedule, args
	}
	return s, nil
}

// doAlterBackupSchedule creates requested schedule (or schedules).
// It is a plan hook implementation responsible for the creating of scheduled backup.
func doAlterBackupSchedules(
	ctx context.Context,
	p sql.PlanHookState,
	eval *alterBackupScheduleEval,
	resultsCh chan<- tree.Datums,
) error {
	s, err := loadSchedules(
		ctx, p, eval)
	if err != nil {
		return err
	}

	// Note that even ADMIN is subject to these restrictions. We expect to
	// add a finer-grained permissions model soon.
	if s.fullJob.Owner() != p.User() {
		return pgerror.Newf(pgcode.InsufficientPrivilege, "only the OWNER of a schedule may alter it")
	}

	if s.incJob != nil && s.incJob.Owner() != p.User() {
		return pgerror.Newf(pgcode.InsufficientPrivilege, "only the OWNER of a schedule may alter it")
	}

	s, err = processFullBackupRecurrence(
		ctx,
		p,
		eval.fullBackupAlways,
		eval.fullBackupRecurrence,
		eval.isEnterpriseUser,
		s,
	)
	if err != nil {
		return err
	}

	s.fullJob, s.incJob, err = processRecurrence(
		eval.recurrence,
		s.fullJob,
		s.incJob,
	)
	if err != nil {
		return err
	}

	// TODO(benbardin): Verify backup statement. Not needed yet since we can't
	// modify that statement yet.

	fullAny, err := pbtypes.MarshalAny(s.fullArgs)
	if err != nil {
		return err
	}
	s.fullJob.SetExecutionDetails(
		tree.ScheduledBackupExecutor.InternalName(),
		jobspb.ExecutionArguments{Args: fullAny})
	if err := s.fullJob.Update(ctx, p.ExecCfg().InternalExecutor, p.Txn()); err != nil {
		return err
	}

	if s.incJob != nil {
		incAny, err := pbtypes.MarshalAny(s.incArgs)
		if err != nil {
			return err
		}
		s.incJob.SetExecutionDetails(
			tree.ScheduledBackupExecutor.InternalName(),
			jobspb.ExecutionArguments{Args: incAny})
		if err := s.incJob.Update(ctx, p.ExecCfg().InternalExecutor, p.Txn()); err != nil {
			return err
		}
	}
	// TODO(benbardin): Emit schedules.
	return nil
}

func processRecurrence(
	recurrence func() (string, error), fullJob *jobs.ScheduledJob, incJob *jobs.ScheduledJob,
) (*jobs.ScheduledJob, *jobs.ScheduledJob, error) {
	if recurrence == nil {
		return fullJob, incJob, nil
	}
	recurrenceStr, err := recurrence()
	if err != nil {
		return nil, nil, err
	}
	if incJob != nil {
		if err := incJob.SetSchedule(recurrenceStr); err != nil {
			return nil, nil, err
		}
	} else {
		if err := fullJob.SetSchedule(recurrenceStr); err != nil {
			return nil, nil, err
		}
	}
	return fullJob, incJob, nil
}

func processFullBackupRecurrence(
	ctx context.Context,
	p sql.PlanHookState,
	fullBackupAlways bool,
	fullBackupRecurrence func() (string, error),
	isEnterpriseUser bool,
	s scheduleDetails,
) (scheduleDetails, error) {
	var err error

	if !fullBackupAlways && fullBackupRecurrence == nil {
		return s, nil
	}

	env := sql.JobSchedulerEnv(p.ExecCfg())
	ex := p.ExecCfg().InternalExecutor
	if fullBackupAlways {
		if s.incJob == nil {
			// Nothing to do.
			return s, nil
		}
		// Copy the cadence from the incremental to the full, and delete the
		// incremental.
		if err := s.fullJob.SetSchedule(s.incJob.ScheduleExpr()); err != nil {
			return scheduleDetails{}, err
		}
		s.fullArgs.DependentScheduleID = 0
		s.fullArgs.UnpauseOnSuccess = 0
		if err := s.incJob.Delete(ctx, ex, p.Txn()); err != nil {
			return scheduleDetails{}, err
		}
		s.incJob = nil
		s.incArgs = nil
		return s, nil
	}

	// We have FULL BACKUP <cron>.
	if !isEnterpriseUser {
		return scheduleDetails{}, errors.Newf("Enterprise license required to use incremental backups. " +
			"To modify the cadence of a full backup, use the 'RECURRING <cron>' clause instead.")
	}

	if s.incJob == nil {
		// No existing incremental job, so we need to create it, copying details
		// from the full.
		node, err := parser.ParseOne(s.fullArgs.BackupStatement)
		if err != nil {
			return scheduleDetails{}, err
		}
		stmt, ok := node.AST.(*tree.Backup)
		if !ok {
			return scheduleDetails{}, errors.Newf("unexpected node type %T", node)
		}
		stmt.AppendToLatest = true

		scheduleExprFn := func() (string, error) {
			return s.fullJob.ScheduleExpr(), nil
		}
		incRecurrence, err := computeScheduleRecurrence(env.Now(), scheduleExprFn)
		if err != nil {
			return scheduleDetails{}, err
		}

		s.incJob, s.incArgs, err = makeBackupSchedule(
			env,
			p.User(),
			s.fullJob.ScheduleLabel(),
			incRecurrence,
			*s.fullJob.ScheduleDetails(),
			jobs.InvalidScheduleID,
			s.fullArgs.UpdatesLastBackupMetric,
			stmt,
			s.fullArgs.ChainProtectedTimestampRecords,
		)

		if err != nil {
			return scheduleDetails{}, err
		}

		// We don't know if a full backup has completed yet, so pause incremental
		// until a full backup completes.
		s.incJob.Pause()
		s.incJob.SetScheduleStatus("Waiting for initial backup to complete")
		s.incArgs.DependentScheduleID = s.fullJob.ScheduleID()

		incAny, err := pbtypes.MarshalAny(s.incArgs)
		if err != nil {
			return scheduleDetails{}, err
		}
		s.incJob.SetExecutionDetails(
			tree.ScheduledBackupExecutor.InternalName(),
			jobspb.ExecutionArguments{Args: incAny})

		if err := s.incJob.Create(ctx, ex, p.Txn()); err != nil {
			return scheduleDetails{}, err
		}
		s.fullArgs.UnpauseOnSuccess = s.incJob.ScheduleID()
		s.fullArgs.DependentScheduleID = s.incJob.ScheduleID()
	}
	// We have an incremental backup at this point.
	// Make no (further) changes, and just edit the cadence on the full.

	fullBackupRecurrenceStr, err := fullBackupRecurrence()
	if err != nil {
		return scheduleDetails{}, err
	}
	if err := s.fullJob.SetSchedule(fullBackupRecurrenceStr); err != nil {
		return scheduleDetails{}, err
	}

	fullAny, err := pbtypes.MarshalAny(s.fullArgs)
	if err != nil {
		return scheduleDetails{}, err
	}
	s.fullJob.SetExecutionDetails(
		tree.ScheduledBackupExecutor.InternalName(),
		jobspb.ExecutionArguments{Args: fullAny})

	return s, nil
}

type alterBackupScheduleEval struct {
	// Schedule specific properties that get evaluated.
	stmt                 *tree.AlterBackupSchedule
	scheduleID           uint64
	recurrence           func() (string, error)
	fullBackupRecurrence func() (string, error)
	fullBackupAlways     bool
	isEnterpriseUser     bool
}

// makeScheduleBackupEval prepares helper scheduledBackupEval struct to assist in evaluation
// of various schedule and backup specific components.
func makeAlterBackupScheduleEval(
	ctx context.Context, p sql.PlanHookState, alterStmt *tree.AlterBackupSchedule,
) (*alterBackupScheduleEval, error) {
	eval := &alterBackupScheduleEval{
		stmt: alterStmt,
	}
	eval.scheduleID = alterStmt.ScheduleID
	var err error
	observed := make(map[string]interface{})
	empty := struct{}{}
	observe := func(key string) error {
		if _, alreadyObserved := observed[key]; alreadyObserved {
			return errors.Newf("can specify %s at most once", key)
		}
		observed[key] = empty
		return nil
	}
	for _, cmd := range alterStmt.Cmds {
		switch typedCmd := cmd.(type) {
		case *tree.AlterBackupScheduleSetFullBackup:
			if err := observe("SET FULL BACKUP"); err != nil {
				return nil, err
			}
			if typedCmd.FullBackup.AlwaysFull {
				eval.fullBackupAlways = true
			} else {
				eval.fullBackupRecurrence, err = p.TypeAsString(ctx, typedCmd.FullBackup.Recurrence, alterBackupScheduleOp)
			}
		case *tree.AlterBackupScheduleSetRecurring:
			if err := observe("SET RECURRING"); err != nil {
				return nil, err
			}
			eval.recurrence, err = p.TypeAsString(ctx, typedCmd.Recurrence, alterBackupScheduleOp)
		default:
			return nil, errors.Newf("not yet implemented: %v", tree.AsString(typedCmd))
		}
		if err != nil {
			return nil, err
		}
	}

	enterpriseCheckErr := utilccl.CheckEnterpriseEnabled(
		p.ExecCfg().Settings, p.ExecCfg().NodeInfo.LogicalClusterID(), p.ExecCfg().Organization(),
		"BACKUP INTO LATEST")
	eval.isEnterpriseUser = enterpriseCheckErr == nil

	return eval, nil
}

func alterBackupScheduleHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	alterScheduleStmt, ok := stmt.(*tree.AlterBackupSchedule)
	if !ok {
		return nil, nil, nil, false, nil
	}

	eval, err := makeAlterBackupScheduleEval(ctx, p, alterScheduleStmt)
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		err := doAlterBackupSchedules(ctx, p, eval, resultsCh)
		if err != nil {
			telemetry.Count("scheduled-backup.alter.failed")
			return err
		}

		return nil
	}
	return fn, scheduledBackupHeader, nil, false, nil
}

func init() {
	sql.AddPlanHook("schedule backup", alterBackupScheduleHook)
}
