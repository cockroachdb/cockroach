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
	"fmt"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

const alterBackupScheduleOp = "ALTER BACKUP SCHEDULE"

func singleInitializedField(obj interface{}) (string, error) {
	names := make([]string, 0)
	objValue := reflect.ValueOf(obj)
	if objValue.Kind() != reflect.Struct {
		// Programming defensively because reflection.
		// This shouldn't happen; its presence indicates developer error.
		return "", errors.Newf("Expected struct, got %s", objValue.Kind())
	}
	for i := 0; i < objValue.NumField(); i++ {
		field := objValue.Field(i)
		if field.IsZero() {
			continue
		}
		names = append(names, objValue.Type().Field(i).Name)
	}
	if len(names) != 1 {
		// Programming defensively because reflection.
		// This shouldn't happen; its presence indicates developer error.
		return "", errors.Newf("Expected 1 backup option, got %d: %s", len(names), names)
	}
	return names[0], nil
}

func validateAlterSchedule(alterSchedule *tree.AlterBackupSchedule) error {
	if len(alterSchedule.Cmds) == 0 {
		return errors.Newf("Found no attributes to alter")
	}
	observedCmds := make(map[string][]string)

	observe := func(key string, val tree.NodeFormatter) {
		if _, ok := observedCmds[key]; !ok {
			observedCmds[key] = make([]string, 0)
		}
		observedCmds[key] = append(observedCmds[key], tree.AsString(val))
	}

	for _, cmd := range alterSchedule.Cmds {
		cmdType := reflect.ValueOf(cmd).Type().String()
		switch typedCmd := cmd.(type) {
		case *tree.AlterBackupScheduleSetWith:
			if typedCmd.With.Detached != nil {
				return errors.Newf("DETACHED is required and cannot be altered.")
			}
			fieldName, err := singleInitializedField(*typedCmd.With)
			if err != nil {
				return err
			}

			observe(fmt.Sprintf("%s:%s", cmdType, fieldName), typedCmd)
		case *tree.AlterBackupScheduleSetScheduleOption:
			observe(fmt.Sprintf("%s:%s", cmdType, string(typedCmd.Option.Key)), typedCmd)
		default:
			observe(cmdType, typedCmd)
		}
	}

	errStrs := make([]string, 0)
	for key := range observedCmds {
		formattedNodes := observedCmds[key]
		if len(formattedNodes) > 1 {
			errStrs = append(errStrs, fmt.Sprintf("[%s]", strings.Join(formattedNodes, ", ")))
		}
	}
	if len(errStrs) > 0 {
		return errors.Newf("Cannot ALTER the same attribute multiple times: %s", strings.Join(errStrs, ", "))
	}
	return nil
}

func loadSchedules(
	ctx context.Context, p sql.PlanHookState, eval *alterBackupScheduleEval,
) (
	*jobs.ScheduledJob,
	*backuppb.ScheduledBackupExecutionArgs,
	*jobs.ScheduledJob,
	*backuppb.ScheduledBackupExecutionArgs,
	error,
) {
	scheduleID, err := eval.scheduleID()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if scheduleID == 0 {
		return nil, nil, nil, nil, errors.Newf("Schedule ID expected, none found")
	}

	execCfg := p.ExecCfg()
	env := sql.JobSchedulerEnv(execCfg)
	schedule, err := jobs.LoadScheduledJob(ctx, env, scheduleID, execCfg.InternalExecutor, p.Txn())
	if err != nil {
		return nil, nil, nil, nil, err
	}

	args := &backuppb.ScheduledBackupExecutionArgs{}
	if err := pbtypes.UnmarshalAny(schedule.ExecutionArgs().Args, args); err != nil {
		return nil, nil, nil, nil, errors.Wrap(err, "un-marshaling args")
	}

	var dependentSchedule *jobs.ScheduledJob
	var dependentArgs *backuppb.ScheduledBackupExecutionArgs

	if args.DependentScheduleID != 0 {
		dependentSchedule, err = jobs.LoadScheduledJob(ctx, env, args.DependentScheduleID, execCfg.InternalExecutor, p.Txn())
		if err != nil {
			return nil, nil, nil, nil, err
		}
		dependentArgs = &backuppb.ScheduledBackupExecutionArgs{}
		if err := pbtypes.UnmarshalAny(dependentSchedule.ExecutionArgs().Args, dependentArgs); err != nil {
			return nil, nil, nil, nil, errors.Wrap(err, "un-marshaling args")
		}
	}

	var fullJob, incJob *jobs.ScheduledJob
	var fullArgs, incArgs *backuppb.ScheduledBackupExecutionArgs
	if args.BackupType == backuppb.ScheduledBackupExecutionArgs_FULL {
		fullJob, fullArgs = schedule, args
		incJob, incArgs = dependentSchedule, dependentArgs
	} else {
		fullJob, fullArgs = dependentSchedule, dependentArgs
		incJob, incArgs = schedule, args
	}
	return fullJob, fullArgs, incJob, incArgs, nil
}

// doAlterBackupSchedule creates requested schedule (or schedules).
// It is a plan hook implementation responsible for the creating of scheduled backup.
func doAlterBackupSchedules(
	ctx context.Context,
	p sql.PlanHookState,
	eval *alterBackupScheduleEval,
	resultsCh chan<- tree.Datums,
) error {
	if err := validateAlterSchedule(eval.stmt); err != nil {
		return errors.Wrapf(err, "Invalid ALTER BACKUP command")
	}

	fullJob, fullArgs, incJob, incArgs, err := loadSchedules(
		ctx, p, eval)
	if err != nil {
		return err
	}

	// Note that even ADMIN is subject to these restrictions. We expect to
	// add a finer-grained permissions model soon.
	if fullJob.Owner() != p.User() {
		return errors.Newf("Permission denied. Only the OWNER of a schedule may alter it.")
	}

	if incJob != nil && incJob.Owner() != p.User() {
		return errors.Newf("Permission denied. Only the OWNER of a schedule may alter it.")
	}

	fullJob, fullArgs, incJob, incArgs, err = processFullBackupRecurrence(
		ctx,
		p,
		eval.fullBackupAlways,
		eval.fullBackupRecurrence,
		eval.isEnterpriseUser,
		fullJob,
		fullArgs,
		incJob,
		incArgs,
	)
	if err != nil {
		return err
	}

	fullJob, incJob, err = processRecurrence(
		eval.recurrence,
		fullJob,
		incJob,
	)
	if err != nil {
		return err
	}

	// TODO(benbardin): Verify backup statement. Not needed yet since we can't
	// modify that statement yet.

	fullAny, err := pbtypes.MarshalAny(fullArgs)
	if err != nil {
		return err
	}
	fullJob.SetExecutionDetails(
		tree.ScheduledBackupExecutor.InternalName(),
		jobspb.ExecutionArguments{Args: fullAny})
	if err := fullJob.Update(ctx, p.ExecCfg().InternalExecutor, p.Txn()); err != nil {
		return err
	}

	if incJob != nil {
		incAny, err := pbtypes.MarshalAny(incArgs)
		if err != nil {
			return err
		}
		incJob.SetExecutionDetails(
			tree.ScheduledBackupExecutor.InternalName(),
			jobspb.ExecutionArguments{Args: incAny})
		if err := incJob.Update(ctx, p.ExecCfg().InternalExecutor, p.Txn()); err != nil {
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
	fullJob *jobs.ScheduledJob,
	fullArgs *backuppb.ScheduledBackupExecutionArgs,
	incJob *jobs.ScheduledJob,
	incArgs *backuppb.ScheduledBackupExecutionArgs,
) (
	*jobs.ScheduledJob,
	*backuppb.ScheduledBackupExecutionArgs,
	*jobs.ScheduledJob,
	*backuppb.ScheduledBackupExecutionArgs,
	error,
) {
	var err error

	if !fullBackupAlways && fullBackupRecurrence == nil {
		return fullJob, fullArgs, incJob, incArgs, nil
	}

	env := sql.JobSchedulerEnv(p.ExecCfg())
	ex := p.ExecCfg().InternalExecutor
	if fullBackupAlways {
		if incJob == nil {
			// Nothing to do.
			return fullJob, fullArgs, incJob, incArgs, nil
		}
		// Copy the cadence from the incremental to the full, and delete the
		// incremental.
		if err := fullJob.SetSchedule(incJob.ScheduleExpr()); err != nil {
			return nil, nil, nil, nil, err
		}
		fullArgs.DependentScheduleID = 0
		fullArgs.UnpauseOnSuccess = 0
		if err := incJob.Delete(ctx, ex, p.Txn()); err != nil {
			return nil, nil, nil, nil, err
		}
		incJob = nil
		incArgs = nil
		return fullJob, fullArgs, incJob, incArgs, nil
	}

	// We have FULL BACKUP <cron>.
	if !isEnterpriseUser {
		return nil, nil, nil, nil, errors.Newf("Enterprise license required to use incremental backups. " +
			"To modify the cadence of a full backup, use the 'RECURRING <cron>' clause instead.")
	}

	if incJob == nil {
		// No existing incremental job, so we need to create it, copying details
		// from the full.
		node, err := parser.ParseOne(fullArgs.BackupStatement)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		stmt, ok := node.AST.(*tree.Backup)
		if !ok {
			return nil, nil, nil, nil, errors.Newf("unexpected node type %T", node)
		}
		stmt.AppendToLatest = true

		scheduleExprFn := func() (string, error) {
			return fullJob.ScheduleExpr(), nil
		}
		incRecurrence, err := computeScheduleRecurrence(env.Now(), scheduleExprFn)
		if err != nil {
			return nil, nil, nil, nil, err
		}

		incJob, incArgs, err = makeBackupSchedule(
			env,
			p.User(),
			fullJob.ScheduleLabel(),
			incRecurrence,
			*fullJob.ScheduleDetails(),
			jobs.InvalidScheduleID,
			fullArgs.UpdatesLastBackupMetric,
			stmt,
			fullArgs.ChainProtectedTimestampRecords,
		)

		if err != nil {
			return nil, nil, nil, nil, err
		}

		// We don't know if a full backup has completed yet, so pause incremental
		// until a full backup completes.
		incJob.Pause()
		incJob.SetScheduleStatus("Waiting for initial backup to complete")
		incArgs.DependentScheduleID = fullJob.ScheduleID()

		incAny, err := pbtypes.MarshalAny(incArgs)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		incJob.SetExecutionDetails(
			tree.ScheduledBackupExecutor.InternalName(),
			jobspb.ExecutionArguments{Args: incAny})

		if err := incJob.Create(ctx, ex, p.Txn()); err != nil {
			return nil, nil, nil, nil, err
		}
		fullArgs.UnpauseOnSuccess = incJob.ScheduleID()
		fullArgs.DependentScheduleID = incJob.ScheduleID()
	}
	// We have an incremental backup at this point.
	// Make no (further) changes, and just edit the cadence on the full.

	fullBackupRecurrenceStr, err := fullBackupRecurrence()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if err := fullJob.SetSchedule(fullBackupRecurrenceStr); err != nil {
		return nil, nil, nil, nil, err
	}

	fullAny, err := pbtypes.MarshalAny(fullArgs)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	fullJob.SetExecutionDetails(
		tree.ScheduledBackupExecutor.InternalName(),
		jobspb.ExecutionArguments{Args: fullAny})

	return fullJob, fullArgs, incJob, incArgs, nil
}

type alterBackupScheduleEval struct {
	// Schedule specific properties that get evaluated.
	stmt                 *tree.AlterBackupSchedule
	scheduleID           func() (int64, error)
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
	var err error

	eval := &alterBackupScheduleEval{
		stmt: alterStmt,
	}
	eval.scheduleID, err = p.TypeAsInt64(ctx, alterStmt.ScheduleID, alterBackupScheduleOp)
	if err != nil {
		return nil, err
	}
	for _, cmd := range alterStmt.Cmds {
		switch typedCmd := cmd.(type) {
		case *tree.AlterBackupScheduleSetFullBackup:
			if typedCmd.FullBackup.AlwaysFull {
				eval.fullBackupAlways = true
			} else {
				eval.fullBackupRecurrence, err = p.TypeAsString(ctx, typedCmd.FullBackup.Recurrence, alterBackupScheduleOp)
			}
		case *tree.AlterBackupScheduleSetRecurring:
			eval.recurrence, err = p.TypeAsString(ctx, typedCmd.Recurrence, alterBackupScheduleOp)
		default:
			return nil, errors.Newf("Not yet implemented: %v", tree.AsString(typedCmd))
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

	if err := p.RequireAdminRole(ctx, alterBackupScheduleOp); err != nil {
		return nil, nil, nil, false, err
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
