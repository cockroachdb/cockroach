// Copyright 2020 The Cockroach Authors.
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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

const (
	optFirstRun          = "first_run"
	optOnExecFailure     = "on_execution_failure"
	optOnPreviousRunning = "on_previous_running"
)

var scheduledBackupOptionExpectValues = map[string]sql.KVStringOptValidate{
	optFirstRun:          sql.KVStringOptRequireValue,
	optOnExecFailure:     sql.KVStringOptRequireValue,
	optOnPreviousRunning: sql.KVStringOptRequireValue,
}

// scheduledBackupEval is a representation of tree.ScheduledBackup, prepared
// for evaluation
type scheduledBackupEval struct {
	*tree.ScheduledBackup

	// Schedule specific properties that get evaluated.
	scheduleName func() (string, error)
	recurrence   func() (string, error)
	scheduleOpts func() (map[string]string, error)

	// TODO(yevgeniy): Support full backup recurrence. For now, assume recurrence is
	// the full backup.
	// fullBackupRecurrence func() (string, error)

	// Backup specific properties that get evaluated.
	// We need to evaluate anything in the tree.Backup node that allows
	// placeholders to be specified so that we store evaluated
	// backup statement in the schedule.
	destination          func() ([]string, error)
	encryptionPassphrase func() (string, error)
}

func parseOnError(onError string, details *jobspb.ScheduleDetails) error {
	switch strings.ToLower(onError) {
	case "retry":
		details.OnError = jobspb.ScheduleDetails_RETRY_SOON
	case "reschedule":
		details.OnError = jobspb.ScheduleDetails_RETRY_SCHED
	case "pause":
		details.OnError = jobspb.ScheduleDetails_PAUSE_SCHED
	default:
		return errors.Newf(
			"%q is not a valid on_execution_error; valid values are [retry|reschedule|pause]",
			onError)
	}
	return nil
}

func parseWaitBehavior(wait string, details *jobspb.ScheduleDetails) error {
	switch strings.ToLower(wait) {
	case "start":
		details.Wait = jobspb.ScheduleDetails_NO_WAIT
	case "skip":
		details.Wait = jobspb.ScheduleDetails_SKIP
	case "wait":
		details.Wait = jobspb.ScheduleDetails_WAIT
	default:
		return errors.Newf(
			"%q is not a valid on_previous_running; valid values are [start|skip|wait]",
			wait)
	}
	return nil
}

func setScheduleOptions(
	eval *scheduledBackupEval, evalCtx *tree.EvalContext, sj *jobs.ScheduledJob,
) error {
	opts, err := eval.scheduleOpts()
	if err != nil {
		return err
	}

	if v, ok := opts[optFirstRun]; ok {
		firstRun, _, err := tree.ParseDTimestampTZ(evalCtx, v, time.Microsecond)
		if err != nil {
			return err
		}
		sj.SetNextRun(firstRun.Time)
	}

	var details jobspb.ScheduleDetails
	if v, ok := opts[optOnExecFailure]; ok {
		if err := parseOnError(v, &details); err != nil {
			return err
		}
	}
	if v, ok := opts[optOnPreviousRunning]; ok {
		if err := parseWaitBehavior(v, &details); err != nil {
			return err
		}
	}

	var defaultDetails jobspb.ScheduleDetails
	if details != defaultDetails {
		sj.SetScheduleDetails(details)
	}

	return nil
}

// doCreateBackupSchedule creates requested schedule (or schedules).
// It is a plan hook implementation responsible for the creating of scheduled backup.
func doCreateBackupSchedule(
	ctx context.Context, p sql.PlanHookState, eval *scheduledBackupEval, resultsCh chan<- tree.Datums,
) error {
	env := scheduledjobs.ProdJobSchedulerEnv
	if knobs, ok := p.ExecCfg().DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs); ok {
		if knobs.JobSchedulerEnv != nil {
			env = knobs.JobSchedulerEnv
		}
	}
	sj := jobs.NewScheduledJob(env)

	// Prepare arguments for scheduled backup execution.
	args := &ScheduledBackupExecutionArgs{}
	// TODO(yevgeniy): Support incremental backup
	args.BackupType = ScheduledBackupExecutionArgs_FULL

	// Set schedule name; if one was not provided, assign default value
	if eval.scheduleName != nil {
		name, err := eval.scheduleName()
		if err != nil {
			return err
		}
		sj.SetScheduleName(name)
	} else {
		fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
		fmt.Fprintf(fmtCtx, "%s BACKUP OF ", args.BackupType)
		if eval.Targets == nil {
			fmtCtx.WriteString("CLUSTER")
		} else {
			eval.Targets.Format(fmtCtx)
		}
		sj.SetScheduleName(fmtCtx.String())
	}

	if eval.recurrence != nil {
		recurrence, err := eval.recurrence()
		if err != nil {
			return err
		}
		err = sj.SetSchedule(recurrence)
		if err != nil {
			return err
		}
	}

	if err := setScheduleOptions(eval, &p.ExtendedEvalContext().EvalContext, sj); err != nil {
		return err
	}

	// Prepare backup statement.
	backupNode := &tree.Backup{
		Options: tree.BackupOptions{
			CaptureRevisionHistory: eval.BackupOptions.CaptureRevisionHistory,
			Detached:               true,
		},
	}

	// Evaluate encryption passphrase if set.
	if eval.encryptionPassphrase != nil {
		pw, err := eval.encryptionPassphrase()
		if err != nil {
			return errors.Wrapf(err, "failed to evaluate backup encryption_passphrase")
		}
		backupNode.Options.EncryptionPassphrase = tree.NewDString(pw)
	}

	// Evaluate required backup destinations.
	destinations, err := eval.destination()
	if err != nil {
		return errors.Wrapf(err, "failed to evaluate backup destination paths")
	}
	for _, dest := range destinations {
		backupNode.To = append(backupNode.To, tree.NewDString(dest))
	}

	// Set backup targets and descriptor coverage.
	if eval.Targets == nil {
		backupNode.DescriptorCoverage = tree.AllDescriptors
	} else {
		backupNode.DescriptorCoverage = tree.RequestedDescriptors
		backupNode.Targets = *eval.Targets
	}

	// TODO(yevgeniy): Validate backup schedule:
	//  * Verify targets exist.  Provide a way for user to override this via option.
	//  * Verify destination paths sane (i.e. valid schema://, etc)

	// We do not set backupNode.AsOf: this is done when the scheduler kicks off the backup.
	// Serialize backup statement and set schedule executor and its args.
	args.BackupStatement = tree.AsString(backupNode)
	any, err := pbtypes.MarshalAny(args)
	if err != nil {
		return err
	}
	sj.SetExecutionDetails(scheduledBackupExecutorName, jobspb.ExecutionArguments{Args: any})

	// Create the schedule.
	if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return sj.Create(ctx, p.ExecCfg().InternalExecutor, txn)
	}); err != nil {
		return err
	}

	var nextRun tree.Datum
	if sj.IsPaused() {
		nextRun = tree.DNull
	} else {
		next, err := tree.MakeDTimestampTZ(sj.NextRun(), time.Microsecond)
		if err != nil {
			return err
		}
		nextRun = next
	}

	resultsCh <- tree.Datums{
		tree.NewDInt(tree.DInt(sj.ScheduleID())),
		nextRun,
		tree.NewDString(tree.AsString(eval.ScheduledBackup)),
	}
	return nil
}

const scheduleBackupOp = "CREATE SCHEDULE FOR BACKUP"

// makeScheduleBackupEval prepares helper scheduledBackupEval struct to assist in evaluation
// of various schedule and backup specific components.
func makeScheduledBackupEval(
	ctx context.Context, p sql.PlanHookState, schedule *tree.ScheduledBackup,
) (*scheduledBackupEval, error) {
	eval := &scheduledBackupEval{ScheduledBackup: schedule}
	var err error

	if schedule.ScheduleName != nil {
		eval.scheduleName, err = p.TypeAsString(ctx, schedule.ScheduleName, scheduleBackupOp)
		if err != nil {
			return nil, err
		}
	}

	if schedule.Recurrence != nil {
		eval.recurrence, err = p.TypeAsString(ctx, schedule.Recurrence, scheduleBackupOp)
		if err != nil {
			return nil, err
		}
	}

	eval.scheduleOpts, err = p.TypeAsStringOpts(
		ctx, schedule.ScheduleOptions, scheduledBackupOptionExpectValues)
	if err != nil {
		return nil, err
	}

	eval.destination, err = p.TypeAsStringArray(ctx, tree.Exprs(schedule.To), scheduleBackupOp)
	if err != nil {
		return nil, err
	}
	if schedule.BackupOptions.EncryptionPassphrase != nil {
		eval.encryptionPassphrase, err =
			p.TypeAsString(ctx, schedule.BackupOptions.EncryptionPassphrase, scheduleBackupOp)
		if err != nil {
			return nil, err
		}
	}
	return eval, nil
}

// scheduledBackupHeader is the header for "CREATE SCHEDULE..." statements results.
var scheduledBackupHeader = sqlbase.ResultColumns{
	{Name: "schedule_id", Typ: types.Int},
	{Name: "next_run", Typ: types.TimestampTZ},
	{Name: "description", Typ: types.String},
}

func createBackupScheduleHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, bool, error) {
	schedule, ok := stmt.(*tree.ScheduledBackup)
	if !ok {
		return nil, nil, nil, false, nil
	}
	eval, err := makeScheduledBackupEval(ctx, p, schedule)
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		return doCreateBackupSchedule(ctx, p, eval, resultsCh)
	}
	return fn, scheduledBackupHeader, nil, false, nil
}

func init() {
	sql.AddPlanHook(createBackupScheduleHook)
}
