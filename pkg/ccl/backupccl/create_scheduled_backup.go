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

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
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
	"github.com/gorhill/cronexpr"
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

	isEnterpriseUser bool

	// Schedule specific properties that get evaluated.
	scheduleName         func() (string, error)
	recurrence           func() (string, error)
	fullBackupRecurrence func() (string, error)
	scheduleOpts         func() (map[string]string, error)

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
	evalCtx *tree.EvalContext, opts map[string]string, sj *jobs.ScheduledJob,
) error {
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

type scheduleRecurrence struct {
	cron      string
	frequency time.Duration
}

var neverRecurs *scheduleRecurrence

func computeScheduleRecurrence(
	now time.Time, evalFn func() (string, error),
) (*scheduleRecurrence, error) {
	if evalFn == nil {
		return neverRecurs, nil
	}
	cron, err := evalFn()
	if err != nil {
		return nil, err
	}
	expr, err := cronexpr.Parse(cron)
	if err != nil {
		return nil, err
	}
	nextRun := expr.Next(now)
	frequency := expr.Next(nextRun).Sub(nextRun)
	return &scheduleRecurrence{cron, frequency}, nil
}

var humanDurations = map[time.Duration]string{
	time.Hour:          "hour",
	24 * time.Hour:     "day",
	7 * 24 * time.Hour: "week",
}

func (r *scheduleRecurrence) Humanize() string {
	if d, ok := humanDurations[r.frequency]; ok {
		return "every " + d
	}
	return "every " + r.frequency.String()
}

var forceFullBackup *scheduleRecurrence

func pickFullRecurrenceFromIncremental(inc *scheduleRecurrence) *scheduleRecurrence {
	if inc.frequency <= time.Hour {
		// If incremental is faster than once an hour, take fulls every day,
		// some time between midnight and 1 am.
		return &scheduleRecurrence{
			cron:      "@daily",
			frequency: 24 * time.Hour,
		}
	}

	if inc.frequency <= 24*time.Hour {
		// If incremental is less than a day, take full weekly;  some day
		// between 0 and 1 am.
		return &scheduleRecurrence{
			cron:      "@weekly",
			frequency: 7 * 24 * time.Hour,
		}
	}

	// Incremental period too large.
	return forceFullBackup
}

const scheduleBackupOp = "CREATE SCHEDULE FOR BACKUP"

// doCreateBackupSchedule creates requested schedule (or schedules).
// It is a plan hook implementation responsible for the creating of scheduled backup.
func doCreateBackupSchedules(
	ctx context.Context, p sql.PlanHookState, eval *scheduledBackupEval, resultsCh chan<- tree.Datums,
) error {
	if err := p.RequireAdminRole(ctx, scheduleBackupOp); err != nil {
		return err
	}
	env := scheduledjobs.ProdJobSchedulerEnv
	if knobs, ok := p.ExecCfg().DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs); ok {
		if knobs.JobSchedulerEnv != nil {
			env = knobs.JobSchedulerEnv
		}
	}

	// Evaluate incremental and full recurrence.
	incRecurrence, err := computeScheduleRecurrence(env.Now(), eval.recurrence)
	if err != nil {
		return err
	}
	fullRecurrence, err := computeScheduleRecurrence(env.Now(), eval.fullBackupRecurrence)
	if err != nil {
		return err
	}

	if incRecurrence != nil && fullRecurrence == nil {
		// It's an enterprise user; let's see if we can pick a reasonable
		// full  backup recurrence based on requested incremental recurrence.
		fullRecurrence = pickFullRecurrenceFromIncremental(incRecurrence)

		if fullRecurrence == forceFullBackup {
			fullRecurrence = incRecurrence
			incRecurrence = nil
		}
	}

	if fullRecurrence == nil {
		return errors.AssertionFailedf(" full backup recurrence should be set")
	}

	// Prepare backup statement (full).
	backupNode := &tree.Backup{
		Options: tree.BackupOptions{
			CaptureRevisionHistory: eval.BackupOptions.CaptureRevisionHistory,
			Detached:               true,
		},
		Nested:         true,
		AppendToLatest: false,
	}

	if backupNode.Options.CaptureRevisionHistory && !eval.isEnterpriseUser {
		// TODO(yevgeniy): Pull license check logic into a common helper.
		if err := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(),
			"revision_history"); err != nil {
			return err
		}
	}

	// Evaluate encryption passphrase if set.
	if eval.encryptionPassphrase != nil {
		if !eval.isEnterpriseUser {
			if err := utilccl.CheckEnterpriseEnabled(
				p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(),
				"encryption"); err != nil {
				return err
			}
		}
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

	if len(destinations) > 1 {
		if !eval.isEnterpriseUser {
			if err := utilccl.CheckEnterpriseEnabled(
				p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(),
				"partitioned destinations"); err != nil {
				return err
			}
		}
	}
	for _, dest := range destinations {
		backupNode.To = append(backupNode.To, tree.NewDString(dest))
	}

	backupNode.Targets = eval.Targets

	var fullScheduleName string
	if eval.scheduleName != nil {
		scheduleName, err := eval.scheduleName()
		if err != nil {
			return err
		}
		fullScheduleName = scheduleName
	} else {
		fullScheduleName = fmt.Sprintf("BACKUP %d", env.Now().Unix())
	}

	scheduleOptions, err := eval.scheduleOpts()
	if err != nil {
		return err
	}

	return p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Create FULL backup schedule.
		if err := createBackupSchedule(
			ctx, p, env, fullScheduleName, fullRecurrence,
			scheduleOptions, backupNode, resultsCh, txn,
		); err != nil {
			return err
		}

		// If needed, create incremental.
		if incRecurrence != nil {
			backupNode.AppendToLatest = true

			if err := createBackupSchedule(
				ctx, p, env, fullScheduleName+": CHANGES", incRecurrence,
				scheduleOptions, backupNode, resultsCh, txn,
			); err != nil {
				return err
			}
		}

		return nil
	})

}

func createBackupSchedule(
	ctx context.Context,
	p sql.PlanHookState,
	env scheduledjobs.JobSchedulerEnv,
	name string,
	recurrence *scheduleRecurrence,
	scheduleOpts map[string]string,
	backupNode *tree.Backup,
	resultsCh chan<- tree.Datums,
	txn *kv.Txn,
) error {
	sj := jobs.NewScheduledJob(env)
	sj.SetScheduleName(name)

	// Prepare arguments for scheduled backup execution.
	args := &ScheduledBackupExecutionArgs{}
	if backupNode.AppendToLatest {
		args.BackupType = ScheduledBackupExecutionArgs_INCREMENTAL
	} else {
		args.BackupType = ScheduledBackupExecutionArgs_FULL
	}

	if err := sj.SetSchedule(recurrence.cron); err != nil {
		return err
	}

	if err := setScheduleOptions(&p.ExtendedEvalContext().EvalContext, scheduleOpts, sj); err != nil {
		return err
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
	sj.SetExecutionDetails(
		tree.ScheduledBackupExecutor.InternalName(),
		jobspb.ExecutionArguments{Args: any},
	)

	// Create the schedule.
	if err := sj.Create(ctx, p.ExecCfg().InternalExecutor, txn); err != nil {
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
		tree.NewDString(name),
		nextRun,
		tree.NewDString(recurrence.Humanize()),
		tree.NewDString(tree.AsString(backupNode)),
	}
	return nil
}

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

	if schedule.Recurrence == nil {
		// Sanity check: recurrence must be specified.
		return nil, errors.New("RECURRING clause required")
	}

	eval.recurrence, err = p.TypeAsString(ctx, schedule.Recurrence, scheduleBackupOp)
	if err != nil {
		return nil, err
	}

	enterpriseCheckErr := utilccl.CheckEnterpriseEnabled(
		p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(),
		"BACKUP INTO LATEST")
	eval.isEnterpriseUser = enterpriseCheckErr == nil

	if eval.isEnterpriseUser && schedule.FullBackup != nil {
		if schedule.FullBackup.AlwaysFull {
			eval.fullBackupRecurrence = eval.recurrence
			eval.recurrence = nil
		} else {
			eval.fullBackupRecurrence, err = p.TypeAsString(
				ctx, schedule.FullBackup.Recurrence, scheduleBackupOp)
			if err != nil {
				return nil, err
			}
		}
	} else if !eval.isEnterpriseUser {
		if schedule.FullBackup == nil || schedule.FullBackup.AlwaysFull {
			// All backups are full cluster backups for free users.
			eval.fullBackupRecurrence = eval.recurrence
			eval.recurrence = nil
		} else {
			// Cannot use incremental backup w/out enterprise license.
			return nil, enterpriseCheckErr
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
	{Name: "name", Typ: types.String},
	{Name: "next_run", Typ: types.TimestampTZ},
	{Name: "frequency", Typ: types.String},
	{Name: "backup_stmt", Typ: types.String},
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
		return doCreateBackupSchedules(ctx, p, eval, resultsCh)
	}
	return fn, scheduledBackupHeader, nil, false, nil
}

func init() {
	sql.AddPlanHook(createBackupScheduleHook)
}
