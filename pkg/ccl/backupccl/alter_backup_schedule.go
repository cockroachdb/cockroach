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
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs/schedulebase"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
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
	fullStmt *tree.Backup
	incJob   *jobs.ScheduledJob
	incArgs  *backuppb.ScheduledBackupExecutionArgs
	incStmt  *tree.Backup
}

func loadSchedules(
	ctx context.Context, p sql.PlanHookState, spec *alterBackupScheduleSpec,
) (scheduleDetails, error) {
	scheduleID := spec.scheduleID
	s := scheduleDetails{}
	if scheduleID == 0 {
		return s, errors.Newf("Schedule ID expected, none found")
	}

	execCfg := p.ExecCfg()
	env := sql.JobSchedulerEnv(execCfg.JobsKnobs())
	schedules := jobs.ScheduledJobTxn(p.InternalSQLTxn())
	schedule, err := schedules.Load(ctx, env, int64(scheduleID))
	if err != nil {
		return s, err
	}
	args := &backuppb.ScheduledBackupExecutionArgs{}
	if err := pbtypes.UnmarshalAny(schedule.ExecutionArgs().Args, args); err != nil {
		return s, errors.Wrap(err, "un-marshaling args")
	}

	node, err := parser.ParseOne(args.BackupStatement)
	if err != nil {
		return scheduleDetails{}, err
	}
	stmt, ok := node.AST.(*tree.Backup)
	if !ok {
		return scheduleDetails{}, errors.Newf("unexpected node type %T", node)
	}

	var dependentSchedule *jobs.ScheduledJob
	var dependentArgs *backuppb.ScheduledBackupExecutionArgs
	var dependentStmt *tree.Backup

	if args.DependentScheduleID != 0 {
		dependentSchedule, err = schedules.Load(ctx, env, args.DependentScheduleID)
		if err != nil {
			return scheduleDetails{}, err
		}
		dependentArgs = &backuppb.ScheduledBackupExecutionArgs{}
		if err := pbtypes.UnmarshalAny(dependentSchedule.ExecutionArgs().Args, dependentArgs); err != nil {
			return s, errors.Wrap(err, "un-marshaling args")
		}
		node, err := parser.ParseOne(dependentArgs.BackupStatement)
		if err != nil {
			return scheduleDetails{}, err
		}
		dependentStmt, ok = node.AST.(*tree.Backup)
		if !ok {
			return scheduleDetails{}, errors.Newf("unexpected node type %T", node)
		}
	}

	if args.BackupType == backuppb.ScheduledBackupExecutionArgs_FULL {
		s.fullJob, s.fullArgs, s.fullStmt = schedule, args, stmt
		s.incJob, s.incArgs, s.incStmt = dependentSchedule, dependentArgs, dependentStmt
	} else {
		s.fullJob, s.fullArgs, s.fullStmt = dependentSchedule, dependentArgs, dependentStmt
		s.incJob, s.incArgs, s.incStmt = schedule, args, stmt
	}
	return s, nil
}

// doAlterBackupSchedule creates requested schedule (or schedules).
// It is a plan hook implementation responsible for the creating of scheduled backup.
func doAlterBackupSchedules(
	ctx context.Context,
	p sql.PlanHookState,
	spec *alterBackupScheduleSpec,
	resultsCh chan<- tree.Datums,
) error {
	s, err := loadSchedules(ctx, p, spec)
	if err != nil {
		return err
	}

	if s.fullJob == nil {
		// This can happen if a user calls DROP SCHEDULE on the full schedule.
		// TODO(benbardin): Resolve https://github.com/cockroachdb/cockroach/issues/87435.
		// Note that this will only prevent this state going forward. It will not
		// repair this state where it already exists.
		return errors.Newf(
			"incremental backup schedule %d has no corresponding full backup schedule; drop schedule %d and recreate",
			s.incJob.ScheduleID(),
			s.incJob.ScheduleID())
	}

	// Check that the user is admin or the owner of the schedules being altered.
	isAdmin, err := p.UserHasAdminRole(ctx, p.User())
	if err != nil {
		return err
	}
	isOwnerOfFullJob := s.fullJob == nil || s.fullJob.Owner() == p.User()
	isOwnerOfIncJob := s.incJob == nil || s.incJob.Owner() == p.User()
	if !isAdmin && !(isOwnerOfFullJob && isOwnerOfIncJob) {
		return pgerror.New(pgcode.InsufficientPrivilege, "must be admin or owner of the "+
			"schedules being altered.")
	}

	if s, err = processFullBackupRecurrence(
		ctx,
		p,
		spec.fullBackupAlways,
		spec.fullBackupRecurrence,
		spec.isEnterpriseUser,
		s,
	); err != nil {
		return err
	}

	if err := processRecurrence(
		spec.recurrence,
		s.fullJob,
		s.incJob,
	); err != nil {
		return err
	}

	if err := validateFullIncrementalFrequencies(p, s); err != nil {
		return err
	}

	if err := processLabel(spec, s); err != nil {
		return err
	}

	if err := processInto(p, spec, s); err != nil {
		return err
	}

	if err := processOptions(spec, s); err != nil {
		return err
	}

	if err := processScheduleOptions(ctx, p, spec, s); err != nil {
		return err
	}

	// Run full backup in dry-run mode.  This will do all of the sanity checks
	// and validation we need to make in order to ensure the schedule is sane.
	if _, err = dryRunBackup(ctx, p, s.fullStmt); err != nil {
		return errors.Wrap(err, "failed to dry run backup")
	}

	s.fullArgs.BackupStatement = tree.AsStringWithFlags(s.fullStmt, tree.FmtParsable|tree.FmtShowPasswords)
	fullAny, err := pbtypes.MarshalAny(s.fullArgs)
	if err != nil {
		return err
	}
	scheduledJobs := jobs.ScheduledJobTxn(p.InternalSQLTxn())
	s.fullJob.SetExecutionDetails(
		tree.ScheduledBackupExecutor.InternalName(),
		jobspb.ExecutionArguments{Args: fullAny})
	if err := scheduledJobs.Update(ctx, s.fullJob); err != nil {
		return err
	}

	if s.incJob != nil {
		s.incArgs.BackupStatement = tree.AsStringWithFlags(s.incStmt, tree.FmtParsable|tree.FmtShowPasswords)
		incAny, err := pbtypes.MarshalAny(s.incArgs)
		if err != nil {
			return err
		}
		s.incJob.SetExecutionDetails(
			tree.ScheduledBackupExecutor.InternalName(),
			jobspb.ExecutionArguments{Args: incAny})

		if err := scheduledJobs.Update(ctx, s.incJob); err != nil {
			return err
		}

		if err := emitAlteredSchedule(s.incJob, s.incStmt, resultsCh); err != nil {
			return err
		}
	}

	// Emit the full backup schedule after the incremental.
	// This matches behavior in CREATE SCHEDULE FOR BACKUP.
	return emitAlteredSchedule(s.fullJob, s.fullStmt, resultsCh)
}

func emitAlteredSchedule(
	job *jobs.ScheduledJob, stmt *tree.Backup, resultsCh chan<- tree.Datums,
) error {
	to := make([]string, len(stmt.To))
	for i, dest := range stmt.To {
		to[i] = tree.AsStringWithFlags(dest, tree.FmtBareStrings)
	}
	kmsURIs := make([]string, len(stmt.Options.EncryptionKMSURI))
	for i, kmsURI := range stmt.Options.EncryptionKMSURI {
		kmsURIs[i] = tree.AsStringWithFlags(kmsURI, tree.FmtBareStrings)
	}
	incDests := make([]string, len(stmt.Options.IncrementalStorage))
	for i, incDest := range stmt.Options.IncrementalStorage {
		incDests[i] = tree.AsStringWithFlags(incDest, tree.FmtBareStrings)
	}
	if err := emitSchedule(job, stmt, to, nil, /* incrementalFrom */
		kmsURIs, incDests, resultsCh); err != nil {
		return err
	}
	return nil
}

func processScheduleOptions(
	ctx context.Context, p sql.PlanHookState, spec *alterBackupScheduleSpec, s scheduleDetails,
) error {
	if spec.scheduleOptions == nil {
		return nil
	}
	scheduleOptions := spec.scheduleOptions
	fullDetails := s.fullJob.ScheduleDetails()
	var incDetails *jobspb.ScheduleDetails
	if s.incJob != nil {
		incDetails = s.incJob.ScheduleDetails()
	}
	for k, v := range scheduleOptions {
		switch k {
		case optOnExecFailure:
			if err := schedulebase.ParseOnError(v, fullDetails); err != nil {
				return err
			}
			// Set the schedule to mark the column as dirty.
			s.fullJob.SetScheduleDetails(*fullDetails)
			if incDetails == nil {
				continue
			}
			if err := schedulebase.ParseOnError(v, incDetails); err != nil {
				return err
			}
			s.incJob.SetScheduleDetails(*incDetails)
		case optOnPreviousRunning:
			if err := schedulebase.ParseWaitBehavior(v, fullDetails); err != nil {
				return err
			}

			s.fullJob.SetScheduleDetails(*fullDetails)
			if incDetails == nil {
				continue
			}
			if err := schedulebase.ParseWaitBehavior(v, incDetails); err != nil {
				return err
			}
			s.incJob.SetScheduleDetails(*incDetails)
		case optUpdatesLastBackupMetric:
			// NB: as of 20.2, schedule creation requires admin so this is duplicative
			// but in the future we might relax so you can schedule anything that you
			// can backup, but then this cluster-wide metric should be admin-only.
			if err := p.RequireAdminRole(ctx, optUpdatesLastBackupMetric); err != nil {
				return pgerror.Wrap(err, pgcode.InsufficientPrivilege, "")
			}

			updatesLastBackupMetric, err := strconv.ParseBool(v)
			if err != nil {
				return errors.Wrapf(err, "unexpected value for %s: %s", k, v)
			}
			s.fullArgs.UpdatesLastBackupMetric = updatesLastBackupMetric
			if s.incArgs == nil {
				continue
			}
			s.incArgs.UpdatesLastBackupMetric = updatesLastBackupMetric
		default:
			return errors.Newf("unexpected schedule option: %s = %s", k, v)
		}
	}
	return nil
}

func processOptions(spec *alterBackupScheduleSpec, s scheduleDetails) error {
	opts := spec.backupOptions
	fullOpts := &s.fullStmt.Options
	if err := processOptionsForArgs(opts, fullOpts); err != nil {
		return err
	}
	if s.incStmt == nil {
		return nil
	}
	incOpts := &s.incStmt.Options
	if err := processOptionsForArgs(opts, incOpts); err != nil {
		return err
	}
	return nil
}

func processOptionsForArgs(inOpts tree.BackupOptions, outOpts *tree.BackupOptions) error {
	if inOpts.CaptureRevisionHistory != nil {
		outOpts.CaptureRevisionHistory = inOpts.CaptureRevisionHistory
	}

	// If a string-y option is set to empty, interpret this as "unset."
	if inOpts.EncryptionPassphrase != nil {
		if tree.AsStringWithFlags(inOpts.EncryptionPassphrase, tree.FmtBareStrings) == "" {
			outOpts.EncryptionPassphrase = nil
		} else {
			outOpts.EncryptionPassphrase = inOpts.EncryptionPassphrase
		}
	}
	if inOpts.EncryptionKMSURI != nil {
		if tree.AsStringWithFlags(&inOpts.EncryptionKMSURI, tree.FmtBareStrings) == "" {
			outOpts.EncryptionKMSURI = nil
		} else {
			outOpts.EncryptionKMSURI = inOpts.EncryptionKMSURI
		}
	}
	if inOpts.IncrementalStorage != nil {
		if tree.AsStringWithFlags(&inOpts.IncrementalStorage, tree.FmtBareStrings) == "" {
			outOpts.IncrementalStorage = nil
		} else {
			outOpts.IncrementalStorage = inOpts.IncrementalStorage
		}
	}
	return nil
}

func processRecurrence(
	recurrence string, fullJob *jobs.ScheduledJob, incJob *jobs.ScheduledJob,
) error {
	if recurrence == "" {
		return nil
	}
	if incJob != nil {
		if err := incJob.SetSchedule(recurrence); err != nil {
			return err
		}
	} else {
		if err := fullJob.SetSchedule(recurrence); err != nil {
			return err
		}
	}
	return nil
}

func processFullBackupRecurrence(
	ctx context.Context,
	p sql.PlanHookState,
	fullBackupAlways bool,
	fullBackupRecurrence string,
	isEnterpriseUser bool,
	s scheduleDetails,
) (scheduleDetails, error) {
	var err error

	if !fullBackupAlways && fullBackupRecurrence == "" {
		return s, nil
	}

	env := sql.JobSchedulerEnv(p.ExecCfg().JobsKnobs())
	scheduledJobs := jobs.ScheduledJobTxn(p.InternalSQLTxn())
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
		if err := scheduledJobs.Delete(ctx, s.incJob); err != nil {
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
		s.incStmt = &tree.Backup{}
		*s.incStmt = *s.fullStmt
		s.incStmt.AppendToLatest = true

		rec := s.fullJob.ScheduleExpr()
		incRecurrence, err := schedulebase.ComputeScheduleRecurrence(env.Now(), &rec)
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
			s.incStmt,
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

		if err := scheduledJobs.Create(ctx, s.incJob); err != nil {
			return scheduleDetails{}, err
		}
		s.fullArgs.UnpauseOnSuccess = s.incJob.ScheduleID()
		s.fullArgs.DependentScheduleID = s.incJob.ScheduleID()
	}
	// We have an incremental backup at this point.
	// Make no (further) changes, and just edit the cadence on the full.
	if err := s.fullJob.SetSchedule(fullBackupRecurrence); err != nil {
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

func validateFullIncrementalFrequencies(p sql.PlanHookState, s scheduleDetails) error {
	if s.incJob == nil {
		return nil
	}
	env := sql.JobSchedulerEnv(p.ExecCfg().JobsKnobs())
	now := env.Now()

	fullFreq, err := frequencyFromCron(now, s.fullJob.ScheduleExpr())
	if err != nil {
		return err
	}
	incFreq, err := frequencyFromCron(now, s.incJob.ScheduleExpr())
	if err != nil {
		return err
	}
	if fullFreq-incFreq < 0 {
		return errors.Newf("incremental backups must occur more often than full backups")
	}
	return nil
}

func processLabel(spec *alterBackupScheduleSpec, s scheduleDetails) error {
	if spec.label == "" {
		return nil
	}
	s.fullJob.SetScheduleLabel(spec.label)
	if s.incJob == nil {
		return nil
	}
	s.incJob.SetScheduleLabel(spec.label)
	return nil
}

func processInto(p sql.PlanHookState, spec *alterBackupScheduleSpec, s scheduleDetails) error {
	if spec.into == nil {
		return nil
	}
	into := spec.into
	s.fullStmt.To = make([]tree.Expr, len(into))
	for i, dest := range spec.into {
		s.fullStmt.To[i] = tree.NewStrVal(dest)
	}

	if s.incJob == nil {
		return nil
	}

	s.incStmt.To = make([]tree.Expr, len(into))
	for i, dest := range into {
		s.incStmt.To[i] = tree.NewStrVal(dest)
	}

	// With a new destination, no full backup has completed yet.
	// Pause incrementals until a full backup completes.
	s.incJob.Pause()
	s.incJob.SetScheduleStatus("Waiting for initial backup to complete")
	s.fullArgs.UnpauseOnSuccess = s.incJob.ScheduleID()

	// Kick off a full backup immediately so we can unpause incrementals.
	// This mirrors the behavior of CREATE SCHEDULE FOR BACKUP.
	env := sql.JobSchedulerEnv(p.ExecCfg().JobsKnobs())
	s.fullJob.SetNextRun(env.Now())

	return nil
}

type alterBackupScheduleSpec struct {
	// Schedule specific properties that get evaluated.
	scheduleID           uint64
	recurrence           string
	fullBackupRecurrence string
	fullBackupAlways     bool
	isEnterpriseUser     bool
	label                string
	into                 []string
	backupOptions        tree.BackupOptions
	scheduleOptions      map[string]string
}

// makeAlterBackupScheduleSpec construct alterBackupScheduleSpec struct to assist
// by evaluating the various parameter for the schedule and backup specific
// components.
func makeAlterBackupScheduleSpec(
	ctx context.Context, p sql.PlanHookState, alterStmt *tree.AlterBackupSchedule,
) (*alterBackupScheduleSpec, error) {
	exprEval := p.ExprEvaluator(alterBackupScheduleOp)
	spec := &alterBackupScheduleSpec{
		scheduleID: alterStmt.ScheduleID,
	}
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
	scheduleOptions := make([]tree.KVOption, 0)
	for _, cmd := range alterStmt.Cmds {
		switch typedCmd := cmd.(type) {
		case *tree.AlterBackupScheduleSetFullBackup:
			if err := observe("SET FULL BACKUP"); err != nil {
				return nil, err
			}
			if typedCmd.FullBackup.AlwaysFull {
				spec.fullBackupAlways = true
			} else {
				spec.fullBackupRecurrence, err = exprEval.String(
					ctx, typedCmd.FullBackup.Recurrence,
				)
			}
		case *tree.AlterBackupScheduleSetRecurring:
			if err := observe("SET RECURRING"); err != nil {
				return nil, err
			}
			spec.recurrence, err = exprEval.String(ctx, typedCmd.Recurrence)
		case *tree.AlterBackupScheduleSetLabel:
			if err := observe("SET LABEL"); err != nil {
				return nil, err
			}
			spec.label, err = exprEval.String(ctx, typedCmd.Label)
		case *tree.AlterBackupScheduleSetInto:
			if err := observe("SET INTO"); err != nil {
				return nil, err
			}
			spec.into, err = exprEval.StringArray(ctx, tree.Exprs(typedCmd.Into))
		case *tree.AlterBackupScheduleSetWith:
			if typedCmd.With.Detached != nil {
				err = errors.Newf("DETACHED is required for scheduled backups and cannot be altered")
			} else {
				err = spec.backupOptions.CombineWith(typedCmd.With)
			}
		case *tree.AlterBackupScheduleSetScheduleOption:
			scheduleOptions = append(scheduleOptions, typedCmd.Option)
		default:
			return nil, errors.Newf("not yet implemented: %v", tree.AsString(typedCmd))
		}
		if err != nil {
			return nil, err
		}
	}
	// TODO(benbardin): Block duplicate schedule options if possible.
	spec.scheduleOptions, err = exprEval.KVOptions(
		ctx, scheduleOptions, alterBackupScheduleOptions,
	)
	if err != nil {
		return nil, err
	}

	enterpriseCheckErr := utilccl.CheckEnterpriseEnabled(
		p.ExecCfg().Settings, p.ExecCfg().NodeInfo.LogicalClusterID(),
		"BACKUP INTO LATEST")
	spec.isEnterpriseUser = enterpriseCheckErr == nil

	return spec, nil
}

var alterBackupScheduleOptions = exprutil.KVOptionValidationMap{
	// optFirstRun and optIgnoreExistingBackups excluded here, as they don't
	// make much sense in the context of ALTER.
	optOnExecFailure:           exprutil.KVStringOptAny,
	optOnPreviousRunning:       exprutil.KVStringOptAny,
	optUpdatesLastBackupMetric: exprutil.KVStringOptAny,
}

func alterBackupScheduleTypeCheck(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (matched bool, header colinfo.ResultColumns, _ error) {
	alterStmt, ok := stmt.(*tree.AlterBackupSchedule)
	if !ok {
		return false, nil, nil
	}
	var strings exprutil.Strings
	var stringArrays exprutil.StringArrays
	var opts tree.KVOptions
	for _, cmd := range alterStmt.Cmds {
		switch typedCmd := cmd.(type) {
		case *tree.AlterBackupScheduleSetFullBackup:
			strings = append(strings, typedCmd.FullBackup.Recurrence)
		case *tree.AlterBackupScheduleSetRecurring:
			strings = append(strings, typedCmd.Recurrence)
		case *tree.AlterBackupScheduleSetLabel:
			strings = append(strings, typedCmd.Label)
		case *tree.AlterBackupScheduleSetInto:
			stringArrays = append(stringArrays, tree.Exprs(typedCmd.Into))
		case *tree.AlterBackupScheduleSetWith:

		case *tree.AlterBackupScheduleSetScheduleOption:
			opts = append(opts, typedCmd.Option)
		}
	}
	if err := exprutil.TypeCheck(
		ctx, alterBackupScheduleOp, p.SemaCtx(),
		strings, stringArrays, exprutil.KVOptions{
			KVOptions:  opts,
			Validation: alterBackupScheduleOptions,
		},
	); err != nil {
		return false, nil, err
	}
	return true, scheduledBackupHeader, nil
}

func alterBackupScheduleHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	alterScheduleStmt, ok := stmt.(*tree.AlterBackupSchedule)
	if !ok {
		return nil, nil, nil, false, nil
	}

	spec, err := makeAlterBackupScheduleSpec(ctx, p, alterScheduleStmt)
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		err := doAlterBackupSchedules(ctx, p, spec, resultsCh)
		if err != nil {
			telemetry.Count("scheduled-backup.alter.failed")
			return err
		}

		return nil
	}
	return fn, scheduledBackupHeader, nil, false, nil
}

func init() {
	sql.AddPlanHook("schedule backup", alterBackupScheduleHook, alterBackupScheduleTypeCheck)
}
