// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
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
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	if scheduleID == jobspb.InvalidScheduleID {
		return s, errors.Newf("Schedule ID expected, none found")
	}

	execCfg := p.ExecCfg()
	env := sql.JobSchedulerEnv(execCfg.JobsKnobs())
	schedules := jobs.ScheduledJobTxn(p.InternalSQLTxn())
	schedule, err := schedules.Load(ctx, env, scheduleID)
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

	// Check that the user has privileges or is the owner of the schedules being altered.
	hasPriv, err := p.HasPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER, p.User())
	if err != nil {
		return err
	}
	isOwnerOfFullJob := s.fullJob == nil || s.fullJob.Owner() == p.User()
	isOwnerOfIncJob := s.incJob == nil || s.incJob.Owner() == p.User()
	if !hasPriv && !(isOwnerOfFullJob && isOwnerOfIncJob) {
		return pgerror.Newf(pgcode.InsufficientPrivilege, "must be admin or the owner of the "+
			"schedules being altered, or have %s privilege", privilege.REPAIRCLUSTER)
	}

	if s, err = processFullBackupRecurrence(
		ctx,
		p,
		spec.fullBackupAlways,
		spec.fullBackupRecurrence,
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

	if err := processNextRunNow(p, spec, s); err != nil {
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

		if err := emitAlteredSchedule(ctx, p, s.incJob, s.incStmt, resultsCh); err != nil {
			return err
		}
	}

	// Emit the full backup schedule after the incremental.
	// This matches behavior in CREATE SCHEDULE FOR BACKUP.
	return emitAlteredSchedule(ctx, p, s.fullJob, s.fullStmt, resultsCh)
}

func emitAlteredSchedule(
	ctx context.Context,
	p sql.PlanHookState,
	job *jobs.ScheduledJob,
	stmt *tree.Backup,
	resultsCh chan<- tree.Datums,
) error {
	exprEval := p.ExprEvaluator("BACKUP")
	to, err := exprEval.StringArray(ctx, tree.Exprs(stmt.To))
	if err != nil {
		return err
	}
	kmsURIs, err := exprEval.StringArray(ctx, tree.Exprs(stmt.Options.EncryptionKMSURI))
	if err != nil {
		return err
	}
	incDests, err := exprEval.StringArray(ctx, tree.Exprs(stmt.Options.IncrementalStorage))
	if err != nil {
		return err
	}
	if err := emitSchedule(job, stmt, to, kmsURIs, incDests, resultsCh); err != nil {
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
			// An incremental backup schedule must always wait if there is a running job
			// that was previously scheduled by this incremental schedule. This is
			// because until the previous incremental backup job completes, all future
			// incremental jobs will attempt to backup data from the same `StartTime`
			// corresponding to the `EndTime` of the last incremental layer. In this
			// case only the first incremental job to complete will succeed, while the
			// remaining jobs will either be rejected or worse corrupt the chain of
			// backups. We can accept both `wait` and `skip` as valid
			// `on_previous_running` options for an incremental schedule.
			//
			// NB: Ideally we'd have a way to configure options for both the full and
			// incremental schedule separately, in which case we could reject the
			// `on_previous_running = start` configuration for incremental schedules.
			// Until then this interception will have to do.
			incDetails.Wait = jobspb.ScheduleDetails_WAIT
			s.incJob.SetScheduleDetails(*incDetails)
		case optUpdatesLastBackupMetric:
			// NB: as of 20.2, schedule creation requires admin so this is duplicative
			// but in the future we might relax so you can schedule anything that you
			// can backup, but then this cluster-wide metric should be admin-only.
			if hasAdmin, err := p.HasAdminRole(ctx); err != nil {
				return err
			} else if !hasAdmin {
				return pgerror.Newf(pgcode.InsufficientPrivilege,
					"only users with the admin role are allowed to change %s", optUpdatesLastBackupMetric)
			}

			// If the option is specified it generally means to set it, unless it has
			// a value and that value parses as false.
			updatesLastBackupMetric := true
			if v != "" {
				var err error
				updatesLastBackupMetric, err = strconv.ParseBool(v)
				if err != nil {
					return errors.Wrapf(err, "unexpected value for %s: %s", k, v)
				}
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

	if inOpts.ExecutionLocality != nil {
		outOpts.ExecutionLocality = inOpts.ExecutionLocality
	}

	if inOpts.IncludeAllSecondaryTenants != nil {
		outOpts.IncludeAllSecondaryTenants = inOpts.IncludeAllSecondaryTenants
	}

	// If a string-y option is set to empty, interpret this as "unset."
	if inOpts.EncryptionPassphrase != nil {
		if tree.AsStringWithFlags(inOpts.EncryptionPassphrase, tree.FmtBareStrings) == "" {
			outOpts.EncryptionPassphrase = nil
		} else {
			outOpts.EncryptionPassphrase = inOpts.EncryptionPassphrase
		}
	}
	if inOpts.ExecutionLocality != nil {
		if tree.AsStringWithFlags(inOpts.ExecutionLocality, tree.FmtBareStrings) == "" {
			outOpts.ExecutionLocality = nil
		} else {
			outOpts.ExecutionLocality = inOpts.ExecutionLocality
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
	if inOpts.UpdatesClusterMonitoringMetrics != nil {
		outOpts.UpdatesClusterMonitoringMetrics = inOpts.UpdatesClusterMonitoringMetrics
	}
	return nil
}

func processRecurrence(
	recurrence string, fullJob *jobs.ScheduledJob, incJob *jobs.ScheduledJob,
) error {
	if recurrence == "" {
		return nil
	}
	// Maintain the pause state of the schedule while updating the schedule.
	if incJob != nil {
		if incJob.IsPaused() {
			incJob.SetScheduleExpr(recurrence)
		} else {
			return incJob.SetScheduleAndNextRun(recurrence)
		}
	} else {
		if fullJob.IsPaused() {
			fullJob.SetScheduleExpr(recurrence)
		} else {
			return fullJob.SetScheduleAndNextRun(recurrence)
		}
	}
	return nil
}

func processFullBackupRecurrence(
	ctx context.Context,
	p sql.PlanHookState,
	fullBackupAlways bool,
	fullBackupRecurrence string,
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
		s.fullJob.SetScheduleExpr(s.incJob.ScheduleExpr())
		s.fullArgs.DependentScheduleID = 0
		s.fullArgs.UnpauseOnSuccess = 0
		if err := scheduledJobs.Delete(ctx, s.incJob); err != nil {
			return scheduleDetails{}, err
		}
		s.incJob = nil
		s.incArgs = nil
		return s, nil
	}

	if s.incJob == nil {
		// No existing incremental job, so we need to create it, copying details
		// from the full.
		s.incStmt = &tree.Backup{}
		*s.incStmt = *s.fullStmt
		s.incStmt.AppendToLatest = true
		// Pre 23.2 schedules did not have a cluster ID, so if we are altering a
		// schedule that was created before 23.2, we need to set the cluster ID on
		// the newly created incremental manually.
		schedDetails := *s.fullJob.ScheduleDetails()
		if schedDetails.ClusterID.Equal(uuid.Nil) {
			schedDetails.ClusterID = p.ExtendedEvalContext().ClusterID
		}

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
			schedDetails,
			jobspb.InvalidScheduleID,
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
	if s.fullJob.IsPaused() {
		s.fullJob.SetScheduleExpr(fullBackupRecurrence)
	} else {
		if err := s.fullJob.SetScheduleAndNextRun(fullBackupRecurrence); err != nil {
			return scheduleDetails{}, err
		}
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
	incPaused := s.incJob.IsPaused()
	s.incJob.Pause()
	s.incJob.SetScheduleStatus("Waiting for initial backup to complete")
	s.fullArgs.UnpauseOnSuccess = s.incJob.ScheduleID()

	// If the inc schedule was not already paused, kick off a full backup immediately
	// so we can unpause incrementals. This mirrors the behavior of
	// CREATE SCHEDULE FOR BACKUP.
	if !incPaused {
		env := sql.JobSchedulerEnv(p.ExecCfg().JobsKnobs())
		s.fullJob.SetNextRun(env.Now())
	}

	return nil
}

func processNextRunNow(
	p sql.PlanHookState, spec *alterBackupScheduleSpec, s scheduleDetails,
) error {
	if !spec.nextRunNow {
		return nil
	}

	env := sql.JobSchedulerEnv(p.ExecCfg().JobsKnobs())

	// Trigger the full schedule, unless there is an inc schedule and the user did
	// not explicitly specify the full.
	schedule := s.fullJob
	if s.incJob != nil && !spec.fullNextRunNow {
		schedule = s.incJob
	}

	// A paused schedule is indicated by having no next_run time. If we triggered
	// a run of a schedule which was previously paused by setting next_run to now,
	// we would be resuming it. This could be surprising, so instead just tell the
	// user to use RESUME explicitly, so they're clear that they need to pause it
	// again later if they don't want it to keep running.
	if schedule.IsPaused() {
		return errors.Newf("cannot execute a paused schedule; use RESUME SCHEDULE instead")
	}
	schedule.SetNextRun(env.Now())
	return nil
}

type alterBackupScheduleSpec struct {
	// Schedule specific properties that get evaluated.
	scheduleID           jobspb.ScheduleID
	recurrence           string
	fullBackupRecurrence string
	fullBackupAlways     bool
	label                string
	into                 []string
	backupOptions        tree.BackupOptions
	scheduleOptions      map[string]string
	nextRunNow           bool
	fullNextRunNow       bool
}

// makeAlterBackupScheduleSpec construct alterBackupScheduleSpec struct to assist
// by evaluating the various parameter for the schedule and backup specific
// components.
func makeAlterBackupScheduleSpec(
	ctx context.Context, p sql.PlanHookState, alterStmt *tree.AlterBackupSchedule,
) (*alterBackupScheduleSpec, error) {
	exprEval := p.ExprEvaluator(alterBackupScheduleOp)
	spec := &alterBackupScheduleSpec{
		scheduleID: jobspb.ScheduleID(alterStmt.ScheduleID),
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
		case *tree.AlterBackupScheduleNextRun:
			spec.nextRunNow = true
			spec.fullNextRunNow = typedCmd.Full
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
		case *tree.AlterBackupScheduleNextRun:
			// no parameters to this cmd so nothing to do here.

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
) (sql.PlanHookRowFn, colinfo.ResultColumns, bool, error) {
	alterScheduleStmt, ok := stmt.(*tree.AlterBackupSchedule)
	if !ok {
		return nil, nil, false, nil
	}

	spec, err := makeAlterBackupScheduleSpec(ctx, p, alterScheduleStmt)
	if err != nil {
		return nil, nil, false, err
	}

	fn := func(ctx context.Context, resultsCh chan<- tree.Datums) error {
		err := doAlterBackupSchedules(ctx, p, spec, resultsCh)
		if err != nil {
			telemetry.Count("scheduled-backup.alter.failed")
			return err
		}

		return nil
	}
	return fn, scheduledBackupHeader, false, nil
}

func init() {
	sql.AddPlanHook("schedule backup", alterBackupScheduleHook, alterBackupScheduleTypeCheck)
}
