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

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupdest"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/exprutil"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
	cron "github.com/robfig/cron/v3"
)

const (
	optFirstRun                = "first_run"
	optOnExecFailure           = "on_execution_failure"
	optOnPreviousRunning       = "on_previous_running"
	optIgnoreExistingBackups   = "ignore_existing_backups"
	optUpdatesLastBackupMetric = "updates_cluster_last_backup_time_metric"
)

var scheduledBackupOptionExpectValues = map[string]exprutil.KVStringOptValidate{
	optFirstRun:                exprutil.KVStringOptRequireValue,
	optOnExecFailure:           exprutil.KVStringOptRequireValue,
	optOnPreviousRunning:       exprutil.KVStringOptRequireValue,
	optIgnoreExistingBackups:   exprutil.KVStringOptRequireNoValue,
	optUpdatesLastBackupMetric: exprutil.KVStringOptRequireNoValue,
}

// scheduledBackupGCProtectionEnabled is used to enable and disable the chaining
// of protected timestamps amongst scheduled backups.
var scheduledBackupGCProtectionEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"schedules.backup.gc_protection.enabled",
	"enable chaining of GC protection across backups run as part of a schedule",
	true, /* defaultValue */
).WithPublic()

// scheduledBackupSpec is a representation of tree.ScheduledBackup, prepared
// for evaluation
type scheduledBackupSpec struct {
	*tree.ScheduledBackup

	isEnterpriseUser bool

	// Schedule specific properties that get evaluated.
	scheduleLabel        *string
	recurrence           *string
	fullBackupRecurrence *string
	scheduleOpts         map[string]string

	// Backup specific properties that get evaluated.
	// We need to evaluate anything in the tree.Backup node that allows
	// placeholders to be specified so that we store evaluated
	// backup statement in the schedule.
	destinations               []string
	encryptionPassphrase       *string
	kmsURIs                    []string
	incrementalStorage         []string
	includeAllSecondaryTenants *bool
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

func parseOnPreviousRunningOption(
	onPreviousRunning jobspb.ScheduleDetails_WaitBehavior,
) (string, error) {
	var onPreviousRunningOption string
	switch onPreviousRunning {
	case jobspb.ScheduleDetails_WAIT:
		onPreviousRunningOption = "WAIT"
	case jobspb.ScheduleDetails_NO_WAIT:
		onPreviousRunningOption = "START"
	case jobspb.ScheduleDetails_SKIP:
		onPreviousRunningOption = "SKIP"
	default:
		return onPreviousRunningOption, errors.Newf("%s is an invalid onPreviousRunning option", onPreviousRunning.String())
	}
	return onPreviousRunningOption, nil
}

func parseOnErrorOption(onError jobspb.ScheduleDetails_ErrorHandlingBehavior) (string, error) {
	var onErrorOption string
	switch onError {
	case jobspb.ScheduleDetails_RETRY_SCHED:
		onErrorOption = "RESCHEDULE"
	case jobspb.ScheduleDetails_RETRY_SOON:
		onErrorOption = "RETRY"
	case jobspb.ScheduleDetails_PAUSE_SCHED:
		onErrorOption = "PAUSE"
	default:
		return onErrorOption, errors.Newf("%s is an invalid onError option", onError.String())
	}
	return onErrorOption, nil
}

func makeScheduleDetails(opts map[string]string) (jobspb.ScheduleDetails, error) {
	var details jobspb.ScheduleDetails
	if v, ok := opts[optOnExecFailure]; ok {
		if err := parseOnError(v, &details); err != nil {
			return details, err
		}
	}

	if v, ok := opts[optOnPreviousRunning]; ok {
		if err := parseWaitBehavior(v, &details); err != nil {
			return details, err
		}
	}
	return details, nil
}

func scheduleFirstRun(evalCtx *eval.Context, opts map[string]string) (*time.Time, error) {
	if v, ok := opts[optFirstRun]; ok {
		firstRun, _, err := tree.ParseDTimestampTZ(evalCtx, v, time.Microsecond)
		if err != nil {
			return nil, err
		}
		return &firstRun.Time, nil
	}
	return nil, nil
}

type scheduleRecurrence struct {
	cron      string
	frequency time.Duration
}

// A sentinel value indicating the schedule never recurs.
var neverRecurs *scheduleRecurrence

func frequencyFromCron(now time.Time, cronStr string) (time.Duration, error) {
	expr, err := cron.ParseStandard(cronStr)
	if err != nil {
		return 0, errors.Newf(
			`error parsing schedule expression: %q; it must be a valid cron expression`,
			cronStr)
	}
	nextRun := expr.Next(now)
	return expr.Next(nextRun).Sub(nextRun), nil
}

func computeScheduleRecurrence(now time.Time, rec *string) (*scheduleRecurrence, error) {
	if rec == nil {
		return neverRecurs, nil
	}
	cronStr := *rec
	frequency, err := frequencyFromCron(now, cronStr)
	if err != nil {
		return nil, err
	}

	return &scheduleRecurrence{cronStr, frequency}, nil
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
	ctx context.Context, p sql.PlanHookState, eval *scheduledBackupSpec, resultsCh chan<- tree.Datums,
) error {
	if eval.ScheduleLabelSpec.IfNotExists {
		exists, err := checkScheduleAlreadyExists(ctx, p, *eval.scheduleLabel)
		if err != nil {
			return err
		}

		if exists {
			p.BufferClientNotice(ctx,
				pgnotice.Newf("schedule %q already exists, skipping", eval.scheduleLabel),
			)
			return nil
		}
	}

	env := sql.JobSchedulerEnv(p.ExecCfg())

	// Evaluate incremental and full recurrence.
	incRecurrence, err := computeScheduleRecurrence(env.Now(), eval.recurrence)
	if err != nil {
		return err
	}
	fullRecurrence, err := computeScheduleRecurrence(env.Now(), eval.fullBackupRecurrence)
	if err != nil {
		return err
	}

	if fullRecurrence != nil && incRecurrence != nil && incRecurrence.frequency > fullRecurrence.frequency {
		return errors.Newf("incremental backups must occur more often than full backups")
	}

	fullRecurrencePicked := false
	if incRecurrence != nil && fullRecurrence == nil {
		// It's an enterprise user; let's see if we can pick a reasonable
		// full  backup recurrence based on requested incremental recurrence.
		fullRecurrence = pickFullRecurrenceFromIncremental(incRecurrence)
		fullRecurrencePicked = true

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
			Detached:               tree.DBoolTrue,
		},
		Nested:         true,
		AppendToLatest: false,
	}

	// Evaluate encryption passphrase if set.
	if eval.encryptionPassphrase != nil {
		backupNode.Options.EncryptionPassphrase = tree.NewStrVal(
			*eval.encryptionPassphrase,
		)
	}

	if eval.includeAllSecondaryTenants != nil {
		if *eval.includeAllSecondaryTenants {
			backupNode.Options.IncludeAllSecondaryTenants = tree.DBoolTrue
		} else {
			backupNode.Options.IncludeAllSecondaryTenants = tree.DBoolFalse
		}
	}

	// Evaluate encryption KMS URIs if set.
	// Only one of encryption passphrase and KMS URI should be set, but this check
	// is done during backup planning so we do not need to worry about it here.
	var kmsURIs []string
	for _, kmsURI := range eval.kmsURIs {
		backupNode.Options.EncryptionKMSURI = append(backupNode.Options.EncryptionKMSURI,
			tree.NewStrVal(kmsURI))
	}

	// Evaluate required backup destinations.
	destinations := eval.destinations
	for _, dest := range destinations {
		backupNode.To = append(backupNode.To, tree.NewStrVal(dest))
	}

	backupNode.Targets = eval.Targets

	// Run full backup in dry-run mode.  This will do all of the sanity checks
	// and validation we need to make in order to ensure the schedule is sane.
	backupEvent, err := dryRunBackup(ctx, p, backupNode)
	if err != nil {
		return errors.Wrapf(err, "failed to dry run backup")
	}

	var scheduleLabel string
	if eval.scheduleLabel != nil {
		scheduleLabel = *eval.scheduleLabel
	} else {
		scheduleLabel = fmt.Sprintf("BACKUP %d", env.Now().Unix())
	}

	scheduleOptions := eval.scheduleOpts

	// Check if backups were already taken to this collection.
	_, ignoreExisting := scheduleOptions[optIgnoreExistingBackups]
	if !ignoreExisting {
		if err := checkForExistingBackupsInCollection(ctx, p, destinations); err != nil {
			return err
		}
	}

	_, updateMetricOnSuccess := scheduleOptions[optUpdatesLastBackupMetric]

	if updateMetricOnSuccess {
		// NB: as of 20.2, schedule creation requires admin so this is duplicative
		// but in the future we might relax so you can schedule anything that you
		// can backup, but then this cluster-wide metric should be admin-only.
		if err := p.RequireAdminRole(ctx, optUpdatesLastBackupMetric); err != nil {
			return err
		}
	}

	evalCtx := &p.ExtendedEvalContext().Context
	firstRun, err := scheduleFirstRun(evalCtx, scheduleOptions)
	if err != nil {
		return err
	}

	details, err := makeScheduleDetails(scheduleOptions)
	if err != nil {
		return err
	}

	ex := p.ExecCfg().InternalExecutor

	unpauseOnSuccessID := jobs.InvalidScheduleID

	var chainProtectedTimestampRecords bool
	// If needed, create incremental.
	var inc *jobs.ScheduledJob
	var incScheduledBackupArgs *backuppb.ScheduledBackupExecutionArgs
	if incRecurrence != nil {
		chainProtectedTimestampRecords = scheduledBackupGCProtectionEnabled.Get(&p.ExecCfg().Settings.SV)
		backupNode.AppendToLatest = true

		var incDests []string
		if eval.incrementalStorage != nil {
			incDests = eval.incrementalStorage
			for _, incDest := range incDests {
				backupNode.Options.IncrementalStorage = append(backupNode.Options.IncrementalStorage, tree.NewStrVal(incDest))
			}
		}
		inc, incScheduledBackupArgs, err = makeBackupSchedule(
			env, p.User(), scheduleLabel, incRecurrence, details, unpauseOnSuccessID,
			updateMetricOnSuccess, backupNode, chainProtectedTimestampRecords)
		if err != nil {
			return err
		}
		// Incremental is paused until FULL completes.
		inc.Pause()
		inc.SetScheduleStatus("Waiting for initial backup to complete")

		if err := inc.Create(ctx, ex, p.Txn()); err != nil {
			return err
		}
		if err := emitSchedule(inc, backupNode, destinations, nil, /* incrementalFrom */
			kmsURIs, incDests, resultsCh); err != nil {
			return err
		}
		unpauseOnSuccessID = inc.ScheduleID()
	}

	// Create FULL backup schedule.
	backupNode.AppendToLatest = false
	backupNode.Options.IncrementalStorage = nil
	var fullScheduledBackupArgs *backuppb.ScheduledBackupExecutionArgs
	full, fullScheduledBackupArgs, err := makeBackupSchedule(
		env, p.User(), scheduleLabel, fullRecurrence, details, unpauseOnSuccessID,
		updateMetricOnSuccess, backupNode, chainProtectedTimestampRecords)
	if err != nil {
		return err
	}

	if firstRun != nil {
		full.SetNextRun(*firstRun)
	} else if eval.isEnterpriseUser && fullRecurrencePicked {
		// The enterprise user did not indicate preference when to run full backups,
		// and we picked the schedule ourselves.
		// Run full backup immediately so that we do not wind up waiting for a long
		// time before the first full backup runs.  Without full backup, we can't
		// execute incremental.
		full.SetNextRun(env.Now())
	}

	// Create the schedule (we need its ID to link dependent schedules below).
	if err := full.Create(ctx, ex, p.Txn()); err != nil {
		return err
	}

	// If schedule creation has resulted in a full and incremental schedule then
	// we update both the schedules with the ID of the other "dependent" schedule.
	if incRecurrence != nil {
		if err := setDependentSchedule(ctx, ex, fullScheduledBackupArgs, full, inc.ScheduleID(),
			p.Txn()); err != nil {
			return errors.Wrap(err,
				"failed to update full schedule with dependent incremental schedule id")
		}
		if err := setDependentSchedule(ctx, ex, incScheduledBackupArgs, inc, full.ScheduleID(),
			p.Txn()); err != nil {
			return errors.Wrap(err,
				"failed to update incremental schedule with dependent full schedule id")
		}
	}

	collectScheduledBackupTelemetry(ctx, incRecurrence, fullRecurrence, firstRun, fullRecurrencePicked, ignoreExisting, details, backupEvent)
	return emitSchedule(full, backupNode, destinations, nil, /* incrementalFrom */
		kmsURIs, nil, resultsCh)
}

func setDependentSchedule(
	ctx context.Context,
	ex *sql.InternalExecutor,
	scheduleExecutionArgs *backuppb.ScheduledBackupExecutionArgs,
	schedule *jobs.ScheduledJob,
	dependentID int64,
	txn *kv.Txn,
) error {
	scheduleExecutionArgs.DependentScheduleID = dependentID
	any, err := pbtypes.MarshalAny(scheduleExecutionArgs)
	if err != nil {
		return errors.Wrap(err, "marshaling args")
	}
	schedule.SetExecutionDetails(
		schedule.ExecutorType(), jobspb.ExecutionArguments{Args: any},
	)
	return schedule.Update(ctx, ex, txn)
}

// checkForExistingBackupsInCollection checks that there are no existing backups
// already in the destination collection. This is used as a safeguard for users
// to avoid creating a backup schedule that will conflict an existing running
// schedule. This may cause an issue because the 2 schedules may be backing up
// different targets and therefore incremental backups into the latest full
// backup may fail if the targets differ.
//
// It is still possible that a user creates 2 schedules pointing to the same
// collection, if both schedules were created before either of them took a full
// backup.
//
// The user should be able to skip this check with a schedule options flag.
func checkForExistingBackupsInCollection(
	ctx context.Context, p sql.PlanHookState, destinations []string,
) error {
	makeCloudFactory := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI
	collectionURI, _, err := backupdest.GetURIsByLocalityKV(destinations, "")
	if err != nil {
		return err
	}

	_, err = backupdest.ReadLatestFile(ctx, collectionURI, makeCloudFactory, p.User())
	if err == nil {
		// A full backup has already been taken to this location.
		return errors.Newf("backups already created in %s; to ignore existing backups, "+
			"the schedule can be created with the 'ignore_existing_backups' option",
			collectionURI)
	}
	if !errors.Is(err, cloud.ErrFileDoesNotExist) {
		return errors.Wrapf(err, "unexpected error occurred when checking for existing backups in %s",
			collectionURI)
	}

	return nil
}

func makeBackupSchedule(
	env scheduledjobs.JobSchedulerEnv,
	owner username.SQLUsername,
	label string,
	recurrence *scheduleRecurrence,
	details jobspb.ScheduleDetails,
	unpauseOnSuccess int64,
	updateLastMetricOnSuccess bool,
	backupNode *tree.Backup,
	chainProtectedTimestampRecords bool,
) (*jobs.ScheduledJob, *backuppb.ScheduledBackupExecutionArgs, error) {
	sj := jobs.NewScheduledJob(env)
	sj.SetScheduleLabel(label)
	sj.SetOwner(owner)

	// Prepare arguments for scheduled backup execution.
	args := &backuppb.ScheduledBackupExecutionArgs{
		UnpauseOnSuccess:               unpauseOnSuccess,
		UpdatesLastBackupMetric:        updateLastMetricOnSuccess,
		ChainProtectedTimestampRecords: chainProtectedTimestampRecords,
	}
	if backupNode.AppendToLatest {
		args.BackupType = backuppb.ScheduledBackupExecutionArgs_INCREMENTAL
	} else {
		args.BackupType = backuppb.ScheduledBackupExecutionArgs_FULL
	}

	if err := sj.SetSchedule(recurrence.cron); err != nil {
		return nil, nil, err
	}

	sj.SetScheduleDetails(details)

	// We do not set backupNode.AsOf: this is done when the scheduler kicks off the backup.
	// Serialize backup statement and set schedule executor and its args.
	args.BackupStatement = tree.AsStringWithFlags(backupNode, tree.FmtParsable|tree.FmtShowPasswords)
	any, err := pbtypes.MarshalAny(args)
	if err != nil {
		return nil, nil, err
	}
	sj.SetExecutionDetails(
		tree.ScheduledBackupExecutor.InternalName(), jobspb.ExecutionArguments{Args: any},
	)

	return sj, args, nil
}

func emitSchedule(
	sj *jobs.ScheduledJob,
	backupNode *tree.Backup,
	to, incrementalFrom, kmsURIs []string,
	incrementalStorage []string,
	resultsCh chan<- tree.Datums,
) error {
	var nextRun tree.Datum
	status := "ACTIVE"
	if sj.IsPaused() {
		nextRun = tree.DNull
		status = "PAUSED"
		if s := sj.ScheduleStatus(); s != "" {
			status += ": " + s
		}
	} else {
		next, err := tree.MakeDTimestampTZ(sj.NextRun(), time.Microsecond)
		if err != nil {
			return err
		}
		nextRun = next
	}

	redactedBackupNode, err := GetRedactedBackupNode(backupNode, to, incrementalFrom, kmsURIs, "",
		incrementalStorage, false /* hasBeenPlanned */)
	if err != nil {
		return err
	}

	resultsCh <- tree.Datums{
		tree.NewDInt(tree.DInt(sj.ScheduleID())),
		tree.NewDString(sj.ScheduleLabel()),
		tree.NewDString(status),
		nextRun,
		tree.NewDString(sj.ScheduleExpr()),
		tree.NewDString(tree.AsString(redactedBackupNode)),
	}
	return nil
}

// checkScheduleAlreadyExists returns true if a schedule with the same label already exists,
// regardless of backup destination.
func checkScheduleAlreadyExists(
	ctx context.Context, p sql.PlanHookState, scheduleLabel string,
) (bool, error) {

	row, err := p.ExecCfg().InternalExecutor.QueryRowEx(ctx, "check-sched",
		p.Txn(), sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		fmt.Sprintf("SELECT count(schedule_name) FROM %s WHERE schedule_name = '%s'",
			scheduledjobs.ProdJobSchedulerEnv.ScheduledJobsTableName(), scheduleLabel))

	if err != nil {
		return false, err
	}
	return int64(tree.MustBeDInt(row[0])) != 0, nil
}

// dryRunBackup executes backup in dry-run mode: we simply execute backup
// under transaction savepoint, and then rollback to that save point.
func dryRunBackup(
	ctx context.Context, p sql.PlanHookState, backupNode *tree.Backup,
) (eventpb.RecoveryEvent, error) {
	sp, err := p.Txn().CreateSavepoint(ctx)
	if err != nil {
		return eventpb.RecoveryEvent{}, err
	}
	backupEvent, err := dryRunInvokeBackup(ctx, p, backupNode)
	if rollbackErr := p.Txn().RollbackToSavepoint(ctx, sp); rollbackErr != nil {
		return backupEvent, rollbackErr
	}
	return backupEvent, err
}

func dryRunInvokeBackup(
	ctx context.Context, p sql.PlanHookState, backupNode *tree.Backup,
) (eventpb.RecoveryEvent, error) {
	backupFn, err := planBackup(ctx, p, backupNode)
	if err != nil {
		return eventpb.RecoveryEvent{}, err
	}
	return invokeBackup(ctx, backupFn, p.ExecCfg().JobRegistry, p.Txn())
}

func fullyQualifyScheduledBackupTargetTables(
	ctx context.Context, p sql.PlanHookState, tables tree.TablePatterns,
) ([]tree.TablePattern, error) {
	fqTablePatterns := make([]tree.TablePattern, len(tables))
	for i, target := range tables {
		tablePattern, err := target.NormalizeTablePattern()
		if err != nil {
			return nil, err
		}
		switch tp := tablePattern.(type) {
		case *tree.TableName:
			if err := sql.DescsTxn(ctx, p.ExecCfg(), func(ctx context.Context, txn *kv.Txn,
				col *descs.Collection) error {
				// Resolve the table.
				un := tp.ToUnresolvedObjectName()
				found, _, tableDesc, err := resolver.ResolveExisting(ctx, un, p, tree.ObjectLookupFlags{},
					p.CurrentDatabase(), p.CurrentSearchPath())
				if err != nil {
					return err
				}
				if !found {
					return errors.Newf("target table %s could not be resolved", tp.String())
				}

				// Resolve the database.
				found, dbDesc, err := col.GetImmutableDatabaseByID(ctx, txn, tableDesc.GetParentID(),
					tree.DatabaseLookupFlags{Required: true})
				if err != nil {
					return err
				}
				if !found {
					return errors.Newf("database of target table %s could not be resolved", tp.String())
				}

				// Resolve the schema.
				schemaDesc, err := col.GetImmutableSchemaByID(ctx, txn, tableDesc.GetParentSchemaID(),
					tree.SchemaLookupFlags{Required: true})
				if err != nil {
					return err
				}
				tn := tree.NewTableNameWithSchema(
					tree.Name(dbDesc.GetName()),
					tree.Name(schemaDesc.GetName()),
					tree.Name(tableDesc.GetName()),
				)
				fqTablePatterns[i] = tn
				return nil
			}); err != nil {
				return nil, err
			}
		case *tree.AllTablesSelector:
			if !tp.ExplicitSchema {
				tp.ExplicitSchema = true
				tp.SchemaName = tree.Name(p.CurrentDatabase())
			} else if tp.ExplicitSchema && !tp.ExplicitCatalog {
				// The schema field could either be a schema or a database. If we can
				// successfully resolve the schema, we will add the DATABASE prefix.
				// Otherwise, no updates are needed since the schema field refers to the
				// database.
				var schemaID descpb.ID
				if err := sql.DescsTxn(ctx, p.ExecCfg(), func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
					flags := tree.DatabaseLookupFlags{Required: true}
					dbDesc, err := col.GetImmutableDatabaseByName(ctx, txn, p.CurrentDatabase(), flags)
					if err != nil {
						return err
					}
					schemaID, err = col.Direct().ResolveSchemaID(ctx, txn, dbDesc.GetID(), tp.SchemaName.String())
					return err
				}); err != nil {
					return nil, err
				}

				if schemaID != descpb.InvalidID {
					tp.ExplicitCatalog = true
					tp.CatalogName = tree.Name(p.CurrentDatabase())
				}
			}
			fqTablePatterns[i] = tp
		}
	}
	return fqTablePatterns, nil
}

// makeScheduleBackupSpec prepares helper scheduledBackupSpec struct to assist in evaluation
// of various schedule and backup specific components.
func makeScheduledBackupSpec(
	ctx context.Context, p sql.PlanHookState, schedule *tree.ScheduledBackup,
) (*scheduledBackupSpec, error) {
	exprEval := p.ExprEvaluator(scheduleBackupOp)

	var err error
	if schedule.Targets != nil && schedule.Targets.Tables.TablePatterns != nil {
		// Table backup targets must be fully qualified during scheduled backup
		// planning. This is because the actual execution of the backup job occurs
		// in a background, scheduled job session, that does not have the same
		// resolution configuration as during planning.
		schedule.Targets.Tables.TablePatterns, err = fullyQualifyScheduledBackupTargetTables(ctx, p,
			schedule.Targets.Tables.TablePatterns)
		if err != nil {
			return nil, errors.Wrap(err, "qualifying backup target tables")
		}
	}

	spec := &scheduledBackupSpec{ScheduledBackup: schedule}

	if schedule.ScheduleLabelSpec.Label != nil {
		label, err := exprEval.String(ctx, schedule.ScheduleLabelSpec.Label)
		if err != nil {
			return nil, err
		}
		spec.scheduleLabel = &label
	}

	if schedule.Recurrence == nil {
		// Sanity check: recurrence must be specified.
		return nil, errors.New("RECURRING clause required")
	}
	{
		rec, err := exprEval.String(ctx, schedule.Recurrence)
		if err != nil {
			return nil, err
		}
		spec.recurrence = &rec
	}

	enterpriseCheckErr := utilccl.CheckEnterpriseEnabled(
		p.ExecCfg().Settings, p.ExecCfg().NodeInfo.LogicalClusterID(), p.ExecCfg().Organization(),
		"BACKUP INTO LATEST")
	spec.isEnterpriseUser = enterpriseCheckErr == nil

	if spec.isEnterpriseUser && schedule.FullBackup != nil {
		if schedule.FullBackup.AlwaysFull {
			spec.fullBackupRecurrence = spec.recurrence
			spec.recurrence = nil
		} else {
			rec, err := exprEval.String(ctx, schedule.FullBackup.Recurrence)
			if err != nil {
				return nil, err
			}
			spec.fullBackupRecurrence = &rec
		}
	} else if !spec.isEnterpriseUser {
		if schedule.FullBackup == nil || schedule.FullBackup.AlwaysFull {
			// All backups are full cluster backups for free users.
			spec.fullBackupRecurrence = spec.recurrence
			spec.recurrence = nil
			if schedule.FullBackup == nil {
				p.BufferClientNotice(ctx,
					pgnotice.Newf("Without an enterprise license,"+
						" this schedule will only run full backups. To mute this notice, "+
						"add the 'FULL BACKUP ALWAYS' clause to your CREATE SCHEDULE command."))
			}
		} else {
			// Cannot use incremental backup w/out enterprise license.
			return nil, enterpriseCheckErr
		}
	}

	spec.scheduleOpts, err = exprEval.KVOptions(
		ctx, schedule.ScheduleOptions, scheduledBackupOptionExpectValues,
	)
	if err != nil {
		return nil, err
	}

	spec.destinations, err = exprEval.StringArray(ctx, tree.Exprs(schedule.To))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to evaluate backup destination paths")
	}
	if schedule.BackupOptions.EncryptionPassphrase != nil {
		passphrase, err := exprEval.String(
			ctx, schedule.BackupOptions.EncryptionPassphrase,
		)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to evaluate backup encryption_passphrase")
		}
		spec.encryptionPassphrase = &passphrase
	}

	if schedule.BackupOptions.EncryptionKMSURI != nil {
		spec.kmsURIs, err = exprEval.StringArray(
			ctx, tree.Exprs(schedule.BackupOptions.EncryptionKMSURI),
		)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to evaluate backup kms_uri")
		}
	}
	if schedule.BackupOptions.IncrementalStorage != nil {
		spec.incrementalStorage, err = exprEval.StringArray(
			ctx, tree.Exprs(schedule.BackupOptions.IncrementalStorage),
		)
		if err != nil {
			return nil, err
		}
	}

	if schedule.BackupOptions.IncludeAllSecondaryTenants != nil {
		includeSecondary, err := exprEval.Bool(ctx,
			schedule.BackupOptions.IncludeAllSecondaryTenants)
		if err != nil {
			return nil, err
		}
		spec.includeAllSecondaryTenants = &includeSecondary
	}

	return spec, nil
}

// scheduledBackupHeader is the header for "CREATE SCHEDULE..." statements results.
var scheduledBackupHeader = colinfo.ResultColumns{
	{Name: "schedule_id", Typ: types.Int},
	{Name: "label", Typ: types.String},
	{Name: "status", Typ: types.String},
	{Name: "first_run", Typ: types.TimestampTZ},
	{Name: "schedule", Typ: types.String},
	{Name: "backup_stmt", Typ: types.String},
}

func collectScheduledBackupTelemetry(
	ctx context.Context,
	incRecurrence *scheduleRecurrence,
	fullRecurrence *scheduleRecurrence,
	firstRun *time.Time,
	fullRecurrencePicked bool,
	ignoreExisting bool,
	details jobspb.ScheduleDetails,
	backupEvent eventpb.RecoveryEvent,
) {
	telemetry.Count("scheduled-backup.create.success")
	if incRecurrence != nil {
		telemetry.Count("scheduled-backup.incremental")
	}
	if firstRun != nil {
		telemetry.Count("scheduled-backup.first-run-picked")
	}
	if fullRecurrencePicked {
		telemetry.Count("scheduled-backup.full-recurrence-picked")
	}
	switch details.Wait {
	case jobspb.ScheduleDetails_WAIT:
		telemetry.Count("scheduled-backup.wait-policy.wait")
	case jobspb.ScheduleDetails_NO_WAIT:
		telemetry.Count("scheduled-backup.wait-policy.no-wait")
	case jobspb.ScheduleDetails_SKIP:
		telemetry.Count("scheduled-backup.wait-policy.skip")
	}
	switch details.OnError {
	case jobspb.ScheduleDetails_RETRY_SCHED:
		telemetry.Count("scheduled-backup.error-policy.retry-schedule")
	case jobspb.ScheduleDetails_RETRY_SOON:
		telemetry.Count("scheduled-backup.error-policy.retry-soon")
	case jobspb.ScheduleDetails_PAUSE_SCHED:
		telemetry.Count("scheduled-backup.error-policy.pause-schedule")
	}

	logCreateScheduleTelemetry(ctx, incRecurrence, fullRecurrence, firstRun, ignoreExisting, details, backupEvent)
}

func createBackupScheduleTypeCheck(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (matched bool, header colinfo.ResultColumns, _ error) {
	schedule, ok := stmt.(*tree.ScheduledBackup)
	if !ok {
		return false, nil, nil
	}
	stringExprs := exprutil.Strings{
		schedule.ScheduleLabelSpec.Label,
		schedule.Recurrence,
		schedule.BackupOptions.EncryptionPassphrase,
	}
	if schedule.FullBackup != nil {
		stringExprs = append(stringExprs, schedule.FullBackup.Recurrence)
	}
	opts := exprutil.KVOptions{
		KVOptions:  schedule.ScheduleOptions,
		Validation: scheduledBackupOptionExpectValues,
	}
	stringArrays := exprutil.StringArrays{
		tree.Exprs(schedule.To),
		tree.Exprs(schedule.BackupOptions.EncryptionKMSURI),
		tree.Exprs(schedule.BackupOptions.IncrementalStorage),
	}
	bools := exprutil.Bools{
		schedule.BackupOptions.IncludeAllSecondaryTenants,
	}
	if err := exprutil.TypeCheck(
		ctx, scheduleBackupOp, p.SemaCtx(), stringExprs, stringArrays, opts, bools,
	); err != nil {
		return false, nil, err
	}
	return true, scheduledBackupHeader, nil
}

func createBackupScheduleHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	schedule, ok := stmt.(*tree.ScheduledBackup)
	if !ok {
		return nil, nil, nil, false, nil
	}

	spec, err := makeScheduledBackupSpec(ctx, p, schedule)
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		err := doCreateBackupSchedules(ctx, p, spec, resultsCh)
		if err != nil {
			telemetry.Count("scheduled-backup.create.failed")
			return err
		}

		return nil
	}
	return fn, scheduledBackupHeader, nil, false, nil
}

func init() {
	sql.AddPlanHook(
		"schedule backup", createBackupScheduleHook, createBackupScheduleTypeCheck,
	)
}
