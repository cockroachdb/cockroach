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
	"encoding/json"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs/schedulebase"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
	pbtypes "github.com/gogo/protobuf/types"
)

const (
	optIgnoreExistingBackups   = "ignore_existing_backups"
	optUpdatesLastBackupMetric = "updates_cluster_last_backup_time_metric"
)

// scheduledBackupOptions augments common schedule options with backup specific ones.
type scheduledBackupOptions struct {
	schedulebase.CommonScheduleOptions
	ignoreExisting bool
	updateRPO      bool
}

// scheduledBackupGCProtectionEnabled is used to enable and disable the chaining
// of protected timestamps amongst scheduled backups.
var scheduledBackupGCProtectionEnabled = settings.RegisterBoolSetting(
	"schedules.backup.gc_protection.enabled",
	"enable chaining of GC protection across backups run as part of a schedule; default is false",
	false, /* defaultValue */
).WithPublic()

// scheduledBackupEval is a representation of tree.ScheduledBackup, prepared
// for evaluation
type scheduledBackupEval struct {
	*tree.ScheduledBackup

	isEnterpriseUser bool

	// Schedule specific properties that get evaluated.
	scheduleLabel        func() (string, error)
	recurrence           func() (string, error)
	fullBackupRecurrence func() (string, error)

	// scheduledBackupOptions struct initialized after setScheduledBackupOptions execution.
	scheduledBackupOptions
	setScheduledBackupOptions func(evalCtx *tree.EvalContext) error

	// Backup specific properties that get evaluated.
	// We need to evaluate anything in the tree.Backup node that allows
	// placeholders to be specified so that we store evaluated
	// backup statement in the schedule.
	destination          func() ([]string, error)
	encryptionPassphrase func() (string, error)
	kmsURIs              func() ([]string, error)
}

func computeScheduleRecurrence(
	now time.Time, evalFn func() (string, error),
) (schedulebase.ScheduleRecurrence, error) {
	if evalFn == nil {
		return schedulebase.ScheduleRecurrence{}, nil
	}
	return schedulebase.EvalScheduleRecurrence(now, evalFn)
}

func pickFullRecurrenceFromIncremental(
	inc schedulebase.ScheduleRecurrence,
) schedulebase.ScheduleRecurrence {
	if inc.Frequency <= time.Hour {
		// If incremental is faster than once an hour, take fulls every day,
		// some time between midnight and 1 am.
		return schedulebase.ScheduleRecurrence{
			Cron:      "@daily",
			Frequency: 24 * time.Hour,
		}
	}

	if inc.Frequency <= 24*time.Hour {
		// If incremental is less than a day, take full weekly;  some day
		// between 0 and 1 am.
		return schedulebase.ScheduleRecurrence{
			Cron:      "@weekly",
			Frequency: 7 * 24 * time.Hour,
		}
	}

	// Incremental period too large.
	return schedulebase.ScheduleRecurrence{}
}

const scheduleBackupOp = "CREATE SCHEDULE FOR BACKUP"

// canChainProtectedTimestampRecords returns true if the schedule is eligible to
// participate in the chaining of protected timestamp records between backup
// jobs running as part of the schedule.
// Currently, full cluster backups, tenant backups and table backups with
// revision history can enable chaining, as the spans to be protected remain
// constant across all backups in the chain. Unlike in database backups where a
// table could be created in between backups thereby widening the scope of what
// is to be protected.
func canChainProtectedTimestampRecords(p sql.PlanHookState, eval *scheduledBackupEval) bool {
	if !scheduledBackupGCProtectionEnabled.Get(&p.ExecCfg().Settings.SV) ||
		!eval.BackupOptions.CaptureRevisionHistory {
		return false
	}

	// Check if this is a full cluster backup.
	if eval.Coverage() == tree.AllDescriptors {
		return true
	}

	// Check if there are any wildcard table selectors in the specified table
	// targets. If we find a wildcard selector then we cannot chain PTS records
	// because of the reason outlined in the comment above the method.
	for _, t := range eval.Targets.Tables {
		pattern, err := t.NormalizeTablePattern()
		if err != nil {
			return false
		}
		if _, ok := pattern.(*tree.AllTablesSelector); ok {
			return false
		}
	}

	// Return true if the backup has table targets or is backing up a tenant.
	return eval.Targets.Tables != nil || eval.Targets.Tenant != roachpb.TenantID{}
}

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

	if eval.ScheduleLabelSpec.IfNotExists {
		scheduleLabel, err := eval.scheduleLabel()
		if err != nil {
			return err
		}

		exists, err := schedulebase.CheckScheduleAlreadyExists(
			ctx, env, p.ExecCfg().InternalExecutor, scheduleLabel, p.ExtendedEvalContext().Txn)
		if err != nil {
			return err
		}

		if exists {
			p.BufferClientNotice(ctx,
				pgnotice.Newf("schedule %q already exists, skipping", scheduleLabel),
			)
			return nil
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

	fullRecurrencePicked := false
	if !incRecurrence.IsZero() && fullRecurrence.IsZero() {
		// It's an enterprise user; let's see if we can pick a reasonable
		// full  backup recurrence based on requested incremental recurrence.
		fullRecurrence = pickFullRecurrenceFromIncremental(incRecurrence)
		fullRecurrencePicked = true

		if fullRecurrence.IsZero() {
			fullRecurrence = incRecurrence
			incRecurrence = schedulebase.ScheduleRecurrence{}
		}
	}

	if fullRecurrence.IsZero() {
		return errors.AssertionFailedf(" full backup recurrence should be set")
	}

	// Prepare backup statement (full).
	backupNode := &tree.Backup{
		Options: tree.BackupOptions{
			CaptureRevisionHistory:       eval.BackupOptions.CaptureRevisionHistory,
			Detached:                     true,
			IncludeDeprecatedInterleaves: eval.BackupOptions.IncludeDeprecatedInterleaves,
		},
		Nested:         true,
		AppendToLatest: false,
	}

	// Evaluate encryption passphrase if set.
	if eval.encryptionPassphrase != nil {
		pw, err := eval.encryptionPassphrase()
		if err != nil {
			return errors.Wrapf(err, "failed to evaluate backup encryption_passphrase")
		}
		backupNode.Options.EncryptionPassphrase = tree.NewStrVal(pw)
	}

	// Evaluate encryption KMS URIs if set.
	// Only one of encryption passphrase and KMS URI should be set, but this check
	// is done during backup planning so we do not need to worry about it here.
	var kmsURIs []string
	if eval.kmsURIs != nil {
		kmsURIs, err = eval.kmsURIs()
		if err != nil {
			return errors.Wrapf(err, "failed to evaluate backup kms_uri")
		}
		for _, kmsURI := range kmsURIs {
			backupNode.Options.EncryptionKMSURI = append(backupNode.Options.EncryptionKMSURI,
				tree.NewStrVal(kmsURI))
		}
	}

	// Evaluate required backup destinations.
	destinations, err := eval.destination()
	if err != nil {
		return errors.Wrapf(err, "failed to evaluate backup destination paths")
	}

	for _, dest := range destinations {
		backupNode.To = append(backupNode.To, tree.NewStrVal(dest))
	}

	backupNode.Targets = eval.Targets

	// Run full backup in dry-run mode.  This will do all of the sanity checks
	// and validation we need to make in order to ensure the schedule is sane.
	if err := dryRunBackup(ctx, p, backupNode); err != nil {
		return errors.Wrapf(err, "failed to dry run backup")
	}

	var scheduleLabel string
	if eval.scheduleLabel != nil {
		label, err := eval.scheduleLabel()
		if err != nil {
			return err
		}
		scheduleLabel = label
	} else {
		scheduleLabel = fmt.Sprintf("BACKUP %d", env.Now().Unix())
	}

	if err := eval.setScheduledBackupOptions(&p.ExtendedEvalContext().EvalContext); err != nil {
		return err
	}

	// Check if backups were already taken to this collection.
	if !eval.ignoreExisting {
		if err := checkForExistingBackupsInCollection(ctx, p, destinations); err != nil {
			return err
		}
	}

	if eval.updateRPO {
		// NB: as of 20.2, schedule creation requires admin so this is duplicative
		// but in the future we might relax so you can schedule anything that you
		// can backup, but then this cluster-wide metric should be admin-only.
		if err := p.RequireAdminRole(ctx, optUpdatesLastBackupMetric); err != nil {
			return err
		}
	}

	details := jobspb.ScheduleDetails{
		Wait:    eval.Wait,
		OnError: eval.OnError,
	}
	ex := p.ExecCfg().InternalExecutor

	unpauseOnSuccessID := jobs.InvalidScheduleID

	var chainProtectedTimestampRecords bool
	// If needed, create incremental.
	var inc *jobs.ScheduledJob
	var incScheduledBackupArgs *ScheduledBackupExecutionArgs
	if !incRecurrence.IsZero() {
		chainProtectedTimestampRecords = canChainProtectedTimestampRecords(p, eval)
		backupNode.AppendToLatest = true
		inc, incScheduledBackupArgs, err = makeBackupSchedule(
			env, p.User(), scheduleLabel, incRecurrence, details, unpauseOnSuccessID,
			eval.updateRPO, backupNode, chainProtectedTimestampRecords)
		if err != nil {
			return err
		}
		// Incremental is paused until FULL completes.
		inc.Pause()
		inc.SetScheduleStatus("Waiting for initial backup to complete")

		if err := inc.Create(ctx, ex, p.ExtendedEvalContext().Txn); err != nil {
			return err
		}
		if err := emitSchedule(inc, backupNode, destinations, nil /* incrementalFrom */, kmsURIs,
			resultsCh); err != nil {
			return err
		}
		unpauseOnSuccessID = inc.ScheduleID()
	}

	// Create FULL backup schedule.
	backupNode.AppendToLatest = false
	var fullScheduledBackupArgs *ScheduledBackupExecutionArgs
	full, fullScheduledBackupArgs, err := makeBackupSchedule(
		env, p.User(), scheduleLabel, fullRecurrence, details, unpauseOnSuccessID,
		eval.updateRPO, backupNode, chainProtectedTimestampRecords)
	if err != nil {
		return err
	}

	if !eval.FirstRun.IsZero() {
		full.SetNextRun(eval.FirstRun)
	} else if eval.isEnterpriseUser && fullRecurrencePicked {
		// The enterprise user did not indicate preference when to run full backups,
		// and we picked the schedule ourselves.
		// Run full backup immediately so that we do not wind up waiting for a long
		// time before the first full backup runs.  Without full backup, we can't
		// execute incremental.
		full.SetNextRun(env.Now())
	}

	// Create the schedule (we need its ID to link dependent schedules below).
	if err := full.Create(ctx, ex, p.ExtendedEvalContext().Txn); err != nil {
		return err
	}

	// If schedule creation has resulted in a full and incremental schedule then
	// we update both the schedules with the ID of the other "dependent" schedule.
	if !incRecurrence.IsZero() {
		if err := setDependentSchedule(ctx, ex, fullScheduledBackupArgs, full, inc.ScheduleID(),
			p.ExtendedEvalContext().Txn); err != nil {
			return errors.Wrap(err,
				"failed to update full schedule with dependent incremental schedule id")
		}
		if err := setDependentSchedule(ctx, ex, incScheduledBackupArgs, inc, full.ScheduleID(),
			p.ExtendedEvalContext().Txn); err != nil {
			return errors.Wrap(err,
				"failed to update incremental schedule with dependent full schedule id")
		}
	}

	collectScheduledBackupTelemetry(incRecurrence, eval.FirstRun, fullRecurrencePicked, details)
	return emitSchedule(full, backupNode, destinations, nil /* incrementalFrom */, kmsURIs,
		resultsCh)
}

func setDependentSchedule(
	ctx context.Context,
	ex *sql.InternalExecutor,
	scheduleExecutionArgs *ScheduledBackupExecutionArgs,
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
) (err error) {
	makeCloudFactory := p.ExecCfg().DistSQLSrv.ExternalStorageFromURI
	collectionURI, _, err := getURIsByLocalityKV(destinations, "")
	if err != nil {
		return err
	}
	defaultStore, err := makeCloudFactory(ctx, collectionURI, p.User())
	if err != nil {
		return err
	}
	defer func() {
		err = errors.CombineErrors(err, defaultStore.Close())
	}()

	r, err := defaultStore.ReadFile(ctx, latestFileName)
	if err == nil {
		// A full backup has already been taken to this location.
		return errors.CombineErrors(
			errors.Newf("backups already created in %s; to ignore existing backups, "+
				"the schedule can be created with the 'ignore_existing_backups' option",
				collectionURI),
			r.Close())
	}
	if !errors.Is(err, cloud.ErrFileDoesNotExist) {
		return errors.Newf("unexpected error occurred when checking for existing backups in %s",
			collectionURI)
	}

	return nil
}

func makeBackupSchedule(
	env scheduledjobs.JobSchedulerEnv,
	owner security.SQLUsername,
	label string,
	recurrence schedulebase.ScheduleRecurrence,
	details jobspb.ScheduleDetails,
	unpauseOnSuccess int64,
	updateLastMetricOnSuccess bool,
	backupNode *tree.Backup,
	chainProtectedTimestampRecords bool,
) (*jobs.ScheduledJob, *ScheduledBackupExecutionArgs, error) {
	sj := jobs.NewScheduledJob(env)
	sj.SetScheduleLabel(label)
	sj.SetOwner(owner)

	// Prepare arguments for scheduled backup execution.
	args := &ScheduledBackupExecutionArgs{
		UnpauseOnSuccess:               unpauseOnSuccess,
		UpdatesLastBackupMetric:        updateLastMetricOnSuccess,
		ChainProtectedTimestampRecords: chainProtectedTimestampRecords,
	}
	if backupNode.AppendToLatest {
		args.BackupType = ScheduledBackupExecutionArgs_INCREMENTAL
	} else {
		args.BackupType = ScheduledBackupExecutionArgs_FULL
	}

	if err := sj.SetSchedule(recurrence.Cron); err != nil {
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
	resultsCh chan<- tree.Datums,
) error {
	redactedBackupNode, err := GetRedactedBackupNode(backupNode, to, incrementalFrom, kmsURIs, "",
		false /* hasBeenPlanned */)
	if err != nil {
		return err
	}
	return schedulebase.EmitSchedule(sj, redactedBackupNode, resultsCh)
}

// dryRunBackup executes backup in dry-run mode: we simply execute backup
// under transaction savepoint, and then rollback to that save point.
func dryRunBackup(ctx context.Context, p sql.PlanHookState, backupNode *tree.Backup) error {
	sp, err := p.ExtendedEvalContext().Txn.CreateSavepoint(ctx)
	if err != nil {
		return err
	}
	err = dryRunInvokeBackup(ctx, p, backupNode)
	if rollbackErr := p.ExtendedEvalContext().Txn.RollbackToSavepoint(ctx, sp); rollbackErr != nil {
		return rollbackErr
	}
	return err
}

func dryRunInvokeBackup(ctx context.Context, p sql.PlanHookState, backupNode *tree.Backup) error {
	backupFn, err := planBackup(ctx, p, backupNode)
	if err != nil {
		return err
	}
	return invokeBackup(ctx, backupFn)
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
				var resolvedSchema bool
				if err := sql.DescsTxn(ctx, p.ExecCfg(), func(ctx context.Context, txn *kv.Txn,
					col *descs.Collection) error {
					dbDesc, err := col.GetImmutableDatabaseByName(ctx, txn, p.CurrentDatabase(),
						tree.DatabaseLookupFlags{Required: true})
					if err != nil {
						return err
					}
					resolvedSchema, _, err = catalogkv.ResolveSchemaID(ctx, txn, p.ExecCfg().Codec,
						dbDesc.GetID(), tp.SchemaName.String())
					return err
				}); err != nil {
					return nil, err
				}

				if resolvedSchema {
					tp.ExplicitCatalog = true
					tp.CatalogName = tree.Name(p.CurrentDatabase())
				}
			}
			fqTablePatterns[i] = tp
		}
	}
	return fqTablePatterns, nil
}

// makeScheduleBackupEval prepares helper scheduledBackupEval struct to assist in evaluation
// of various schedule and backup specific components.
func makeScheduledBackupEval(
	ctx context.Context, p sql.PlanHookState, schedule *tree.ScheduledBackup,
) (*scheduledBackupEval, error) {
	var err error
	if schedule.Targets != nil && schedule.Targets.Tables != nil {
		// Table backup targets must be fully qualified during scheduled backup
		// planning. This is because the actual execution of the backup job occurs
		// in a background, scheduled job session, that does not have the same
		// resolution configuration as during planning.
		schedule.Targets.Tables, err = fullyQualifyScheduledBackupTargetTables(ctx, p,
			schedule.Targets.Tables)
		if err != nil {
			return nil, errors.Wrap(err, "qualifying backup target tables")
		}
	}

	eval := &scheduledBackupEval{ScheduledBackup: schedule}

	if schedule.ScheduleLabelSpec.Label != nil {
		eval.scheduleLabel, err = p.TypeAsString(ctx, schedule.ScheduleLabelSpec.Label, scheduleBackupOp)
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

	eval.setScheduledBackupOptions, err = schedulebase.MakeScheduleOptionsEval(
		ctx, p, schedule.ScheduleOptions, &eval.scheduledBackupOptions)
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

	if schedule.BackupOptions.EncryptionKMSURI != nil {
		eval.kmsURIs, err = p.TypeAsStringArray(ctx, tree.Exprs(schedule.BackupOptions.EncryptionKMSURI),
			scheduleBackupOp)
		if err != nil {
			return nil, err
		}
	}
	return eval, nil
}

func collectScheduledBackupTelemetry(
	incRecurrence schedulebase.ScheduleRecurrence,
	firstRun time.Time,
	fullRecurrencePicked bool,
	details jobspb.ScheduleDetails,
) {
	telemetry.Count("scheduled-backup.create.success")
	if !incRecurrence.IsZero() {
		telemetry.Count("scheduled-backup.incremental")
	}
	if !firstRun.IsZero() {
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
}

func createBackupScheduleHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	schedule, ok := stmt.(*tree.ScheduledBackup)
	if !ok {
		return nil, nil, nil, false, nil
	}

	eval, err := makeScheduledBackupEval(ctx, p, schedule)
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		err := doCreateBackupSchedules(ctx, p, eval, resultsCh)
		if err != nil {
			telemetry.Count("scheduled-backup.create.failed")
			return err
		}

		return nil
	}
	return fn, schedulebase.CreateScheduleHeader, nil, false, nil
}

// MarshalJSONPB provides a custom Marshaller for jsonpb that redacts secrets in
// URI fields.
func (m ScheduledBackupExecutionArgs) MarshalJSONPB(x *jsonpb.Marshaler) ([]byte, error) {
	stmt, err := parser.ParseOne(m.BackupStatement)
	if err != nil {
		return nil, err
	}
	backup, ok := stmt.AST.(*tree.Backup)
	if !ok {
		return nil, errors.Errorf("unexpected %T statement in backup schedule: %v", backup, backup)
	}

	for i := range backup.To {
		raw, ok := backup.To[i].(*tree.StrVal)
		if !ok {
			return nil, errors.Errorf("unexpected %T arg in backup schedule: %v", raw, raw)
		}
		clean, err := cloud.SanitizeExternalStorageURI(raw.RawString(), nil /* extraParams */)
		if err != nil {
			return nil, err
		}
		backup.To[i] = tree.NewDString(clean)
	}

	// NB: this will never be non-nil with current schedule syntax but is here for
	// completeness.
	for i := range backup.IncrementalFrom {
		raw, ok := backup.IncrementalFrom[i].(*tree.StrVal)
		if !ok {
			return nil, errors.Errorf("unexpected %T arg in backup schedule: %v", raw, raw)
		}
		clean, err := cloud.SanitizeExternalStorageURI(raw.RawString(), nil /* extraParams */)
		if err != nil {
			return nil, err
		}
		backup.IncrementalFrom[i] = tree.NewDString(clean)
	}

	for i := range backup.Options.EncryptionKMSURI {
		raw, ok := backup.Options.EncryptionKMSURI[i].(*tree.StrVal)
		if !ok {
			return nil, errors.Errorf("unexpected %T arg in backup schedule: %v", raw, raw)
		}
		clean, err := cloud.RedactKMSURI(raw.RawString())
		if err != nil {
			return nil, err
		}
		backup.Options.EncryptionKMSURI[i] = tree.NewDString(clean)
	}

	if backup.Options.EncryptionPassphrase != nil {
		backup.Options.EncryptionPassphrase = tree.NewDString("redacted")
	}

	m.BackupStatement = backup.String()
	return json.Marshal(m)
}

// ExpectValues implements schedulebase.ScheduleOptions interface
func (o *scheduledBackupOptions) ExpectValues() map[string]schedulebase.OptionSetter {
	backupOptions := map[string]schedulebase.OptionSetter{
		optIgnoreExistingBackups: {
			Expect: sql.KVStringOptRequireNoValue,
			Set: func(evalCtx *tree.EvalContext, v string) error {
				o.ignoreExisting = true
				return nil
			},
		},
		optUpdatesLastBackupMetric: {
			Expect: sql.KVStringOptRequireNoValue,
			Set: func(evalCtx *tree.EvalContext, v string) error {
				o.updateRPO = true
				return nil
			},
		},
	}

	// Merge common options
	for k, v := range o.CommonScheduleOptions.ExpectValues() {
		backupOptions[k] = v
	}
	return backupOptions
}

// KVOptions implements schedulebase.ScheduleOptions interface
func (o *scheduledBackupOptions) KVOptions() (tree.KVOptions, error) {
	opts, err := o.CommonScheduleOptions.KVOptions()
	if err != nil {
		return nil, err
	}
	if o.ignoreExisting {
		opts = append(opts, tree.KVOption{Key: optIgnoreExistingBackups})
	}
	if o.updateRPO {
		opts = append(opts, tree.KVOption{Key: optUpdatesLastBackupMetric})
	}
	return opts, nil
}

func init() {
	sql.AddPlanHook(createBackupScheduleHook)
}
