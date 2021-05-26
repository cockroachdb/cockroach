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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/gorhill/cronexpr"
)

const (
	optFirstRun                = "first_run"
	optOnExecFailure           = "on_execution_failure"
	optOnPreviousRunning       = "on_previous_running"
	optIgnoreExistingBackups   = "ignore_existing_backups"
	optUpdatesLastBackupMetric = "updates_cluster_last_backup_time_metric"
)

var scheduledBackupOptionExpectValues = map[string]sql.KVStringOptValidate{
	optFirstRun:                sql.KVStringOptRequireValue,
	optOnExecFailure:           sql.KVStringOptRequireValue,
	optOnPreviousRunning:       sql.KVStringOptRequireValue,
	optIgnoreExistingBackups:   sql.KVStringOptRequireNoValue,
	optUpdatesLastBackupMetric: sql.KVStringOptRequireNoValue,
}

// scheduledBackupEval is a representation of tree.ScheduledBackup, prepared
// for evaluation
type scheduledBackupEval struct {
	*tree.ScheduledBackup

	isEnterpriseUser bool

	// Schedule specific properties that get evaluated.
	scheduleLabel        func() (string, error)
	recurrence           func() (string, error)
	fullBackupRecurrence func() (string, error)
	scheduleOpts         func() (map[string]string, error)

	// Backup specific properties that get evaluated.
	// We need to evaluate anything in the tree.Backup node that allows
	// placeholders to be specified so that we store evaluated
	// backup statement in the schedule.
	destination          func() ([]string, error)
	encryptionPassphrase func() (string, error)
	kmsURIs              func() ([]string, error)
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

func scheduleFirstRun(evalCtx *tree.EvalContext, opts map[string]string) (*time.Time, error) {
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
		return nil, errors.Newf(
			`error parsing schedule expression: %q; it must be a valid cron expression`,
			cron)
	}
	nextRun := expr.Next(now)
	frequency := expr.Next(nextRun).Sub(nextRun)
	return &scheduleRecurrence{cron, frequency}, nil
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
		backupNode.Options.EncryptionPassphrase = tree.NewDString(pw)
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
				tree.NewDString(kmsURI))
		}
	}

	// Evaluate required backup destinations.
	destinations, err := eval.destination()
	if err != nil {
		return errors.Wrapf(err, "failed to evaluate backup destination paths")
	}

	for _, dest := range destinations {
		backupNode.To = append(backupNode.To, tree.NewDString(dest))
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

	scheduleOptions, err := eval.scheduleOpts()
	if err != nil {
		return err
	}

	// Check if backups were already taken to this collection.
	if _, ignoreExisting := scheduleOptions[optIgnoreExistingBackups]; !ignoreExisting {
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

	evalCtx := &p.ExtendedEvalContext().EvalContext
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

	// If needed, create incremental.
	if incRecurrence != nil {
		backupNode.AppendToLatest = true
		inc, err := makeBackupSchedule(
			env, p.User(), scheduleLabel,
			incRecurrence, details, unpauseOnSuccessID, updateMetricOnSuccess, backupNode)

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
	full, err := makeBackupSchedule(
		env, p.User(), scheduleLabel,
		fullRecurrence, details, unpauseOnSuccessID, updateMetricOnSuccess, backupNode)
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

	// Create the schedule (we need its ID to create incremental below).
	if err := full.Create(ctx, ex, p.ExtendedEvalContext().Txn); err != nil {
		return err
	}
	collectScheduledBackupTelemetry(incRecurrence, firstRun, fullRecurrencePicked, details)
	return emitSchedule(full, backupNode, destinations, nil /* incrementalFrom */, kmsURIs,
		resultsCh)
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
	collectionURI, _, err := getURIsByLocalityKV(destinations, "")
	if err != nil {
		return err
	}
	defaultStore, err := makeCloudFactory(ctx, collectionURI, p.User())
	if err != nil {
		return err
	}
	defer defaultStore.Close()

	r, err := defaultStore.ReadFile(ctx, latestFileName)
	if err == nil {
		// A full backup has already been taken to this location.
		r.Close()
		return errors.Newf("backups already created in %s; to ignore existing backups, "+
			"the schedule can be created with the 'ignore_existing_backups' option",
			collectionURI)
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
	recurrence *scheduleRecurrence,
	details jobspb.ScheduleDetails,
	unpauseOnSuccess int64,
	updateLastMetricOnSuccess bool,
	backupNode *tree.Backup,
) (*jobs.ScheduledJob, error) {
	sj := jobs.NewScheduledJob(env)
	sj.SetScheduleLabel(label)
	sj.SetOwner(owner)

	// Prepare arguments for scheduled backup execution.
	args := &ScheduledBackupExecutionArgs{
		UnpauseOnSuccess:        unpauseOnSuccess,
		UpdatesLastBackupMetric: updateLastMetricOnSuccess,
	}
	if backupNode.AppendToLatest {
		args.BackupType = ScheduledBackupExecutionArgs_INCREMENTAL
	} else {
		args.BackupType = ScheduledBackupExecutionArgs_FULL
	}

	if err := sj.SetSchedule(recurrence.cron); err != nil {
		return nil, err
	}

	sj.SetScheduleDetails(details)

	// We do not set backupNode.AsOf: this is done when the scheduler kicks off the backup.
	// Serialize backup statement and set schedule executor and its args.
	//
	// TODO(bulkio): this serialization is erroneous, see issue
	// https://github.com/cockroachdb/cockroach/issues/63216
	args.BackupStatement = tree.AsStringWithFlags(backupNode, tree.FmtSimple|tree.FmtShowPasswords)
	any, err := pbtypes.MarshalAny(args)
	if err != nil {
		return nil, err
	}
	sj.SetExecutionDetails(
		tree.ScheduledBackupExecutor.InternalName(),
		jobspb.ExecutionArguments{Args: any},
	)

	return sj, nil
}

func emitSchedule(
	sj *jobs.ScheduledJob,
	backupNode *tree.Backup,
	to, incrementalFrom, kmsURIs []string,
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
		false /* hasBeenPlanned */)
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

// makeScheduleBackupEval prepares helper scheduledBackupEval struct to assist in evaluation
// of various schedule and backup specific components.
func makeScheduledBackupEval(
	ctx context.Context, p sql.PlanHookState, schedule *tree.ScheduledBackup,
) (*scheduledBackupEval, error) {
	eval := &scheduledBackupEval{ScheduledBackup: schedule}
	var err error

	if schedule.ScheduleLabel != nil {
		eval.scheduleLabel, err = p.TypeAsString(ctx, schedule.ScheduleLabel, scheduleBackupOp)
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

	if schedule.BackupOptions.EncryptionKMSURI != nil {
		eval.kmsURIs, err = p.TypeAsStringArray(ctx, tree.Exprs(schedule.BackupOptions.EncryptionKMSURI),
			scheduleBackupOp)
		if err != nil {
			return nil, err
		}
	}
	return eval, nil
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
	incRecurrence *scheduleRecurrence,
	firstRun *time.Time,
	fullRecurrencePicked bool,
	details jobspb.ScheduleDetails,
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
	return fn, scheduledBackupHeader, nil, false, nil
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

func init() {
	sql.AddPlanHook(createBackupScheduleHook)
}
