// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs/schedulebase"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

type scheduledBackupExecutor struct {
	metrics backupMetrics
}

type backupMetrics struct {
	*jobs.ExecutorMetrics
	*jobs.ExecutorPTSMetrics
	// TODO(rui): move this to the backup job so it can be controlled by the
	// updates_cluster_monitoring_metrics option.
	RpoMetric       *metric.Gauge
	RpoTenantMetric *metric.GaugeVec
}

var _ metric.Struct = &backupMetrics{}

// MetricStruct implements metric.Struct interface
func (m *backupMetrics) MetricStruct() {}

// PTSMetrics implements jobs.PTSMetrics interface.
func (m *backupMetrics) PTSMetrics() *jobs.ExecutorPTSMetrics {
	return m.ExecutorPTSMetrics
}

var _ jobs.ScheduledJobExecutor = &scheduledBackupExecutor{}

// ExecuteJob implements jobs.ScheduledJobExecutor interface.
func (e *scheduledBackupExecutor) ExecuteJob(
	ctx context.Context,
	txn isql.Txn,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	schedule *jobs.ScheduledJob,
) error {
	if err := e.executeBackup(ctx, cfg, schedule, txn); err != nil {
		e.metrics.NumFailed.Inc(1)
		return err
	}
	e.metrics.NumStarted.Inc(1)
	return nil
}

func (e *scheduledBackupExecutor) executeBackup(
	ctx context.Context, cfg *scheduledjobs.JobExecutionConfig, sj *jobs.ScheduledJob, txn isql.Txn,
) error {
	backupStmt, err := extractBackupStatement(sj)
	if err != nil {
		return err
	}

	// Sanity check: backup should be detached.
	if backupStmt.Options.Detached != tree.DBoolTrue {
		backupStmt.Options.Detached = tree.DBoolTrue
		log.Warningf(ctx, "force setting detached option for backup schedule %d",
			sj.ScheduleID())
	}

	// Sanity check: make sure the schedule is not paused so that
	// we don't set end time to 0 (this shouldn't happen since job scheduler
	// ignores paused schedules).
	if sj.IsPaused() {
		return errors.New("scheduled unexpectedly paused")
	}

	// Set endTime (AsOf) to be the time this schedule was supposed to have run.
	endTime, err := tree.MakeDTimestampTZ(sj.ScheduledRunTime(), time.Microsecond)
	if err != nil {
		return err
	}
	backupStmt.AsOf = tree.AsOfClause{Expr: endTime}

	// Invoke backup plan hook.
	hook, cleanup := cfg.PlanHookMaker(ctx, "exec-backup", txn.KV(), sj.Owner())
	defer cleanup()

	planner := hook.(sql.PlanHookState)
	currentClusterID := planner.ExtendedEvalContext().ClusterID
	currentDetails := sj.ScheduleDetails()

	// If the current cluster ID is different than the schedule's cluster ID,
	// pause the schedule. To maintain backward compatability with schedules
	// without a clusterID, don't pause schedules without a clusterID.
	if !currentDetails.ClusterID.Equal(uuid.Nil) && currentClusterID != currentDetails.ClusterID {
		log.Infof(ctx, "schedule %d last run by different cluster %s, pausing until manually resumed",
			sj.ScheduleID(),
			currentDetails.ClusterID)
		currentDetails.ClusterID = currentClusterID
		sj.SetScheduleDetails(*currentDetails)
		sj.Pause()
		return nil
	}

	log.Infof(ctx, "Starting scheduled backup %d", sj.ScheduleID())

	if knobs, ok := cfg.TestingKnobs.(*jobs.TestingKnobs); ok {
		if knobs.OverrideAsOfClause != nil {
			knobs.OverrideAsOfClause(&backupStmt.AsOf, hook.(sql.PlanHookState).ExtendedEvalContext().StmtTimestamp)
		}
	}

	backupFn, err := planBackup(ctx, planner, backupStmt)
	if err != nil {
		return err
	}
	_, err = invokeBackup(ctx, backupFn, nil, nil)
	return err
}

func invokeBackup(
	ctx context.Context, backupFn sql.PlanHookRowFn, registry *jobs.Registry, txn isql.Txn,
) (eventpb.RecoveryEvent, error) {
	resultCh := make(chan tree.Datums) // No need to close
	g := ctxgroup.WithContext(ctx)

	var backupEvent eventpb.RecoveryEvent
	g.GoCtx(func(ctx context.Context) error {
		select {
		case res := <-resultCh:
			backupEvent = getBackupFnTelemetry(ctx, registry, txn, res)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	g.GoCtx(func(ctx context.Context) error {
		return backupFn(ctx, resultCh)
	})

	err := g.Wait()
	return backupEvent, err
}

func planBackup(
	ctx context.Context, p sql.PlanHookState, backupStmt tree.Statement,
) (sql.PlanHookRowFn, error) {
	fn, cols, _, err := backupPlanHook(ctx, backupStmt, p)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to evaluate backup stmt")
	}
	if fn == nil {
		return nil, errors.Newf("failed to evaluate backup stmt")
	}
	if len(cols) != len(jobs.DetachedJobExecutionResultHeader) {
		return nil, errors.Newf("unexpected result columns")
	}
	return fn, nil
}

// NotifyJobTermination implements jobs.ScheduledJobExecutor interface.
func (e *scheduledBackupExecutor) NotifyJobTermination(
	ctx context.Context,
	txn isql.Txn,
	jobID jobspb.JobID,
	jobState jobs.State,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	schedule *jobs.ScheduledJob,
) error {
	if jobState == jobs.StateSucceeded {
		e.metrics.NumSucceeded.Inc(1)
		log.Infof(ctx, "backup job %d scheduled by %d succeeded", jobID, schedule.ScheduleID())
		return e.backupSucceeded(ctx, jobs.ScheduledJobTxn(txn), schedule, details, env)
	}

	e.metrics.NumFailed.Inc(1)
	err := errors.Errorf(
		"backup job %d scheduled by %d failed with state %s",
		jobID, schedule.ScheduleID(), jobState)
	log.Errorf(ctx, "backup error: %v	", err)
	jobs.DefaultHandleFailedRun(schedule, "backup job %d failed with err=%v", jobID, err)
	return nil
}

func (e *scheduledBackupExecutor) GetCreateScheduleStatement(
	ctx context.Context, txn isql.Txn, env scheduledjobs.JobSchedulerEnv, sj *jobs.ScheduledJob,
) (string, error) {
	backupNode, err := extractBackupStatement(sj)
	if err != nil {
		return "", err
	}

	args := &backuppb.ScheduledBackupExecutionArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return "", errors.Wrap(err, "un-marshaling args")
	}

	recurrence := sj.ScheduleExpr()
	fullBackup := &tree.FullBackupClause{AlwaysFull: true}

	// Check if sj has a dependent full or incremental schedule associated with it.
	var dependentSchedule *jobs.ScheduledJob
	if args.DependentScheduleID != 0 {
		dependentSchedule, err = jobs.ScheduledJobTxn(txn).
			Load(ctx, env, args.DependentScheduleID)
		if err != nil {
			return "", err
		}

		fullBackup.AlwaysFull = false
		// If sj refers to the incremental schedule, then the dependentSchedule
		// refers to the full schedule that sj was created as a child of. In this
		// case, we want to set the full backup recurrence to the dependent full
		// schedules recurrence.
		if backupNode.AppendToLatest {
			fullBackup.Recurrence = tree.NewDString(dependentSchedule.ScheduleExpr())
		} else {
			// If sj refers to the full schedule, then the dependentSchedule refers to
			// the incremental schedule that was created as a child of sj. In this
			// case, we want to set the incremental recurrence to the dependent
			// incremental schedules recurrence.
			fullBackup.Recurrence = tree.NewDString(recurrence)
			recurrence = dependentSchedule.ScheduleExpr()
		}
	} else {
		// If sj does not have a dependent schedule and is an incremental backup
		// schedule, this is only possible if the dependent full schedule has been
		// dropped.
		// In this case we set the recurrence to sj's ScheduleExpr() but we leave
		// the full backup recurrence empty so that it is decided by the scheduler.
		if backupNode.AppendToLatest {
			fullBackup.AlwaysFull = false
			fullBackup.Recurrence = nil
		}
	}

	// Pick first_run to be the sooner of the scheduled run time on sj and its
	// dependent schedule. If both are null then set it to now().
	//
	// If a user were to execute the `CREATE SCHEDULE` query returned by this
	// method, the statement would run a full backup at the time when the next
	// backup was supposed to run, as part of the old schedule.
	firstRunTime := sj.ScheduledRunTime()
	if dependentSchedule != nil {
		dependentScheduleFirstRun := dependentSchedule.ScheduledRunTime()
		if firstRunTime.IsZero() {
			firstRunTime = dependentScheduleFirstRun
		}
		if !dependentScheduleFirstRun.IsZero() && dependentScheduleFirstRun.Before(firstRunTime) {
			firstRunTime = dependentScheduleFirstRun
		}
	}
	if firstRunTime.IsZero() {
		firstRunTime = env.Now()
	}

	firstRun, err := tree.MakeDTimestampTZ(firstRunTime, time.Microsecond)
	if err != nil {
		return "", err
	}

	wait, err := schedulebase.ParseOnPreviousRunningOption(sj.ScheduleDetails().Wait)
	if err != nil {
		return "", err
	}
	onError, err := schedulebase.ParseOnErrorOption(sj.ScheduleDetails().OnError)
	if err != nil {
		return "", err
	}
	scheduleOptions := tree.KVOptions{
		tree.KVOption{
			Key:   optFirstRun,
			Value: firstRun,
		},
		tree.KVOption{
			Key:   optOnExecFailure,
			Value: tree.NewDString(onError),
		},
		tree.KVOption{
			Key:   optOnPreviousRunning,
			Value: tree.NewDString(wait),
		},
	}

	var destinations []string
	for i := range backupNode.To {
		dest, ok := backupNode.To[i].(*tree.StrVal)
		if !ok {
			return "", errors.Errorf("unexpected %T destination in backup statement", dest)
		}
		destinations = append(destinations, dest.RawString())
	}

	var kmsURIs []string
	for i := range backupNode.Options.EncryptionKMSURI {
		kmsURI, ok := backupNode.Options.EncryptionKMSURI[i].(*tree.StrVal)
		if !ok {
			return "", errors.Errorf("unexpected %T kmsURI in backup statement", kmsURI)
		}
		kmsURIs = append(kmsURIs, kmsURI.RawString())
	}

	redactedBackupNode, err := GetRedactedBackupNode(
		backupNode.Backup,
		destinations,
		kmsURIs,
		"",
		nil,
		false /* hasBeenPlanned */)
	if err != nil {
		return "", err
	}

	node := &tree.ScheduledBackup{
		ScheduleLabelSpec: tree.LabelSpec{
			IfNotExists: false, Label: tree.NewDString(sj.ScheduleLabel()),
		},
		Recurrence:      tree.NewDString(recurrence),
		FullBackup:      fullBackup,
		Targets:         redactedBackupNode.Targets,
		To:              redactedBackupNode.To,
		BackupOptions:   redactedBackupNode.Options,
		ScheduleOptions: scheduleOptions,
	}

	return tree.AsString(node), nil
}

func (e *scheduledBackupExecutor) backupSucceeded(
	ctx context.Context,
	txn jobs.ScheduledJobStorage,
	schedule *jobs.ScheduledJob,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
) error {
	args := &backuppb.ScheduledBackupExecutionArgs{}
	if err := pbtypes.UnmarshalAny(schedule.ExecutionArgs().Args, args); err != nil {
		return errors.Wrap(err, "un-marshaling args")
	}

	// If this schedule is designated as maintaining the "LastBackup" metric used
	// for monitoring an RPO SLA, update that metric.
	if args.UpdatesLastBackupMetric {
		e.metrics.RpoMetric.Update(details.(jobspb.BackupDetails).EndTime.GoTime().Unix())
		if details.(jobspb.BackupDetails).SpecificTenantIds != nil {
			for _, tenantID := range details.(jobspb.BackupDetails).SpecificTenantIds {
				e.metrics.RpoTenantMetric.Update(map[string]string{"tenant_id": tenantID.String()},
					details.(jobspb.BackupDetails).EndTime.GoTime().Unix())
			}
		}
	}

	if args.UnpauseOnSuccess == jobspb.InvalidScheduleID {
		return nil
	}

	s, err := txn.Load(ctx, env, args.UnpauseOnSuccess)
	if err != nil {
		if jobs.HasScheduledJobNotFoundError(err) {
			log.Warningf(ctx, "cannot find schedule %d to unpause; it may have been dropped",
				args.UnpauseOnSuccess)
			return nil
		}
		return err
	}
	s.ClearScheduleStatus()
	if s.HasRecurringSchedule() {
		if err := s.ScheduleNextRun(); err != nil {
			return err
		}
	}
	if err := txn.Update(ctx, s); err != nil {
		return err
	}

	// Clear UnpauseOnSuccess; caller updates schedule.
	args.UnpauseOnSuccess = jobspb.InvalidScheduleID
	any, err := pbtypes.MarshalAny(args)
	if err != nil {
		return errors.Wrap(err, "marshaling args")
	}
	schedule.SetExecutionDetails(
		schedule.ExecutorType(), jobspb.ExecutionArguments{Args: any},
	)

	return nil
}

// Metrics implements ScheduledJobExecutor interface
func (e *scheduledBackupExecutor) Metrics() metric.Struct {
	return &e.metrics
}

// extractBackupStatement returns tree.Backup node encoded inside scheduled job.
func extractBackupStatement(sj *jobs.ScheduledJob) (*annotatedBackupStatement, error) {
	args := &backuppb.ScheduledBackupExecutionArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return nil, errors.Wrap(err, "un-marshaling args")
	}

	node, err := parser.ParseOne(args.BackupStatement)
	if err != nil {
		return nil, errors.Wrap(err, "parsing backup statement")
	}

	if backupStmt, ok := node.AST.(*tree.Backup); ok {
		return &annotatedBackupStatement{
			Backup: backupStmt,
			CreatedByInfo: &jobs.CreatedByInfo{
				Name: jobs.CreatedByScheduledJobs,
				ID:   int64(sj.ScheduleID()),
			},
		}, nil
	}

	return nil, errors.Newf("unexpect node type %T", node)
}

var _ jobs.ScheduledJobController = &scheduledBackupExecutor{}

// unlinkOrDropDependentSchedule handles cases when the dependent schedule is either full or incremental.
//
// In the case of an incremental dependent schedule, the incremental schedule is dropped and the corresponding
// PTS is released. It returns the number of schedules dropped i.e. 1.
//
// In the case of a full dependent schedule, the full schedule is unlinked from the corresponding incremental
// schedule. It returns 0 due to no schedules being dropped within the function.
func unlinkOrDropDependentSchedule(
	ctx context.Context,
	scheduleControllerEnv scheduledjobs.ScheduleControllerEnv,
	env scheduledjobs.JobSchedulerEnv,
	txn isql.Txn,
	args *backuppb.ScheduledBackupExecutionArgs,
) (int, error) {
	if args.DependentScheduleID == 0 {
		return 0, nil
	}

	// Load the dependent schedule.
	dependentSj, dependentArgs, err := getScheduledBackupExecutionArgsFromSchedule(
		ctx, env, jobs.ScheduledJobTxn(txn), args.DependentScheduleID,
	)
	if err != nil {
		if jobs.HasScheduledJobNotFoundError(err) {
			log.Warningf(ctx, "failed to resolve dependent schedule %d", args.DependentScheduleID)
			return 0, nil
		}
		return 0, errors.Wrapf(err, "failed to resolve dependent schedule %d", args.DependentScheduleID)
	}

	if args.BackupType == backuppb.ScheduledBackupExecutionArgs_FULL &&
		dependentArgs.BackupType == backuppb.ScheduledBackupExecutionArgs_INCREMENTAL {
		any, err := pbtypes.MarshalAny(dependentArgs)
		if err != nil {
			return 0, err
		}
		dependentSj.SetExecutionDetails(dependentSj.ExecutorType(), jobspb.ExecutionArguments{Args: any})

		if err := jobs.ScheduledJobTxn(txn).Delete(ctx, dependentSj); err != nil {
			return 0, err
		}

		return 1, releaseProtectedTimestamp(ctx, scheduleControllerEnv.PTSProvider(), dependentArgs.ProtectedTimestampRecord)
	}

	// Clear the DependentID field since we are dropping the record associated
	// with it.
	dependentArgs.DependentScheduleID = 0
	dependentArgs.UnpauseOnSuccess = 0
	any, err := pbtypes.MarshalAny(dependentArgs)
	if err != nil {
		return 0, err
	}
	dependentSj.SetExecutionDetails(
		dependentSj.ExecutorType(), jobspb.ExecutionArguments{Args: any},
	)

	return 0, jobs.ScheduledJobTxn(txn).Update(ctx, dependentSj)
}

// OnDrop implements the ScheduledJobController interface.
//
// The method is responsible for releasing the pts record stored on the
// schedule. It is also responsible for unlinking the dependent schedule by
// clearing the DependentID.
func (e *scheduledBackupExecutor) OnDrop(
	ctx context.Context,
	scheduleControllerEnv scheduledjobs.ScheduleControllerEnv,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	txn isql.Txn,
	descsCol *descs.Collection,
) (int, error) {
	args := &backuppb.ScheduledBackupExecutionArgs{}

	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return 0, errors.Wrap(err, "un-marshaling args")
	}

	dependentRowsDropped, err := unlinkOrDropDependentSchedule(ctx, scheduleControllerEnv, env, txn, args)
	if err != nil {
		return dependentRowsDropped, errors.Wrap(err, "failed to unlink dependent schedule")
	}

	pts := scheduleControllerEnv.PTSProvider()
	return dependentRowsDropped, releaseProtectedTimestamp(ctx, pts, args.ProtectedTimestampRecord)
}

// getBackupFnTelemetry collects the telemetry from the dry-run backup
// corresponding to backupFnResult.
func getBackupFnTelemetry(
	ctx context.Context, registry *jobs.Registry, txn isql.Txn, backupFnResult tree.Datums,
) eventpb.RecoveryEvent {
	if registry == nil {
		return eventpb.RecoveryEvent{}
	}

	getInitialDetails := func() (jobspb.BackupDetails, error) {
		if len(backupFnResult) == 0 {
			return jobspb.BackupDetails{}, errors.New("unexpected empty result")
		}

		jobIDDatum := backupFnResult[0]
		if jobIDDatum == tree.DNull {
			return jobspb.BackupDetails{}, errors.New("expected job ID as first column of result")
		}

		jobID, ok := tree.AsDInt(jobIDDatum)
		if !ok {
			return jobspb.BackupDetails{}, errors.New("expected job ID as first column of result")
		}

		job, err := registry.LoadJobWithTxn(ctx, jobspb.JobID(jobID), txn)
		if err != nil {
			return jobspb.BackupDetails{}, errors.Wrap(err, "failed to load dry-run backup job")
		}

		backupDetails, ok := job.Details().(jobspb.BackupDetails)
		if !ok {
			return jobspb.BackupDetails{}, errors.Newf("unexpected job details type %T", job.Details())
		}
		return backupDetails, nil
	}

	initialDetails, err := getInitialDetails()
	if err != nil {
		log.Warningf(ctx, "error collecting telemetry from dry-run backup during schedule creation: %v", err)
		return eventpb.RecoveryEvent{}
	}
	return createBackupRecoveryEvent(ctx, initialDetails, 0)
}

func init() {
	jobs.RegisterScheduledJobExecutorFactory(
		tree.ScheduledBackupExecutor.InternalName(),
		func() (jobs.ScheduledJobExecutor, error) {
			m := jobs.MakeExecutorMetrics(tree.ScheduledBackupExecutor.UserName())
			pm := jobs.MakeExecutorPTSMetrics(tree.ScheduledBackupExecutor.UserName())
			return &scheduledBackupExecutor{
				metrics: backupMetrics{
					ExecutorMetrics:    &m,
					ExecutorPTSMetrics: &pm,
					RpoMetric: metric.NewGauge(metric.Metadata{
						Name:        "schedules.BACKUP.last-completed-time",
						Help:        "The unix timestamp of the most recently completed backup by a schedule specified as maintaining this metric",
						Measurement: "Jobs",
						Unit:        metric.Unit_TIMESTAMP_SEC,
					}),
					RpoTenantMetric: metric.NewExportedGaugeVec(metric.Metadata{
						Name:        "schedules.BACKUP.last-completed-time-by-virtual_cluster",
						Help:        "The unix timestamp of the most recently completed host scheduled backup by virtual cluster specified as maintaining this metric",
						Measurement: "Jobs",
						Unit:        metric.Unit_TIMESTAMP_SEC,
					}, []string{"tenant_id"}),
				},
			}, nil
		})
}
