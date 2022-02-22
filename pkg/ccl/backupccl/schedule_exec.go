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
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

type scheduledBackupExecutor struct {
	metrics backupMetrics
}

type backupMetrics struct {
	*jobs.ExecutorMetrics
	RpoMetric *metric.Gauge
}

var _ metric.Struct = &backupMetrics{}

// MetricStruct implements metric.Struct interface
func (m *backupMetrics) MetricStruct() {}

var _ jobs.ScheduledJobExecutor = &scheduledBackupExecutor{}

// ExecuteJob implements jobs.ScheduledJobExecutor interface.
func (e *scheduledBackupExecutor) ExecuteJob(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	if err := e.executeBackup(ctx, cfg, sj, txn); err != nil {
		e.metrics.NumFailed.Inc(1)
		return err
	}
	e.metrics.NumStarted.Inc(1)
	return nil
}

func (e *scheduledBackupExecutor) executeBackup(
	ctx context.Context, cfg *scheduledjobs.JobExecutionConfig, sj *jobs.ScheduledJob, txn *kv.Txn,
) error {
	backupStmt, err := extractBackupStatement(sj)
	if err != nil {
		return err
	}

	// Sanity check: backup should be detached.
	if !backupStmt.Options.Detached {
		backupStmt.Options.Detached = true
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

	if knobs, ok := cfg.TestingKnobs.(*jobs.TestingKnobs); ok {
		if knobs.OverrideAsOfClause != nil {
			knobs.OverrideAsOfClause(&backupStmt.AsOf)
		}
	}

	log.Infof(ctx, "Starting scheduled backup %d: %s",
		sj.ScheduleID(), tree.AsString(backupStmt))

	// Invoke backup plan hook.
	hook, cleanup := cfg.PlanHookMaker("exec-backup", txn, sj.Owner())
	defer cleanup()
	backupFn, err := planBackup(ctx, hook.(sql.PlanHookState), backupStmt)
	if err != nil {
		return err
	}
	return invokeBackup(ctx, backupFn)
}

func invokeBackup(ctx context.Context, backupFn sql.PlanHookRowFn) error {
	resultCh := make(chan tree.Datums) // No need to close
	g := ctxgroup.WithContext(ctx)

	g.GoCtx(func(ctx context.Context) error {
		select {
		case <-resultCh:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	g.GoCtx(func(ctx context.Context) error {
		return backupFn(ctx, nil, resultCh)
	})

	return g.Wait()
}

func planBackup(
	ctx context.Context, p sql.PlanHookState, backupStmt tree.Statement,
) (sql.PlanHookRowFn, error) {
	fn, cols, _, _, err := backupPlanHook(ctx, backupStmt, p)
	if err != nil {
		return nil, errors.Wrapf(err, "backup eval: %q", tree.AsString(backupStmt))
	}
	if fn == nil {
		return nil, errors.Newf("backup eval: %q", tree.AsString(backupStmt))
	}
	if len(cols) != len(jobs.DetachedJobExecutionResultHeader) {
		return nil, errors.Newf("unexpected result columns")
	}
	return fn, nil
}

// NotifyJobTermination implements jobs.ScheduledJobExecutor interface.
func (e *scheduledBackupExecutor) NotifyJobTermination(
	ctx context.Context,
	jobID jobspb.JobID,
	jobStatus jobs.Status,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	schedule *jobs.ScheduledJob,
	ex sqlutil.InternalExecutor,
	txn *kv.Txn,
) error {
	if jobStatus == jobs.StatusSucceeded {
		e.metrics.NumSucceeded.Inc(1)
		log.Infof(ctx, "backup job %d scheduled by %d succeeded", jobID, schedule.ScheduleID())
		return e.backupSucceeded(ctx, schedule, details, env, ex, txn)
	}

	e.metrics.NumFailed.Inc(1)
	err := errors.Errorf(
		"backup job %d scheduled by %d failed with status %s",
		jobID, schedule.ScheduleID(), jobStatus)
	log.Errorf(ctx, "backup error: %v	", err)
	jobs.DefaultHandleFailedRun(schedule, "backup job %d failed with err=%v", jobID, err)
	return nil
}

func (e *scheduledBackupExecutor) GetCreateScheduleStatement(
	ctx context.Context,
	env scheduledjobs.JobSchedulerEnv,
	txn *kv.Txn,
	sj *jobs.ScheduledJob,
	ex sqlutil.InternalExecutor,
) (string, error) {
	backupNode, err := extractBackupStatement(sj)
	if err != nil {
		return "", err
	}

	args := &ScheduledBackupExecutionArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return "", errors.Wrap(err, "un-marshaling args")
	}

	recurrence := sj.ScheduleExpr()
	fullBackup := &tree.FullBackupClause{AlwaysFull: true}

	// Check if sj has a dependent full or incremental schedule associated with it.
	var dependentSchedule *jobs.ScheduledJob
	if args.DependentScheduleID != 0 {
		dependentSchedule, err = jobs.LoadScheduledJob(ctx, env, args.DependentScheduleID, ex, txn)
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

	wait, err := parseOnPreviousRunningOption(sj.ScheduleDetails().Wait)
	if err != nil {
		return "", err
	}
	onError, err := parseOnErrorOption(sj.ScheduleDetails().OnError)
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
		nil, /* incrementalFrom */
		kmsURIs,
		"",
		nil,
		false /* hasBeenPlanned */)
	if err != nil {
		return "", err
	}

	node := &tree.ScheduledBackup{
		ScheduleLabelSpec: tree.ScheduleLabelSpec{
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
	schedule *jobs.ScheduledJob,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	ex sqlutil.InternalExecutor,
	txn *kv.Txn,
) error {
	args := &ScheduledBackupExecutionArgs{}
	if err := pbtypes.UnmarshalAny(schedule.ExecutionArgs().Args, args); err != nil {
		return errors.Wrap(err, "un-marshaling args")
	}

	// If this schedule is designated as maintaining the "LastBackup" metric used
	// for monitoring an RPO SLA, update that metric.
	if args.UpdatesLastBackupMetric {
		e.metrics.RpoMetric.Update(details.(jobspb.BackupDetails).EndTime.GoTime().Unix())
	}

	if args.UnpauseOnSuccess == jobs.InvalidScheduleID {
		return nil
	}

	s, err := jobs.LoadScheduledJob(ctx, env, args.UnpauseOnSuccess, ex, txn)
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
	if err := s.Update(ctx, ex, txn); err != nil {
		return err
	}

	// Clear UnpauseOnSuccess; caller updates schedule.
	args.UnpauseOnSuccess = jobs.InvalidScheduleID
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
	args := &ScheduledBackupExecutionArgs{}
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
				ID:   sj.ScheduleID(),
			},
		}, nil
	}

	return nil, errors.Newf("unexpect node type %T", node)
}

var _ jobs.ScheduledJobController = &scheduledBackupExecutor{}

func unlinkDependentSchedule(
	ctx context.Context,
	scheduleControllerEnv scheduledjobs.ScheduleControllerEnv,
	env scheduledjobs.JobSchedulerEnv,
	txn *kv.Txn,
	args *ScheduledBackupExecutionArgs,
) error {
	if args.DependentScheduleID == 0 {
		return nil
	}

	// Load the dependent schedule.
	dependentSj, dependentArgs, err := getScheduledBackupExecutionArgsFromSchedule(ctx, env, txn,
		scheduleControllerEnv.InternalExecutor().(*sql.InternalExecutor), args.DependentScheduleID)
	if err != nil {
		if jobs.HasScheduledJobNotFoundError(err) {
			log.Warningf(ctx, "failed to resolve dependent schedule %d", args.DependentScheduleID)
			return nil
		}
		return errors.Wrapf(err, "failed to resolve dependent schedule %d", args.DependentScheduleID)
	}

	// Clear the DependentID field since we are dropping the record associated
	// with it.
	dependentArgs.DependentScheduleID = 0
	any, err := pbtypes.MarshalAny(dependentArgs)
	if err != nil {
		return err
	}
	dependentSj.SetExecutionDetails(dependentSj.ExecutorType(), jobspb.ExecutionArguments{Args: any})
	return dependentSj.Update(ctx, scheduleControllerEnv.InternalExecutor(), txn)
}

// OnDrop implements the ScheduledJobController interface.
// The method is responsible for releasing the pts record stored on the schedule
// if schedules.backup.gc_protection.enabled = true.
// It is also responsible for unlinking the dependent schedule by clearing the
// DependentID.
func (e *scheduledBackupExecutor) OnDrop(
	ctx context.Context,
	scheduleControllerEnv scheduledjobs.ScheduleControllerEnv,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	args := &ScheduledBackupExecutionArgs{}
	if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, args); err != nil {
		return errors.Wrap(err, "un-marshaling args")
	}

	if err := unlinkDependentSchedule(ctx, scheduleControllerEnv, env, txn, args); err != nil {
		return errors.Wrap(err, "failed to unlink dependent schedule")
	}
	return releaseProtectedTimestamp(ctx, txn, scheduleControllerEnv.PTSProvider(),
		args.ProtectedTimestampRecord)
}

func init() {
	jobs.RegisterScheduledJobExecutorFactory(
		tree.ScheduledBackupExecutor.InternalName(),
		func() (jobs.ScheduledJobExecutor, error) {
			m := jobs.MakeExecutorMetrics(tree.ScheduledBackupExecutor.UserName())
			return &scheduledBackupExecutor{
				metrics: backupMetrics{
					ExecutorMetrics: &m,
					RpoMetric: metric.NewGauge(metric.Metadata{
						Name:        "schedules.BACKUP.last-completed-time",
						Help:        "The unix timestamp of the most recently completed backup by a schedule specified as maintaining this metric",
						Measurement: "Jobs",
						Unit:        metric.Unit_TIMESTAMP_SEC,
					}),
				},
			}, nil
		})
}
