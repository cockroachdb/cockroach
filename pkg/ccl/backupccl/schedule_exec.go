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

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
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
	if len(cols) != len(utilccl.DetachedJobExecutionResultHeader) {
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
		schedule.ExecutorType(),
		jobspb.ExecutionArguments{Args: any},
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
