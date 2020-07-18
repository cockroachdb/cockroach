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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

const scheduledBackupExecutorName = "scheduled-backup-executor"

type scheduledBackupExecutor struct{}

var _ jobs.ScheduledJobExecutor = &scheduledBackupExecutor{}

// ExecuteJob implements jobs.ScheduledJobExecutor interface.
func (se *scheduledBackupExecutor) ExecuteJob(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	txn *kv.Txn,
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

	// Invoke backup plan hook.
	// TODO(yevgeniy): Invoke backup as the owner of the schedule.
	hook, cleanup := cfg.PlanHookMaker("exec-backup", txn, security.RootUser)
	defer cleanup()
	planBackup, cols, _, _, err := backupPlanHook(ctx, backupStmt, hook.(sql.PlanHookState))

	if err != nil {
		return errors.Wrapf(err, "backup eval: %q", tree.AsString(backupStmt))
	}
	if planBackup == nil {
		return errors.Newf("backup eval: %q", tree.AsString(backupStmt))
	}
	if len(cols) != len(utilccl.DetachedJobExecutionResultHeader) {
		return errors.Newf("unexpected result columns")
	}

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
		if err := planBackup(ctx, nil, resultCh); err != nil {
			return errors.Wrapf(err, "backup planning error: %q", tree.AsString(backupStmt))
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

// NotifyJobTermination implements jobs.ScheduledJobExecutor interface.
func (se *scheduledBackupExecutor) NotifyJobTermination(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	md *jobs.JobMetadata,
	sj *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	return errors.New("unimplemented yet")
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
		scheduledBackupExecutorName,
		func() (jobs.ScheduledJobExecutor, error) {
			return &scheduledBackupExecutor{}, nil
		})
}
