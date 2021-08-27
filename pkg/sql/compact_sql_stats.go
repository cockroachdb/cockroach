// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
)

type sqlStatsCompactionResumer struct {
	job *jobs.Job
	st  *cluster.Settings
	sj  *jobs.ScheduledJob
}

var _ jobs.Resumer = &sqlStatsCompactionResumer{}

// Resume implements the jobs.Resumer interface.
func (r *sqlStatsCompactionResumer) Resume(ctx context.Context, execCtx interface{}) error {
	log.Infof(ctx, "starting sql stats compaction job")
	p := execCtx.(JobExecContext)
	ie := p.ExecCfg().InternalExecutor
	db := p.ExecCfg().DB

	var (
		scheduledJobID int64
		err            error
	)

	if err = db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		scheduledJobID, err = r.getScheduleID(ctx, ie, txn, scheduledjobs.ProdJobSchedulerEnv)
		if err != nil {
			return err
		}

		if scheduledJobID != jobs.InvalidScheduleID {
			r.sj, err = jobs.LoadScheduledJob(ctx, scheduledjobs.ProdJobSchedulerEnv, scheduledJobID, ie, txn)
			r.sj.SetScheduleStatus(string(jobs.StatusRunning))
			return r.sj.Update(ctx, ie, txn)
		}
		return nil
	}); err != nil {
		return err
	}

	// We check for concurrently running SQL Stats compaction jobs. We only allow
	// one job to be running at the same time.
	if err := persistedsqlstats.CheckExistingCompactionJob(ctx, r.job, ie, nil /* txn */); err != nil {
		if errors.Is(err, persistedsqlstats.ErrConcurrentSQLStatsCompaction) {
			log.Infof(ctx, "exiting due to a running sql stats compaction job")
		}
		return err
	}

	statsCompactor := persistedsqlstats.NewStatsCompactor(
		r.st,
		ie,
		db,
		ie.s.Metrics.StatsMetrics.SQLStatsRemovedRows,
		p.ExecCfg().SQLStatsTestingKnobs)
	if err = statsCompactor.DeleteOldestEntries(ctx); err != nil {
		return err
	}

	return r.maybeNotifyJobTerminated(
		ctx,
		ie,
		p.ExecCfg(),
		jobs.StatusSucceeded)
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (r *sqlStatsCompactionResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(JobExecContext)
	execCfg := p.ExecCfg()
	ie := execCfg.InternalExecutor
	return r.maybeNotifyJobTerminated(ctx, ie, execCfg, jobs.StatusFailed)
}

// maybeNotifyJobTerminated will notify the job termination
// (with termination status).
func (r *sqlStatsCompactionResumer) maybeNotifyJobTerminated(
	ctx context.Context, ie sqlutil.InternalExecutor, exec *ExecutorConfig, status jobs.Status,
) error {
	log.Infof(ctx, "sql stats compaction job terminated with status = %s", status)
	if r.sj != nil {
		env := scheduledjobs.ProdJobSchedulerEnv
		if knobs, ok := exec.DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs); ok {
			if knobs.JobSchedulerEnv != nil {
				env = knobs.JobSchedulerEnv
			}
		}
		if err := jobs.NotifyJobTermination(
			ctx, env, r.job.ID(), status, r.job.Details(), r.sj.ScheduleID(),
			ie, nil /* txn */); err != nil {
			return err
		}

		return nil
	}
	return nil
}

func (r *sqlStatsCompactionResumer) getScheduleID(
	ctx context.Context, ie sqlutil.InternalExecutor, txn *kv.Txn, env scheduledjobs.JobSchedulerEnv,
) (scheduleID int64, _ error) {
	row, err := ie.QueryRowEx(ctx, "lookup-sql-stats-schedule", txn,
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
		fmt.Sprintf("SELECT created_by_id FROM %s WHERE id=$1 AND created_by_type=$2", env.SystemJobsTableName()),
		r.job.ID(), jobs.CreatedByScheduledJobs,
	)
	if err != nil {
		return jobs.InvalidScheduleID, errors.Wrap(err, "fail to look up scheduled information")
	}

	if row == nil {
		// Compaction not triggered by a scheduled job.
		return jobs.InvalidScheduleID, nil
	}

	scheduleID = int64(tree.MustBeDInt(row[0]))
	return scheduleID, nil
}

type sqlStatsCompactionMetrics struct {
	*jobs.ExecutorMetrics
}

var _ metric.Struct = &sqlStatsCompactionMetrics{}

// MetricStruct implements metric.Struct interface.
func (m *sqlStatsCompactionMetrics) MetricStruct() {}

// scheduledSQLStatsCompactionExecutor is executed by scheduledjob subsystem
// to launch sqlStatsCompactionResumer through the job subsystem.
type scheduledSQLStatsCompactionExecutor struct {
	metrics sqlStatsCompactionMetrics
}

var _ jobs.ScheduledJobExecutor = &scheduledSQLStatsCompactionExecutor{}
var _ jobs.ScheduledJobController = &scheduledSQLStatsCompactionExecutor{}

// OnDrop implements the jobs.ScheduledJobController interface.
func (e *scheduledSQLStatsCompactionExecutor) OnDrop(
	ctx context.Context,
	scheduleControllerEnv scheduledjobs.ScheduleControllerEnv,
	env scheduledjobs.JobSchedulerEnv,
	schedule *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	return persistedsqlstats.ErrScheduleUndroppable
}

// ExecuteJob implements the jobs.ScheduledJobExecutor interface.
func (e *scheduledSQLStatsCompactionExecutor) ExecuteJob(
	ctx context.Context,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	txn *kv.Txn,
) error {
	if err := e.createSQLStatsCompactionJob(ctx, cfg, sj, txn); err != nil {
		e.metrics.NumFailed.Inc(1)
	}

	e.metrics.NumStarted.Inc(1)
	return nil
}

func (e *scheduledSQLStatsCompactionExecutor) createSQLStatsCompactionJob(
	ctx context.Context, cfg *scheduledjobs.JobExecutionConfig, sj *jobs.ScheduledJob, txn *kv.Txn,
) error {
	p, cleanup := cfg.PlanHookMaker("invoke-sql-stats-compact", txn, security.NodeUserName())
	defer cleanup()

	_, err :=
		persistedsqlstats.CreateCompactionJob(ctx, &jobs.CreatedByInfo{
			ID:   sj.ScheduleID(),
			Name: jobs.CreatedByScheduledJobs,
		}, txn, cfg.InternalExecutor, p.(*planner).ExecCfg().JobRegistry)

	if err != nil {
		return err
	}

	return nil
}

// NotifyJobTermination implements the jobs.ScheduledJobExecutor interface.
func (e *scheduledSQLStatsCompactionExecutor) NotifyJobTermination(
	ctx context.Context,
	jobID jobspb.JobID,
	jobStatus jobs.Status,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
	ex sqlutil.InternalExecutor,
	txn *kv.Txn,
) error {
	if jobStatus == jobs.StatusFailed {
		jobs.DefaultHandleFailedRun(sj, "sql stats compaction %d failed", jobID)
		e.metrics.NumFailed.Inc(1)
		return nil
	}

	if jobStatus == jobs.StatusSucceeded {
		e.metrics.NumSucceeded.Inc(1)
	}

	sj.SetScheduleStatus(string(jobStatus))

	return nil
}

// Metrics implements the jobs.ScheduledJobExecutor interface.
func (e *scheduledSQLStatsCompactionExecutor) Metrics() metric.Struct {
	return &e.metrics
}

// GetCreateScheduleStatement implements the jobs.ScheduledJobExecutor interface.
func (e *scheduledSQLStatsCompactionExecutor) GetCreateScheduleStatement(
	_ context.Context,
	_ scheduledjobs.JobSchedulerEnv,
	_ *kv.Txn,
	_ *jobs.ScheduledJob,
	_ sqlutil.InternalExecutor,
) (string, error) {
	return "SELECT crdb_internal.schedule_sql_stats_compact()", nil
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeSQLStatsCompaction, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &sqlStatsCompactionResumer{
			job: job,
			st:  settings,
		}
	})

	jobs.RegisterScheduledJobExecutorFactory(
		tree.ScheduledSQLStatsCompactionExecutor.InternalName(),
		func() (jobs.ScheduledJobExecutor, error) {
			m := jobs.MakeExecutorMetrics(tree.ScheduledSQLStatsCompactionExecutor.InternalName())
			return &scheduledSQLStatsCompactionExecutor{
				metrics: sqlStatsCompactionMetrics{
					ExecutorMetrics: &m,
				},
			}, nil
		})
}
