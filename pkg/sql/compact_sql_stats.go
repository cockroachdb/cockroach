// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
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

	var (
		scheduledJobID jobspb.ScheduleID
		err            error
	)

	if err = p.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		scheduledJobID, err = r.getScheduleID(ctx, txn, scheduledjobs.ProdJobSchedulerEnv)
		if err != nil {
			return err
		}

		if scheduledJobID != jobspb.InvalidScheduleID {
			schedules := jobs.ScheduledJobTxn(txn)
			r.sj, err = schedules.Load(ctx, scheduledjobs.ProdJobSchedulerEnv, scheduledJobID)
			if err != nil {
				return err
			}
			r.sj.SetScheduleStatus(string(jobs.StateRunning))

			return schedules.Update(ctx, r.sj)
		}
		return nil
	}); err != nil {
		return err
	}

	statsCompactor := persistedsqlstats.NewStatsCompactor(
		r.st,
		p.ExecCfg().InternalDB,
		p.ExecCfg().InternalDB.server.ServerMetrics.StatsMetrics.SQLStatsRemovedRows,
		p.ExecCfg().SQLStatsTestingKnobs)
	if err = statsCompactor.DeleteOldestEntries(ctx); err != nil {
		return err
	}

	return r.maybeNotifyJobTerminated(
		ctx,
		p.ExecCfg().InternalDB,
		p.ExecCfg().JobsKnobs(),
		jobs.StateSucceeded)
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (r *sqlStatsCompactionResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, _ error,
) error {
	p := execCtx.(JobExecContext)
	execCfg := p.ExecCfg()
	return r.maybeNotifyJobTerminated(ctx, execCfg.InternalDB, execCfg.JobsKnobs(), jobs.StateFailed)
}

// CollectProfile implements the jobs.Resumer interface.
func (r *sqlStatsCompactionResumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

// maybeNotifyJobTerminated will notify the job termination
// (with termination status).
func (r *sqlStatsCompactionResumer) maybeNotifyJobTerminated(
	ctx context.Context, db isql.DB, jobKnobs *jobs.TestingKnobs, status jobs.State,
) error {
	log.Infof(ctx, "sql stats compaction job terminated with status = %s", status)
	if r.sj == nil {
		return nil
	}
	return db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		env := scheduledjobs.ProdJobSchedulerEnv
		if jobKnobs != nil && jobKnobs.JobSchedulerEnv != nil {
			env = jobKnobs.JobSchedulerEnv
		}
		return jobs.NotifyJobTermination(
			ctx, txn, env, r.job.ID(), status, r.job.Details(), r.sj.ScheduleID(),
		)
	})
}

func (r *sqlStatsCompactionResumer) getScheduleID(
	ctx context.Context, txn isql.Txn, env scheduledjobs.JobSchedulerEnv,
) (scheduleID jobspb.ScheduleID, _ error) {
	row, err := txn.QueryRowEx(ctx, "lookup-sql-stats-schedule", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf("SELECT created_by_id FROM %s WHERE id=$1 AND created_by_type=$2", env.SystemJobsTableName()),
		r.job.ID(), jobs.CreatedByScheduledJobs,
	)
	if err != nil {
		return jobspb.InvalidScheduleID, errors.Wrap(err, "fail to look up scheduled information")
	}

	if row == nil {
		// Compaction not triggered by a scheduled job.
		return jobspb.InvalidScheduleID, nil
	}

	scheduleID = jobspb.ScheduleID(tree.MustBeDInt(row[0]))
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
	txn isql.Txn,
	descsCol *descs.Collection,
) (int, error) {
	return 0, persistedsqlstats.ErrScheduleUndroppable
}

// ExecuteJob implements the jobs.ScheduledJobExecutor interface.
func (e *scheduledSQLStatsCompactionExecutor) ExecuteJob(
	ctx context.Context,
	txn isql.Txn,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
) error {
	if err := e.createSQLStatsCompactionJob(ctx, cfg, sj, txn); err != nil {
		e.metrics.NumFailed.Inc(1)
	}

	e.metrics.NumStarted.Inc(1)
	return nil
}

func (e *scheduledSQLStatsCompactionExecutor) createSQLStatsCompactionJob(
	ctx context.Context, cfg *scheduledjobs.JobExecutionConfig, sj *jobs.ScheduledJob, txn isql.Txn,
) error {
	p, cleanup := cfg.PlanHookMaker(ctx, "invoke-sql-stats-compact", txn.KV(), username.NodeUserName())
	defer cleanup()

	_, err :=
		persistedsqlstats.CreateCompactionJob(ctx, &jobs.CreatedByInfo{
			ID:   int64(sj.ScheduleID()),
			Name: jobs.CreatedByScheduledJobs,
		}, txn, p.(*planner).ExecCfg().JobRegistry)

	if err != nil {
		return err
	}

	return nil
}

// NotifyJobTermination implements the jobs.ScheduledJobExecutor interface.
func (e *scheduledSQLStatsCompactionExecutor) NotifyJobTermination(
	ctx context.Context,
	txn isql.Txn,
	jobID jobspb.JobID,
	jobStatus jobs.State,
	details jobspb.Details,
	env scheduledjobs.JobSchedulerEnv,
	sj *jobs.ScheduledJob,
) error {
	if jobStatus == jobs.StateFailed {
		jobs.DefaultHandleFailedRun(sj, "sql stats compaction %d failed", jobID)
		e.metrics.NumFailed.Inc(1)
		return nil
	}

	if jobStatus == jobs.StateSucceeded {
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
	ctx context.Context, txn isql.Txn, env scheduledjobs.JobSchedulerEnv, sj *jobs.ScheduledJob,
) (string, error) {
	return "SELECT crdb_internal.schedule_sql_stats_compact()", nil
}

func init() {
	// Do not include the cost of stats compaction in tenant accounting.
	jobs.RegisterConstructor(jobspb.TypeAutoSQLStatsCompaction, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &sqlStatsCompactionResumer{
			job: job,
			st:  settings,
		}
	}, jobs.DisablesTenantCostControl)

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
