// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// CreatedByScheduledJobs identifies the job that was created
// by scheduled jobs system.
const CreatedByScheduledJobs = "crdb_schedule"

// jobScheduler is responsible for finding and starting scheduled
// jobs that need to be executed.
type jobScheduler struct {
	*scheduledjobs.JobExecutionConfig
	env       scheduledjobs.JobSchedulerEnv
	registry  *metric.Registry
	metrics   SchedulerMetrics
	executors map[string]ScheduledJobExecutor
}

func newJobScheduler(
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
	registry *metric.Registry,
) *jobScheduler {
	if env == nil {
		env = scheduledjobs.ProdJobSchedulerEnv
	}

	stats := MakeSchedulerMetrics()
	registry.AddMetricStruct(stats)

	return &jobScheduler{
		JobExecutionConfig: cfg,
		env:                env,
		registry:           registry,
		metrics:            stats,
		executors:          make(map[string]ScheduledJobExecutor),
	}
}

const allSchedules = 0

// getFindSchedulesStatement returns SQL statement used for finding
// scheduled jobs that should be started.
func getFindSchedulesStatement(env scheduledjobs.JobSchedulerEnv, maxSchedules int64) string {
	limitClause := ""
	if maxSchedules != allSchedules {
		limitClause = fmt.Sprintf("LIMIT %d", maxSchedules)
	}

	return fmt.Sprintf(
		`
SELECT
  (SELECT count(*)
   FROM %s J
   WHERE
      J.created_by_type = '%s' AND J.created_by_id = S.schedule_id AND
      J.status NOT IN ('%s', '%s', '%s')
  ) AS num_running, S.*
FROM %s S
WHERE next_run < %s
ORDER BY random()
%s`, env.SystemJobsTableName(), CreatedByScheduledJobs,
		StatusSucceeded, StatusCanceled, StatusFailed,
		env.ScheduledJobsTableName(), env.NowExpr(), limitClause)
}

// unmarshalScheduledJob is a helper to deserialize a row returned by
// getFindSchedulesStatement() into a ScheduledJob
func (s *jobScheduler) unmarshalScheduledJob(
	row []tree.Datum, cols []colinfo.ResultColumn,
) (*ScheduledJob, int64, error) {
	j := NewScheduledJob(s.env)
	if err := j.InitFromDatums(row[1:], cols[1:]); err != nil {
		return nil, 0, err
	}

	if n, ok := row[0].(*tree.DInt); ok {
		return j, int64(*n), nil
	}

	return nil, 0, errors.Newf("expected int found %T instead", row[0])
}

const recheckRunningAfter = 1 * time.Minute

type loopStats struct {
	rescheduleWait, rescheduleSkip, started int64
	readyToRun, jobsRunning                 int64
}

func (s *loopStats) updateMetrics(m *SchedulerMetrics) {
	m.NumStarted.Update(s.started)
	m.ReadyToRun.Update(s.readyToRun)
	m.NumRunning.Update(s.jobsRunning)
	m.RescheduleSkip.Update(s.rescheduleSkip)
	m.RescheduleWait.Update(s.rescheduleWait)
}

func (s *jobScheduler) processSchedule(
	ctx context.Context, schedule *ScheduledJob, numRunning int64, stats *loopStats, txn *kv.Txn,
) error {
	if numRunning > 0 {
		switch schedule.ScheduleDetails().Wait {
		case jobspb.ScheduleDetails_WAIT:
			// TODO(yevgeniy): We might need to persist more state.
			// In particular, it'd be nice to add more time when repeatedly rescheduling
			// a job.  It would also be nice not to log each event.
			schedule.SetNextRun(s.env.Now().Add(recheckRunningAfter))
			schedule.AddScheduleChangeReason("reschedule: %d running", numRunning)
			stats.rescheduleWait++
			return schedule.Update(ctx, s.InternalExecutor, txn)
		case jobspb.ScheduleDetails_SKIP:
			if err := schedule.ScheduleNextRun(); err != nil {
				return err
			}
			schedule.AddScheduleChangeReason("rescheduled: %d running", numRunning)
			stats.rescheduleSkip++
			return schedule.Update(ctx, s.InternalExecutor, txn)
		}
	}

	// Schedule the next job run.
	// We do this step early, before the actual execution, to grab a lock on
	// the scheduledjobs table.
	if schedule.HasRecurringSchedule() {
		if err := schedule.ScheduleNextRun(); err != nil {
			return err
		}
	} else {
		// It's a one-off schedule.  Clear next run to indicate that this schedule executed.
		schedule.SetNextRun(time.Time{})
	}

	if err := schedule.Update(ctx, s.InternalExecutor, txn); err != nil {
		return err
	}

	executor, err := s.lookupExecutor(schedule.ExecutorType())
	if err != nil {
		return err
	}

	// Grab job executor and execute the job.
	log.Infof(ctx,
		"Starting job for schedule %d (%s); next run scheduled for %s",
		schedule.ScheduleID(), schedule.ScheduleLabel(), schedule.NextRun())

	if err := executor.ExecuteJob(ctx, s.JobExecutionConfig, s.env, schedule, txn); err != nil {
		return errors.Wrapf(err, "executing schedule %d", schedule.ScheduleID())
	}

	stats.started++

	// Persist any mutations to the underlying schedule.
	return schedule.Update(ctx, s.InternalExecutor, txn)
}

func (s *jobScheduler) lookupExecutor(name string) (ScheduledJobExecutor, error) {
	if ex, ok := s.executors[name]; ok {
		return ex, nil
	}
	ex, err := NewScheduledJobExecutor(name)
	if err != nil {
		return nil, err
	}
	if m := ex.Metrics(); m != nil {
		s.registry.AddMetricStruct(m)
	}
	return ex, nil
}

// TODO(yevgeniy): Re-evaluate if we need to have per-loop execution statistics.
func newLoopStats(
	ctx context.Context, env scheduledjobs.JobSchedulerEnv, ex sqlutil.InternalExecutor,
) (*loopStats, error) {
	numRunningJobsStmt := fmt.Sprintf(
		"SELECT count(*) FROM %s WHERE created_by_type = '%s' AND status NOT IN ('%s', '%s', '%s')",
		env.SystemJobsTableName(), CreatedByScheduledJobs,
		StatusSucceeded, StatusCanceled, StatusFailed)
	readyToRunStmt := fmt.Sprintf(
		"SELECT count(*) FROM %s WHERE next_run < %s",
		env.ScheduledJobsTableName(), env.NowExpr())
	statsStmt := fmt.Sprintf(
		"SELECT (%s) numReadySchedules, (%s) numRunningJobs",
		readyToRunStmt, numRunningJobsStmt)

	datums, err := ex.QueryRowEx(ctx, "scheduler-stats", nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		statsStmt)
	if err != nil {
		return nil, err
	}
	stats := &loopStats{}
	stats.readyToRun = int64(tree.MustBeDInt(datums[0]))
	stats.jobsRunning = int64(tree.MustBeDInt(datums[1]))
	return stats, nil
}

func (s *jobScheduler) executeSchedules(
	ctx context.Context, maxSchedules int64, txn *kv.Txn,
) error {
	stats, err := newLoopStats(ctx, s.env, s.InternalExecutor)
	if err != nil {
		return err
	}

	defer stats.updateMetrics(&s.metrics)

	findSchedulesStmt := getFindSchedulesStatement(s.env, maxSchedules)
	rows, cols, err := s.InternalExecutor.QueryWithCols(
		ctx, "find-scheduled-jobs",
		txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.NodeUser},
		findSchedulesStmt)

	if err != nil {
		return err
	}

	for _, row := range rows {
		schedule, numRunning, err := s.unmarshalScheduledJob(row, cols)
		if err != nil {
			s.metrics.NumBadSchedules.Inc(1)
			log.Errorf(ctx, "error parsing schedule: %+v", row)
			continue
		}

		sp, err := txn.CreateSavepoint(ctx)
		if err != nil {
			return err
		}

		if err := s.processSchedule(ctx, schedule, numRunning, stats, txn); err != nil {
			log.Errorf(ctx, "error processing schedule %d: %+v", schedule.ScheduleID(), err)

			if err := txn.RollbackToSavepoint(ctx, sp); err != nil {
				return errors.Wrapf(err, "failed to rollback savepoint for schedule %d", schedule.ScheduleID())
			}
		}

		if err := txn.ReleaseSavepoint(ctx, sp); err != nil {
			return err
		}
	}
	return nil
}

func (s *jobScheduler) runDaemon(ctx context.Context, stopper *stop.Stopper) {
	stopper.RunWorker(ctx, func(ctx context.Context) {
		initialDelay := getInitialScanDelay(s.TestingKnobs)
		log.Infof(ctx, "waiting %s before scheduled jobs daemon start", initialDelay.String())

		for timer := time.NewTimer(initialDelay); ; timer.Reset(
			getWaitPeriod(&s.Settings.SV, s.TestingKnobs)) {
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-timer.C:
				if !schedulerEnabledSetting.Get(&s.Settings.SV) {
					log.Info(ctx, "scheduled job daemon disabled")
					continue
				}

				maxSchedules := schedulerMaxJobsPerIterationSetting.Get(&s.Settings.SV)
				err := s.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					return s.executeSchedules(ctx, maxSchedules, txn)
				})
				if err != nil {
					s.metrics.NumBadSchedules.Inc(1)
					log.Errorf(ctx, "error executing schedules: %+v", err)
				}

			}
		}
	})
}

var schedulerEnabledSetting = settings.RegisterBoolSetting(
	"jobs.scheduler.enabled",
	"enable/disable job scheduler",
	true,
)

var schedulerPaceSetting = settings.RegisterDurationSetting(
	"jobs.scheduler.pace",
	"how often to scan system.scheduled_jobs table",
	time.Minute,
)

var schedulerMaxJobsPerIterationSetting = settings.RegisterIntSetting(
	"jobs.scheduler.max_jobs_per_iteration",
	"how many schedules to start per iteration; setting to 0 turns off this limit",
	10,
)

// Returns the amount of time to wait before starting initial scan.
func getInitialScanDelay(knobs base.ModuleTestingKnobs) time.Duration {
	if k, ok := knobs.(*TestingKnobs); ok && k.SchedulerDaemonInitialScanDelay != nil {
		return k.SchedulerDaemonInitialScanDelay()
	}

	// By default, we'll wait between 2 and 5 minutes before performing initial scan.
	return time.Minute * time.Duration(2+rand.Intn(3))
}

// Fastest pace for the scheduler.
const minPacePeriod = 10 * time.Second

// Frequency to recheck if the daemon is enabled.
const recheckEnabledAfterPeriod = 5 * time.Minute

var warnIfPaceTooLow = log.Every(time.Minute)

// Returns duration to wait before scanning system.scheduled_jobs.
func getWaitPeriod(sv *settings.Values, knobs base.ModuleTestingKnobs) time.Duration {
	if k, ok := knobs.(*TestingKnobs); ok && k.SchedulerDaemonScanDelay != nil {
		return k.SchedulerDaemonScanDelay()
	}

	if !schedulerEnabledSetting.Get(sv) {
		return recheckEnabledAfterPeriod
	}

	pace := schedulerPaceSetting.Get(sv)
	if pace < minPacePeriod {
		if warnIfPaceTooLow.ShouldLog() {
			log.Warningf(context.Background(),
				"job.scheduler.pace setting too low (%s < %s)", pace, minPacePeriod)
		}
		pace = minPacePeriod
	}

	return pace
}

// StartJobSchedulerDaemon starts a daemon responsible for periodically scanning
// system.scheduled_jobs table to find and executing eligible scheduled jobs.
func StartJobSchedulerDaemon(
	ctx context.Context,
	stopper *stop.Stopper,
	registry *metric.Registry,
	cfg *scheduledjobs.JobExecutionConfig,
	env scheduledjobs.JobSchedulerEnv,
) {
	schedulerEnv := env
	var daemonKnobs *TestingKnobs
	if jobsKnobs, ok := cfg.TestingKnobs.(*TestingKnobs); ok {
		daemonKnobs = jobsKnobs
	}

	if daemonKnobs != nil && daemonKnobs.CaptureJobExecutionConfig != nil {
		daemonKnobs.CaptureJobExecutionConfig(cfg)
	}
	if daemonKnobs != nil && daemonKnobs.JobSchedulerEnv != nil {
		schedulerEnv = daemonKnobs.JobSchedulerEnv
	}

	scheduler := newJobScheduler(cfg, schedulerEnv, registry)

	if daemonKnobs != nil && daemonKnobs.TakeOverJobsScheduling != nil {
		daemonKnobs.TakeOverJobsScheduling(
			func(ctx context.Context, maxSchedules int64, txn *kv.Txn) error {
				return scheduler.executeSchedules(ctx, maxSchedules, txn)
			})
		return
	}

	scheduler.runDaemon(ctx, stopper)
}
