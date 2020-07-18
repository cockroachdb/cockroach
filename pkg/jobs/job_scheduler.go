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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	env scheduledjobs.JobSchedulerEnv
}

func newJobScheduler(
	cfg *scheduledjobs.JobExecutionConfig, env scheduledjobs.JobSchedulerEnv,
) *jobScheduler {
	if env == nil {
		env = scheduledjobs.ProdJobSchedulerEnv
	}
	return &jobScheduler{
		JobExecutionConfig: cfg,
		env:                env,
	}
}

const allSchedules = 0

// getFindSchedulesStatement returns SQL statement used for finding
// scheduled jobs that should be started.
func getFindSchedulesStatement(env scheduledjobs.JobSchedulerEnv, maxSchedules int64) string {
	limitClause := ""
	if maxSchedules > 0 {
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
ORDER BY next_run
%s`, env.SystemJobsTableName(), CreatedByScheduledJobs,
		StatusSucceeded, StatusCanceled, StatusFailed,
		env.ScheduledJobsTableName(), env.NowExpr(), limitClause)
}

// unmarshalScheduledJob is a helper to deserialize a row returned by
// getFindSchedulesStatement() into a ScheduledJob
func (s *jobScheduler) unmarshalScheduledJob(
	row []tree.Datum, cols []sqlbase.ResultColumn,
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

func (s *jobScheduler) processSchedule(
	ctx context.Context, schedule *ScheduledJob, numRunning int64, txn *kv.Txn,
) error {
	if numRunning > 0 {
		switch schedule.ScheduleDetails().Wait {
		case jobspb.ScheduleDetails_WAIT:
			// TODO(yevgeniy): We might need to persist more state.
			// In particular, it'd be nice to add more time when repeatedly rescheduling
			// a job.  It would also be nice not to log each event.
			schedule.SetNextRun(s.env.Now().Add(recheckRunningAfter))
			schedule.AddScheduleChangeReason("reschedule: %d running", numRunning)
			return schedule.Update(ctx, s.InternalExecutor, txn)
		case jobspb.ScheduleDetails_SKIP:
			if err := schedule.ScheduleNextRun(); err != nil {
				return err
			}
			schedule.AddScheduleChangeReason("rescheduled: %d running", numRunning)
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

	executor, err := NewScheduledJobExecutor(schedule.ExecutorType())
	if err != nil {
		return err
	}

	// Grab job executor and execute the job.
	if err := executor.ExecuteJob(ctx, s.JobExecutionConfig, s.env, schedule, txn); err != nil {
		return errors.Wrapf(err, "executing schedule %d", schedule.ScheduleID())
	}

	// Persist any mutations to the underlying schedule.
	return schedule.Update(ctx, s.InternalExecutor, txn)
}

func (s *jobScheduler) executeSchedules(
	ctx context.Context, maxSchedules int64, txn *kv.Txn,
) error {
	findSchedulesStmt := getFindSchedulesStatement(s.env, maxSchedules)
	rows, cols, err := s.InternalExecutor.QueryWithCols(ctx, "find-scheduled-jobs", nil,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		findSchedulesStmt)

	if err != nil {
		return err
	}

	for _, row := range rows {
		// TODO(yevgeniy): Stopping entire loop because of one bad schedule is probably
		// not a great idea.  Improve handling of parsing/job execution errors.
		schedule, numRunning, err := s.unmarshalScheduledJob(row, cols)

		if err != nil {
			return err
		}

		if err := s.processSchedule(ctx, schedule, numRunning, txn); err != nil {
			return err
		}
	}
	return nil
}

func (s *jobScheduler) runDaemon(ctx context.Context, stopper *stop.Stopper) {
	stopper.RunWorker(ctx, func(ctx context.Context) {
		for timer := time.NewTimer(getInitialScanDelay(s.TestingKnobs)); ; timer.Reset(
			getWaitPeriod(&s.Settings.SV, s.TestingKnobs)) {
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-timer.C:
				if !schedulerEnabledSetting.Get(&s.Settings.SV) {
					continue
				}

				maxSchedules := schedulerMaxJobsPerIterationSetting.Get(&s.Settings.SV)
				err := s.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					return s.executeSchedules(ctx, maxSchedules, txn)
				})
				if err != nil {
					// TODO(yevgeniy): Add more visibility into failed daemon runs.
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

	scheduler := newJobScheduler(cfg, schedulerEnv)

	if daemonKnobs != nil && daemonKnobs.TakeOverJobsScheduling != nil {
		daemonKnobs.TakeOverJobsScheduling(
			func(ctx context.Context, maxSchedules int64, txn *kv.Txn) error {
				return scheduler.executeSchedules(ctx, maxSchedules, txn)
			})
		return
	}

	scheduler.runDaemon(ctx, stopper)
}
