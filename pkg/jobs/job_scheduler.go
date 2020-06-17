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

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// jobSchedulerEnv is an environment for running scheduled jobs.
// This environment facilitates dependency injection mechanism for tests.
type jobSchedulerEnv interface {
	// ScheduledJobsTableName returns the name of the scheduled_jobs table.
	ScheduledJobsTableName() string
	// SystemJobsTableName returns the name of the system jobs table.
	SystemJobsTableName() string
	// Now returns current time.
	Now() time.Time
	// NowExpr returns expression representing current time when
	// used in the database queries.
	NowExpr() string
}

// production jobSchedulerEnv implementation.
type prodJobSchedulerEnvImpl struct{}

// ProdJobSchedulerEnv is a jobSchedulerEnv implementation suitable for production.
var ProdJobSchedulerEnv jobSchedulerEnv = &prodJobSchedulerEnvImpl{}

const createdByName = "crdb_schedule"

func (e *prodJobSchedulerEnvImpl) ScheduledJobsTableName() string {
	return "system.scheduled_jobs"
}

func (e *prodJobSchedulerEnvImpl) SystemJobsTableName() string {
	return "system.jobs"
}

func (e *prodJobSchedulerEnvImpl) Now() time.Time {
	return timeutil.Now()
}

func (e *prodJobSchedulerEnvImpl) NowExpr() string {
	return "current_timestamp()"
}

// jobScheduler is responsible for finding and starting scheduled
// jobs that need to be executed.
type jobScheduler struct {
	env jobSchedulerEnv
	ex  sqlutil.InternalExecutor
}

func newJobScheduler(env jobSchedulerEnv, ex sqlutil.InternalExecutor) *jobScheduler {
	if env == nil {
		env = ProdJobSchedulerEnv
	}
	return &jobScheduler{
		env: env,
		ex:  ex,
	}
}

const allSchedules = 0

// getFindSchedulesStatement returns SQL statement used for finding
// scheduled jobs that should be started.
func getFindSchedulesStatement(env jobSchedulerEnv, maxSchedules int64) string {
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
      J.status NOT IN ('failed', 'succeeded', 'cancelled')
  ) AS num_running, S.*
FROM %s S
WHERE next_run < %s
ORDER BY next_run
%s
`, env.SystemJobsTableName(), createdByName, env.ScheduledJobsTableName(), env.NowExpr(), limitClause)
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
			return schedule.Update(ctx, s.ex, txn)
		case jobspb.ScheduleDetails_SKIP:
			if err := schedule.ScheduleNextRun(); err != nil {
				return err
			}
			schedule.AddScheduleChangeReason("rescheduled: %d running", numRunning)
			return schedule.Update(ctx, s.ex, txn)
		}
	}

	// Schedule the next job run.
	// We do this step early, before the actual execution, to grab a lock on
	// the scheduled_jobs table.
	if schedule.HasRecurringSchedule() {
		if err := schedule.ScheduleNextRun(); err != nil {
			return err
		}
	} else {
		// It's a one-off schedule.  Clear next run to indicate that this schedule executed.
		schedule.SetNextRun(time.Time{})
	}

	if err := schedule.Update(ctx, s.ex, txn); err != nil {
		return err
	}

	executor, err := NewScheduledJobExecutor(schedule.ExecutorType(), s.ex)
	if err != nil {
		return err
	}

	// Grab job executor and execute the job.
	if err := executor.ExecuteJob(ctx, schedule, txn); err != nil {
		return err
	}

	// Persist any mutations to the underlying schedule.
	return schedule.Update(ctx, s.ex, txn)
}

func (s *jobScheduler) executeSchedules(
	ctx context.Context, maxSchedules int64, txn *kv.Txn,
) error {
	findSchedulesStmt := getFindSchedulesStatement(s.env, maxSchedules)
	rows, cols, err := s.ex.QueryWithCols(ctx, "find-scheduled-jobs", nil,
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
// Package visible for testing.
var getInitialScanDelay = func() time.Duration {
	// By default, we'll wait between 2 and 5 minutes before performing initial scan.
	return time.Minute * time.Duration(2+rand.Intn(3))
}

// Fastest pace for the scheduler.
const minPacePeriod = 10 * time.Second

// Frequency to recheck if the daemon is enabled.
const recheckEnabledAfterPeriod = 5 * time.Minute

// Returns duration to wait before scanning scheduled_jobs.
// Package visible for testing.
var warnIfPaceTooLow = log.Every(time.Minute)
var getWaitPeriod = func(sv *settings.Values) time.Duration {
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
	sv *settings.Values,
	env jobSchedulerEnv,
	db *kv.DB,
	ex sqlutil.InternalExecutor,
) {
	scheduler := newJobScheduler(env, ex)

	stopper.RunWorker(ctx, func(ctx context.Context) {
		for waitPeriod := getInitialScanDelay(); ; waitPeriod = getWaitPeriod(sv) {
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-time.After(waitPeriod):
				if !schedulerEnabledSetting.Get(sv) {
					continue
				}

				maxSchedules := schedulerMaxJobsPerIterationSetting.Get(sv)
				err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					return scheduler.executeSchedules(ctx, maxSchedules, txn)
				})
				if err != nil {
					// TODO(yevgeniy): Add more visibility into failed daemon runs.
					log.Errorf(ctx, "error executing schedules: %+v", err)
				}
			}
		}
	})
}
