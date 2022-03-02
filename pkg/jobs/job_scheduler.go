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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// CreatedByScheduledJobs identifies the job that was created
// by scheduled jobs system.
const CreatedByScheduledJobs = "crdb_schedule"

// jobScheduler is responsible for finding and starting scheduled
// jobs that need to be executed.
type jobScheduler struct {
	*scheduledjobs.JobExecutionConfig
	env      scheduledjobs.JobSchedulerEnv
	registry *metric.Registry
	metrics  SchedulerMetrics
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
      J.status NOT IN ('%s', '%s', '%s', '%s')
  ) AS num_running, S.*
FROM %s S
WHERE next_run < %s
ORDER BY random()
%s
FOR UPDATE`, env.SystemJobsTableName(), CreatedByScheduledJobs,
		StatusSucceeded, StatusCanceled, StatusFailed, StatusRevertFailed,
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
	malformed                               int64
}

func (s *loopStats) updateMetrics(m *SchedulerMetrics) {
	m.NumStarted.Update(s.started)
	m.ReadyToRun.Update(s.readyToRun)
	m.NumRunning.Update(s.jobsRunning)
	m.RescheduleSkip.Update(s.rescheduleSkip)
	m.RescheduleWait.Update(s.rescheduleWait)
	m.NumMalformedSchedules.Update(s.malformed)
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
			schedule.SetScheduleStatus("delayed due to %d already running", numRunning)
			stats.rescheduleWait++
			return schedule.Update(ctx, s.InternalExecutor, txn)
		case jobspb.ScheduleDetails_SKIP:
			if err := schedule.ScheduleNextRun(); err != nil {
				return err
			}
			schedule.SetScheduleStatus("rescheduled due to %d already running", numRunning)
			stats.rescheduleSkip++
			return schedule.Update(ctx, s.InternalExecutor, txn)
		}
	}

	schedule.ClearScheduleStatus()

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

	executor, err := GetScheduledJobExecutor(schedule.ExecutorType())
	if err != nil {
		return err
	}

	// Grab job executor and execute the job.
	log.Infof(ctx,
		"Starting job for schedule %d (%q); scheduled to run at %s; next run scheduled for %s",
		schedule.ScheduleID(), schedule.ScheduleLabel(),
		schedule.ScheduledRunTime(), schedule.NextRun())

	execCtx := logtags.AddTag(ctx, "schedule", schedule.ScheduleID())
	if err := executor.ExecuteJob(execCtx, s.JobExecutionConfig, s.env, schedule, txn); err != nil {
		return errors.Wrapf(err, "executing schedule %d", schedule.ScheduleID())
	}

	stats.started++

	// Persist any mutations to the underlying schedule.
	return schedule.Update(ctx, s.InternalExecutor, txn)
}

// TODO(yevgeniy): Re-evaluate if we need to have per-loop execution statistics.
func newLoopStats(
	ctx context.Context, env scheduledjobs.JobSchedulerEnv, ex sqlutil.InternalExecutor, txn *kv.Txn,
) (*loopStats, error) {
	numRunningJobsStmt := fmt.Sprintf(
		"SELECT count(*) FROM %s WHERE created_by_type = '%s' AND status NOT IN ('%s', '%s', '%s', '%s')",
		env.SystemJobsTableName(), CreatedByScheduledJobs,
		StatusSucceeded, StatusCanceled, StatusFailed, StatusRevertFailed)
	readyToRunStmt := fmt.Sprintf(
		"SELECT count(*) FROM %s WHERE next_run < %s",
		env.ScheduledJobsTableName(), env.NowExpr())
	statsStmt := fmt.Sprintf(
		"SELECT (%s) numReadySchedules, (%s) numRunningJobs",
		readyToRunStmt, numRunningJobsStmt)

	datums, err := ex.QueryRowEx(ctx, "scheduler-stats", txn,
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
		statsStmt)
	if err != nil {
		return nil, err
	}
	if datums == nil {
		return nil, errors.New("failed to read scheduler stats")
	}
	stats := &loopStats{}
	stats.readyToRun = int64(tree.MustBeDInt(datums[0]))
	stats.jobsRunning = int64(tree.MustBeDInt(datums[1]))
	return stats, nil
}

type savePointError struct {
	err error
}

func (e savePointError) Error() string {
	return e.err.Error()
}

// withSavePoint executes function fn() wrapped with savepoint.
// The savepoint is either released (upon successful completion of fn())
// or it is rolled back.
// If an error occurs while performing savepoint operations, an instance
// of savePointError is returned.  If fn() returns an error, then that
// error is returned.
func withSavePoint(ctx context.Context, txn *kv.Txn, fn func() error) error {
	sp, err := txn.CreateSavepoint(ctx)
	if err != nil {
		return &savePointError{err}
	}
	execErr := fn()

	if execErr == nil {
		if err := txn.ReleaseSavepoint(ctx, sp); err != nil {
			return &savePointError{err}
		}
		return nil
	}

	if errors.HasType(execErr, (*roachpb.TransactionRetryWithProtoRefreshError)(nil)) {
		// If function execution failed because transaction was restarted,
		// treat this error as a savePointError so that the execution code bails out
		// and retries scheduling loop.
		return &savePointError{execErr}
	}

	if err := txn.RollbackToSavepoint(ctx, sp); err != nil {
		return &savePointError{errors.WithDetail(err, execErr.Error())}
	}
	return execErr
}

func (s *jobScheduler) executeSchedules(
	ctx context.Context, maxSchedules int64, txn *kv.Txn,
) (retErr error) {
	stats, err := newLoopStats(ctx, s.env, s.InternalExecutor, txn)
	if err != nil {
		return err
	}

	defer stats.updateMetrics(&s.metrics)

	findSchedulesStmt := getFindSchedulesStatement(s.env, maxSchedules)
	it, err := s.InternalExecutor.QueryIteratorEx(
		ctx, "find-scheduled-jobs",
		txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		findSchedulesStmt)

	if err != nil {
		return err
	}

	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

	// The loop below might encounter an error after some schedules have been
	// executed (i.e. previous iterations succeeded), and this is ok.
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		schedule, numRunning, err := s.unmarshalScheduledJob(row, it.Types())
		if err != nil {
			stats.malformed++
			log.Errorf(ctx, "error parsing schedule: %+v", row)
			continue
		}

		if !s.env.IsExecutorEnabled(schedule.ExecutorType()) {
			log.Infof(ctx, "Ignoring schedule %d: %s executor disabled",
				schedule.ScheduleID(), schedule.ExecutorType())
			continue
		}

		if processErr := withSavePoint(ctx, txn, func() error {
			return s.processSchedule(ctx, schedule, numRunning, stats, txn)
		}); processErr != nil {
			if errors.HasType(processErr, (*savePointError)(nil)) {
				return errors.Wrapf(processErr, "savepoint error for schedule %d", schedule.ScheduleID())
			}

			// Failed to process schedule.
			s.metrics.NumErrSchedules.Inc(1)
			log.Errorf(ctx,
				"error processing schedule %d: %+v", schedule.ScheduleID(), processErr)

			// Try updating schedule record to indicate schedule execution error.
			if err := withSavePoint(ctx, txn, func() error {
				// Discard changes already made to the schedule, and treat schedule
				// execution failure the same way we treat job failure.
				schedule.ClearDirty()
				DefaultHandleFailedRun(schedule,
					"failed to create job for schedule %d: err=%s",
					schedule.ScheduleID(), processErr)

				// DefaultHandleFailedRun assumes schedule already had its next run set.
				// So, if the policy is to reschedule based on regular recurring schedule,
				// we need to set next run again..
				if schedule.HasRecurringSchedule() &&
					schedule.ScheduleDetails().OnError == jobspb.ScheduleDetails_RETRY_SCHED {
					if err := schedule.ScheduleNextRun(); err != nil {
						return err
					}
				}
				return schedule.Update(ctx, s.InternalExecutor, txn)
			}); err != nil {
				if errors.HasType(err, (*savePointError)(nil)) {
					return errors.Wrapf(err,
						"savepoint error for schedule %d", schedule.ScheduleID())
				}
				log.Errorf(ctx, "error recording processing error for schedule %d: %+v",
					schedule.ScheduleID(), err)
			}
		}
	}

	return err
}

// An internal, safety valve setting to revert scheduler execution to distributed mode.
// This setting should be removed once scheduled job system no longer locks tables for excessive
// periods of time.
var schedulerRunsOnSingleNode = settings.RegisterBoolSetting(
	settings.TenantReadOnly,
	"jobs.scheduler.single_node_scheduler.enabled",
	"execute scheduler on a single node in a cluster",
	false,
)

func (s *jobScheduler) schedulerEnabledOnThisNode(ctx context.Context) bool {
	if s.ShouldRunScheduler == nil || !schedulerRunsOnSingleNode.Get(&s.Settings.SV) {
		return true
	}

	enabled, err := s.ShouldRunScheduler(ctx, s.DB.Clock().NowAsClockTimestamp())
	if err != nil {
		log.Errorf(ctx, "error determining if the scheduler enabled: %v; will recheck after %s",
			err, recheckEnabledAfterPeriod)
		return false
	}
	return enabled
}

type syncCancelFunc struct {
	syncutil.Mutex
	context.CancelFunc
}

// newCancelWhenDisabled arranges for scheduler enabled setting callback to cancel
// currently executing context.
func newCancelWhenDisabled(sv *settings.Values) *syncCancelFunc {
	sf := &syncCancelFunc{}
	schedulerEnabledSetting.SetOnChange(sv, func(ctx context.Context) {
		if !schedulerEnabledSetting.Get(sv) {
			sf.Lock()
			if sf.CancelFunc != nil {
				sf.CancelFunc()
			}
			sf.Unlock()
		}
	})
	return sf
}

// withCancelOnDisabled executes provided function with the context which will be cancelled
// if scheduler is disabled.
func (sf *syncCancelFunc) withCancelOnDisabled(
	ctx context.Context, sv *settings.Values, f func(ctx context.Context) error,
) error {
	ctx, cancel := func() (context.Context, context.CancelFunc) {
		sf.Lock()
		defer sf.Unlock()

		ctx, cancel := context.WithCancel(ctx)
		sf.CancelFunc = cancel

		if !schedulerEnabledSetting.Get(sv) {
			cancel()
		}

		return ctx, func() {
			sf.Lock()
			defer sf.Unlock()
			cancel()
			sf.CancelFunc = nil
		}
	}()
	defer cancel()
	return f(ctx)
}

func (s *jobScheduler) runDaemon(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "job-scheduler", func(ctx context.Context) {
		initialDelay := getInitialScanDelay(s.TestingKnobs)
		log.Infof(ctx, "waiting %v before scheduled jobs daemon start", initialDelay)

		if err := RegisterExecutorsMetrics(s.registry); err != nil {
			log.Errorf(ctx, "error registering executor metrics: %+v", err)
		}

		whenDisabled := newCancelWhenDisabled(&s.Settings.SV)

		for timer := time.NewTimer(initialDelay); ; timer.Reset(
			getWaitPeriod(ctx, &s.Settings.SV, s.schedulerEnabledOnThisNode, jitter, s.TestingKnobs)) {
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-timer.C:
				if !schedulerEnabledSetting.Get(&s.Settings.SV) || !s.schedulerEnabledOnThisNode(ctx) {
					continue
				}

				maxSchedules := schedulerMaxJobsPerIterationSetting.Get(&s.Settings.SV)
				if err := whenDisabled.withCancelOnDisabled(ctx, &s.Settings.SV, func(ctx context.Context) error {
					return s.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
						return s.executeSchedules(ctx, maxSchedules, txn)
					})
				}); err != nil {
					log.Errorf(ctx, "error executing schedules: %+v", err)
				}

			}
		}
	})
}

var schedulerEnabledSetting = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"jobs.scheduler.enabled",
	"enable/disable job scheduler",
	true,
)

var schedulerPaceSetting = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"jobs.scheduler.pace",
	"how often to scan system.scheduled_jobs table",
	time.Minute,
)

var schedulerMaxJobsPerIterationSetting = settings.RegisterIntSetting(
	settings.TenantWritable,
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

type jitterFn func(duration time.Duration) time.Duration

// Returns duration to wait before scanning system.scheduled_jobs.
func getWaitPeriod(
	ctx context.Context,
	sv *settings.Values,
	enabledOnThisNode func(ctx context.Context) bool,
	jitter jitterFn,
	knobs base.ModuleTestingKnobs,
) time.Duration {
	if k, ok := knobs.(*TestingKnobs); ok && k.SchedulerDaemonScanDelay != nil {
		return k.SchedulerDaemonScanDelay()
	}

	if !schedulerEnabledSetting.Get(sv) {
		return recheckEnabledAfterPeriod
	}

	if enabledOnThisNode != nil && !enabledOnThisNode(ctx) {
		return recheckEnabledAfterPeriod
	}

	pace := schedulerPaceSetting.Get(sv)
	if pace < minPacePeriod {
		if warnIfPaceTooLow.ShouldLog() {
			log.Warningf(ctx,
				"job.scheduler.pace setting too low (%s < %s)", pace, minPacePeriod)
		}
		pace = minPacePeriod
	}

	return jitter(pace)
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

	if daemonKnobs != nil && daemonKnobs.CaptureJobScheduler != nil {
		daemonKnobs.CaptureJobScheduler(scheduler)
	}

	scheduler.runDaemon(ctx, stopper)
}
