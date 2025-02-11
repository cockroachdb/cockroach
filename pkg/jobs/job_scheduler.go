// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
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

var errScheduleNotRunnable = errors.New("schedule not runnable")

// loadCandidateScheduleForExecution looks up and locks candidate schedule for execution.
// The schedule is locked via FOR UPDATE clause to ensure that only this scheduler can modify it.
// If schedule cannot execute, a errScheduleNotRunnable error is returned.
func (s scheduledJobStorageTxn) loadCandidateScheduleForExecution(
	ctx context.Context, scheduleID int64, env scheduledjobs.JobSchedulerEnv,
) (*ScheduledJob, error) {
	lookupStmt := fmt.Sprintf(
		"SELECT * FROM %s WHERE schedule_id=%d AND next_run < %s FOR UPDATE",
		env.ScheduledJobsTableName(), scheduleID, env.NowExpr())
	row, cols, err := s.txn.QueryRowExWithCols(
		ctx, "find-scheduled-jobs-exec",
		s.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		lookupStmt)
	if err != nil {
		return nil, err
	}

	if row == nil {
		return nil, errScheduleNotRunnable
	}

	j := NewScheduledJob(env)
	if err := j.InitFromDatums(row, cols); err != nil {
		return nil, err
	}
	return j, nil
}

// lookupNumRunningJobs returns the number of running jobs for the specified schedule.
func lookupNumRunningJobs(
	ctx context.Context,
	scheduleID jobspb.ScheduleID,
	env scheduledjobs.JobSchedulerEnv,
	txn isql.Txn,
) (int64, error) {
	lookupStmt := fmt.Sprintf(
		"SELECT count(*) FROM %s WHERE created_by_type = '%s' AND created_by_id = %d AND status IN %s",
		env.SystemJobsTableName(), CreatedByScheduledJobs, scheduleID, NonTerminalStateTupleString)
	row, err := txn.QueryRowEx(
		ctx, "lookup-num-running",
		txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		lookupStmt)
	if err != nil {
		return 0, err
	}
	return int64(tree.MustBeDInt(row[0])), nil
}

const recheckRunningAfter = 1 * time.Minute

func (s *jobScheduler) processSchedule(
	ctx context.Context, schedule *ScheduledJob, numRunning int64, txn isql.Txn,
) error {
	scheduleStorage := ScheduledJobTxn(txn)
	if numRunning > 0 {
		switch schedule.ScheduleDetails().Wait {
		case jobspb.ScheduleDetails_WAIT:
			// TODO(yevgeniy): We might need to persist more state.
			// In particular, it'd be nice to add more time when repeatedly rescheduling
			// a job.  It would also be nice not to log each event.
			schedule.SetNextRun(s.env.Now().Add(recheckRunningAfter))
			schedule.SetScheduleStatusf("delayed due to %d already running", numRunning)
			s.metrics.RescheduleWait.Inc(1)
			return scheduleStorage.Update(ctx, schedule)
		case jobspb.ScheduleDetails_SKIP:
			if err := schedule.ScheduleNextRun(); err != nil {
				return err
			}
			schedule.SetScheduleStatusf("rescheduled due to %d already running", numRunning)
			s.metrics.RescheduleSkip.Inc(1)
			return scheduleStorage.Update(ctx, schedule)
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

	if err := scheduleStorage.Update(ctx, schedule); err != nil {
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
	if err := executor.ExecuteJob(execCtx, txn, s.JobExecutionConfig, s.env, schedule); err != nil {
		return errors.Wrapf(err, "executing schedule %d", schedule.ScheduleID())
	}

	s.metrics.NumStarted.Inc(1)

	// Persist any mutations to the underlying schedule.
	return scheduleStorage.Update(ctx, schedule)
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

	if errors.HasType(execErr, (*kvpb.TransactionRetryWithProtoRefreshError)(nil)) {
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

// executeCandidateSchedule attempts to execute schedule.
// The schedule is executed only if it's running.
func (s *jobScheduler) executeCandidateSchedule(
	ctx context.Context, candidate int64, txn isql.Txn,
) error {
	sj := scheduledJobStorageTxn{txn}
	schedule, err := sj.loadCandidateScheduleForExecution(
		ctx, candidate, s.env,
	)
	if err != nil {
		if errors.Is(err, errScheduleNotRunnable) {
			return nil
		}
		s.metrics.NumMalformedSchedules.Inc(1)
		log.Errorf(ctx, "error parsing schedule %d: %s", candidate, err)
		return err
	}

	if !s.env.IsExecutorEnabled(schedule.ExecutorType()) {
		log.Infof(ctx, "Ignoring schedule %d: %s executor disabled",
			schedule.ScheduleID(), schedule.ExecutorType())
		return nil
	}

	numRunning, err := lookupNumRunningJobs(
		ctx, schedule.ScheduleID(), s.env, txn,
	)
	if err != nil {
		return err
	}

	timeout := schedulerScheduleExecutionTimeout.Get(&s.Settings.SV)
	if processErr := withSavePoint(ctx, txn.KV(), func() error {
		if timeout > 0 {
			return timeutil.RunWithTimeout(
				ctx, redact.Sprintf("process-schedule-%d", schedule.ScheduleID()), timeout,
				func(ctx context.Context) error {
					return s.processSchedule(ctx, schedule, numRunning, txn)
				})
		}
		return s.processSchedule(ctx, schedule, numRunning, txn)
	}); processErr != nil {
		if errors.HasType(processErr, (*savePointError)(nil)) {
			return errors.Wrapf(processErr, "savepoint error for schedule %d", schedule.ScheduleID())
		}

		// Failed to process schedule.
		s.metrics.NumErrSchedules.Inc(1)
		log.Errorf(ctx,
			"error processing schedule %d: %+v", schedule.ScheduleID(), processErr)

		// Try updating schedule record to indicate schedule execution error.
		if err := withSavePoint(ctx, txn.KV(), func() error {
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
			return sj.Update(ctx, schedule)
		}); err != nil {
			if errors.HasType(err, (*savePointError)(nil)) {
				return errors.Wrapf(err,
					"savepoint error for schedule %d", schedule.ScheduleID())
			}
			log.Errorf(ctx, "error recording processing error for schedule %d: %+v",
				schedule.ScheduleID(), err)
		}
	}
	return nil
}

func (s *jobScheduler) executeSchedules(ctx context.Context, maxSchedules int64) (retErr error) {
	// Lookup a set of candidate schedules to execute.
	// NB: this lookup happens under nil txn; we ensure the schedule is still runnable
	// using per-schedule exclusive (for update) query.
	limitClause := ""
	if maxSchedules != allSchedules {
		limitClause = fmt.Sprintf("LIMIT %d", maxSchedules)
	}

	findSchedulesStmt := fmt.Sprintf(
		`SELECT schedule_id FROM %s WHERE next_run < %s ORDER BY random() %s`,
		s.env.ScheduledJobsTableName(), s.env.NowExpr(), limitClause)
	it, err := s.DB.Executor().QueryIteratorEx(
		ctx, "find-scheduled-jobs",
		/*txn=*/ nil,
		sessiondata.NodeUserSessionDataOverride,
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
		candidateID := int64(tree.MustBeDInt(row[0]))
		if err := s.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return s.executeCandidateSchedule(ctx, candidateID, txn)
		}); err != nil {
			log.Errorf(ctx, "error executing candidate schedule %d: %s", candidateID, err)
		}
	}

	return err
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
			defer sf.Unlock()
			if sf.CancelFunc != nil {
				sf.CancelFunc()
			}
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
			log.Warning(ctx, "scheduled job system disabled by setting, cancelling execution")
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
		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		initialDelay := getInitialScanDelay(s.TestingKnobs)
		log.Infof(ctx, "waiting %v before scheduled jobs daemon start", initialDelay)

		if err := RegisterExecutorsMetrics(s.registry); err != nil {
			log.Errorf(ctx, "error registering executor metrics: %+v", err)
		}

		whenDisabled := newCancelWhenDisabled(&s.Settings.SV)

		for timer := time.NewTimer(initialDelay); ; timer.Reset(
			getWaitPeriod(ctx, &s.Settings.SV, jitter, s.TestingKnobs)) {
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-timer.C:
				if !schedulerEnabledSetting.Get(&s.Settings.SV) {
					log.Warning(ctx, "scheduled job system disabled by setting")
					continue
				}

				maxSchedules := schedulerMaxJobsPerIterationSetting.Get(&s.Settings.SV)
				if err := whenDisabled.withCancelOnDisabled(ctx, &s.Settings.SV, func(ctx context.Context) error {
					return s.executeSchedules(ctx, maxSchedules)
				}); err != nil {
					log.Errorf(ctx, "error executing schedules: %v", err)
				}
			}
		}
	})
}

var schedulerEnabledSetting = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"jobs.scheduler.enabled",
	"enable/disable job scheduler",
	true,
)

var schedulerPaceSetting = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"jobs.scheduler.pace",
	"how often to scan system.scheduled_jobs table",
	time.Minute,
)

var schedulerMaxJobsPerIterationSetting = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"jobs.scheduler.max_jobs_per_iteration",
	"how many schedules to start per iteration; setting to 0 turns off this limit",
	5,
)

var schedulerScheduleExecutionTimeout = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"jobs.scheduler.schedule_execution.timeout",
	"sets a timeout on for schedule execution; 0 disables timeout",
	30*time.Second,
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
	ctx context.Context, sv *settings.Values, jitter jitterFn, knobs base.ModuleTestingKnobs,
) time.Duration {
	if k, ok := knobs.(*TestingKnobs); ok && k.SchedulerDaemonScanDelay != nil {
		return k.SchedulerDaemonScanDelay()
	}

	if !schedulerEnabledSetting.Get(sv) {
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
	// Since the job scheduler system is not under user control, exclude it from
	// from cost accounting and control. Individual jobs are not part of this
	// exclusion.
	ctx = multitenant.WithTenantCostControlExemption(ctx)

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
			func(ctx context.Context, maxSchedules int64) error {
				return scheduler.executeSchedules(ctx, maxSchedules)
			})
		return
	}

	if daemonKnobs != nil && daemonKnobs.CaptureJobScheduler != nil {
		daemonKnobs.CaptureJobScheduler(scheduler)
	}

	scheduler.runDaemon(ctx, stopper)
}
