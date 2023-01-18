// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package persistedsqlstats

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// We don't need this monitor to run very frequent. Normally, the schedule
// should remain in the system table once it is created. However, some operations
// such as RESTORE would wipe the system table and populate it with the data
// from BACKUP. In this case, it would be nice for us to preemptively check
// for the abnormal state of the schedule and restore it.
var defaultScanInterval = time.Hour * 6

var (
	errScheduleNotFound = errors.New("sql stats compaction schedule not found")

	// ErrScheduleIntervalTooLong is returned when monitor detects that sql stats
	// compaction's schedule for next run is too far into the future. Default
	// warning threshold is 24 hours.
	ErrScheduleIntervalTooLong = errors.New("sql stats compaction schedule interval too long")

	// ErrSchedulePaused is returned when monitor detects that the schedule is
	// paused.
	ErrSchedulePaused = errors.New("sql stats compaction schedule paused")

	// ErrScheduleUndroppable is returned when user is attempting to drop sql stats
	// compaction schedule.
	ErrScheduleUndroppable = errors.New("sql stats compaction schedule cannot be dropped")
)

var longIntervalWarningThreshold = time.Hour * 24

// jobMonitor monitors the system.scheduled_jobs table to ensure that we would
// always have one sql stats scheduled compaction job running.
// It performs this check immediately upon start() and runs the check
// periodically every scanInterval (subject to jittering).
type jobMonitor struct {
	st           *cluster.Settings
	db           isql.DB
	scanInterval time.Duration
	jitterFn     func(time.Duration) time.Duration
	testingKnobs struct {
		updateCheckInterval time.Duration
	}
}

func (j *jobMonitor) start(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "sql-stats-scheduled-compaction-job-monitor", func(ctx context.Context) {
		nextJobScheduleCheck := timeutil.Now()
		currentRecurrence := SQLStatsCleanupRecurrence.Get(&j.st.SV)

		stopCtx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		timer := timeutil.NewTimer()
		// Ensure schedule at startup.
		timer.Reset(0)
		defer timer.Stop()

		updateCheckInterval := time.Minute
		if j.testingKnobs.updateCheckInterval != 0 {
			updateCheckInterval = j.testingKnobs.updateCheckInterval
		}

		// This loop runs every minute to check if we need to update the job schedule.
		// We only hit the jobs table if the schedule needs to be updated due to a
		// change in the recurrence cluster setting or as a scheduled check to
		// ensure the schedule exists, which defaults to every 6 hours.
		for {
			select {
			case <-timer.C:
				timer.Read = true
			case <-stopCtx.Done():
				return
			}
			if SQLStatsCleanupRecurrence.Get(&j.st.SV) != currentRecurrence || nextJobScheduleCheck.Before(timeutil.Now()) {
				j.updateSchedule(stopCtx)
				nextJobScheduleCheck = timeutil.Now().Add(j.jitterFn(j.scanInterval))
				currentRecurrence = SQLStatsCleanupRecurrence.Get(&j.st.SV)
			}

			timer.Reset(updateCheckInterval)
		}
	})
}

func (j *jobMonitor) getSchedule(
	ctx context.Context, txn isql.Txn,
) (sj *jobs.ScheduledJob, _ error) {
	row, err := txn.QueryRowEx(
		ctx,
		"load-sql-stats-scheduled-job",
		txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		"SELECT schedule_id FROM system.scheduled_jobs WHERE schedule_name = $1",
		compactionScheduleName,
	)
	if err != nil {
		return nil, err
	}

	if row == nil {
		return nil, errScheduleNotFound
	}

	scheduledJobID := int64(tree.MustBeDInt(row[0]))

	sj, err = jobs.ScheduledJobTxn(txn).Load(ctx, scheduledjobs.ProdJobSchedulerEnv, scheduledJobID)
	if err != nil {
		return nil, err
	}

	return sj, nil
}

func (j *jobMonitor) updateSchedule(ctx context.Context) {
	var sj *jobs.ScheduledJob
	var err error
	retryOptions := retry.Options{
		InitialBackoff: time.Second,
		MaxBackoff:     10 * time.Minute,
	}
	for r := retry.StartWithCtx(ctx, retryOptions); r.Next(); {
		if err = j.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			// We check if we can get load the schedule, if the schedule cannot be
			// loaded because it's not found, we recreate the schedule.
			sj, err = j.getSchedule(ctx, txn)

			if err != nil {
				if !jobs.HasScheduledJobNotFoundError(err) && !errors.Is(err, errScheduleNotFound) {
					return err
				}
				sj, err = CreateSQLStatsCompactionScheduleIfNotYetExist(ctx, txn, j.st)
				if err != nil {
					return err
				}
			}
			// Update schedule with new recurrence, if different.
			cronExpr := SQLStatsCleanupRecurrence.Get(&j.st.SV)
			if sj.ScheduleExpr() == cronExpr {
				return nil
			}
			if err := sj.SetSchedule(cronExpr); err != nil {
				return err
			}
			sj.SetScheduleStatus(string(jobs.StatusPending))
			return jobs.ScheduledJobTxn(txn).Update(ctx, sj)
		}); err != nil && ctx.Err() == nil {
			log.Errorf(ctx, "failed to update stats scheduled compaction job: %s", err)
		} else {
			break
		}
	}

	if ctx.Err() == nil {
		if err = CheckScheduleAnomaly(sj); err != nil {
			log.Warningf(ctx, "schedule anomaly detected, disabling sql stats compaction may cause performance impact: %s", err)
		}
	}

}

// CheckScheduleAnomaly checks a given schedule to see if it is either paused
// or has unusually long run interval.
func CheckScheduleAnomaly(sj *jobs.ScheduledJob) error {
	if (sj.NextRun() == time.Time{}) {
		return ErrSchedulePaused
	}

	if nextRunInterval := sj.NextRun().Sub(timeutil.Now()); nextRunInterval > longIntervalWarningThreshold {
		return errors.Wrapf(ErrScheduleIntervalTooLong, "sql stats compaction schedule next run interval "+
			"(%s) exceeds warning threshold (%s)", nextRunInterval,
			longIntervalWarningThreshold)
	}
	return nil
}
