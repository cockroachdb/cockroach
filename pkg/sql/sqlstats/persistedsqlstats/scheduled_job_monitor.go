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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// We don't need this monitor to run very frequent. Normally, the schedule
// should remain in the system table once it is created. However, some operations
// such as RESTORE would wipe the system table and populate it with the data
// from BAKCUP. In this case, it would be nice for us to preemptively check
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
// It immediately performs this check upon start() and runs the check
// periodically every scanInterval (subject to jittering).
type jobMonitor struct {
	st           *cluster.Settings
	ie           sqlutil.InternalExecutor
	db           *kv.DB
	scanInterval time.Duration
	jitterFn     func(time.Duration) time.Duration
}

func (j *jobMonitor) start(ctx context.Context, stopper *stop.Stopper) {
	j.ensureSchedule(ctx)
	j.registerClusterSettingHook()

	_ = stopper.RunAsyncTask(ctx, "sql-stats-scheduled-compaction-job-monitor", func(ctx context.Context) {
		for timer := timeutil.NewTimer(); ; timer.Reset(j.jitterFn(j.scanInterval)) {
			select {
			case <-timer.C:
				timer.Read = true
			case <-stopper.ShouldQuiesce():
				return
			}
			j.ensureSchedule(ctx)
		}
	})
}

func (j *jobMonitor) registerClusterSettingHook() {
	SQLStatsCleanupRecurrence.SetOnChange(&j.st.SV, func(ctx context.Context) {
		if !j.isVersionCompatible(ctx) {
			return
		}
		j.ensureSchedule(ctx)
		if err := j.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			sj, err := j.getSchedule(ctx, txn)
			if err != nil {
				return err
			}
			cronExpr := SQLStatsCleanupRecurrence.Get(&j.st.SV)
			if err = sj.SetSchedule(cronExpr); err != nil {
				return err
			}
			if err = CheckScheduleAnomaly(sj); err != nil {
				log.Warningf(ctx, "schedule anomaly detected, disabled sql stats compaction may cause performance impact: %s", err)
			}
			return sj.Update(ctx, j.ie, txn)
		}); err != nil {
			log.Errorf(ctx, "unable to find sqlstats clean up schedule: %s", err)
		}
	})
}

func (j *jobMonitor) getSchedule(
	ctx context.Context, txn *kv.Txn,
) (sj *jobs.ScheduledJob, _ error) {
	row, err := j.ie.QueryRowEx(
		ctx,
		"load-sql-stats-scheduled-job",
		txn,
		sessiondata.InternalExecutorOverride{
			User: security.NodeUserName(),
		},
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

	sj, err = jobs.LoadScheduledJob(ctx, scheduledjobs.ProdJobSchedulerEnv, scheduledJobID, j.ie, txn)
	if err != nil {
		return nil, err
	}

	return sj, nil
}

func (j *jobMonitor) ensureSchedule(ctx context.Context) {
	if !j.isVersionCompatible(ctx) {
		log.Infof(ctx, "cannot create sql stats scheduled compaction job because current cluster version is too low")
		return
	}

	var sj *jobs.ScheduledJob
	var err error
	if err = j.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// We check if we can get load the schedule, if the schedule cannot be
		// loaded because it's not found, we recreate the schedule.
		sj, err = j.getSchedule(ctx, txn)
		if err != nil {
			if !jobs.HasScheduledJobNotFoundError(err) && !errors.Is(err, errScheduleNotFound) {
				return err
			}
			sj, err = CreateSQLStatsCompactionScheduleIfNotYetExist(ctx, j.ie, txn, j.st)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		log.Errorf(ctx, "fail to ensure sql stats scheduled compaction job is created: %s", err)
		return
	}

	if err = CheckScheduleAnomaly(sj); err != nil {
		log.Warningf(ctx, "schedule anomaly detected: %s", err)
	}
}

func (j *jobMonitor) isVersionCompatible(ctx context.Context) bool {
	clusterVersion := j.st.Version.ActiveVersionOrEmpty(ctx)
	return clusterVersion.IsActive(clusterversion.SQLStatsCompactionScheduledJob)
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
