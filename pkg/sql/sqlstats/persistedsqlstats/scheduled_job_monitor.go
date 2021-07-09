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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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
// for the abonormal state of the schedule and restore it.
var defaultScanInterval = time.Hour * 6

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

func (j *jobMonitor) ensureSchedule(ctx context.Context) {
	clusterVersion := j.st.Version.ActiveVersionOrEmpty(ctx)
	if !clusterVersion.IsActive(clusterversion.SQLStatsCompactionScheduledJob) {
		log.Warningf(ctx, "cannot create sql stats scheduled compaction job because current cluster version is too low")
	}

	if err := j.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		err := CreateSQLStatsCompactionScheduleIfNotYetExist(ctx, j.ie, txn)
		if !errors.Is(err, ErrDuplicatedSchedules) {
			return err
		}
		return nil
	}); err != nil {
		log.Errorf(ctx, "fail to ensure sql stats scheduled compaction job is created %s", err)
	}

}
