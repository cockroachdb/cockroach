// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupccl

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

func TestBackupSucceededUpdatesMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	executor := &scheduledBackupExecutor{
		metrics: backupMetrics{
			RpoMetric: metric.NewGauge(metric.Metadata{}),
		},
	}

	schedule := createSchedule(t, true)
	endTime := hlc.Timestamp{WallTime: hlc.UnixNano()}
	details := jobspb.BackupDetails{EndTime: endTime}

	err := executor.backupSucceeded(ctx, nil, schedule, details, nil)
	require.NoError(t, err)
	require.Equal(t, endTime.GoTime().Unix(), executor.metrics.RpoMetric.Value())
}

func TestBackupFailedUpdatesMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()

	th.setOverrideAsOfClauseKnob(t)
	schedules, err := th.createBackupSchedule(
		t,
		`CREATE SCHEDULE FOR
		BACKUP INTO 'nodelocal://1/backup' WITH kms = 'aws-kms:///not-a-real-kms-jpeg?AUTH=implicit&REGION=us-east-1'
		RECURRING '@hourly' FULL BACKUP ALWAYS
		WITH SCHEDULE OPTIONS updates_cluster_last_backup_time_metric`,
	)
	require.NoError(t, err)
	require.Len(t, schedules, 1)
	schedule := schedules[0]

	th.env.SetTime(schedule.NextRun().Add(1 * time.Second))
	require.NoError(t, th.executeSchedules())
	th.waitForScheduledJobState(t, schedule.ScheduleID(), jobs.StatusFailed)

	metrics, ok := th.server.JobRegistry().(*jobs.Registry).MetricsStruct().Backup.(*BackupMetrics)
	require.True(t, ok)

	require.Greater(t, metrics.LastKMSInaccessibleErrorTime.Value(), int64(0))
}

func createSchedule(t *testing.T, updatesLastBackupMetric bool) *jobs.ScheduledJob {
	schedule := jobs.NewScheduledJob(nil)

	args := &backuppb.ScheduledBackupExecutionArgs{
		UpdatesLastBackupMetric: updatesLastBackupMetric,
	}
	any, err := pbtypes.MarshalAny(args)
	require.NoError(t, err)
	schedule.SetExecutionDetails(schedule.ExecutorType(), jobspb.ExecutionArguments{Args: any})
	return schedule
}
