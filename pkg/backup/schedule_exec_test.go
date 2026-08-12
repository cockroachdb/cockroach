// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
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
			RpoMetric:       metric.NewGauge(metric.Metadata{}),
			RpoTenantMetric: metric.NewExportedGaugeVec(metric.Metadata{}, []string{"tenant_id"}),
		},
	}

	t.Run("updates RPO metric", func(t *testing.T) {
		schedule := createSchedule(t, true)
		endTime := hlc.Timestamp{WallTime: hlc.UnixNano()}
		details := jobspb.BackupDetails{EndTime: endTime}

		err := executor.backupSucceeded(ctx, nil, schedule, details, nil)
		require.NoError(t, err)
		require.Equal(t, endTime.GoTime().Unix(), executor.metrics.RpoMetric.Value())
	})

	t.Run("updates RPO tenant metric", func(t *testing.T) {
		schedule := createSchedule(t, true)
		tenantIDs := mustMakeTenantIDs(t, 1, 3)
		endTime := hlc.Timestamp{WallTime: hlc.UnixNano()}
		details := jobspb.BackupDetails{
			EndTime:           endTime,
			SpecificTenantIds: tenantIDs,
		}

		err := executor.backupSucceeded(ctx, nil, schedule, details, nil)
		require.NoError(t, err)

		expectedTenantIDs := []string{"system", "3"}
		verifyRPOTenantMetricLabels(t, executor.metrics.RpoTenantMetric, expectedTenantIDs)
		verifyRPOTenantMetricGaugeValue(t, executor.metrics.RpoTenantMetric, details.EndTime)
	})

	t.Run("tenant backup does not update RPO metric", func(t *testing.T) {
		// Use a fresh executor to ensure RpoMetric starts at zero.
		freshExecutor := &scheduledBackupExecutor{
			metrics: backupMetrics{
				RpoMetric:       metric.NewGauge(metric.Metadata{}),
				RpoTenantMetric: metric.NewExportedGaugeVec(metric.Metadata{}, []string{"tenant_id"}),
			},
		}
		schedule := createSchedule(t, true)
		tenantIDs := mustMakeTenantIDs(t, 2)
		endTime := hlc.Timestamp{WallTime: hlc.UnixNano()}
		details := jobspb.BackupDetails{
			EndTime:           endTime,
			SpecificTenantIds: tenantIDs,
		}

		err := freshExecutor.backupSucceeded(ctx, nil, schedule, details, nil)
		require.NoError(t, err)

		// RpoMetric should NOT be updated for tenant-specific backups.
		require.Equal(t, int64(0), freshExecutor.metrics.RpoMetric.Value())
		// But RpoTenantMetric should be updated.
		verifyRPOTenantMetricLabels(t, freshExecutor.metrics.RpoTenantMetric, []string{"2"})
		verifyRPOTenantMetricGaugeValue(t, freshExecutor.metrics.RpoTenantMetric, details.EndTime)
	})
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
	th.waitForScheduledJobState(t, schedule.ScheduleID(), jobs.StateFailed)

	metrics, ok := th.server.JobRegistry().(*jobs.Registry).MetricsStruct().Backup.(*BackupMetrics)
	require.True(t, ok)

	require.Greater(t, metrics.LastKMSInaccessibleErrorTime.Value(), int64(0))
}

// TestScheduledBackupKMSExternalConnectionUsage exercises the USAGE check on a
// KMS external connection from the scheduled backup executor rather than from a
// statement typed by a user. A schedule is replanned as its owner on every run,
// so the check runs again each time. A schedule that names a KMS external
// connection its owner has no USAGE on therefore keeps running only for as long
// as the check is off, and stops making progress the moment it is turned on.
// Granting USAGE back lets it make progress again.
func TestScheduledBackupKMSExternalConnectionUsage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	th, cleanup := newTestHelper(t)
	defer cleanup()
	th.setOverrideAsOfClauseKnob(t)

	th.sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
	th.sqlDB.Exec(t, `CREATE USER testuser`)
	th.sqlDB.Exec(t, `GRANT BACKUP ON TABLE foo TO testuser`)
	th.sqlDB.Exec(t, `CREATE EXTERNAL CONNECTION "dest-ec" AS 'nodelocal://1/schedule-kms'`)
	th.sqlDB.Exec(t, `GRANT USAGE ON EXTERNAL CONNECTION "dest-ec" TO testuser`)
	th.sqlDB.Exec(t, `CREATE EXTERNAL CONNECTION "kms-ec" AS 'testkms:///schedule-key?AUTH=implicit'`)
	th.sqlDB.Exec(t, `GRANT USAGE ON EXTERNAL CONNECTION "kms-ec" TO testuser`)

	testuserDB := sqlutils.MakeSQLRunner(th.server.SQLConn(t, serverutils.User("testuser")))
	testuserDB.Exec(t, `CREATE SCHEDULE kms_sched FOR BACKUP TABLE defaultdb.public.foo
INTO 'external://dest-ec' WITH kms = 'external://kms-ec'
RECURRING '@hourly' FULL BACKUP ALWAYS`)

	var scheduleID jobspb.ScheduleID
	th.sqlDB.QueryRow(t,
		`SELECT id FROM [SHOW SCHEDULES FOR BACKUP] WHERE label = 'kms_sched'`,
	).Scan(&scheduleID)

	runSchedule := func() {
		t.Helper()
		th.env.SetTime(th.loadSchedule(t, scheduleID).NextRun().Add(time.Second))
		require.NoError(t, th.executeSchedules())
	}

	// Losing the grant is not enough to break the schedule while the check is
	// off. The run still succeeds.
	th.sqlDB.Exec(t, `SET CLUSTER SETTING sql.auth.external_connection.require_usage_privilege.enabled = false`)
	th.sqlDB.Exec(t, `REVOKE USAGE ON EXTERNAL CONNECTION "kms-ec" FROM testuser`)
	runSchedule()
	th.waitForSuccessfulScheduledJobCount(t, scheduleID, 1)

	th.sqlDB.Exec(t, `SET CLUSTER SETTING sql.auth.external_connection.require_usage_privilege.enabled = true`)
	runSchedule()
	require.Contains(t, th.loadSchedule(t, scheduleID).ScheduleStatus(),
		"user testuser does not have USAGE privilege on external_connection kms-ec")
	th.waitForSuccessfulScheduledJobCount(t, scheduleID, 1)

	// Restoring the grant unblocks the schedule.
	th.sqlDB.Exec(t, `GRANT USAGE ON EXTERNAL CONNECTION "kms-ec" TO testuser`)
	runSchedule()
	th.waitForSuccessfulScheduledJobCount(t, scheduleID, 2)
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

func mustMakeTenantIDs(t *testing.T, ids ...int) []roachpb.TenantID {
	var tenantIDs []roachpb.TenantID
	for _, id := range ids {
		tid, err := roachpb.MakeTenantID(uint64(id))
		require.NoError(t, err)
		tenantIDs = append(tenantIDs, tid)
	}
	return tenantIDs
}

func verifyRPOTenantMetricLabels(
	t *testing.T, metric *metric.GaugeVec, expectedTenantIDs []string,
) {
	prometheusMetrics := metric.ToPrometheusMetrics()
	var actualTenantIDs []string
	for _, promMetric := range prometheusMetrics {
		labels := promMetric.GetLabel()
		for _, label := range labels {
			if label.GetName() == "tenant_id" {
				actualTenantIDs = append(actualTenantIDs, label.GetValue())
			}
		}
	}
	require.ElementsMatch(t, expectedTenantIDs, actualTenantIDs)
}

func verifyRPOTenantMetricGaugeValue(t *testing.T, metric *metric.GaugeVec, endTime hlc.Timestamp) {
	prometheusMetrics := metric.ToPrometheusMetrics()
	for _, promMetric := range prometheusMetrics {
		value := promMetric.Gauge.GetValue()
		require.Equal(t, float64(endTime.GoTime().Unix()), value)
	}
}
