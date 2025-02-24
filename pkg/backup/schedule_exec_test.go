package backup

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

func TestBackupSucceededUpdatesMetrics(t *testing.T) {
	ctx := context.Background()
	executor := &scheduledBackupExecutor{
		metrics: backupMetrics{
			RpoMetric:       metric.NewGauge(metric.Metadata{}),
			RpoTenantMetric: metric.NewExportedGaugeVec(metric.Metadata{}, []string{"tenant_id"}),
		},
	}
	env := scheduledjobs.ProdJobSchedulerEnv

	t.Run("updates RPO metric", func(t *testing.T) {
		schedule := createSchedule(t, true)
		endTime := hlc.Timestamp{WallTime: hlc.UnixNano()}
		details := jobspb.BackupDetails{EndTime: endTime}

		err := executor.backupSucceeded(ctx, nil, schedule, details, env)
		require.NoError(t, err)
		require.Equal(t, endTime.GoTime().Unix(), executor.metrics.RpoMetric.Value())
	})

	t.Run("updates RPO tenant metric", func(t *testing.T) {
		schedule := createSchedule(t, true)
		tenantIDs := mustMakeTenantIDs(t, 1, 2)
		endTime := hlc.Timestamp{WallTime: hlc.UnixNano()}
		details := jobspb.BackupDetails{
			EndTime:           endTime,
			SpecificTenantIds: tenantIDs,
		}

		err := executor.backupSucceeded(ctx, nil, schedule, details, env)
		require.NoError(t, err)

		expectedTenantIDs := []string{"system", "2"}
		verifyRPOTenantMetricLabels(t, executor.metrics.RpoTenantMetric, expectedTenantIDs)
	})
}

func createSchedule(t *testing.T, updatesLastBackupMetric bool) *jobs.ScheduledJob {
	env := scheduledjobs.ProdJobSchedulerEnv
	schedule := jobs.NewScheduledJob(env)

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
