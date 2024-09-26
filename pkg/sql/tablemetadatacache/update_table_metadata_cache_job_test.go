// // Copyright 2024 The Cockroach Authors.
// //
// // Use of this software is governed by the Business Source License
// // included in the file licenses/BSL.txt.
// //
// // As of the Change Date specified in that file, in accordance with
// // the Business Source License, use of this software will be governed
// // by the Apache License, Version 2.0, included in the file
// // licenses/APL.txt.
package tablemetadatacache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestUpdateTableMetadataCacheJobRunsOnRPCTrigger tests that
// signalling the update table metadata cache job via the status
// server triggers the job to run.
func TestUpdateTableMetadataCacheJobRunsOnRPCTrigger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{})

	defer tc.Stopper().Stop(context.Background())
	metrics := tc.Server(0).JobRegistry().(*jobs.Registry).MetricsStruct().
		JobSpecificMetrics[jobspb.TypeUpdateTableMetadataCache].(TableMetadataUpdateJobMetrics)
	conn := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	testJobComplete := func(runNum int64) {
		testutils.SucceedsSoon(t, func() error {
			if metrics.NumRuns.Count() != runNum {
				return errors.New("job hasn't run yet")
			}
			row := conn.Query(t,
				`SELECT running_status, fraction_completed FROM crdb_internal.jobs WHERE job_id = $1 AND running_status IS NOT NULL`,
				jobs.UpdateTableMetadataCacheJobID)
			if !row.Next() {
				return errors.New("last_run_time not updated")
			}
			var runningStatus string
			var fractionCompleted float32
			require.NoError(t, row.Scan(&runningStatus, &fractionCompleted))
			if !strings.Contains(runningStatus, "Job completed at") {
				return errors.New("running_status not updated")
			}
			if fractionCompleted != 1.0 {
				return errors.New("fraction_completed not updated")
			}
			return nil
		})
	}

	// Get the node id that claimed the update job. We'll issue the
	// RPC to a node that doesn't own the job to test that the RPC can
	// propagate the request to the correct node.
	var nodeID int
	testutils.SucceedsSoon(t, func() error {
		row := conn.Query(t, `
SELECT claim_instance_id FROM system.jobs
WHERE id = $1 AND claim_instance_id IS NOT NULL`, jobs.UpdateTableMetadataCacheJobID)
		if !row.Next() {
			return errors.New("no node has claimed the job")
		}
		if err := row.Scan(&nodeID); err != nil {
			return err
		}
		return nil
	})

	rpcGatewayNode := (nodeID + 1) % 3
	_, err := tc.Server(rpcGatewayNode).GetStatusClient(t).UpdateTableMetadataCache(ctx,
		&serverpb.UpdateTableMetadataCacheRequest{Local: false})
	require.NoError(t, err)
	testJobComplete(1)
	count := metrics.UpdatedTables.Count()
	meanDuration := metrics.Duration.CumulativeSnapshot().Mean()
	require.Greater(t, count, int64(0))
	require.Greater(t, meanDuration, float64(0))

	_, err = tc.Server(rpcGatewayNode).GetStatusClient(t).UpdateTableMetadataCache(ctx,
		&serverpb.UpdateTableMetadataCacheRequest{Local: false})
	require.NoError(t, err)
	testJobComplete(2)
	require.Greater(t, metrics.UpdatedTables.Count(), count)
	require.NotEqual(t, metrics.Duration.CumulativeSnapshot().Mean(), meanDuration)
	meanDuration = metrics.Duration.CumulativeSnapshot().Mean()
	count = metrics.UpdatedTables.Count()

	defer testutils.TestingHook(&upsertQuery, "UPSERT INTO error")()
	_, err = tc.Server(rpcGatewayNode).GetStatusClient(t).UpdateTableMetadataCache(ctx,
		&serverpb.UpdateTableMetadataCacheRequest{Local: false})
	require.NoError(t, err)
	testJobComplete(3)
	require.Equal(t, count, metrics.UpdatedTables.Count())
	require.NotEqual(t, metrics.Duration.CumulativeSnapshot().Mean(), meanDuration)

}

// TestUpdateTableMetadataCacheAutomaticUpdates tests that:
// 1. The update table metadata cache job does not run automatically by default.
// 2. The job runs automatically on the data validity interval when automatic
// updates are enabled.
func TestUpdateTableMetadataCacheAutomaticUpdates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderStress(t, "too slow under stress")

	ctx := context.Background()

	jobRunCh := make(chan struct{})
	updater := mockUpdater{jobRunCh: jobRunCh}

	// Mock newTableMetadataUpdater to return a mockUpdater struct.
	defer testutils.TestingHook(&newTableMetadataUpdater,
		func(job *jobs.Job, metrics *TableMetadataUpdateJobMetrics, ie isql.Executor) iTableMetadataUpdater {
			return &updater
		})()

	waitForJobRuns := func(count int, timeout time.Duration) error {
		ctxWithCancel, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		for i := 0; i < count; i++ {
			select {
			case <-jobRunCh:
			case <-ctxWithCancel.Done():
				return fmt.Errorf("timed out waiting for job run %d", i+1)
			}
		}
		return nil
	}

	// Server setup.
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	conn := sqlutils.MakeSQLRunner(s.ApplicationLayer().SQLConn(t))

	// Wait for the job to be claimed by a node.
	testutils.SucceedsSoon(t, func() error {
		row := conn.Query(t, `
				SELECT claim_instance_id, status FROM system.jobs
				WHERE id = $1 AND claim_instance_id IS NOT NULL
                    AND status = 'running'`,
			jobs.UpdateTableMetadataCacheJobID)
		if !row.Next() {
			return errors.New("no node has claimed the job")
		}
		return nil
	})

	require.Zero(t, len(updater.mockCalls), "Job should not run automatically by default")

	t.Run("AutomaticUpdatesEnabled", func(t *testing.T) {
		conn.Exec(t, `SET CLUSTER SETTING obs.tablemetadata.automatic_updates.enabled = true`)
		DataValidDurationSetting.Override(ctx, &s.ClusterSettings().SV, 50*time.Millisecond)
		err := waitForJobRuns(3, 10*time.Second)
		require.NoError(t, err, "Job should have run at least 3 times")
		require.GreaterOrEqual(t, len(updater.mockCalls), 3, "Job should have run at least 3 times")
		conn.Exec(t, `RESET CLUSTER SETTING obs.tablemetadata.automatic_updates.enabled`)
		// We'll wait for one more signal in case the job was running when the setting was disabled.
		// Ignore the error since it could timeout or be successful.
		_ = waitForJobRuns(1, 200*time.Millisecond)

		// Verify time between calls.
		for i := 1; i < len(updater.mockCalls); i++ {
			timeBetweenCalls := updater.mockCalls[i].Sub(updater.mockCalls[i-1])
			require.GreaterOrEqual(t, timeBetweenCalls, 50*time.Millisecond,
				"Time between calls %d and %d should be at least 50ms", i-1, i)
		}
	})

	t.Run("AutomaticUpdatesDisabled", func(t *testing.T) {
		conn.Exec(t, `SET CLUSTER SETTING obs.tablemetadata.automatic_updates.enabled = f`)
		DataValidDurationSetting.Override(ctx, &s.ClusterSettings().SV, 50*time.Millisecond)
		initialCount := len(updater.mockCalls)
		err := waitForJobRuns(1, 200*time.Millisecond)
		require.Error(t, err, "Job should not run after being disabled")
		require.Equal(t, initialCount, len(updater.mockCalls), "Job count should not increase after being disabled")
	})
}

type mockUpdater struct {
	mockCalls []time.Time
	jobRunCh  chan struct{}
}

func (t *mockUpdater) RunUpdater(ctx context.Context) error {
	t.mockCalls = append(t.mockCalls, time.Now())
	t.jobRunCh <- struct{}{}
	return nil
}

var _ iTableMetadataUpdater = &mockUpdater{}
