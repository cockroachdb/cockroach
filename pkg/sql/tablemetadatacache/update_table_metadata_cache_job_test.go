// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tablemetadatacache_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/tablemetadatacache"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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

	conn := sqlutils.MakeSQLRunner(tc.ServerConn(0))

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
		require.NoError(t, row.Scan(&nodeID))

		rpcGatewayNode := (nodeID + 1) % 3
		_, err := tc.Server(rpcGatewayNode).GetStatusClient(t).UpdateTableMetadataCache(ctx,
			&serverpb.UpdateTableMetadataCacheRequest{Local: false})
		if err != nil {
			return err
		}
		// The job shouldn't be busy.
		return nil
	})

	metrics := tc.Server(0).JobRegistry().(*jobs.Registry).MetricsStruct().
		JobSpecificMetrics[jobspb.TypeUpdateTableMetadataCache].(tablemetadatacache.TableMetadataUpdateJobMetrics)
	testutils.SucceedsSoon(t, func() error {
		if metrics.NumRuns.Count() != 1 {
			return errors.New("job hasn't run yet")
		}
		row := conn.Query(t,
			`SELECT running_status FROM crdb_internal.jobs WHERE job_id = $1 AND running_status IS NOT NULL`,
			jobs.UpdateTableMetadataCacheJobID)
		if !row.Next() {
			return errors.New("last_run_time not updated")
		}
		var runningStatus string
		require.NoError(t, row.Scan(&runningStatus))
		if !strings.Contains(runningStatus, "last metadata update at") {
			return errors.New("last run time not updated")
		}
		return nil
	})
}

// TestUpdateTableMetadataCacheAutomaticUpdates tests that:
// 1. The update table metadata cache job does not run automatically by default.
// 2. The job runs automatically on the data validity interval when automatic
// updates are enabled.
func TestUpdateTableMetadataCacheAutomaticUpdates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// We'll mock the job execution function to track when the job is run and
	// to avoid running the actual job which could take longer - we don't care
	// about the actual update logic in this test.
	var mockCalls []time.Time
	mockMutex := syncutil.RWMutex{}
	jobRunCh := make(chan struct{})
	tablemetadatacache.MockJobExecFn(func(ctx context.Context, ie isql.Executor) error {
		mockMutex.Lock()
		defer mockMutex.Unlock()
		mockCalls = append(mockCalls, timeutil.Now())
		select {
		case jobRunCh <- struct{}{}:
		default:
		}
		return nil
	})

	// Server setup.
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	conn := sqlutils.MakeSQLRunner(s.ApplicationLayer().SQLConn(t))
	metrics := s.JobRegistry().(*jobs.Registry).MetricsStruct().
		JobSpecificMetrics[jobspb.TypeUpdateTableMetadataCache].(tablemetadatacache.TableMetadataUpdateJobMetrics)

	getMockCallCount := func() int {
		mockMutex.RLock()
		defer mockMutex.RUnlock()
		return len(mockCalls)
	}

	waitForJobRuns := func(count int, timeout time.Duration) error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		for i := 0; i < count; i++ {
			select {
			case <-jobRunCh:
			case <-ctx.Done():
				return fmt.Errorf("timed out waiting for job run %d", i+1)
			}
		}
		return nil
	}

	testutils.SucceedsSoon(t, func() error {
		row := conn.Query(t, `
				SELECT claim_instance_id FROM system.jobs 
				WHERE id = $1 AND claim_instance_id IS NOT NULL`, jobs.UpdateTableMetadataCacheJobID)
		if !row.Next() {
			return errors.New("no node has claimed the job")
		}
		return nil
	})

	t.Run("JobNotRunningByDefault", func(t *testing.T) {
		require.Zero(t, metrics.NumRuns.Count(), "Job should not run automatically by default")
	})

	t.Run("AutomaticUpdatesEnabled", func(t *testing.T) {
		conn.Exec(t, `SET CLUSTER SETTING tablemetadata.automatic_updates.enabled = true`)
		conn.Exec(t, `SET CLUSTER SETTING tablemetadata.data_valid_duration = '50ms'`)
		err := waitForJobRuns(3, 5*time.Second)
		require.NoError(t, err, "Job should have run at least 3 times")
		mockCallsCount := getMockCallCount()
		require.GreaterOrEqual(t, mockCallsCount, 3, "Job should have run at least 3 times")
		require.Equal(t, int64(mockCallsCount), metrics.NumRuns.Count())
		conn.Exec(t, `RESET CLUSTER SETTING tablemetadata.automatic_updates.enabled`)
		// We'll wait for one more signal in case the job was running when the setting was disabled.
		// Ignore the error since it could timeout or be successful.
		_ = waitForJobRuns(1, 200*time.Millisecond)
	})

	t.Run("AutomaticUpdatesDisabled", func(t *testing.T) {
		conn.Exec(t, `SET CLUSTER SETTING tablemetadata.automatic_updates.enabled = f`)
		initialCount := getMockCallCount()
		err := waitForJobRuns(1, 200*time.Millisecond)
		require.Error(t, err, "Job should not run after being disabled")
		require.Equal(t, initialCount, getMockCallCount(), "Job count should not increase after being disabled")
	})

	t.Run("VerifyTimeBetweenCalls", func(t *testing.T) {
		mockMutex.Lock()
		defer mockMutex.Unlock()
		for i := 1; i < len(mockCalls); i++ {
			timeBetweenCalls := mockCalls[i].Sub(mockCalls[i-1])
			require.GreaterOrEqual(t, timeBetweenCalls, 50*time.Millisecond,
				"Time between calls %d and %d should be at least 50ms", i-1, i)
		}
	})
}
