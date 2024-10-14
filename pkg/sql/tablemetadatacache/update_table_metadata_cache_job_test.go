// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tablemetadatacache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	tablemetadatacacheutil "github.com/cockroachdb/cockroach/pkg/sql/tablemetadatacache/util"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestUpdateTableMetadataCacheJobRunsOnRPCTrigger tests that
// signalling the update table metadata cache job via the status
// server triggers the job to run.
func TestUpdateTableMetadataCacheJobRunsOnRPCTrigger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	jobCompleteCh := make(chan struct{})
	ctx := context.Background()
	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				TableMetadata: &tablemetadatacacheutil.TestingKnobs{
					TableMetadataUpdater: &noopUpdater{},
					OnJobComplete: func() {
						jobCompleteCh <- struct{}{}
					},
				},
			},
		},
	})
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

	// Wait for the job to complete.
	t.Log("waiting for job to complete")
	<-jobCompleteCh
	t.Log("job completed")

	row := conn.Query(t,
		`SELECT running_status FROM crdb_internal.jobs WHERE job_id = $1 AND running_status IS NOT NULL`,
		jobs.UpdateTableMetadataCacheJobID)
	if !row.Next() {
		t.Fatal("last_run_time not updated")
	}
	var runningStatus string
	require.NoError(t, row.Scan(&runningStatus))
	require.Containsf(t, runningStatus, "Job completed at", "running_status not updated: %s", runningStatus)
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
	updater := mockUpdaterWithSignal{jobRunCh: jobRunCh}

	// Server setup.
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			TableMetadata: &tablemetadatacacheutil.TestingKnobs{
				TableMetadataUpdater: &updater,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	conn := sqlutils.MakeSQLRunner(s.ApplicationLayer().SQLConn(t))

	waitForJobRuns := func(count int, timeout time.Duration) error {
		ctxWithCancel, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		for i := 0; i < count; i++ {
			select {
			case <-jobRunCh:
			case <-ctxWithCancel.Done():
				var enabled bool
				var duration string
				conn.QueryRow(t, "SHOW CLUSTER SETTING obs.tablemetadata.data_valid_duration").Scan(&duration)
				conn.QueryRow(t, "SHOW CLUSTER SETTING obs.tablemetadata.automatic_updates.enabled").Scan(&enabled)
				return fmt.Errorf("timed out waiting for job run %d. auto_updates_enabled:%t, data_valid_duration: %s",
					i+1, enabled, duration)
			}
		}
		return nil
	}
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

	// Since this test explicitly calls DataValidDurationSetting.Override instead of using `SET CLUSTER SETTING`,
	// there are no logs emitted when the setting  changes. This callback will do that logging.
	DataValidDurationSetting.SetOnChange(&s.ClusterSettings().SV, func(ctx context.Context) {
		log.Infof(ctx, "Updating data valid duration setting to %s",
			DataValidDurationSetting.Get(&s.ClusterSettings().SV))
	})
	require.Zero(t, len(updater.mockCalls), "Job should not run automatically by default")

	t.Run("AutomaticUpdatesEnabled", func(t *testing.T) {
		conn.Exec(t, `SET CLUSTER SETTING obs.tablemetadata.automatic_updates.enabled = true`)
		DataValidDurationSetting.Override(ctx, &s.ClusterSettings().SV, 50*time.Millisecond)
		err := waitForJobRuns(3, 30*time.Second)
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

// mockUpdaterWithSignal is a mock implementation of ITableMetadataUpdater that
// records the time of each call to RunUpdater and signals when the job
// is complete.
type mockUpdaterWithSignal struct {
	mockCalls []time.Time
	jobRunCh  chan struct{}
}

func (m *mockUpdaterWithSignal) RunUpdater(ctx context.Context) error {
	log.Info(ctx, "mockUpdater.RunUpdater started")
	m.mockCalls = append(m.mockCalls, time.Now())
	m.jobRunCh <- struct{}{}
	log.Info(ctx, "mockUpdater.RunUpdater completed")
	return nil
}

var _ tablemetadatacacheutil.ITableMetadataUpdater = &mockUpdaterWithSignal{}

type noopUpdater struct{}

func (nu *noopUpdater) RunUpdater(_ctx context.Context) error {
	return nil
}

var _ tablemetadatacacheutil.ITableMetadataUpdater = &noopUpdater{}
