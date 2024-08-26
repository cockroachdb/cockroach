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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/tablemetadatacache"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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
		return nil
	})
	row := conn.QueryRow(t, `SELECT running_status FROM crdb_internal.jobs WHERE job_id = $1`, jobs.UpdateTableMetadataCacheJobID)
	var runningStatus string
	row.Scan(&runningStatus)
	require.Contains(t, runningStatus, "last_run_time")
}
