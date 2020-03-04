// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobsprotectedts_test

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestJobsProtectedTimestamp is an end-to-end test of protected timestamp
// reconciliation for jobs.
func TestJobsProtectedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	// Now I want to create some artifacts that should get reconciled away and
	// then make sure that they do and others which should not do not.
	s0 := tc.Server(0)
	ptp := s0.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
	runner := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	runner.Exec(t, "SET CLUSTER SETTING kv.protectedts.reconciliation.interval = '1ms';")
	jr := s0.JobRegistry().(*jobs.Registry)
	mkJobRec := func() jobs.Record {
		return jobs.Record{
			Description: "testing",
			Statement:   "SELECT 1",
			Username:    "root",
			Details: jobspb.ImportDetails{
				Tables: []jobspb.ImportDetails_Table{
					{
						Desc: &sqlbase.TableDescriptor{
							ID: 1,
						},
					},
				},
			},
			Progress:      jobspb.ImportProgress{},
			DescriptorIDs: []sqlbase.ID{1},
		}
	}
	mkJobAndRecord := func() (j *jobs.Job, rec *ptpb.Record) {
		ts := s0.Clock().Now()
		require.NoError(t, s0.DB().Txn(ctx, func(ctx context.Context, txn *client.Txn) (err error) {
			if j, err = jr.CreateJobWithTxn(ctx, mkJobRec(), txn); err != nil {
				return err
			}
			rec = jobsprotectedts.MakeRecord(uuid.MakeV4(), *j.ID(), ts, []roachpb.Span{{Key: keys.MinKey, EndKey: keys.MaxKey}})
			return ptp.Protect(ctx, txn, rec)
		}))
		return j, rec
	}
	jMovedToFailed, recMovedToFailed := mkJobAndRecord()
	require.NoError(t, jMovedToFailed.Failed(ctx, io.ErrUnexpectedEOF, func(ctx context.Context, txn *client.Txn) error {
		return nil
	}))
	jFinished, recFinished := mkJobAndRecord()
	require.NoError(t, jFinished.Succeeded(ctx, func(ctx context.Context, txn *client.Txn) error {
		return nil
	}))
	_, recRemains := mkJobAndRecord()
	ensureNotExists := func(ctx context.Context, txn *client.Txn, ptsID uuid.UUID) (err error) {
		_, err = ptp.GetRecord(ctx, txn, ptsID)
		if err == protectedts.ErrNotExists {
			return nil
		}
		return fmt.Errorf("waiting for %v, got %v", protectedts.ErrNotExists, err)
	}
	testutils.SucceedsSoon(t, func() (err error) {
		return s0.DB().Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			if err := ensureNotExists(ctx, txn, recMovedToFailed.ID); err != nil {
				return err
			}
			if err := ensureNotExists(ctx, txn, recFinished.ID); err != nil {
				return err
			}
			_, err := ptp.GetRecord(ctx, txn, recRemains.ID)
			require.NoError(t, err)
			return err
		})
	})

	// Verify that the two jobs we just observed as removed were recorded in the
	// metrics.
	var removed int
	runner.QueryRow(t, `
SELECT
    value
FROM
    crdb_internal.node_metrics
WHERE
    name = 'kv.protectedts.reconciliation.records_removed';
`).Scan(&removed)
	require.Equal(t, 2, removed)
}
