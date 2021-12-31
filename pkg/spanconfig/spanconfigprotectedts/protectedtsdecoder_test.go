// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigprotectedts_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigprotectedts"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestProtectedTimestampDecoder verifies that we can decode a row stored in a
// system.protected_ts_records like table.
func TestProtectedTimestampDecoder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	s0 := tc.Server(0)
	ptp := s0.DistSQLServer().(*distsql.ServerImpl).ServerConfig.ProtectedTimestampProvider
	jr := s0.JobRegistry().(*jobs.Registry)
	k := keys.SystemSQLCodec.TablePrefix(keys.ProtectedTimestampsRecordsTableID)

	var rec *ptpb.Record
	ts := s0.Clock().Now()
	jobID := jr.MakeJobID()
	require.NoError(t, s0.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		targetToProtect := ptpb.MakeRecordClusterTarget()
		rec = jobsprotectedts.MakeRecord(uuid.MakeV4(), int64(jobID), ts,
			nil /* deprecatedSpans */, jobsprotectedts.Jobs, targetToProtect)
		return ptp.Protect(ctx, txn, rec)
	}))

	rows, err := tc.Server(0).DB().Scan(ctx, k, k.PrefixEnd(), 0 /* maxRows */)
	require.NoError(t, err)
	require.Equal(t, 1, len(rows))

	for _, row := range rows {
		kv := roachpb.KeyValue{
			Key:   row.Key,
			Value: *row.Value,
		}
		recID, r, err := spanconfigprotectedts.TestingProtectedTimestampDecoder(keys.SystemSQLCodec, kv)
		require.NoError(t, err)
		require.Equal(t, rec.ID.GetUUID(), recID)
		require.Equal(t, *rec.Target, *r.Target)

		// Test the tombstone logic.
		{
			kv.Value.Reset()
			recID, r, err = spanconfigprotectedts.TestingProtectedTimestampDecoder(keys.SystemSQLCodec, kv)
			require.NoError(t, err)
			require.Equal(t, rec.ID.GetUUID(), recID)
			require.Zero(t, r.Target)
		}
	}

}
