// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigsqlwatcher_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigsqlwatcher"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestProtectedTimestampDecoder verifies that we can decode a row stored in
// system.protected_ts_records.
func TestProtectedTimestampDecoder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	s0 := tc.Server(0)
	ptp := s0.DistSQLServer().(*distsql.ServerImpl).ServerConfig.ProtectedTimestampProvider
	jr := s0.JobRegistry().(*jobs.Registry)
	k := keys.SystemSQLCodec.TablePrefix(keys.ProtectedTimestampsRecordsTableID)

	for _, testCase := range []struct {
		name   string
		target *ptpb.Target
	}{
		{
			name:   "cluster",
			target: ptpb.MakeClusterTarget(),
		},
		{
			name: "tenant",
			target: ptpb.MakeTenantsTarget([]roachpb.TenantID{roachpb.MustMakeTenantID(1),
				roachpb.MustMakeTenantID(2)}),
		},
		{
			name:   "schema-object",
			target: ptpb.MakeSchemaObjectsTarget([]descpb.ID{1, 2}),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ts := s0.Clock().Now()
			jobID := jr.MakeJobID()
			pts := ptstorage.WithDatabase(ptp, s0.InternalDB().(isql.DB))

			rec := jobsprotectedts.MakeRecord(
				uuid.MakeV4(), int64(jobID), ts,
				nil, /* deprecatedSpans */
				jobsprotectedts.Jobs, testCase.target,
			)
			require.NoError(t, pts.Protect(ctx, rec))

			rows, err := tc.Server(0).DB().Scan(ctx, k, k.PrefixEnd(), 0 /* maxRows */)
			require.NoError(t, err)
			require.Equal(t, 1, len(rows))

			last := rows[len(rows)-1]
			got, err := spanconfigsqlwatcher.TestingProtectedTimestampDecoderFn()(
				roachpb.KeyValue{
					Key:   last.Key,
					Value: *last.Value,
				},
			)
			require.NoError(t, err)
			require.Truef(t, rec.Target.Equal(got),
				"expected target=%s, got target=%s", rec.Target.String(), got.String())

			require.NoError(t, pts.Release(ctx, rec.ID.GetUUID()))
		})
	}
}
