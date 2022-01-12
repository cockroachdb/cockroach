// Copyright 2022 The Cockroach Authors.
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
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigprotectedts"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestProtectedTimestampTableReader(t *testing.T) {
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
	execCfg := s0.ExecutorConfig().(sql.ExecutorConfig)
	ptp := s0.DistSQLServer().(*distsql.ServerImpl).ServerConfig.ProtectedTimestampProvider
	jr := s0.JobRegistry().(*jobs.Registry)

	mkRecordAndProtect := func(ts hlc.Timestamp, target *ptpb.Target) {
		jobID := jr.MakeJobID()
		require.NoError(t, s0.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			rec := jobsprotectedts.MakeRecord(uuid.MakeV4(), int64(jobID), ts,
				nil /* deprecatedSpans */, jobsprotectedts.Jobs, target)
			return ptp.Protect(ctx, txn, rec)
		}))
	}

	// Protect the cluster.
	clusterProtect := s0.Clock().Now()
	mkRecordAndProtect(clusterProtect, ptpb.MakeClusterTarget())

	// Protect a few tenants.
	tenantProtect := s0.Clock().Now()
	mkRecordAndProtect(tenantProtect, ptpb.MakeTenantsTarget([]roachpb.TenantID{roachpb.MakeTenantID(1),
		roachpb.MakeTenantID(2)}))

	// Protect a few tables.
	tableProtect := s0.Clock().Now()
	mkRecordAndProtect(tableProtect, ptpb.MakeSchemaObjectsTarget([]descpb.ID{3, 4}))

	// Protect one of those tables again for fun.
	tableProtectAgain := s0.Clock().Now()
	mkRecordAndProtect(tableProtectAgain, ptpb.MakeSchemaObjectsTarget([]descpb.ID{4}))

	txn := execCfg.DB.NewTxn(ctx, "read-pts-table")
	ptsTableReader, err := spanconfigprotectedts.New(ctx,
		spanconfigprotectedts.ProdSystemProtectedTimestampTable, execCfg.InternalExecutor, txn)
	require.NoError(t, err)

	// Verify that we are seeing the protections we expect on the tables.
	require.Equal(t, []hlc.Timestamp{tableProtect},
		ptsTableReader.GetProtectedTimestampsForSchemaObject(3))

	protectedTS := ptsTableReader.GetProtectedTimestampsForSchemaObject(4)
	sort.Slice(protectedTS, func(i, j int) bool {
		return protectedTS[i].Less(protectedTS[j])
	})
	require.Equal(t, []hlc.Timestamp{tableProtect, tableProtectAgain}, protectedTS)

	// Verify cluster protected timestamps.
	require.Equal(t, []hlc.Timestamp{clusterProtect}, ptsTableReader.GetProtectedTimestampsForCluster())

	// Verify tenant protected timestamps.
	tenantProtections := ptsTableReader.GetProtectedTimestampsForTenants()
	sort.Slice(tenantProtections, func(i, j int) bool {
		return tenantProtections[i].TenantID.ToUint64() < tenantProtections[j].TenantID.ToUint64()
	})
	require.Equal(t, []spanconfig.TenantProtectedTimestamps{
		{
			Protections: []hlc.Timestamp{tenantProtect},
			TenantID:    roachpb.MakeTenantID(1),
		},
		{
			Protections: []hlc.Timestamp{tenantProtect},
			TenantID:    roachpb.MakeTenantID(2),
		},
	}, tenantProtections)
}
