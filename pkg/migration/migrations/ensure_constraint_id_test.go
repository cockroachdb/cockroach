// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestEnsureConstraintIDs tests that constraint IDs are added as expected.
func TestEnsureConstraintIDs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Start off with the version that did not support
	// constraint IDs.
	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.ConstraintIDsForTableDescs - 1),
				},
			},
		},
	}
	c := keys.SystemSQLCodec
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, clusterArgs)
	s := tc.Server(0)
	defer tc.Stopper().Stop(ctx)
	sqlDB := tc.ServerConn(0)
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	// Create table with a primary key constraint.
	tdb.Exec(t, "CREATE TABLE t(name int primary key)")
	// Validate the comments on constraints are blocked.
	tdb.ExpectErr(t,
		"pq: cannot comment on constraint",
		"COMMENT ON CONSTRAINT \"t_pkey\" ON t IS 'primary_comment'")
	// Validate that we have a constraint ID due to post deserialization logic.
	desc := catalogkv.TestingGetMutableExistingTableDescriptor(s.DB(), c, "defaultdb", "t")
	desc.PrimaryIndex.ConstraintID = 0
	require.NoError(t, s.DB().Put(
		context.Background(),
		catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, desc.GetID()),
		desc.DescriptorProto(),
	))
	// Validate that the post serialization will recompute the constraint IDs
	// if they are missing.
	desc = catalogkv.TestingGetMutableExistingTableDescriptor(s.DB(), c, "defaultdb", "t")
	require.Equal(t, desc.PrimaryIndex.ConstraintID, descpb.ConstraintID(2))
	// If we set both the constraint ID / next value to 0, then we will have
	// it assigned form scratch.
	desc.PrimaryIndex.ConstraintID = 0
	desc.NextConstraintID = 0
	require.NoError(t, s.DB().Put(
		context.Background(),
		catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, desc.GetID()),
		desc.DescriptorProto(),
	))
	// Validate that the descriptor is invalid, since the constraint IDs
	// are blanked out.
	txn := s.DB().NewTxn(ctx, "for-descriptors")
	tableID := desc.GetID()
	_, err := catalogkv.GetAllDescriptors(ctx, txn, c, false)
	require.Error(t, err, "failed to fetch all descriptors")
	// Migrate to the new cluster version.
	tdb.Exec(t, `SET CLUSTER SETTING version = $1`,
		clusterversion.ByKey(clusterversion.ConstraintIDsForTableDescs).String())
	tdb.CheckQueryResultsRetry(t, "SHOW CLUSTER SETTING version",
		[][]string{{clusterversion.ByKey(clusterversion.ConstraintIDsForTableDescs).String()}})
	// Fetch descriptors without the post serialization to validate the constraint
	// IDs were assigned in storage.
	txn = s.DB().NewTxn(ctx, "for-descriptors")
	allDescriptors, err := catalogkv.GetAllDescriptors(ctx, txn, c, false)
	require.NoError(t, err, "failed to fetch all descriptors")
	for _, desc := range allDescriptors {
		if desc.GetID() != tableID {
			continue
		}
		require.Equal(t, desc.(catalog.TableDescriptor).GetPrimaryIndex().GetConstraintID(), descpb.ConstraintID(1))
		require.Equal(t, desc.(catalog.TableDescriptor).GetNextConstraintID(), descpb.ConstraintID(2))
	}
	// Validate we can comment constraints.
	tdb.Exec(t,
		"COMMENT ON CONSTRAINT \"t_pkey\" ON t IS 'primary_comment'")
}
