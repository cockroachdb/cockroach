// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descs_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCollectionWriteDescToBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	s0 := tc.Server(0)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `USE db`)
	tdb.Exec(t, `CREATE SCHEMA schema`)
	tdb.Exec(t, `CREATE TABLE db.schema.table()`)

	db := s0.DB()

	descriptors := descs.NewCollection(
		ctx,
		s0.ClusterSettings(),
		s0.LeaseManager().(*lease.Manager),
		nil, /* hydratedTables */
	)
	// Note this transaction abuses the mechanisms normally required for updating
	// tables and is just for testing what this test intends to exercise.
	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		defer descriptors.ReleaseAll(ctx)
		tn := tree.MakeTableNameWithSchema("db", "schema", "table")
		flags := tree.ObjectLookupFlagsWithRequired()
		flags.RequireMutable = true
		mut, err := descriptors.GetMutableTableDescriptor(ctx, txn, &tn, flags)
		require.NoError(t, err)
		require.NotNil(t, mut)
		// We want to create some descriptors and then ensure that writing them to a
		// batch works as expected.
		newTable := tabledesc.NewCreatedMutable(descpb.TableDescriptor{
			ID:                      42,
			Name:                    "table2",
			Version:                 1,
			ParentID:                mut.GetParentID(),
			UnexposedParentSchemaID: mut.GetParentSchemaID(),
		})
		b := txn.NewBatch()

		// Ensure that there are no errors and that the version is incremented.
		require.NoError(t, descriptors.WriteDescToBatch(ctx, false /* kvTrace */, mut, b))
		require.Equal(t, descpb.DescriptorVersion(2), mut.Version)
		require.NoError(t, descriptors.WriteDescToBatch(ctx, false /* kvTrace */, newTable, b))
		require.Equal(t, descpb.DescriptorVersion(1), newTable.Version)

		// Ensure that the descriptor has been added to the collection.
		mut2, err := descriptors.GetMutableTableDescriptor(ctx, txn, &tn, tree.ObjectLookupFlagsWithRequired())
		require.NoError(t, err)
		require.Equal(t, mut, mut2)

		t2n := tree.MakeTableNameWithSchema("db", "schema", "table2")
		newTableResolved, err := descriptors.GetMutableTableDescriptor(ctx, txn, &t2n, tree.ObjectLookupFlagsWithRequired())
		require.NoError(t, err)
		require.Equal(t, newTable, newTableResolved)
		return txn.Run(ctx, b)
	}))
}
