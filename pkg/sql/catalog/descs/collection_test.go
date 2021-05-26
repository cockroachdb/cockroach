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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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

// TestTxnClearsCollectionOnRetry tests that descs.Txn() correctly clears the
// state of its associated descs.Collection on a retry error at commit time.
// Regression test for #51197.
func TestTxnClearsCollectionOnRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	const txnName = "descriptor update"
	haveInjectedRetry := false
	var serverArgs base.TestServerArgs
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: func(ctx context.Context, r roachpb.BatchRequest) *roachpb.Error {
			if r.Txn == nil || r.Txn.Name != txnName {
				return nil
			}
			if _, ok := r.GetArg(roachpb.EndTxn); ok {
				if !haveInjectedRetry {
					haveInjectedRetry = true
					// Force a retry error the first time.
					return roachpb.NewError(roachpb.NewTransactionRetryError(roachpb.RETRY_REASON_UNKNOWN, "injected error"))
				}
			}
			return nil
		},
	}
	tc := testcluster.StartTestCluster(t, 1, params)
	defer tc.Stopper().Stop(ctx)

	s := tc.Server(0)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `USE db`)
	tdb.Exec(t, `CREATE SCHEMA schema`)
	tdb.Exec(t, `CREATE TABLE db.schema.table()`)
	tn := tree.MakeTableNameWithSchema("db", "schema", "table")

	err := descs.Txn(
		ctx,
		s.ClusterSettings(),
		s.LeaseManager().(*lease.Manager),
		s.InternalExecutor().(sqlutil.InternalExecutor),
		s.DB(),
		func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
			txn.SetDebugName(txnName)

			flags := tree.ObjectLookupFlagsWithRequired()
			flags.RequireMutable = true
			desc, err := descriptors.GetMutableTableDescriptor(ctx, txn, &tn, flags)
			require.NoError(t, err)
			// Verify that the descriptor version is always 1 prior to the write and 2
			// after the write even after a retry.
			require.Equal(t, descpb.DescriptorVersion(1), desc.Version)
			require.NoError(t, descriptors.WriteDesc(ctx, false /* kvTrace */, desc, txn))
			require.Equal(t, descpb.DescriptorVersion(2), desc.Version)
			return nil
		},
	)
	require.NoError(t, err)
}

// Regression test to ensure that resolving a type descriptor which is not a
// type using the DistSQLTypeResolver is properly handled.
func TestDistSQLTypeResolver_GetTypeDescriptor_WrongType(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	s := tc.Server(0)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, `CREATE TABLE t()`)
	var id descpb.ID
	tdb.QueryRow(t, "SELECT ($1)::regclass::int", "t").Scan(&id)

	err := descs.Txn(
		ctx,
		s.ClusterSettings(),
		s.LeaseManager().(*lease.Manager),
		s.InternalExecutor().(sqlutil.InternalExecutor),
		s.DB(),
		func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
			tr := descs.NewDistSQLTypeResolver(descriptors, txn)
			_, _, err := tr.GetTypeDescriptor(ctx, id)
			return err
		})
	require.Regexp(t, `descriptor \d+ is a relation not a type`, err)
	require.Equal(t, pgcode.WrongObjectType, pgerror.GetPGCode(err))
}

// TestMaybeFixSchemaPrivilegesIntegration ensures that schemas that have
// invalid privileges have their privilege descriptors fixed on read-time when
// grabbing the descriptor.
func TestMaybeFixSchemaPrivilegesIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	conn, err := db.Conn(ctx)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, `
CREATE DATABASE test;
USE test;
CREATE SCHEMA schema;
CREATE USER testuser;
GRANT CREATE ON SCHEMA schema TO testuser;
CREATE TABLE schema.t(x INT);
`)
	require.NoError(t, err)

	require.NoError(
		t,
		descs.Txn(
			ctx,
			s.ClusterSettings(),
			s.LeaseManager().(*lease.Manager),
			s.InternalExecutor().(sqlutil.InternalExecutor),
			kvDB,
			func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
				// descCols.GetMutableDatabaseDescriptor sometimes cannot
				// resolve the MutableDesc from the ImmutableDesc.
				dbDesc := catalogkv.TestingGetDatabaseDescriptor(kvDB, keys.SystemSQLCodec, "test")

				_, schemaDesc, err := descsCol.ResolveSchema(
					ctx, txn, dbDesc.GetID(), "schema", tree.SchemaLookupFlags{Required: true},
				)
				if err != nil {
					return err
				}
				// Write garbage privileges into the schema desc.
				privs := schemaDesc.Desc.GetPrivileges()
				for i := range privs.Users {
					// SELECT is valid on a database but not a schema, however
					// due to issue #65697, after running ALTER DATABASE ...
					// CONVERT TO SCHEMA, schemas could end up with
					// SELECT on it's privilege descriptor. This test
					// mimics a schema that was originally a database.
					// We want to ensure the schema's privileges are fixed
					// on read.
					privs.Users[i].Privileges |= privilege.SELECT.Mask()
				}

				mut := schemadesc.NewMutableExisting(*schemaDesc.Desc.SchemaDesc())
				return descsCol.WriteDesc(ctx, false, mut, txn)
			}),
	)

	// Make sure using the schema is fine and we don't encounter a
	// privilege validation error.
	_, err = db.Query("USE test; GRANT USAGE ON SCHEMA schema TO testuser;")
	require.NoError(t, err)
}
