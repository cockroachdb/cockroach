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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
		s0.ClusterSettings(),
		s0.LeaseManager().(*lease.Manager),
		nil, // hydratedTables
		nil, // virtualSchemas
	)
	// Note this transaction abuses the mechanisms normally required for updating
	// tables and is just for testing what this test intends to exercise.
	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		defer descriptors.ReleaseAll(ctx)
		tn := tree.MakeTableNameWithSchema("db", "schema", "table")
		flags := tree.ObjectLookupFlagsWithRequired()
		flags.RequireMutable = true
		_, mut, err := descriptors.GetMutableTableByName(ctx, txn, &tn, flags)
		require.NoError(t, err)
		require.NotNil(t, mut)
		// We want to create some descriptors and then ensure that writing them to a
		// batch works as expected.
		newTable := tabledesc.NewBuilder(&descpb.TableDescriptor{
			ID:                      142,
			Name:                    "table2",
			Version:                 1,
			ParentID:                mut.GetParentID(),
			UnexposedParentSchemaID: mut.GetParentSchemaID(),
			Columns: []descpb.ColumnDescriptor{
				{ID: 1, Name: "a", Type: types.Int},
			},
			Families: []descpb.ColumnFamilyDescriptor{
				{
					ID:              0,
					Name:            "primary",
					ColumnNames:     []string{"a"},
					ColumnIDs:       []descpb.ColumnID{1},
					DefaultColumnID: 1,
				},
			},
			PrimaryIndex: descpb.IndexDescriptor{
				ID:               1,
				Name:             "pk",
				ColumnIDs:        []descpb.ColumnID{1},
				ColumnNames:      []string{"a"},
				ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
			},
			Privileges:     descpb.NewDefaultPrivilegeDescriptor(security.AdminRoleName()),
			NextColumnID:   2,
			NextFamilyID:   1,
			NextIndexID:    2,
			NextMutationID: 1,
			FormatVersion:  descpb.FamilyFormatVersion,
		}).BuildCreatedMutableTable()
		b := txn.NewBatch()

		// Ensure that there are no errors and that the version is incremented.
		require.NoError(t, descriptors.WriteDescToBatch(ctx, false /* kvTrace */, mut, b))
		require.Equal(t, descpb.DescriptorVersion(2), mut.Version)
		require.NoError(t, descriptors.WriteDescToBatch(ctx, false /* kvTrace */, newTable, b))
		require.Equal(t, descpb.DescriptorVersion(1), newTable.Version)

		// Ensure that the descriptor has been added to the collection.
		_, mut2, err := descriptors.GetMutableTableByName(ctx, txn, &tn, flags)
		require.NoError(t, err)
		require.Equal(t, mut, mut2)

		t2n := tree.MakeTableNameWithSchema("db", "schema", "table2")
		_, newTableResolved, err := descriptors.GetMutableTableByName(ctx, txn, &t2n, flags)
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
			_, mut, err := descriptors.GetMutableTableByName(ctx, txn, &tn, flags)
			require.NoError(t, err)
			// Verify that the descriptor version is always 1 prior to the write and 2
			// after the write even after a retry.
			require.Equal(t, descpb.DescriptorVersion(1), mut.Version)
			require.NoError(t, descriptors.WriteDesc(ctx, false /* kvTrace */, mut, txn))
			require.Equal(t, descpb.DescriptorVersion(2), mut.Version)
			return nil
		},
	)
	require.NoError(t, err)
}

// TestAddUncommittedDescriptorAndMutableResolution tests the collection to
// ensure that subsequent resolution of mutable descriptors yields the same
// object. It also ensures that immutable resolution only yields a modified
// immutable descriptor after a modified version has been explicitly added
// with AddUncommittedDescriptor.
func TestAddUncommittedDescriptorAndMutableResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	s0 := tc.Server(0)
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, "CREATE DATABASE db")
	tdb.Exec(t, "USE db")
	tdb.Exec(t, "CREATE SCHEMA db.sc")
	tdb.Exec(t, "CREATE TABLE db.sc.tab (i INT PRIMARY KEY)")
	tdb.Exec(t, "CREATE TYPE db.sc.typ AS ENUM ('foo')")
	lm := s0.LeaseManager().(*lease.Manager)
	ie := s0.InternalExecutor().(sqlutil.InternalExecutor)
	var dbID descpb.ID
	t.Run("database descriptors", func(t *testing.T) {
		require.NoError(t, descs.Txn(ctx, s0.ClusterSettings(), lm, ie, s0.DB(), func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
			flags := tree.DatabaseLookupFlags{}
			flags.RequireMutable = true
			flags.Required = true

			_, db, err := descriptors.GetMutableDatabaseByName(ctx, txn, "db", flags)
			require.NoError(t, err)
			dbID = db.GetID()

			_, resolved, err := descriptors.GetMutableDatabaseByName(ctx, txn, "db", flags)
			require.NoError(t, err)

			require.Same(t, db, resolved)

			byID, err := descriptors.GetMutableDescriptorByID(ctx, db.GetID(), txn)
			require.NoError(t, err)
			require.Same(t, db, byID)

			mut := db
			mut.MaybeIncrementVersion()
			mut.Schemas["foo"] = descpb.DatabaseDescriptor_SchemaInfo{ID: 2, Dropped: true}

			flags.RequireMutable = false

			_, immByName, err := descriptors.GetImmutableDatabaseByName(ctx, txn, "db", flags)
			require.NoError(t, err)
			require.Equal(t, mut.OriginalVersion(), immByName.GetVersion())

			_, immByID, err := descriptors.GetImmutableDatabaseByID(ctx, txn, db.GetID(), flags)
			require.NoError(t, err)
			require.Same(t, immByName, immByID)

			mut.Name = "new_name"
			mut.SetDrainingNames([]descpb.NameInfo{{
				Name: "db",
			}})

			b := &kv.Batch{}
			b.CPut(catalogkeys.NewDatabaseKey(mut.Name).Key(lm.Codec()), mut.GetID(), nil)
			err = txn.Run(ctx, b)
			require.NoError(t, err)

			// Try to get the database descriptor by the old name and fail.
			_, failedToResolve, err := descriptors.GetImmutableDatabaseByName(ctx, txn, "db", flags)
			require.Regexp(t, `database "db" does not exist`, err)
			require.Nil(t, failedToResolve)

			// Try to get the database descriptor by the new name and succeed but get
			// the old version with the old name (this is bizarre but is the
			// contract now).
			_, immResolvedWithNewNameButHasOldName, err := descriptors.GetImmutableDatabaseByName(ctx, txn, "new_name", flags)
			require.NoError(t, err)
			require.Same(t, immByID, immResolvedWithNewNameButHasOldName)

			require.NoError(t, descriptors.AddUncommittedDescriptor(mut))

			_, immByNameAfter, err := descriptors.GetImmutableDatabaseByName(ctx, txn, "new_name", flags)
			require.NoError(t, err)
			require.Equal(t, db.GetVersion(), immByNameAfter.GetVersion())
			require.Equal(t, mut.ImmutableCopy(), immByNameAfter)

			_, immByIDAfter, err := descriptors.GetImmutableDatabaseByID(ctx, txn, db.GetID(), flags)
			require.NoError(t, err)
			require.Same(t, immByNameAfter, immByIDAfter)

			return nil
		}))
	})
	t.Run("schema descriptors", func(t *testing.T) {
		require.NoError(t, descs.Txn(ctx, s0.ClusterSettings(), lm, ie, s0.DB(), func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
			flags := tree.SchemaLookupFlags{}
			flags.RequireMutable = true
			flags.Required = true

			ok, schema, err := descriptors.GetMutableSchemaByName(ctx, txn, dbID, "sc", flags)
			require.NoError(t, err)
			require.True(t, ok)

			ok, resolved, err := descriptors.GetMutableSchemaByName(ctx, txn, dbID, "sc", flags)
			require.NoError(t, err)
			require.True(t, ok)

			require.Same(t, schema.Desc, resolved.Desc)

			byID, err := descriptors.GetMutableDescriptorByID(ctx, schema.ID, txn)
			require.NoError(t, err)

			require.Same(t, schema.Desc, byID)
			return nil
		}))
	})
	t.Run("table descriptors", func(t *testing.T) {
		require.NoError(t, descs.Txn(ctx, s0.ClusterSettings(), lm, ie, s0.DB(), func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
			flags := tree.ObjectLookupFlags{}
			flags.RequireMutable = true
			flags.Required = true
			tn := tree.MakeTableNameWithSchema("db", "sc", "tab")

			_, tab, err := descriptors.GetMutableTableByName(ctx, txn, &tn, flags)
			require.NoError(t, err)

			_, resolved, err := descriptors.GetMutableTableByName(ctx, txn, &tn, flags)
			require.NoError(t, err)

			require.Same(t, tab, resolved)

			byID, err := descriptors.GetMutableDescriptorByID(ctx, tab.GetID(), txn)
			require.NoError(t, err)

			require.Same(t, tab, byID)
			return nil
		}))
	})
	t.Run("type descriptors", func(t *testing.T) {
		require.NoError(t, descs.Txn(ctx, s0.ClusterSettings(), lm, ie, s0.DB(), func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
			flags := tree.ObjectLookupFlags{}
			flags.RequireMutable = true
			flags.Required = true
			tn := tree.MakeNewQualifiedTypeName("db", "sc", "typ")
			_, typ, err := descriptors.GetMutableTypeByName(ctx, txn, &tn, flags)
			require.NoError(t, err)

			_, resolved, err := descriptors.GetMutableTypeByName(ctx, txn, &tn, flags)
			require.NoError(t, err)

			require.Same(t, typ, resolved)

			byID, err := descriptors.GetMutableTypeVersionByID(ctx, txn, typ.GetID())
			require.NoError(t, err)

			require.Same(t, typ, byID)

			return nil
		}))
	})
}

// TestSyntheticDescriptorResolution tests descriptor resolution when synthetic
// descriptors are injected into the collection.
func TestSyntheticDescriptorResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	s0 := tc.Server(0)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `USE db`)
	tdb.Exec(t, `CREATE TABLE tbl(foo INT)`)
	row := tdb.QueryRow(t, `SELECT 'tbl'::regclass::int`)
	var tableID descpb.ID
	row.Scan(&tableID)

	lm := s0.LeaseManager().(*lease.Manager)
	ie := s0.InternalExecutor().(sqlutil.InternalExecutor)
	require.NoError(t, descs.Txn(ctx, s0.ClusterSettings(), lm, ie, s0.DB(), func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {
		// Resolve the descriptor so we can mutate it.
		tn := tree.MakeTableNameWithSchema("db", tree.PublicSchemaName, "tbl")
		found, desc, err := descriptors.GetImmutableTableByName(ctx, txn, &tn, tree.ObjectLookupFlags{})
		require.True(t, found)
		require.NoError(t, err)

		// Modify the column name.
		desc.TableDesc().Columns[0].Name = "bar"
		descriptors.SetSyntheticDescriptors([]catalog.Descriptor{desc})

		// Resolve the table by name again.
		found, desc, err = descriptors.GetImmutableTableByName(ctx, txn, &tn, tree.ObjectLookupFlags{})
		require.True(t, found)
		require.NoError(t, err)
		require.Equal(t, "bar", desc.PublicColumns()[0].GetName())

		// Attempting to resolve the table mutably is not allowed.
		_, _, err = descriptors.GetMutableTableByName(ctx, txn, &tn, tree.ObjectLookupFlags{})
		require.EqualError(t, err, fmt.Sprintf("attempted mutable access of synthetic descriptor %d", tableID))

		// Resolution by ID.

		desc, err = descriptors.GetImmutableTableByID(ctx, txn, tableID, tree.ObjectLookupFlags{})
		require.NoError(t, err)
		require.Equal(t, "bar", desc.PublicColumns()[0].GetName())

		// Attempting to resolve the table mutably is not allowed.
		_, err = descriptors.GetMutableTableByID(ctx, txn, tableID, tree.ObjectLookupFlags{})
		require.EqualError(t, err, fmt.Sprintf("attempted mutable access of synthetic descriptor %d", tableID))

		return nil
	}),
	)
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
	tdb.QueryRow(t, "SELECT $1::regclass::int", "t").Scan(&id)

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
