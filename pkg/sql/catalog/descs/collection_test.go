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
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/assert"
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
	descriptors := s0.ExecutorConfig().(sql.ExecutorConfig).CollectionFactory.
		NewCollection(ctx, nil /* TemporarySchemaProvider */)

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
				ID:                  1,
				Name:                "pk",
				KeyColumnIDs:        []descpb.ColumnID{1},
				KeyColumnNames:      []string{"a"},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				EncodingType:        descpb.PrimaryIndexEncoding,
				Version:             descpb.PrimaryIndexWithStoredColumnsVersion,
				ConstraintID:        1,
			},
			Privileges:       catpb.NewBasePrivilegeDescriptor(security.AdminRoleName()),
			NextColumnID:     2,
			NextConstraintID: 2,
			NextFamilyID:     1,
			NextIndexID:      2,
			NextMutationID:   1,
			FormatVersion:    descpb.InterleavedFormatVersion,
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

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	err := sql.DescsTxn(ctx, &execCfg, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {
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
	execCfg := s0.ExecutorConfig().(sql.ExecutorConfig)
	t.Run("database descriptors", func(t *testing.T) {
		require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
			flags := tree.DatabaseLookupFlags{}
			flags.RequireMutable = true
			flags.Required = true

			db, err := descriptors.GetMutableDatabaseByName(ctx, txn, "db", flags)
			require.NoError(t, err)

			resolved, err := descriptors.GetMutableDatabaseByName(ctx, txn, "db", flags)
			require.NoError(t, err)

			require.Same(t, db, resolved)

			byID, err := descriptors.GetMutableDescriptorByID(ctx, txn, db.GetID())
			require.NoError(t, err)
			require.Same(t, db, byID)

			mut := db
			mut.MaybeIncrementVersion()
			mut.Schemas["foo"] = descpb.DatabaseDescriptor_SchemaInfo{ID: 2, Dropped: true}

			flags.RequireMutable = false

			immByName, err := descriptors.GetImmutableDatabaseByName(ctx, txn, "db", flags)
			require.NoError(t, err)
			require.Equal(t, mut.OriginalVersion(), immByName.GetVersion())

			_, immByID, err := descriptors.GetImmutableDatabaseByID(ctx, txn, db.GetID(), flags)
			require.NoError(t, err)
			require.Same(t, immByName, immByID)

			mut.Name = "new_name"

			// Don't write the descriptor, just write the namespace entry.
			// This will mean that resolution still is based on the old name.
			b := &kv.Batch{}
			b.CPut(catalogkeys.MakeDatabaseNameKey(lm.Codec(), mut.Name), mut.GetID(), nil)
			err = txn.Run(ctx, b)
			require.NoError(t, err)

			// Try to get the database descriptor by the new name and fail.
			failedToResolve, err := descriptors.GetImmutableDatabaseByName(ctx, txn, "new_name", flags)
			require.Regexp(t, `database "new_name" does not exist`, err)
			require.Nil(t, failedToResolve)

			// Try to get the database descriptor by the old name and succeed but get
			// the old version with the old name because the new version has not yet
			// been written.
			immResolvedWithNewNameButHasOldName, err := descriptors.GetImmutableDatabaseByName(ctx, txn, "db", flags)
			require.NoError(t, err)
			require.Same(t, immByID, immResolvedWithNewNameButHasOldName)

			require.NoError(t, descriptors.AddUncommittedDescriptor(mut))

			immByNameAfter, err := descriptors.GetImmutableDatabaseByName(ctx, txn, "new_name", flags)
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
		require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
			flags := tree.SchemaLookupFlags{}
			flags.RequireMutable = true
			flags.Required = true

			db, err := descriptors.GetMutableDatabaseByName(ctx, txn, "db", flags)
			require.NoError(t, err)

			schema, err := descriptors.GetMutableSchemaByName(ctx, txn, db, "sc", flags)
			require.NoError(t, err)
			require.NotNil(t, schema)

			resolved, err := descriptors.GetMutableSchemaByName(ctx, txn, db, "sc", flags)
			require.NoError(t, err)
			require.NotNil(t, schema)

			require.Same(t, schema, resolved)

			byID, err := descriptors.GetMutableDescriptorByID(ctx, txn, schema.GetID())
			require.NoError(t, err)

			require.Same(t, schema, byID)
			return nil
		}))
	})
	t.Run("table descriptors", func(t *testing.T) {
		require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
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

			byID, err := descriptors.GetMutableDescriptorByID(ctx, txn, tab.GetID())
			require.NoError(t, err)

			require.Same(t, tab, byID)
			return nil
		}))
	})
	t.Run("type descriptors", func(t *testing.T) {
		require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
			flags := tree.ObjectLookupFlags{}
			flags.RequireMutable = true
			flags.Required = true
			tn := tree.MakeQualifiedTypeName("db", "sc", "typ")
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

	execCfg := s0.ExecutorConfig().(sql.ExecutorConfig)
	require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
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
func TestDistSQLTypeResolver_GetTypeDescriptor_FromTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	s := tc.Server(0)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, `CREATE TABLE t(a INT PRIMARY KEY, b STRING)`)
	var id descpb.ID
	tdb.QueryRow(t, "SELECT $1::regclass::int", "t").Scan(&id)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	var name tree.TypeName
	var typedesc catalog.TypeDescriptor
	err := sql.DescsTxn(ctx, &execCfg, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {
		tr := descs.NewDistSQLTypeResolver(descriptors, txn)
		var err error
		name, typedesc, err = tr.GetTypeDescriptor(ctx, id)
		return err
	})
	require.NoError(t, err)
	require.Equal(t, "t", name.ObjectName.String())
	typ, err := typedesc.MakeTypesT(ctx, &name, nil)
	require.NoError(t, err)
	require.Equal(t, types.TupleFamily, typ.Family())
	require.Equal(t, "t", typ.TypeMeta.Name.Name)
	require.Equal(t, []string{"a", "b"}, typ.TupleLabels())
	require.Equal(t, types.IntFamily, typ.TupleContents()[0].Family())
	require.Equal(t, types.StringFamily, typ.TupleContents()[1].Family())
	require.Equal(t, oid.Oid(id+100000), typ.Oid())
}

// TestMaybeFixSchemaPrivilegesIntegration ensures that schemas that have
// invalid privileges have their privilege descriptors fixed on read-time when
// grabbing the descriptor.
func TestMaybeFixSchemaPrivilegesIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	conn, err := db.Conn(ctx)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, `
CREATE DATABASE test;
CREATE SCHEMA test.schema;
CREATE USER testuser;
GRANT CREATE ON SCHEMA test.schema TO testuser;
CREATE TABLE test.schema.t(x INT);
`)
	require.NoError(t, err)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		dbDesc, err := descsCol.GetImmutableDatabaseByName(
			ctx, txn, "test", tree.DatabaseLookupFlags{Required: true},
		)
		if err != nil {
			return err
		}
		schemaDesc, err := descsCol.GetMutableSchemaByName(ctx, txn, dbDesc, "schema", tree.SchemaLookupFlags{Required: true})
		if err != nil {
			return err
		}
		// Write garbage privileges into the schema desc.
		privs := schemaDesc.GetPrivileges()
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

		descsCol.SkipValidationOnWrite()
		return descsCol.WriteDesc(ctx, false, schemaDesc.(catalog.MutableDescriptor), txn)
	}),
	)

	// Make sure using the schema is fine and we don't encounter a
	// privilege validation error.
	_, err = db.Query("GRANT USAGE ON SCHEMA test.schema TO testuser;")
	require.NoError(t, err)
}

// TestCollectionPreservesPostDeserializationChanges ensures that when
// descriptors are retrieved from the collection and in need of post-
// deserialization changes, that the fact that those changes happened
// is preserved in both the mutable and immutable forms of the descriptor.
func TestCollectionPreservesPostDeserializationChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, "CREATE DATABASE db")
	tdb.Exec(t, "CREATE SCHEMA db.sc")
	tdb.Exec(t, "CREATE TYPE db.sc.typ AS ENUM ('a')")
	tdb.Exec(t, "CREATE TABLE db.sc.tab (c db.sc.typ)")
	var dbID, scID, typID, tabID descpb.ID
	const q = "SELECT id FROM system.namespace WHERE name = $1"
	tdb.QueryRow(t, q, "db").Scan(&dbID)
	tdb.QueryRow(t, q, "sc").Scan(&scID)
	tdb.QueryRow(t, q, "typ").Scan(&typID)
	tdb.QueryRow(t, q, "tab").Scan(&tabID)

	// Make some bespoke modifications to each of the descriptors to make sure
	// they'd need post-deserialization changes.
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
		ctx context.Context, txn *kv.Txn, col *descs.Collection,
	) error {
		descs, err := col.GetMutableDescriptorsByID(ctx, txn, dbID, scID, typID, tabID)
		if err != nil {
			return err
		}
		// Remove the grant option, this will ensure we get some
		// post-deserialization changes. These grant options should always exist
		// for admin and root.
		b := txn.NewBatch()
		for _, d := range descs {
			p := d.GetPrivileges()
			for i := range p.Users {
				p.Users[i].WithGrantOption = 0
			}
			if err := col.WriteDescToBatch(ctx, false, d, b); err != nil {
				return err
			}
		}
		return txn.Run(ctx, b)
	}))
	require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
		ctx context.Context, txn *kv.Txn, col *descs.Collection,
	) error {
		immuts, err := col.GetImmutableDescriptorsByID(ctx, txn, tree.CommonLookupFlags{
			Required: true,
		}, dbID, scID, typID, tabID)
		if err != nil {
			return err
		}
		for _, d := range immuts {
			assert.True(t, d.GetPostDeserializationChanges().
				Contains(catalog.UpgradedPrivileges))
		}

		muts, err := col.GetMutableDescriptorsByID(ctx, txn, dbID, scID, typID, tabID)
		if err != nil {
			return err
		}
		for _, d := range muts {
			assert.True(t, d.GetPostDeserializationChanges().
				Contains(catalog.UpgradedPrivileges))
		}

		return nil
	}))
}
