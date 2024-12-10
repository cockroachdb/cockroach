// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descs_test

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
)

func TestCollectionWriteDescToBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `USE db`)
	tdb.Exec(t, `CREATE SCHEMA schema`)
	tdb.Exec(t, `CREATE TABLE db.schema.table()`)

	descriptors := s.ExecutorConfig().(sql.ExecutorConfig).CollectionFactory.NewCollection(ctx)

	// Note this transaction abuses the mechanisms normally required for updating
	// tables and is just for testing what this test intends to exercise.
	require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		defer descriptors.ReleaseAll(ctx)
		tn := tree.MakeTableNameWithSchema("db", "schema", "table")
		_, mut, err := descs.PrefixAndMutableTable(ctx, descriptors.MutableByName(txn), &tn)
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
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
				EncodingType:        catenumpb.PrimaryIndexEncoding,
				Version:             descpb.LatestIndexDescriptorVersion,
				ConstraintID:        1,
			},
			Privileges:       catpb.NewBasePrivilegeDescriptor(username.AdminRoleName()),
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
		_, mut2, err := descs.PrefixAndMutableTable(ctx, descriptors.MutableByName(txn), &tn)
		require.NoError(t, err)
		require.Equal(t, mut, mut2)

		t2n := tree.MakeTableNameWithSchema("db", "schema", "table2")
		_, newTableResolved, err := descs.PrefixAndMutableTable(ctx, descriptors.MutableByName(txn), &t2n)
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
	var serverArgs base.TestServerArgs
	filterFunc, _ := testutils.TestingRequestFilterRetryTxnWithPrefix(t, txnName, 1)
	serverArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: filterFunc,
	}
	srv, sqlDB, _ := serverutils.StartServer(t, serverArgs)
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `USE db`)
	tdb.Exec(t, `CREATE SCHEMA schema`)
	tdb.Exec(t, `CREATE TABLE db.schema.table()`)
	tn := tree.MakeTableNameWithSchema("db", "schema", "table")

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	err := sql.DescsTxn(ctx, &execCfg, func(
		ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
	) error {
		txn.KV().SetDebugName(txnName)
		_, mut, err := descs.PrefixAndMutableTable(ctx, descriptors.MutableByName(txn.KV()), &tn)
		require.NoError(t, err)
		// Verify that the descriptor version is always 1 prior to the write and 2
		// after the write even after a retry.
		require.Equal(t, descpb.DescriptorVersion(1), mut.Version)
		require.NoError(t, descriptors.WriteDesc(ctx, false /* kvTrace */, mut, txn.KV()))
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
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, "CREATE DATABASE db")
	tdb.Exec(t, "USE db")
	tdb.Exec(t, "CREATE SCHEMA db.sc")
	tdb.Exec(t, "CREATE TABLE db.sc.tab (i INT PRIMARY KEY)")
	tdb.Exec(t, "CREATE TYPE db.sc.typ AS ENUM ('foo')")
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	t.Run("database descriptors", func(t *testing.T) {
		require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
			ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
		) error {
			mut, err := descriptors.MutableByName(txn.KV()).Database(ctx, "db")
			require.NoError(t, err)

			dbID := mut.GetID()
			byID, err := descriptors.MutableByID(txn.KV()).Desc(ctx, dbID)
			require.NoError(t, err)
			require.Same(t, mut, byID)

			mut.MaybeIncrementVersion()
			mut.Name = "new_name"

			// Check that changes to the mutable descriptor don't impact the
			// collection until they're added as uncommitted
			immByName, err := descriptors.ByNameWithLeased(txn.KV()).Get().Database(ctx, "db")
			require.NoError(t, err)
			require.Equal(t, dbID, immByName.GetID())
			require.Equal(t, mut.OriginalVersion(), immByName.GetVersion())

			immByID, err := descriptors.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, dbID)
			require.NoError(t, err)
			require.Same(t, immByName, immByID)

			// Don't write the descriptor, just write the namespace entry.
			b := txn.KV().NewBatch()
			err = descriptors.InsertNamespaceEntryToBatch(ctx, false /* kvTrace */, mut, b)
			require.NoError(t, err)
			err = txn.KV().Run(ctx, b)
			require.NoError(t, err)

			// Should be able to get the database descriptor by the new name.
			resolved, err := descriptors.ByNameWithLeased(txn.KV()).Get().Database(ctx, "new_name")
			require.Nil(t, err)
			require.Equal(t, dbID, resolved.GetID())

			// Try to get the database descriptor by the old name and succeed but get
			// the old version with the old name because the new version has not yet
			// been written.
			immResolvedWithNewNameButHasOldName, err := descriptors.ByNameWithLeased(txn.KV()).Get().Database(ctx, "db")
			require.NoError(t, err)
			require.Same(t, immByID, immResolvedWithNewNameButHasOldName)

			require.NoError(t, descriptors.AddUncommittedDescriptor(ctx, mut))

			immByNameAfter, err := descriptors.ByNameWithLeased(txn.KV()).Get().Database(ctx, "new_name")
			require.NoError(t, err)
			require.Equal(t, mut.GetVersion(), immByNameAfter.GetVersion())
			require.Equal(t, mut.ImmutableCopy().DescriptorProto(), immByNameAfter.DescriptorProto())

			immByIDAfter, err := descriptors.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, dbID)
			require.NoError(t, err)
			require.Same(t, immByNameAfter, immByIDAfter)

			// The name must be non-empty.
			_, err = descriptors.ByNameWithLeased(txn.KV()).Get().Database(ctx, "")
			require.Equal(t, sqlerrors.ErrEmptyDatabaseName, err)

			return nil
		}))
	})
	t.Run("schema descriptors", func(t *testing.T) {
		require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
			ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
		) error {
			db, err := descriptors.MutableByName(txn.KV()).Database(ctx, "db")
			require.NoError(t, err)

			schema, err := descriptors.MutableByName(txn.KV()).Schema(ctx, db, "sc")
			require.NoError(t, err)
			require.NotNil(t, schema)

			resolved, err := descriptors.MutableByName(txn.KV()).Schema(ctx, db, "sc")
			require.NoError(t, err)
			require.NotNil(t, schema)

			require.Same(t, schema, resolved)

			byID, err := descriptors.MutableByID(txn.KV()).Desc(ctx, schema.GetID())
			require.NoError(t, err)

			require.Same(t, schema, byID)
			return nil
		}))
	})
	t.Run("table descriptors", func(t *testing.T) {
		require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
			ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
		) error {
			tn := tree.MakeTableNameWithSchema("db", "sc", "tab")

			_, tab, err := descs.PrefixAndMutableTable(ctx, descriptors.MutableByName(txn.KV()), &tn)
			require.NoError(t, err)

			_, resolved, err := descs.PrefixAndMutableTable(ctx, descriptors.MutableByName(txn.KV()), &tn)
			require.NoError(t, err)

			require.Same(t, tab, resolved)

			byID, err := descriptors.MutableByID(txn.KV()).Desc(ctx, tab.GetID())
			require.NoError(t, err)

			require.Same(t, tab, byID)
			return nil
		}))
	})
	t.Run("type descriptors", func(t *testing.T) {
		require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
			ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
		) error {
			tn := tree.MakeQualifiedTypeName("db", "sc", "typ")
			_, typ, err := descs.PrefixAndMutableType(ctx, descriptors.MutableByName(txn.KV()), &tn)
			require.NoError(t, err)

			_, resolved, err := descs.PrefixAndMutableType(ctx, descriptors.MutableByName(txn.KV()), &tn)
			require.NoError(t, err)

			require.Same(t, typ, resolved)

			byID, err := descriptors.MutableByID(txn.KV()).Type(ctx, typ.GetID())
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
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `USE db`)
	tdb.Exec(t, `CREATE TABLE tbl(foo INT)`)
	row := tdb.QueryRow(t, `SELECT 'tbl'::regclass::int`)
	var tableID descpb.ID
	row.Scan(&tableID)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
		ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
	) error {
		// Resolve the descriptor so we can mutate it.
		tn := tree.MakeTableNameWithSchema("db", catconstants.PublicSchemaName, "tbl")
		_, desc, err := descs.PrefixAndTable(ctx, descriptors.ByNameWithLeased(txn.KV()).MaybeGet(), &tn)
		require.NotNil(t, desc)
		require.NoError(t, err)

		// Modify the column name.
		desc.TableDesc().Columns[0].Name = "bar"
		descriptors.SetSyntheticDescriptors([]catalog.Descriptor{desc})

		// Resolve the table by name again.
		_, desc, err = descs.PrefixAndTable(ctx, descriptors.ByNameWithLeased(txn.KV()).MaybeGet(), &tn)
		require.NotNil(t, desc)
		require.NoError(t, err)
		require.Equal(t, "bar", desc.PublicColumns()[0].GetName())

		// Attempting to resolve the table mutably is not allowed.
		_, _, err = descs.PrefixAndMutableTable(ctx, descriptors.MutableByName(txn.KV()), &tn)
		require.EqualError(t, err, fmt.Sprintf("attempted mutable access of synthetic descriptor %d", tableID))

		// Resolution by ID.

		desc, err = descriptors.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, tableID)
		require.NoError(t, err)
		require.Equal(t, "bar", desc.PublicColumns()[0].GetName())

		// Attempting to resolve the table mutably is not allowed.
		_, err = descriptors.MutableByID(txn.KV()).Table(ctx, tableID)
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
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `CREATE TABLE t(a INT PRIMARY KEY, b STRING)`)
	var id descpb.ID
	tdb.QueryRow(t, "SELECT $1::regclass::int", "t").Scan(&id)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	var name tree.TypeName
	var tdesc catalog.TypeDescriptor
	err := sql.DescsTxn(ctx, &execCfg, func(
		ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
	) error {
		tr := descs.NewDistSQLTypeResolver(descriptors, txn.KV())
		var err error
		name, tdesc, err = tr.GetTypeDescriptor(ctx, id)
		return err
	})
	require.NoError(t, err)
	require.Equal(t, "t", name.ObjectName.String())
	typ, err := typedesc.HydratedTFromDesc(ctx, &name, tdesc, nil /* res */)
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
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	_, err := sqlDB.ExecContext(ctx, `
CREATE DATABASE test;
CREATE SCHEMA test.schema;
CREATE USER testuser;
GRANT CREATE ON SCHEMA test.schema TO testuser;
CREATE TABLE test.schema.t(x INT);
`)
	require.NoError(t, err)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
		ctx context.Context, txn isql.Txn, descsCol *descs.Collection,
	) error {
		dbDesc, err := descsCol.ByNameWithLeased(txn.KV()).Get().Database(ctx, "test")
		if err != nil {
			return err
		}
		schemaDesc, err := descsCol.MutableByName(txn.KV()).Schema(ctx, dbDesc, "schema")
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
		return descsCol.WriteDesc(ctx, false, schemaDesc, txn.KV())
	}),
	)

	// Make sure using the schema is fine and we don't encounter a
	// privilege validation error.
	_, err = sqlDB.Query("GRANT USAGE ON SCHEMA test.schema TO testuser;")
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
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

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
		ctx context.Context, txn isql.Txn, col *descs.Collection,
	) error {
		descs, err := col.MutableByID(txn.KV()).Descs(ctx, []descpb.ID{dbID, scID, typID, tabID})
		if err != nil {
			return err
		}
		// Set version lower than minimum to force post-deserialization change.
		b := txn.KV().NewBatch()
		for _, d := range descs {
			p := d.GetPrivileges()
			p.SetVersion(catpb.Version21_2 - 1)
			if err := col.WriteDescToBatch(ctx, false, d, b); err != nil {
				return err
			}
		}
		return txn.KV().Run(ctx, b)
	}))
	require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
		ctx context.Context, txn isql.Txn, col *descs.Collection,
	) error {
		immuts, err := col.ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Descs(ctx, []descpb.ID{dbID, scID, typID, tabID})
		if err != nil {
			return err
		}
		for _, d := range immuts {
			p := d.GetPrivileges()
			require.NotEqual(t, catpb.Version21_2-1, p.Version)
			if !d.GetPostDeserializationChanges().Contains(catalog.UpgradedPrivileges) {
				t.Errorf("immutable %s %q (%d) missing post-deserialization change flag",
					d.DescriptorType(), d.GetName(), d.GetID())
			}
		}
		return nil
	}))
	require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
		ctx context.Context, txn isql.Txn, col *descs.Collection,
	) error {
		muts, err := col.MutableByID(txn.KV()).Descs(ctx, []descpb.ID{dbID, scID, typID, tabID})
		if err != nil {
			return err
		}
		for _, d := range muts {
			if !d.GetPostDeserializationChanges().Contains(catalog.UpgradedPrivileges) {
				t.Errorf("mutable %s %q (%d) missing post-deserialization change flag",
					d.DescriptorType(), d.GetName(), d.GetID())
			}
		}
		return nil
	}))

}

// TestCollectionProperlyUsesMemoryMonitoring ensures that memory monitoring
// on Collection is working properly.
// Namely, we are currently only tracking memory usage on Collection.storedDescriptors
// since it reads all descriptors from storage, which can be huge.
//
// The testing strategy is to
//  1. Create tables that are very large into the database (so that when we read them
//     into memory later with Collection, a lot of memory will be allocated and used).
//  2. Hook up a monitor with infinite budget to this Collection and invoke method
//     so that this Collection reads all the descriptors into memory. With an unlimited
//     monitor, this should succeed without error.
//  3. Change the monitor budget to something small. Repeat step 2 and expect an error
//     being thrown out when reading all those descriptors into memory to validate the
//     memory monitor indeed kicked in and had an effect.
func TestCollectionProperlyUsesMemoryMonitoring(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	txn := kvDB.NewTxn(ctx, "test txn")

	// Create a lot of descriptors.
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	numTblsToInsert := 100
	for i := 0; i < numTblsToInsert; i++ {
		tdb.Exec(t, fmt.Sprintf("CREATE TABLE table_%v()", i))
	}

	// Create a monitor to be used to track memory usage in a Collection.
	monitor := mon.NewMonitor(mon.Options{
		Name:     mon.MakeMonitorName("test_monitor"),
		Settings: cluster.MakeTestingClusterSettings(),
	})

	// Start the monitor with unlimited budget.
	monitor.Start(ctx, nil, mon.NewStandaloneBudget(math.MaxInt64))

	// Create a `Collection` with monitor hooked up.
	col := s.ExecutorConfig().(sql.ExecutorConfig).CollectionFactory.NewCollection(
		ctx, descs.WithMonitor(monitor),
	)
	require.Equal(t, int64(0), monitor.AllocBytes())

	// Read all the descriptors into `col` and assert this read will finish without error.
	_, err1 := col.GetAllDescriptors(ctx, txn)
	require.NoError(t, err1)

	// Clean up and assert the monitor's allocation is back to 0 properly after releasing.
	allocatedMemoryInBytes := monitor.AllocBytes()
	col.ReleaseAll(ctx)
	require.Equal(t, int64(0), monitor.AllocBytes())

	// Restart the monitor to a smaller budget (in fact, let's be bold by setting it to be only one byte below
	// what has been allocated in the previous round).
	monitor.Start(ctx, nil, mon.NewStandaloneBudget(allocatedMemoryInBytes-1))
	require.Equal(t, int64(0), monitor.AllocBytes())

	// Repeat the process again and assert this time memory allocation will err out.
	col = s.ExecutorConfig().(sql.ExecutorConfig).CollectionFactory.NewCollection(
		ctx, descs.WithMonitor(monitor),
	)
	_, err2 := col.GetAllDescriptors(ctx, txn)
	require.Error(t, err2)

	// Clean up
	col.ReleaseAll(ctx)
	require.Equal(t, int64(0), monitor.AllocBytes())
	monitor.Stop(ctx)
}

// TestDescriptorCache ensures that when descriptors are modified, a batch
// lookup on the Collection views the latest changes.
func TestDescriptorCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `USE db`)
	tdb.Exec(t, `CREATE SCHEMA schema`)
	tdb.Exec(t, `CREATE TABLE db.schema.table()`)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	t.Run("all descriptors", func(t *testing.T) {
		require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
			ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
		) error {
			descriptors.SkipValidationOnWrite()
			// Warm up cache.
			_, err := descriptors.GetAllDescriptors(ctx, txn.KV())
			if err != nil {
				return err
			}
			// Modify table descriptor.
			tn := tree.MakeTableNameWithSchema("db", "schema", "table")
			_, mut, err := descs.PrefixAndMutableTable(ctx, descriptors.MutableByName(txn.KV()), &tn)
			if err != nil {
				return err
			}
			require.NotNil(t, mut)
			mut.Name = "new_name"
			mut.Version++
			err = descriptors.AddUncommittedDescriptor(ctx, mut)
			if err != nil {
				return err
			}
			// The collection's all descriptors should include the modification.
			cat, err := descriptors.GetAllDescriptors(ctx, txn.KV())
			if err != nil {
				return err
			}
			found := cat.LookupDescriptor(mut.ID)
			require.NotEmpty(t, found)
			require.Equal(t, mut.ImmutableCopy().DescriptorProto(), found.DescriptorProto())
			return nil
		}))
	})
	t.Run("all db descriptors", func(t *testing.T) {
		require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
			ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
		) error {
			descriptors.SkipValidationOnWrite()
			// Warm up cache.
			dbDescs, err := descriptors.GetAllDatabaseDescriptors(ctx, txn.KV())
			if err != nil {
				return err
			}
			require.Len(t, dbDescs, 4)
			// Modify database descriptor.
			mut, err := descriptors.MutableByName(txn.KV()).Database(ctx, "db")
			if err != nil {
				return err
			}
			require.NotNil(t, mut)
			mut.Version++
			err = descriptors.AddUncommittedDescriptor(ctx, mut)
			if err != nil {
				return err
			}
			// The collection's all database descriptors should reflect the
			// modification.
			dbDescs, err = descriptors.GetAllDatabaseDescriptors(ctx, txn.KV())
			if err != nil {
				return err
			}
			require.Len(t, dbDescs, 4)
			require.Equal(t, mut.ImmutableCopy().DescriptorProto(), dbDescs[0].DescriptorProto())
			return nil
		}))
	})
	t.Run("schemas for database", func(t *testing.T) {
		require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
			ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
		) error {
			descriptors.SkipValidationOnWrite()
			// Warm up cache.
			dbDesc, err := descriptors.MutableByName(txn.KV()).Database(ctx, "db")
			if err != nil {
				return err
			}
			_, err = descriptors.GetSchemasForDatabase(ctx, txn.KV(), dbDesc)
			if err != nil {
				return err
			}
			// Modify schema name.
			var db catalog.DatabaseDescriptor = dbDesc
			schemaDesc, err := descriptors.MutableByName(txn.KV()).Schema(ctx, db, "schema")
			if err != nil {
				return err
			}
			schemaDesc.Name = "new_name"
			schemaDesc.Version++
			delete(dbDesc.Schemas, "schema")
			dbDesc.Schemas["new_name"] = descpb.DatabaseDescriptor_SchemaInfo{ID: schemaDesc.ID}
			dbDesc.Version++
			err = descriptors.AddUncommittedDescriptor(ctx, schemaDesc)
			if err != nil {
				return err
			}
			err = descriptors.AddUncommittedDescriptor(ctx, dbDesc)
			if err != nil {
				return err
			}
			// The collection's schemas for database should reflect the modification.
			schemas, err := descriptors.GetSchemasForDatabase(ctx, txn.KV(), dbDesc)
			if err != nil {
				return err
			}
			require.Len(t, schemas, 2)
			require.Equal(t, schemaDesc.Name, schemas[schemaDesc.ID])
			return nil
		}))
	})
}

// TestAllDescriptorsInDatabase ensures that the appropriate descriptors
// are returned from GetAllDescriptorsInDatabase.
func TestGetAllDescriptorsInDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `USE db`)
	tdb.Exec(t, `CREATE SCHEMA schema`)
	tdb.Exec(t, `CREATE TABLE db.public.table()`)
	tdb.Exec(t, `CREATE TABLE db.schema.table()`)

	tm := s.InternalDB().(descs.DB)

	run := func(
		ctx context.Context, txn descs.Txn,
	) error {
		descriptors := txn.Descriptors()
		dbDesc, err := descriptors.ByName(txn.KV()).Get().Database(ctx, "db")
		if err != nil {
			return err
		}
		allDescs, err := txn.Descriptors().GetAllDescriptorsForDatabase(ctx, txn.KV(), dbDesc)
		if err != nil {
			return err
		}
		require.Equal(t, `
parent schema name   id  kind     version dropped public
0      0      db     104 database 2       false   true
104    0      public 105 schema   1       false   true
104    0      schema 106 schema   1       false   true
104    105    table  107 relation 1       false   true
104    106    table  108 relation 1       false   true
`, formatCatalog(allDescs.OrderedDescriptors()))
		for _, stmt := range []string{
			`ALTER SCHEMA schema RENAME TO sc`,
			`CREATE SCHEMA sc_foo`,
			`DROP TABLE public.table`,
			`CREATE TABLE sc.foo()`,
			`CREATE FUNCTION f() RETURNS INT LANGUAGE SQL IMMUTABLE AS $$ SELECT 1 $$`,
			`CREATE FUNCTION sc_foo.f() RETURNS INT LANGUAGE SQL IMMUTABLE AS $$ SELECT 1 $$`,
		} {
			if _, err = txn.Exec(ctx, "test", txn.KV(), stmt); err != nil {
				return err
			}
		}
		allDescs, err = txn.Descriptors().GetAllDescriptorsForDatabase(ctx, txn.KV(), dbDesc)
		if err != nil {
			return err
		}
		require.Equal(t, `
parent schema name   id  kind     version dropped public
0      0      db     104 database 3       false   true
104    0      public 105 schema   2       false   true
104    0      sc     106 schema   2       false   true
104    0      sc_foo 109 schema   1       false   true
104    105    f      111 function 1       false   true
104    105    table  107 relation 2       true    false
104    106    foo    110 relation 1       false   true
104    106    table  108 relation 1       false   true
104    109    f      112 function 1       false   true
`, formatCatalog(allDescs.OrderedDescriptors()))
		return nil
	}
	sd := sql.NewInternalSessionData(ctx, s.ClusterSettings(), "TestGetAllDescriptorsInDatabase")
	sd.Database = "db"
	require.NoError(t, tm.DescsTxn(ctx, run, isql.WithSessionData(sd)))
}

// formatDescriptors formats descriptors into a text string
// for testing.
func formatCatalog(descs []catalog.Descriptor) string {
	sort.Slice(descs, func(i, j int) bool {
		return nameEntryLess(descs[i], descs[j])
	})
	var buf strings.Builder
	_ = buf.WriteByte('\n')
	tr := tabwriter.NewWriter(&buf, 3, 2, 1, ' ', 0)
	_, _ = fmt.Fprint(tr,
		"parent\tschema\tname\tid\tkind\tversion\tdropped\tpublic\n",
	)
	for _, d := range descs {
		if d.DescriptorType() != catalog.Database && d.GetParentID() == descpb.InvalidID {
			// Skip virtual schemas and virtual tables.
			continue
		}
		_, _ = fmt.Fprintf(tr,
			"%d\t%d\t%s\t%d\t%s\t%d\t%v\t%v\n",
			d.GetParentID(), d.GetParentSchemaID(), d.GetName(),
			d.GetID(), d.DescriptorType(), d.GetVersion(),
			d.Dropped(), d.Public(),
		)
	}
	_ = tr.Flush()
	return buf.String()
}

func nameEntryLess(di, dj catalog.NameEntry) bool {
	if pi, pj := di.GetParentID(), dj.GetParentID(); pi != pj {
		return pi < pj
	}
	if psi, psj := di.GetParentSchemaID(), dj.GetParentSchemaID(); psi != psj {
		return psi < psj
	}
	if ni, nj := di.GetName(), dj.GetName(); ni != nj {
		return ni < nj
	}
	return di.GetID() < dj.GetID()
}

// TestCollectionTimeTravelLookingTooFarBack encodes expected behavior from the
// Collection when performing historical queries.
func TestCollectionTimeTravelLookingTooFarBack(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `USE db`)
	tdb.Exec(t, `CREATE SCHEMA schema`)
	tdb.Exec(t, `CREATE TABLE db.schema.table()`)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	goFarBackInTime := func(fn func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error) error {
		return sql.DescsTxn(ctx, &execCfg, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
			veryFarBack := execCfg.Clock.Now().Add(-1000*time.Hour.Nanoseconds(), 0)
			if err := txn.KV().SetFixedTimestamp(ctx, veryFarBack); err != nil {
				return err
			}
			return fn(ctx, txn.KV(), col)
		})
	}

	t.Run("full scan returns nothing", func(t *testing.T) {
		require.NoError(t, goFarBackInTime(func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
			c, err := col.GetAllDescriptors(ctx, txn)
			if err != nil {
				return err
			}
			require.Empty(t, c.OrderedDescriptors())
			return nil
		}))
	})
	t.Run("db scan returns nothing", func(t *testing.T) {
		require.NoError(t, goFarBackInTime(func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
			c, err := col.GetAllDatabaseDescriptors(ctx, txn)
			if err != nil {
				return err
			}
			require.Empty(t, c)
			return nil
		}))
	})
}

// TestHydrateCatalog verifies that hydrateCatalog hydrates the type metadata
// in a catalog's table descriptors.
func TestHydrateCatalog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `USE db`)
	tdb.Exec(t, `CREATE SCHEMA schema`)
	tdb.Exec(t, `CREATE TYPE db.schema.typ AS ENUM ('a', 'b')`)
	tdb.Exec(t, `CREATE TYPE db.schema.ctyp AS (a INT, b TEXT)`)
	tdb.Exec(t, `CREATE TABLE db.schema.table(x db.schema.typ, y db.schema.ctyp)`)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	t.Run("invalid catalog", func(t *testing.T) {
		type catalogTamperFn func(nstree.Catalog) nstree.Catalog
		deleteDescriptor := func(name string) catalogTamperFn {
			return func(cat nstree.Catalog) nstree.Catalog {
				var descToDelete catid.DescID
				_ = cat.ForEachDescriptor(func(desc catalog.Descriptor) error {
					if desc.GetName() == name {
						descToDelete = desc.GetID()
					}
					return nil
				})
				mutCat := nstree.MutableCatalog{Catalog: cat}
				mutCat.DeleteByID(descToDelete)
				return mutCat.Catalog
			}
		}
		replaceTypeDescWithNonTypeDesc := func(name string) catalogTamperFn {
			return func(cat nstree.Catalog) nstree.Catalog {
				var typeDescID catid.DescID
				_ = cat.ForEachDescriptor(func(desc catalog.Descriptor) error {
					if desc.GetName() == name {
						typeDescID = desc.GetID()
					}
					return nil
				})
				// Make a dummy database descriptor to replace the type descriptor.
				dbDesc := dbdesc.NewBuilder(&descpb.DatabaseDescriptor{ID: typeDescID}).BuildImmutable()
				mutCat := nstree.MutableCatalog{Catalog: cat}
				mutCat.UpsertDescriptor(dbDesc)
				return mutCat.Catalog
			}
		}
		type testCase struct {
			tamper        catalogTamperFn
			expectedError string
		}
		for _, tc := range []testCase{
			{deleteDescriptor("typ"), "type \"[107]\" does not exist"},
			{deleteDescriptor("ctyp"), "type \"[109]\" does not exist"},
			{deleteDescriptor("db"), "database \"[104]\" does not exist"},
			{deleteDescriptor("schema"), "unknown schema \"[106]\""},
			{replaceTypeDescWithNonTypeDesc("typ"), "referenced type ID 107: descriptor is a *dbdesc." + "immutable: unexpected descriptor type"},
			{replaceTypeDescWithNonTypeDesc("ctyp"), "referenced type ID 109: descriptor is a *dbdesc." + "" + "immutable: unexpected descriptor type"},
		} {
			require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
				ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
			) error {
				cat, err := descriptors.GetAllFromStorageUnvalidated(ctx, txn.KV())
				if err != nil {
					return err
				}
				// Hydration should fail when the given catalog is invalid.
				cat = tc.tamper(cat)
				mc := nstree.MutableCatalog{Catalog: cat}
				require.EqualError(t, descs.HydrateCatalog(ctx, mc), tc.expectedError)
				return nil
			}))
		}
	})
	t.Run("valid catalog", func(t *testing.T) {
		require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
			ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
		) error {
			cat, err := descriptors.GetAllFromStorageUnvalidated(ctx, txn.KV())
			if err != nil {
				return err
			}
			mc := nstree.MutableCatalog{Catalog: cat}
			require.NoError(t, descs.HydrateCatalog(ctx, mc))
			tbl := desctestutils.TestingGetTableDescriptor(txn.KV().DB(), s.Codec(), "db", "schema", "table")
			tblDesc := cat.LookupDescriptor(tbl.GetID()).(catalog.TableDescriptor)
			expectedEnum := types.UserDefinedTypeMetadata{
				Name: &types.UserDefinedTypeName{
					Catalog:        "db",
					ExplicitSchema: true,
					Schema:         "schema",
					Name:           "typ",
				},
				Version: 2,
				EnumData: &types.EnumMetadata{
					PhysicalRepresentations: [][]uint8{{0x40}, {0x80}},
					LogicalRepresentations:  []string{"a", "b"},
					IsMemberReadOnly:        []bool{false, false},
				},
			}
			expectedComposite := types.UserDefinedTypeMetadata{
				Name: &types.UserDefinedTypeName{
					Catalog:        "db",
					ExplicitSchema: true,
					Schema:         "schema",
					Name:           "ctyp",
				},
				Version: 2,
			}
			actualEnum := tblDesc.UserDefinedTypeColumns()[0].GetType().TypeMeta
			actualComposite := tblDesc.UserDefinedTypeColumns()[1].GetType()
			// Verify that the table descriptor was hydrated.
			require.Equal(t, expectedEnum, actualEnum)
			require.Equal(t, expectedComposite, actualComposite.TypeMeta)
			require.Equal(t, []*types.T{types.Int, types.String}, actualComposite.TupleContents())
			require.Equal(t, []string{"a", "b"}, actualComposite.TupleLabels())
			return nil
		}))
	})
}

// TestSyntheticDescriptors ensures that synthetic descriptors show up in
// AllDescriptors and its peers and that the act of adding synthetic
// descriptors clears the current modifications.
func TestSyntheticDescriptors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY)")
	tdb.Exec(t, "CREATE SCHEMA sc")
	tdb.Exec(t, `CREATE DATABASE "otherDB"`)

	var tabID, scID, curDatabaseID, otherDatabaseID descpb.ID
	tdb.QueryRow(t, "SELECT 'foo'::regclass::int").Scan(&tabID)
	tdb.QueryRow(t, `
SELECT id
  FROM system.namespace
 WHERE name = 'sc' AND "parentSchemaID" = 0`).Scan(&scID)
	tdb.QueryRow(t, `
SELECT id
  FROM system.namespace
 WHERE name = current_database() AND "parentID" = 0`).Scan(&curDatabaseID)
	tdb.QueryRow(t, `
SELECT id
  FROM system.namespace
 WHERE name = 'otherDB' AND "parentID" = 0`).Scan(&otherDatabaseID)

	ec := s.ExecutorConfig().(sql.ExecutorConfig)
	codec := ec.Codec
	descIDGen := ec.DescIDGenerator
	require.NoError(t, ec.InternalDB.DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) error {
		descriptors := txn.Descriptors()
		checkImmutableDescriptor := func(id descpb.ID, expName string, f func(t *testing.T, desc catalog.Descriptor)) error {
			tabImm, err := descriptors.ByIDWithLeased(txn.KV()).WithoutOffline().Get().Table(ctx, id)
			require.NoError(t, err)
			require.Equal(t, expName, tabImm.GetName())
			f(t, tabImm)
			all, err := txn.Descriptors().GetAllDescriptors(ctx, txn.KV())
			if err != nil {
				return err
			}
			require.Equal(t, tabImm, all.LookupDescriptor(tabImm.GetID()))
			return nil
		}

		// Modify the table to have the name "bar", synthetically
		{
			tab, err := descriptors.MutableByID(txn.KV()).Table(ctx, tabID)
			if err != nil {
				return err
			}
			tab = tabledesc.NewBuilder(tab.TableDesc()).BuildCreatedMutableTable()
			tab.Name = "bar"
			tab.SetDropped()
			txn.Descriptors().AddSyntheticDescriptor(tab)
		}
		// Retrieve the immutable descriptor, find the name "bar"
		if err := checkImmutableDescriptor(tabID, "bar", func(t *testing.T, desc catalog.Descriptor) {
			require.True(t, desc.Dropped())
			require.False(t, desc.Public())
		}); err != nil {
			return err
		}
		// Attempt to retrieve the mutable descriptor, validate the error.
		_, err := descriptors.MutableByID(txn.KV()).Table(ctx, tabID)
		require.Regexp(t, `attempted mutable access of synthetic descriptor \d+`, err)
		txn.Descriptors().ResetSyntheticDescriptors()
		// Retrieve the mutable descriptor, find the unmodified "foo".
		// Then modify the name to "baz" and write it.
		{
			tabMut, err := descriptors.MutableByID(txn.KV()).Table(ctx, tabID)
			require.NoError(t, err)
			require.Equal(t, "foo", tabMut.GetName())
			tabMut.Name = "baz"
			if _, err := txn.KV().Del(ctx, catalogkeys.EncodeNameKey(codec, &descpb.NameInfo{
				ParentID:       tabMut.GetParentID(),
				ParentSchemaID: tabMut.GetParentSchemaID(),
				Name:           tabMut.OriginalName(),
			})); err != nil {
				return err
			}
			if err := txn.KV().Put(ctx, catalogkeys.EncodeNameKey(codec, tabMut), int64(tabMut.ID)); err != nil {
				return err
			}
			const kvTrace = false
			if err := txn.Descriptors().WriteDesc(ctx, kvTrace, tabMut, txn.KV()); err != nil {
				return err
			}
		}
		// Retrieve the immutable descriptor, find the modified "baz".
		if err := checkImmutableDescriptor(tabID, "baz", func(t *testing.T, desc catalog.Descriptor) {
			require.True(t, desc.Public())
		}); err != nil {
			return err
		}

		// Make a synthetic database descriptor, ensure it shows up in all
		// descriptors.
		newDBID, err := descIDGen.GenerateUniqueDescID(ctx)
		require.NoError(t, err)
		newDB := dbdesc.NewInitial(newDBID, "newDB", username.RootUserName())
		txn.Descriptors().AddSyntheticDescriptor(newDB)

		curDatabase, err := descriptors.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, curDatabaseID)
		if err != nil {
			return err
		}

		// Check that AllDatabaseDescriptors includes the synthetic database.
		{
			allDBs, err := txn.Descriptors().GetAllDatabaseDescriptors(ctx, txn.KV())
			require.NoError(t, err)
			require.ElementsMatch(t, []string{
				"system", "postgres", "defaultdb", "newDB", "otherDB",
			}, getDatabaseNames(allDBs))
		}

		{
			defaultDBSchemaNames, err := txn.Descriptors().GetSchemasForDatabase(ctx, txn.KV(), curDatabase)
			if err != nil {
				return err
			}
			require.Equal(t, []string{"public", "sc"},
				getSchemaNames(defaultDBSchemaNames))
		}

		// Rename a schema synthetically, make sure that that propagates.
		scDesc, err := descriptors.MutableByID(txn.KV()).Schema(ctx, scID)
		if err != nil {
			return err
		}
		scDesc.SetName("sc2")
		scDesc.Version++
		require.NoError(t, txn.Descriptors().AddUncommittedDescriptor(ctx, scDesc))
		newSchema, _, err := sql.CreateSchemaDescriptorWithPrivileges(ctx, descIDGen,
			curDatabase, "newSC", username.RootUserName(), username.RootUserName(), true)
		require.NoError(t, err)
		txn.Descriptors().AddSyntheticDescriptor(newSchema)

		{
			defaultDBSchemaNames, err := txn.Descriptors().GetSchemasForDatabase(ctx, txn.KV(), curDatabase)
			if err != nil {
				return err
			}
			require.Equal(t, []string{"newSC", "public", "sc2"},
				getSchemaNames(defaultDBSchemaNames))
		}

		// Rename schema back to old name to prevent validation failure on commit.
		scDesc.SetName("sc")
		require.NoError(t, txn.Descriptors().AddUncommittedDescriptor(ctx, scDesc))
		return nil
	}))
}

func TestDescriptorErrorWrap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `USE db`)
	tdb.Exec(t, `CREATE SCHEMA schema`)
	tdb.Exec(t, `CREATE TABLE db.schema.table()`)

	monitor := mon.NewMonitor(mon.Options{
		Name:     mon.MakeMonitorName("test_monitor"),
		Settings: cluster.MakeTestingClusterSettings(),
	})
	monitor.Start(ctx, nil, mon.NewStandaloneBudget(1))
	ba := monitor.MakeBoundAccount()

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	for _, tc := range []struct {
		desc        string
		err         error
		isAssertion bool
	}{
		{"bare error", errors.New("bare error is treated as an assertion"), true},
		{"out of memory", ba.Grow(ctx, monitor.Limit()), false},
		{"pgcode error", sqlerrors.NewAlterColTypeInCombinationNotSupportedError(), false},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
				ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
			) error {
				tn := tree.MakeTableNameWithSchema("db", "schema", "table")
				_, mut, err := descs.PrefixAndMutableTable(ctx, descriptors.MutableByName(txn.KV()), &tn)
				if err != nil {
					return err
				}

				require.False(t, errors.HasAssertionFailure(tc.err))
				err = descs.DecorateDescriptorError(mut, tc.err)
				// Ensure err is still an error
				require.Error(t, err)
				// Ensure descriptor info is wrapped in the error
				require.Contains(t, err.Error(), mut.GetName())
				// Ensure error is promoted to assertion as expected
				require.Equal(t, tc.isAssertion, errors.HasAssertionFailure(err))
				return nil
			}))
		})
	}
	monitor.Stop(ctx)
}

func getSchemaNames(defaultDBSchemaNames map[descpb.ID]string) []string {
	var names []string
	for _, name := range defaultDBSchemaNames {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func getDatabaseNames(allDBs []catalog.DatabaseDescriptor) []string {
	var names []string
	for _, db := range allDBs {
		names = append(names, db.GetName())
	}
	return names
}
