// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCreateFunction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tDB := sqlutils.MakeSQLRunner(sqlDB)

	tDB.Exec(t, `
CREATE TABLE t(
  a INT PRIMARY KEY,
  b INT,
  C INT,
  INDEX t_idx_b(b),
  INDEX t_idx_c(c)
);
CREATE SEQUENCE sq1;
CREATE TABLE t2(a INT PRIMARY KEY);
CREATE VIEW v AS SELECT a FROM t2;
CREATE TYPE notmyworkday AS ENUM ('Monday', 'Tuesday');
CREATE FUNCTION f(a notmyworkday) RETURNS INT IMMUTABLE LANGUAGE SQL AS $$
  SELECT a FROM t;
  SELECT b FROM t@t_idx_b;
  SELECT c FROM t@t_idx_c;
  SELECT a FROM v;
  SELECT nextval('sq1');
$$;
CREATE SCHEMA test_sc;
`,
	)

	err := sql.TestingDescsTxn(ctx, s, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
		funcDesc, err := col.GetImmutableFunctionByID(ctx, txn, 110, tree.ObjectLookupFlagsWithRequired())
		require.NoError(t, err)
		require.Equal(t, funcDesc.GetName(), "f")

		require.Equal(t,
			`SELECT a FROM defaultdb.public.t;
SELECT b FROM defaultdb.public.t@t_idx_b;
SELECT c FROM defaultdb.public.t@t_idx_c;
SELECT a FROM defaultdb.public.v;
SELECT nextval(105:::REGCLASS);`,
			funcDesc.GetFunctionBody())

		sort.Slice(funcDesc.GetDependsOn(), func(i, j int) bool {
			return funcDesc.GetDependsOn()[i] < funcDesc.GetDependsOn()[j]
		})
		require.Equal(t,
			[]descpb.ID{104, 105, 107},
			funcDesc.GetDependsOn(),
		)
		sort.Slice(funcDesc.GetDependsOnTypes(), func(i, j int) bool {
			return funcDesc.GetDependsOnTypes()[i] < funcDesc.GetDependsOnTypes()[j]
		})
		require.Equal(t,
			[]descpb.ID{108, 109},
			funcDesc.GetDependsOnTypes(),
		)

		// Make sure columns and indexes has correct back references.
		tn := tree.MakeTableNameWithSchema("defaultdb", "public", "t")
		_, tbl, err := col.GetImmutableTableByName(ctx, txn, &tn, tree.ObjectLookupFlagsWithRequired())
		require.NoError(t, err)
		require.Equal(t, "t", tbl.GetName())
		require.Equal(t,
			[]descpb.TableDescriptor_Reference{
				{ID: 110, ColumnIDs: []catid.ColumnID{1}},
				{ID: 110, IndexID: 2, ColumnIDs: []catid.ColumnID{2}},
				{ID: 110, IndexID: 3, ColumnIDs: []catid.ColumnID{3}},
			},
			tbl.GetDependedOnBy(),
		)

		// Make sure sequence has correct back references.
		sqn := tree.MakeTableNameWithSchema("defaultdb", "public", "sq1")
		_, seq, err := col.GetImmutableTableByName(ctx, txn, &sqn, tree.ObjectLookupFlagsWithRequired())
		require.NoError(t, err)
		require.Equal(t, "sq1", seq.GetName())
		require.Equal(t,
			[]descpb.TableDescriptor_Reference{
				{ID: 110, ByID: true},
			},
			seq.GetDependedOnBy(),
		)

		// Make sure view has correct back references.
		vn := tree.MakeTableNameWithSchema("defaultdb", "public", "v")
		_, view, err := col.GetImmutableTableByName(ctx, txn, &vn, tree.ObjectLookupFlagsWithRequired())
		require.NoError(t, err)
		require.Equal(t, "v", view.GetName())
		require.Equal(t,
			[]descpb.TableDescriptor_Reference{
				{ID: 110, ColumnIDs: []catid.ColumnID{1}},
			},
			view.GetDependedOnBy(),
		)

		// Make sure type has correct back references.
		typn := tree.MakeQualifiedTypeName("defaultdb", "public", "notmyworkday")
		_, typ, err := col.GetImmutableTypeByName(ctx, txn, &typn, tree.ObjectLookupFlagsWithRequired())
		require.NoError(t, err)
		require.Equal(t, "notmyworkday", typ.GetName())
		require.Equal(t,
			[]descpb.ID{110},
			typ.GetReferencingDescriptorIDs(),
		)

		return nil
	})
	require.NoError(t, err)
}

func TestCreateFunctionGating(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("new_schema_changer_version_enabled", func(t *testing.T) {
		params, _ := tests.CreateTestServerParams()
		// Override binary version to be older.
		params.Knobs.Server = &server.TestingKnobs{
			DisableAutomaticVersionUpgrade: make(chan struct{}),
			BinaryVersionOverride:          clusterversion.ByKey(clusterversion.SchemaChangeSupportsCreateFunction),
		}

		s, sqlDB, _ := serverutils.StartServer(t, params)
		defer s.Stopper().Stop(context.Background())

		_, err := sqlDB.Exec(`CREATE FUNCTION f() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$`)
		require.NoError(t, err)
	})

	t.Run("new_schema_changer_version_disabled", func(t *testing.T) {
		params, _ := tests.CreateTestServerParams()
		// Override binary version to be older.
		params.Knobs.Server = &server.TestingKnobs{
			DisableAutomaticVersionUpgrade: make(chan struct{}),
			BinaryVersionOverride:          clusterversion.ByKey(clusterversion.SchemaChangeSupportsCreateFunction - 1),
		}

		s, sqlDB, _ := serverutils.StartServer(t, params)
		defer s.Stopper().Stop(context.Background())

		_, err := sqlDB.Exec(`CREATE FUNCTION f() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$`)
		require.Error(t, err)
		require.Equal(t, "pq: cannot run CREATE FUNCTION before system is fully upgraded to v22.2", err.Error())
	})
}

func TestCreateOrReplaceFunctionUpdateReferences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tDB := sqlutils.MakeSQLRunner(sqlDB)

	validateReferences := func(
		ctx context.Context, txn *kv.Txn, col *descs.Collection, nonEmptyRelID string, emptyRelID string,
	) {
		// Make sure columns and indexes has correct back references.
		tn := tree.MakeTableNameWithSchema("defaultdb", "public", tree.Name("t"+nonEmptyRelID))
		_, tbl, err := col.GetImmutableTableByName(ctx, txn, &tn, tree.ObjectLookupFlagsWithRequired())
		require.NoError(t, err)
		require.Equal(t,
			[]descpb.TableDescriptor_Reference{{ID: 112, IndexID: 2, ColumnIDs: []catid.ColumnID{2}}},
			tbl.GetDependedOnBy())

		// Make sure sequence has correct back references.
		sqn := tree.MakeTableNameWithSchema("defaultdb", "public", tree.Name("sq"+nonEmptyRelID))
		_, seq, err := col.GetImmutableTableByName(ctx, txn, &sqn, tree.ObjectLookupFlagsWithRequired())
		require.NoError(t, err)
		require.Equal(t, []descpb.TableDescriptor_Reference{{ID: 112, ByID: true}}, seq.GetDependedOnBy())

		// Make sure view has empty back references.
		vn := tree.MakeTableNameWithSchema("defaultdb", "public", tree.Name("v"+nonEmptyRelID))
		_, view, err := col.GetImmutableTableByName(ctx, txn, &vn, tree.ObjectLookupFlagsWithRequired())
		require.NoError(t, err)
		require.Equal(t,
			[]descpb.TableDescriptor_Reference{{ID: 112, ColumnIDs: []catid.ColumnID{1}}},
			view.GetDependedOnBy())

		// Make sure columns and indexes has empty back references.
		tn = tree.MakeTableNameWithSchema("defaultdb", "public", tree.Name("t"+emptyRelID))
		_, tbl, err = col.GetImmutableTableByName(ctx, txn, &tn, tree.ObjectLookupFlagsWithRequired())
		require.NoError(t, err)
		require.Nil(t, tbl.GetDependedOnBy())

		// Make sure sequence has empty back references.
		sqn = tree.MakeTableNameWithSchema("defaultdb", "public", tree.Name("sq"+emptyRelID))
		_, seq, err = col.GetImmutableTableByName(ctx, txn, &sqn, tree.ObjectLookupFlagsWithRequired())
		require.NoError(t, err)
		require.Nil(t, seq.GetDependedOnBy())

		// Make sure view has emtpy back references.
		vn = tree.MakeTableNameWithSchema("defaultdb", "public", tree.Name("v"+emptyRelID))
		_, view, err = col.GetImmutableTableByName(ctx, txn, &vn, tree.ObjectLookupFlagsWithRequired())
		require.NoError(t, err)
		require.Nil(t, view.GetDependedOnBy())
	}

	tDB.Exec(t, `
CREATE TABLE t1(a INT PRIMARY KEY, b INT, INDEX t1_idx_b(b));
CREATE TABLE t2(a INT PRIMARY KEY, b INT, INDEX t2_idx_b(b));
CREATE SEQUENCE sq1;
CREATE SEQUENCE sq2;
CREATE VIEW v1 AS SELECT 1;
CREATE VIEW v2 AS SELECT 2;
CREATE TYPE notmyworkday AS ENUM ('Monday', 'Tuesday');
CREATE FUNCTION f(a notmyworkday) RETURNS INT IMMUTABLE LANGUAGE SQL AS $$
  SELECT b FROM t1@t1_idx_b;
  SELECT a FROM v1;
  SELECT nextval('sq1');
$$;
`,
	)

	err := sql.TestingDescsTxn(ctx, s, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
		funcDesc, err := col.GetImmutableFunctionByID(ctx, txn, 112, tree.ObjectLookupFlagsWithRequired())
		require.NoError(t, err)
		require.Equal(t, funcDesc.GetName(), "f")

		require.Equal(t,
			`SELECT b FROM defaultdb.public.t1@t1_idx_b;
SELECT a FROM defaultdb.public.v1;
SELECT nextval(106:::REGCLASS);`,
			funcDesc.GetFunctionBody())

		sort.Slice(funcDesc.GetDependsOn(), func(i, j int) bool {
			return funcDesc.GetDependsOn()[i] < funcDesc.GetDependsOn()[j]
		})
		require.Equal(t, []descpb.ID{104, 106, 108}, funcDesc.GetDependsOn())
		sort.Slice(funcDesc.GetDependsOnTypes(), func(i, j int) bool {
			return funcDesc.GetDependsOnTypes()[i] < funcDesc.GetDependsOnTypes()[j]
		})
		require.Equal(t, []descpb.ID{110, 111}, funcDesc.GetDependsOnTypes())

		// Make sure type has correct back references.
		typn := tree.MakeQualifiedTypeName("defaultdb", "public", "notmyworkday")
		_, typ, err := col.GetImmutableTypeByName(ctx, txn, &typn, tree.ObjectLookupFlagsWithRequired())
		require.NoError(t, err)
		require.Equal(t, []descpb.ID{112}, typ.GetReferencingDescriptorIDs())

		// All objects with "1" suffix should have back references to the function,
		// "2" should have empty references since it's not used yet.
		validateReferences(ctx, txn, col, "1", "2")
		return nil
	})
	require.NoError(t, err)

	// Replace the function body with another group of objects and make sure
	// references are modified correctly.
	tDB.Exec(t, `
CREATE OR REPLACE FUNCTION f(a notmyworkday) RETURNS INT IMMUTABLE LANGUAGE SQL AS $$
  SELECT b FROM t2@t2_idx_b;
  SELECT a FROM v2;
  SELECT nextval('sq2');
$$;
`)

	err = sql.TestingDescsTxn(ctx, s, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
		flags := tree.ObjectLookupFlags{
			CommonLookupFlags: tree.CommonLookupFlags{
				Required:    true,
				AvoidLeased: true,
			},
		}
		funcDesc, err := col.GetImmutableFunctionByID(ctx, txn, 112, flags)
		require.NoError(t, err)
		require.Equal(t, funcDesc.GetName(), "f")

		require.Equal(t,
			`SELECT b FROM defaultdb.public.t2@t2_idx_b;
SELECT a FROM defaultdb.public.v2;
SELECT nextval(107:::REGCLASS);`,
			funcDesc.GetFunctionBody())

		sort.Slice(funcDesc.GetDependsOn(), func(i, j int) bool {
			return funcDesc.GetDependsOn()[i] < funcDesc.GetDependsOn()[j]
		})
		require.Equal(t, []descpb.ID{105, 107, 109}, funcDesc.GetDependsOn())
		sort.Slice(funcDesc.GetDependsOnTypes(), func(i, j int) bool {
			return funcDesc.GetDependsOnTypes()[i] < funcDesc.GetDependsOnTypes()[j]
		})
		require.Equal(t, []descpb.ID{110, 111}, funcDesc.GetDependsOnTypes())

		// Make sure type has correct back references.
		typn := tree.MakeQualifiedTypeName("defaultdb", "public", "notmyworkday")
		_, typ, err := col.GetImmutableTypeByName(ctx, txn, &typn, tree.ObjectLookupFlagsWithRequired())
		require.NoError(t, err)
		require.Equal(t, []descpb.ID{112}, typ.GetReferencingDescriptorIDs())

		// Now all objects with "2" suffix in name should have back references "1"
		// had before, and "1" should have empty references.
		validateReferences(ctx, txn, col, "2", "1")
		return nil
	})
	require.NoError(t, err)
}
