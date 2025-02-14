// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
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
SET use_declarative_schema_changer = 'on';
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
`,
	)

	tDB.Exec(t, `
CREATE FUNCTION f(a notmyworkday) RETURNS INT VOLATILE LANGUAGE SQL AS $$
  SELECT a FROM t;
  SELECT b FROM t@t_idx_b;
  SELECT c FROM t@t_idx_c;
  SELECT a FROM v;
  SELECT nextval('sq1');
$$;
`)

	tDB.Exec(t, `CREATE SCHEMA test_sc;`)

	err := sqltestutils.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		funcDesc, err := col.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Function(ctx, 110)
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
		_, tbl, err := descs.PrefixAndTable(ctx, col.ByNameWithLeased(txn.KV()).Get(), &tn)
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
		_, seq, err := descs.PrefixAndTable(ctx, col.ByNameWithLeased(txn.KV()).Get(), &sqn)
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
		_, view, err := descs.PrefixAndTable(ctx, col.ByNameWithLeased(txn.KV()).Get(), &vn)
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
		_, typ, err := descs.PrefixAndType(ctx, col.ByNameWithLeased(txn.KV()).Get(), &typn)
		require.NoError(t, err)
		require.Equal(t, "notmyworkday", typ.GetName())
		require.Equal(t, 1, typ.NumReferencingDescriptors())
		require.Equal(t, descpb.ID(110), typ.GetReferencingDescriptorID(0))

		return nil
	})
	require.NoError(t, err)
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
		_, tbl, err := descs.PrefixAndTable(ctx, col.ByNameWithLeased(txn).Get(), &tn)
		require.NoError(t, err)
		require.Equal(t,
			[]descpb.TableDescriptor_Reference{{ID: 112, IndexID: 2, ColumnIDs: []catid.ColumnID{2}}},
			tbl.GetDependedOnBy())

		// Make sure sequence has correct back references.
		sqn := tree.MakeTableNameWithSchema("defaultdb", "public", tree.Name("sq"+nonEmptyRelID))
		_, seq, err := descs.PrefixAndTable(ctx, col.ByNameWithLeased(txn).Get(), &sqn)
		require.NoError(t, err)
		require.Equal(t, []descpb.TableDescriptor_Reference{{ID: 112, ByID: true}}, seq.GetDependedOnBy())

		// Make sure view has empty back references.
		vn := tree.MakeTableNameWithSchema("defaultdb", "public", tree.Name("v"+nonEmptyRelID))
		_, view, err := descs.PrefixAndTable(ctx, col.ByNameWithLeased(txn).Get(), &vn)
		require.NoError(t, err)
		require.Equal(t,
			[]descpb.TableDescriptor_Reference{{ID: 112, ColumnIDs: []catid.ColumnID{1}}},
			view.GetDependedOnBy())

		// Make sure columns and indexes has empty back references.
		tn = tree.MakeTableNameWithSchema("defaultdb", "public", tree.Name("t"+emptyRelID))
		_, tbl, err = descs.PrefixAndTable(ctx, col.ByNameWithLeased(txn).Get(), &tn)
		require.NoError(t, err)
		require.Nil(t, tbl.GetDependedOnBy())

		// Make sure sequence has empty back references.
		sqn = tree.MakeTableNameWithSchema("defaultdb", "public", tree.Name("sq"+emptyRelID))
		_, seq, err = descs.PrefixAndTable(ctx, col.ByNameWithLeased(txn).Get(), &sqn)
		require.NoError(t, err)
		require.Nil(t, seq.GetDependedOnBy())

		// Make sure view has emtpy back references.
		vn = tree.MakeTableNameWithSchema("defaultdb", "public", tree.Name("v"+emptyRelID))
		_, view, err = descs.PrefixAndTable(ctx, col.ByNameWithLeased(txn).Get(), &vn)
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
CREATE FUNCTION f(a notmyworkday) RETURNS INT VOLATILE LANGUAGE SQL AS $$
  SELECT b FROM t1@t1_idx_b;
  SELECT a FROM v1;
  SELECT nextval('sq1');
$$;
`,
	)

	err := sqltestutils.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		funcDesc, err := col.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Function(ctx, 112)
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
		_, typ, err := descs.PrefixAndType(ctx, col.ByNameWithLeased(txn.KV()).Get(), &typn)
		require.NoError(t, err)
		require.Equal(t, 1, typ.NumReferencingDescriptors())
		require.Equal(t, descpb.ID(112), typ.GetReferencingDescriptorID(0))

		// All objects with "1" suffix should have back references to the function,
		// "2" should have empty references since it's not used yet.
		validateReferences(ctx, txn.KV(), col, "1", "2")
		return nil
	})
	require.NoError(t, err)

	// Replace the function body with another group of objects and make sure
	// references are modified correctly.
	tDB.Exec(t, `
CREATE OR REPLACE FUNCTION f(a notmyworkday) RETURNS INT VOLATILE LANGUAGE SQL AS $$
  SELECT b FROM t2@t2_idx_b;
  SELECT a FROM v2;
  SELECT nextval('sq2');
$$;
`)

	err = sqltestutils.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		funcDesc, err := col.ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Function(ctx, 112)
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
		_, typ, err := descs.PrefixAndType(ctx, col.ByNameWithLeased(txn.KV()).Get(), &typn)
		require.NoError(t, err)
		require.Equal(t, 1, typ.NumReferencingDescriptors())
		require.Equal(t, descpb.ID(112), typ.GetReferencingDescriptorID(0))

		// Now all objects with "2" suffix in name should have back references "1"
		// had before, and "1" should have empty references.
		validateReferences(ctx, txn.KV(), col, "2", "1")
		return nil
	})
	require.NoError(t, err)
}

func TestCreateFunctionVisibilityInExplicitTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testingKnob := &scexec.TestingKnobs{}
	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLDeclarativeSchemaChanger: testingKnob,
		},
	})
	defer s.Stopper().Stop(ctx)
	tDB := sqlutils.MakeSQLRunner(sqlDB)

	tDB.Exec(t, `SET use_declarative_schema_changer = 'unsafe_always'`)
	tDB.Exec(t, `CREATE TABLE t (a INT PRIMARY KEY, b INT NOT NULL)`)
	tDB.Exec(t, `INSERT INTO t VALUES (1,1), (2,1)`)

	// Make sure that everything is rolled back if post commit job fails.
	_, err := sqlDB.Exec(`
BEGIN;
SET LOCAL autocommit_before_ddl = false;
CREATE FUNCTION f() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$;
CREATE UNIQUE INDEX idx ON t(b);
COMMIT;
`)
	require.Error(t, err, "")
	require.Contains(t, err.Error(), "transaction committed but schema change aborted")
	_, err = sqlDB.Exec(`SELECT f()`)
	require.Error(t, err, "")
	require.Contains(t, err.Error(), "unknown function: f()")

	// Make data valid for the unique index so that the job won't fail.
	tDB.Exec(t, `DELETE FROM t WHERE a = 2`)

	// Make sure function cannot be used before job completes.
	testingKnob.RunBeforeBackfill = func() error {
		_, err = sqlDB.Exec(`SELECT f()`)
		require.Error(t, err, "")
		require.Contains(t, err.Error(), `function "f" is being added`)
		return nil
	}

	//tDB.Exec(t, `SET CLUSTER SETTING jobs.debug.pausepoints='newschemachanger.before.exec'`)
	_, err = sqlDB.Exec(`
BEGIN;
SET LOCAL autocommit_before_ddl = false;
CREATE FUNCTION f() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$;
CREATE UNIQUE INDEX idx ON t(b);
COMMIT;
`)
	tDB.Exec(t, `SELECT f()`)
}
