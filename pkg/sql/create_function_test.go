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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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

		// Make sure sequence has correct back references.
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

	// Test drop/rename behavior in legacy schema changer.
	tDB.Exec(t, "SET use_declarative_schema_changer = off")

	testCases := []struct {
		stmt        string
		expectedErr string
	}{
		{
			stmt:        "DROP SEQUENCE sq1",
			expectedErr: "pq: cannot drop sequence sq1 because other objects depend on it",
		},
		{
			stmt:        "DROP SEQUENCE sq1 CASCADE",
			expectedErr: "pq: unimplemented: drop function not supported",
		},
		{
			stmt:        "ALTER SEQUENCE sq1 RENAME TO sq1_new",
			expectedErr: "",
		},
		{
			stmt:        "DROP TABLE t",
			expectedErr: "pq: unimplemented: drop function not supported",
		},
		{
			stmt:        "DROP TABLE t CASCADE",
			expectedErr: "pq: unimplemented: drop function not supported",
		},
		{
			stmt:        "ALTER TABLE t RENAME TO t_new",
			expectedErr: "pq: unimplemented: drop function not supported",
		},
		{
			stmt:        "ALTER TABLE t SET SCHEMA test_sc",
			expectedErr: "pq: unimplemented: drop function not supported",
		},
		{
			stmt:        "ALTER TABLE t DROP COLUMN b",
			expectedErr: "pq: unimplemented: drop function not supported",
		},
		{
			stmt:        "ALTER TABLE t DROP COLUMN b CASCADE",
			expectedErr: "pq: unimplemented: drop function not supported",
		},
		{
			stmt:        "ALTER TABLE t RENAME COLUMN b TO bb",
			expectedErr: "pq: unimplemented: drop function not supported",
		},
		{
			stmt:        "ALTER TABLE t ALTER COLUMN b TYPE STRING",
			expectedErr: "pq: unimplemented: drop function not supported",
		},
		{
			stmt:        "DROP INDEX t@t_idx_b",
			expectedErr: "pq: unimplemented: drop function not supported",
		},
		{
			stmt:        "DROP INDEX t@t_idx_b CASCADE",
			expectedErr: "pq: unimplemented: drop function not supported",
		},
		{
			stmt:        "ALTER INDEX t@t_idx_b RENAME TO t_idx_b_new",
			expectedErr: "pq: unimplemented: drop function not supported",
		},
	}

	for _, tc := range testCases {
		_, err = sqlDB.Exec(tc.stmt)
		if tc.expectedErr == "" {
			require.NoError(t, err)
			continue
		}
		require.Equal(t, tc.expectedErr, err.Error())
	}

	// Test drop/rename behavior in declarative schema changer.
	tDB.Exec(t, "SET use_declarative_schema_changer = on")
	_, err = sqlDB.Exec("DROP TABLE t CASCADE ")
	require.Equal(t, "pq: unimplemented: function descriptor not supported in declarative schema changer", err.Error())
}

func TestCreateFunctionWithTableImplicitType(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tDB := sqlutils.MakeSQLRunner(sqlDB)

	tDB.Exec(t, `
CREATE TABLE t_implicit_type(
  a INT PRIMARY KEY,
  b STRING
);
CREATE FUNCTION f() RETURNS t_implicit_type IMMUTABLE LANGUAGE SQL AS $$
	SELECT 1, 'hello';
$$`,
	)

	err := sql.TestingDescsTxn(ctx, s, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
		funcDesc, err := col.GetImmutableFunctionByID(ctx, txn, 105, tree.ObjectLookupFlagsWithRequired())
		require.NoError(t, err)
		require.Equal(t, funcDesc.GetName(), "f")
		require.Equal(t, []descpb.ID{104}, funcDesc.GetDependsOn())
		require.Equal(t, []catid.DescID(nil), funcDesc.GetDependsOnTypes())

		// Make sure columns and indexes has correct back references.
		tn := tree.MakeTableNameWithSchema("defaultdb", "public", "t_implicit_type")
		_, tbl, err := col.GetImmutableTableByName(ctx, txn, &tn, tree.ObjectLookupFlagsWithRequired())
		require.NoError(t, err)
		require.Equal(t, "t_implicit_type", tbl.GetName())
		require.Equal(t,
			[]descpb.TableDescriptor_Reference{{ID: 105}},
			tbl.GetDependedOnBy(),
		)

		return nil
	})
	require.NoError(t, err)
}
