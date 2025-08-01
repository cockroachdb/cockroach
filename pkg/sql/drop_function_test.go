// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"regexp"
	"sort"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestDropFunction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestDoesNotWorkWithSecondaryTenantsButWeDontKnowWhyYet(107322),
	})
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
CREATE VIEW v AS SELECT a FROM t;
CREATE TYPE notmyworkday AS ENUM ('Monday', 'Tuesday');
CREATE FUNCTION f(a notmyworkday) RETURNS INT VOLATILE LANGUAGE SQL AS $$
  SELECT a FROM t;
  SELECT b FROM t@t_idx_b;
  SELECT c FROM t@t_idx_c;
  SELECT a FROM v;
  SELECT nextval('sq1');
$$;
CREATE SCHEMA test_sc;
`,
	)

	err := sqltestutils.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		funcDesc, err := col.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Function(ctx, 109)
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
			[]descpb.ID{104, 105, 106},
			funcDesc.GetDependsOn(),
		)
		sort.Slice(funcDesc.GetDependsOnTypes(), func(i, j int) bool {
			return funcDesc.GetDependsOnTypes()[i] < funcDesc.GetDependsOnTypes()[j]
		})
		require.Equal(t,
			[]descpb.ID{107, 108},
			funcDesc.GetDependsOnTypes(),
		)

		// Make sure columns and indexes has correct back references.
		tn := tree.MakeTableNameWithSchema("defaultdb", "public", "t")
		_, tbl, err := descs.PrefixAndTable(ctx, col.ByNameWithLeased(txn.KV()).Get(), &tn)
		require.NoError(t, err)
		require.Equal(t, "t", tbl.GetName())
		require.Equal(t,
			[]descpb.TableDescriptor_Reference{
				{ID: 106, ColumnIDs: []catid.ColumnID{1}},
				{ID: 109, ColumnIDs: []catid.ColumnID{1}},
				{ID: 109, IndexID: 2, ColumnIDs: []catid.ColumnID{2}},
				{ID: 109, IndexID: 3, ColumnIDs: []catid.ColumnID{3}},
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
				{ID: 109, ByID: true},
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
				{ID: 109, ColumnIDs: []catid.ColumnID{1}},
			},
			view.GetDependedOnBy(),
		)

		// Make sure type has correct back references.
		typn := tree.MakeQualifiedTypeName("defaultdb", "public", "notmyworkday")
		_, typ, err := descs.PrefixAndType(ctx, col.ByNameWithLeased(txn.KV()).Get(), &typn)
		require.NoError(t, err)
		require.Equal(t, "notmyworkday", typ.GetName())
		require.Equal(t, 1, typ.NumReferencingDescriptors())
		require.Equal(t, descpb.ID(109), typ.GetReferencingDescriptorID(0))

		return nil
	})
	require.NoError(t, err)

	// DROP the function and make sure dependencies are cleared.
	tDB.Exec(t, "DROP FUNCTION f")
	err = sqltestutils.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		_, err := col.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Function(ctx, 109)
		require.Error(t, err)
		require.Regexp(t, regexp.MustCompile(`function \d+ does not exist`), err.Error())

		// Make sure columns and indexes has correct back references.
		tn := tree.MakeTableNameWithSchema("defaultdb", "public", "t")
		_, tbl, err := descs.PrefixAndTable(ctx, col.ByNameWithLeased(txn.KV()).Get(), &tn)
		require.NoError(t, err)
		require.Equal(t,
			[]descpb.TableDescriptor_Reference{
				{ID: 106, ColumnIDs: []catid.ColumnID{1}},
			},
			tbl.GetDependedOnBy(),
		)

		// Make sure sequence has correct back references.
		sqn := tree.MakeTableNameWithSchema("defaultdb", "public", "sq1")
		_, seq, err := descs.PrefixAndTable(ctx, col.ByNameWithLeased(txn.KV()).Get(), &sqn)
		require.NoError(t, err)
		require.Nil(t, seq.GetDependedOnBy())

		// Make sure view has correct back references.
		vn := tree.MakeTableNameWithSchema("defaultdb", "public", "v")
		_, view, err := descs.PrefixAndTable(ctx, col.ByNameWithLeased(txn.KV()).Get(), &vn)
		require.NoError(t, err)
		require.Nil(t, view.GetDependedOnBy())

		// Make sure type has correct back references.
		typn := tree.MakeQualifiedTypeName("defaultdb", "public", "notmyworkday")
		_, typ, err := descs.PrefixAndType(ctx, col.ByNameWithLeased(txn.KV()).Get(), &typn)
		require.NoError(t, err)
		require.Zero(t, typ.NumReferencingDescriptors())

		return nil
	})
	require.NoError(t, err)
}

func TestDropFailOnDependentFunction(t *testing.T) {
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
  d INT,
  INDEX t_idx_b(b),
  INDEX t_idx_c(c)
);
CREATE SEQUENCE sq1;
CREATE TABLE t2(a INT PRIMARY KEY);
CREATE VIEW v AS SELECT a FROM t2;
CREATE TYPE notmyworkday AS ENUM ('Monday', 'Tuesday');
CREATE SCHEMA test_sc;
CREATE FUNCTION test_sc.f(a notmyworkday) RETURNS INT VOLATILE LANGUAGE SQL AS $$
  SELECT a FROM t;
  SELECT b FROM t@t_idx_b;
  SELECT c FROM t@t_idx_c;
  SELECT d FROM t;
  SELECT a FROM v;
  SELECT nextval('sq1');
$$;
CREATE DATABASE test_udf_db;
USE test_udf_db;
CREATE FUNCTION test_udf_db.public.f() RETURNS INT VOLATILE LANGUAGE SQL AS $$
	SELECT 1;
$$;
USE defaultdb;
`,
	)

	// Test drop/rename behavior in legacy schema changer.
	tDB.Exec(t, "SET use_declarative_schema_changer = off;")

	testCases := []struct {
		stmt           string
		expectedErr    string
		dscExpectedErr string
	}{
		{
			stmt:        "DROP SEQUENCE sq1",
			expectedErr: "pq: cannot drop sequence sq1 because other objects depend on it",
		},
		{
			stmt:           "DROP TABLE t",
			expectedErr:    `pq: cannot drop relation "t" because function "f" depends on it`,
			dscExpectedErr: `pq: cannot drop table t because other objects depend on it`,
		},
		{
			stmt:           "DROP VIEW v",
			expectedErr:    `pq: cannot drop relation "v" because function "f" depends on it`,
			dscExpectedErr: `pq: cannot drop view v because other objects depend on it`,
		},
		{
			stmt:        "ALTER TABLE t RENAME TO t_new",
			expectedErr: `pq: cannot rename relation "t" because function "f" depends on it`,
		},
		{
			stmt:        "ALTER TABLE t SET SCHEMA test_sc",
			expectedErr: `pq: cannot set schema on relation "t" because function "f" depends on it`,
		},
		{
			stmt:        "ALTER TABLE t DROP COLUMN d",
			expectedErr: `pq: cannot drop column "d" because function "f" depends on it`,
		},
		{
			stmt:        "ALTER TABLE t RENAME COLUMN b TO bb",
			expectedErr: `pq: cannot rename column "b" because function "f" depends on it`,
		},
		{
			stmt:        "ALTER TABLE t ALTER COLUMN b TYPE STRING",
			expectedErr: `pq: cannot alter type of column "b" because function "f" depends on it`,
		},
		{
			stmt:        "DROP INDEX t@t_idx_b",
			expectedErr: `pq: cannot drop index "t_idx_b" because function "f" depends on it`,
		},
		{
			stmt:        "ALTER INDEX t@t_idx_b RENAME TO t_idx_b_new",
			expectedErr: `pq: cannot rename index "t_idx_b" because function "f" depends on it`,
		},
		{
			stmt:        "DROP SCHEMA test_sc",
			expectedErr: `pq: schema "test_sc" is not empty and CASCADE was not specified`,
		},
		{
			stmt:        "DROP DATABASE test_udf_db RESTRICT",
			expectedErr: `pq: database "test_udf_db" is not empty and RESTRICT was specified`,
		},
		{
			stmt:        "DROP TYPE notmyworkday",
			expectedErr: `pq: cannot drop type "notmyworkday" because other objects ([defaultdb.test_sc.f]) still depend on it`,
		},
	}

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			_, err := sqlDB.Exec(tc.stmt)
			require.Equal(t, tc.expectedErr, err.Error())
		})
	}

	// Test drop behavior in declarative schema changer, make sure it falls back
	// to legacy schema changer.
	tDB.Exec(t, "SET use_declarative_schema_changer = on")

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			_, err := sqlDB.Exec(tc.stmt)
			if tc.dscExpectedErr != "" {
				require.Equal(t, tc.dscExpectedErr, err.Error())
			} else {
				require.Equal(t, tc.expectedErr, err.Error())
			}
		})
	}
}

func TestDropCascadeRemoveFunction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	setupQuery := `
CREATE DATABASE test_db;
USE test_db;
CREATE TABLE t(
  a INT PRIMARY KEY,
  b INT,
  C INT,
  d INT,
  INDEX t_idx_b(b),
  INDEX t_idx_c(c)
);
CREATE SEQUENCE sq1;
CREATE TABLE t2(a INT PRIMARY KEY);
CREATE VIEW v AS SELECT a FROM t2;
CREATE TYPE notmyworkday AS ENUM ('Monday', 'Tuesday');
CREATE SCHEMA test_sc;
CREATE FUNCTION test_sc.f(a notmyworkday) RETURNS INT VOLATILE LANGUAGE SQL AS $$
  SELECT a FROM t;
  SELECT b FROM t@t_idx_b;
  SELECT c FROM t@t_idx_c;
  SELECT d FROM t;
  SELECT a FROM v;
  SELECT nextval('sq1');
$$;
`

	testCases := []struct {
		testName string
		stmt     string
	}{
		{
			testName: "drop sequence",
			stmt:     "DROP SEQUENCE sq1 CASCADE",
		},
		{
			testName: "drop table",
			stmt:     "DROP TABLE t CASCADE",
		},
		{
			testName: "drop view",
			stmt:     "DROP VIEW v CASCADE",
		},
		{
			testName: "drop column",
			stmt:     "ALTER TABLE t DROP COLUMN d CASCADE",
		},
		{
			testName: "drop index",
			stmt:     "DROP INDEX t@t_idx_b CASCADE",
		},
		{
			testName: "drop database",
			stmt:     "DROP DATABASE test_db CASCADE",
		},
		{
			testName: "drop schema",
			stmt:     "DROP SCHEMA test_sc CASCADE",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			ctx := context.Background()
			s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
			defer s.Stopper().Stop(ctx)
			tDB := sqlutils.MakeSQLRunner(sqlDB)
			tDB.Exec(t, setupQuery)
			// Test drop/rename behavior in legacy schema changer.
			tDB.Exec(t, "SET use_declarative_schema_changer = off;")

			err := sqltestutils.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
				fnDesc, err := col.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Function(ctx, 113)
				require.NoError(t, err)
				require.Equal(t, "f", fnDesc.GetName())
				require.True(t, fnDesc.Public())
				return nil
			})
			require.NoError(t, err)

			tDB.Exec(t, tc.stmt)

			err = sqltestutils.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
				_, err := col.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Function(ctx, 113)
				require.Error(t, err)
				require.Regexp(t, regexp.MustCompile(`function \d+ does not exist`), err.Error())
				return nil
			})
			require.NoError(t, err)
		})
	}

	// Make sure declarative schema changer falls back to legacy schema changer to
	// drop the function.
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			ctx := context.Background()
			s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
			defer s.Stopper().Stop(ctx)
			tDB := sqlutils.MakeSQLRunner(sqlDB)
			tDB.Exec(t, setupQuery)
			// Test drop/rename behavior in legacy schema changer.
			tDB.Exec(t, "SET use_declarative_schema_changer = on;")

			err := sqltestutils.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
				fnDesc, err := col.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Function(ctx, 113)
				require.NoError(t, err)
				require.Equal(t, "f", fnDesc.GetName())
				require.True(t, fnDesc.Public())
				return nil
			})
			require.NoError(t, err)

			tDB.Exec(t, tc.stmt)

			err = sqltestutils.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
				_, err := col.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Function(ctx, 113)
				require.Error(t, err)
				require.Regexp(t, regexp.MustCompile(`function \d+ does not exist`), err.Error())
				return nil
			})
			require.NoError(t, err)
		})
	}

}
