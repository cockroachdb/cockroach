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
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestSimpleResolveFunction(t *testing.T) {
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
CREATE FUNCTION f(a notmyworkday) RETURNS INT VOLATILE LANGUAGE SQL AS $$
  SELECT a FROM t;
  SELECT b FROM t@t_idx_b;
  SELECT c FROM t@t_idx_c;
  SELECT a FROM v;
  SELECT nextval('sq1');
$$;
CREATE FUNCTION f() RETURNS VOID VOLATILE LANGUAGE SQL AS $$ SELECT 1 $$;
CREATE FUNCTION f(INT) RETURNS INT VOLATILE LANGUAGE SQL AS $$ SELECT a FROM t $$;
`)

	var m sessiondatapb.MigratableSession
	var sessionSerialized []byte
	tDB.QueryRow(t, "SELECT crdb_internal.serialize_session()").Scan(&sessionSerialized)
	require.NoError(t, protoutil.Unmarshal(sessionSerialized, &m))
	sd, err := sessiondata.UnmarshalNonLocal(m.SessionData)
	require.NoError(t, err)
	sd.SessionData = m.SessionData
	sd.LocalOnlySessionData = m.LocalOnlySessionData

	err = sqltestutils.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
		planner, cleanup := sql.NewInternalPlanner(
			"resolve-index", txn.KV(), username.NodeUserName(), &sql.MemoryMetrics{}, &execCfg, sd,
		)
		defer cleanup()
		ec := planner.(interface{ EvalContext() *eval.Context }).EvalContext()
		// Set "defaultdb" as current database.
		ec.SessionData().Database = "defaultdb"
		searchPathArray := ec.SessionData().SearchPath.GetPathArray()

		funcResolver := planner.(tree.FunctionReferenceResolver)
		fname := tree.UnresolvedName{NumParts: 1, Star: false}
		fname.Parts[0] = "f"
		path := sessiondata.MakeSearchPath(searchPathArray)
		funcDef, err := funcResolver.ResolveFunction(
			ctx,
			tree.MakeUnresolvedFunctionName(&fname),
			&path,
		)
		require.NoError(t, err)
		require.Equal(t, 3, len(funcDef.Overloads))

		// Verify Function Signature looks good
		sort.Slice(funcDef.Overloads, func(i, j int) bool {
			return funcDef.Overloads[i].Overload.Oid < funcDef.Overloads[j].Overload.Oid
		})
		require.Equal(t, 100110, int(funcDef.Overloads[0].Oid))
		require.True(t, funcDef.Overloads[0].UDFContainsOnlySignature)
		require.Equal(t, funcDef.Overloads[0].Type, tree.UDFRoutine)
		require.Equal(t, 1, len(funcDef.Overloads[0].Types.Types()))
		require.NotZero(t, funcDef.Overloads[0].Types.Types()[0].TypeMeta)
		require.Equal(t, types.EnumFamily, funcDef.Overloads[0].Types.Types()[0].Family())
		require.Equal(t, types.Int, funcDef.Overloads[0].ReturnType([]tree.TypedExpr{}))

		require.Equal(t, 100111, int(funcDef.Overloads[1].Oid))
		require.True(t, funcDef.Overloads[1].UDFContainsOnlySignature)
		require.Equal(t, funcDef.Overloads[1].Type, tree.UDFRoutine)
		require.Equal(t, 0, len(funcDef.Overloads[1].Types.Types()))
		require.Equal(t, types.Void, funcDef.Overloads[1].ReturnType([]tree.TypedExpr{}))

		require.Equal(t, 100112, int(funcDef.Overloads[2].Oid))
		require.True(t, funcDef.Overloads[2].UDFContainsOnlySignature)
		require.Equal(t, funcDef.Overloads[2].Type, tree.UDFRoutine)
		require.Equal(t, 1, len(funcDef.Overloads[2].Types.Types()))
		require.Equal(t, types.Int, funcDef.Overloads[2].Types.Types()[0])
		require.Equal(t, types.Int, funcDef.Overloads[2].ReturnType([]tree.TypedExpr{}))

		_, overload, err := funcResolver.ResolveFunctionByOID(ctx, funcDef.Overloads[0].Oid)
		require.NoError(t, err)
		require.Equal(t, `SELECT a FROM defaultdb.public.t;
SELECT b FROM defaultdb.public.t@t_idx_b;
SELECT c FROM defaultdb.public.t@t_idx_c;
SELECT a FROM defaultdb.public.v;
SELECT nextval(105:::REGCLASS);`, overload.Body)
		require.Equal(t, overload.Type, tree.UDFRoutine)
		require.False(t, overload.UDFContainsOnlySignature)
		require.Equal(t, 1, len(overload.Types.Types()))
		require.NotEqual(t, overload.Types.Types()[0].TypeMeta, types.UserDefinedTypeMetadata{})
		require.Equal(t, types.EnumFamily, overload.Types.Types()[0].Family())
		require.Equal(t, types.Int, overload.ReturnType([]tree.TypedExpr{}))

		_, overload, err = funcResolver.ResolveFunctionByOID(ctx, funcDef.Overloads[1].Oid)
		require.NoError(t, err)
		require.Equal(t, `SELECT 1;`, overload.Body)
		require.Equal(t, overload.Type, tree.UDFRoutine)
		require.False(t, overload.UDFContainsOnlySignature)
		require.Equal(t, 0, len(overload.Types.Types()))
		require.Equal(t, types.Void, overload.ReturnType([]tree.TypedExpr{}))

		_, overload, err = funcResolver.ResolveFunctionByOID(ctx, funcDef.Overloads[2].Oid)
		require.NoError(t, err)
		require.Equal(t, `SELECT a FROM defaultdb.public.t;`, overload.Body)
		require.Equal(t, overload.Type, tree.UDFRoutine)
		require.False(t, overload.UDFContainsOnlySignature)
		require.Equal(t, 1, len(overload.Types.Types()))
		require.Equal(t, types.Int, overload.Types.Types()[0])
		require.Equal(t, types.Int, overload.ReturnType([]tree.TypedExpr{}))

		return nil
	})
	require.NoError(t, err)
}

func TestResolveFunctionRespectSearchPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tDB := sqlutils.MakeSQLRunner(sqlDB)

	tDB.Exec(t, `
CREATE SCHEMA sc1;
CREATE SCHEMA sc2;
CREATE FUNCTION sc1.f() RETURNS INT VOLATILE LANGUAGE SQL AS $$ SELECT 1 $$;
CREATE FUNCTION sc2.f() RETURNS INT VOLATILE LANGUAGE SQL AS $$ SELECT 2 $$;
CREATE FUNCTION sc1.lower() RETURNS INT VOLATILE LANGUAGE SQL AS $$ SELECT 3 $$;
`,
	)

	testCases := []struct {
		testName       string
		funName        tree.UnresolvedName
		searchPath     []string
		expectUDF      []bool
		expectedBody   []string
		expectedSchema []string
		expectedErr    string
	}{
		{
			testName:    "cross db should fail",
			funName:     tree.UnresolvedName{NumParts: 3, Parts: tree.NameParts{"some_f", "some_sc", "some_db", ""}},
			expectedErr: "cross-database function references not allowed",
		},
		{
			testName:    "schema not found",
			funName:     tree.UnresolvedName{NumParts: 2, Parts: tree.NameParts{"some_f", "some_sc", "", ""}},
			expectedErr: "schema \"some_sc\" does not exist",
		},
		{
			testName:    "function not found",
			funName:     tree.UnresolvedName{NumParts: 2, Parts: tree.NameParts{"some_f", "sc1", "", ""}},
			searchPath:  []string{"sc1", "sc2"},
			expectedErr: "unknown function: sc1.some_f()",
		},
		{
			testName:    "function not found",
			funName:     tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{"some_f", "", "", ""}},
			searchPath:  []string{"sc1", "sc2"},
			expectedErr: "unknown function: some_f()",
		},
		{
			testName:       "function with explicit schema skip first schema in path",
			funName:        tree.UnresolvedName{NumParts: 2, Parts: tree.NameParts{"f", "sc2", "", ""}},
			searchPath:     []string{"sc1", "sc2"},
			expectUDF:      []bool{true},
			expectedBody:   []string{"SELECT 2;"},
			expectedSchema: []string{"sc2"},
		},
		{
			testName:       "use functions from search path",
			funName:        tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{"f", "", "", ""}},
			searchPath:     []string{"sc1", "sc2"},
			expectUDF:      []bool{true, true},
			expectedBody:   []string{"SELECT 1;", "SELECT 2;"},
			expectedSchema: []string{"sc1", "sc2"},
		},
		{
			testName:       "builtin function schema is respected",
			funName:        tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{"lower", "", "", ""}},
			searchPath:     []string{"sc1", "sc2"},
			expectUDF:      []bool{false, true},
			expectedBody:   []string{"", "SELECT 3;"},
			expectedSchema: []string{"pg_catalog", "sc1"},
		},
		{
			testName:       "explicit builtin function schema",
			funName:        tree.UnresolvedName{NumParts: 2, Parts: tree.NameParts{"lower", "pg_catalog", "", ""}},
			searchPath:     []string{"sc1", "sc2"},
			expectUDF:      []bool{false},
			expectedBody:   []string{""},
			expectedSchema: []string{"pg_catalog"},
		},
		{
			testName:    "unsupported builtin function",
			funName:     tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{"querytree", "", "", ""}},
			searchPath:  []string{"sc1", "sc2"},
			expectedErr: `querytree\(\): unimplemented: this function is not yet supported`,
		},
	}

	var m sessiondatapb.MigratableSession
	var sessionSerialized []byte
	tDB.QueryRow(t, "SELECT crdb_internal.serialize_session()").Scan(&sessionSerialized)
	require.NoError(t, protoutil.Unmarshal(sessionSerialized, &m))
	sd, err := sessiondata.UnmarshalNonLocal(m.SessionData)
	require.NoError(t, err)
	sd.SessionData = m.SessionData
	sd.LocalOnlySessionData = m.LocalOnlySessionData

	err = sqltestutils.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
		planner, cleanup := sql.NewInternalPlanner(
			"resolve-index", txn.KV(), username.NodeUserName(), &sql.MemoryMetrics{}, &execCfg, sd,
		)
		defer cleanup()
		ec := planner.(interface{ EvalContext() *eval.Context }).EvalContext()
		// Set "defaultdb" as current database.
		ec.SessionData().Database = "defaultdb"

		funcResolver := planner.(tree.FunctionReferenceResolver)

		for _, tc := range testCases {
			t.Run(tc.testName, func(t *testing.T) {
				path := sessiondata.MakeSearchPath(tc.searchPath)
				funcDef, err := funcResolver.ResolveFunction(
					ctx,
					tree.MakeUnresolvedFunctionName(&tc.funName),
					&path,
				)
				if tc.expectedErr != "" {
					require.Regexp(t, tc.expectedErr, err.Error())
					return
				}
				require.NoError(t, err)

				require.Equal(t, len(tc.expectUDF), len(funcDef.Overloads))
				require.Equal(t, len(tc.expectedBody), len(funcDef.Overloads))
				bodies := make([]string, len(funcDef.Overloads))
				schemas := make([]string, len(funcDef.Overloads))
				isUDF := make([]bool, len(funcDef.Overloads))
				for i, o := range funcDef.Overloads {
					_, overload, err := funcResolver.ResolveFunctionByOID(ctx, o.Oid)
					require.NoError(t, err)
					bodies[i] = overload.Body
					schemas[i] = o.Schema
					isUDF[i] = o.Type == tree.UDFRoutine
				}
				require.Equal(t, tc.expectedBody, bodies)
				require.Equal(t, tc.expectedSchema, schemas)
				require.Equal(t, tc.expectUDF, isUDF)
			})
		}
		return nil
	})
	require.NoError(t, err)
}

func TestFuncExprTypeCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tDB := sqlutils.MakeSQLRunner(sqlDB)

	tDB.Exec(t, `
CREATE SCHEMA sc1;
CREATE SCHEMA sc2;
CREATE FUNCTION sc1.f(a INT, b INT) RETURNS INT VOLATILE LANGUAGE SQL AS $$ SELECT 1 $$;
CREATE FUNCTION sc1.f(a INT) RETURNS INT VOLATILE LANGUAGE SQL AS $$ SELECT 2 $$;
CREATE FUNCTION sc2.f(a INT) RETURNS INT VOLATILE LANGUAGE SQL AS $$ SELECT 3 $$;
CREATE FUNCTION sc1.lower(a STRING) RETURNS STRING VOLATILE LANGUAGE SQL AS $$ SELECT lower('HI') $$;
`,
	)

	testCases := []struct {
		testName         string
		exprStr          string
		searchPath       []string
		expectedFuncOID  int
		expectedErr      string
		expectedFuncBody string
		desiredType      *types.T
	}{
		{
			testName:    "explicit schema but function not found",
			exprStr:     "sc1.g(1)",
			searchPath:  []string{"sc2", "sc1"},
			expectedErr: "unknown function: sc1.g()",
		},
		{
			testName:    "implicit schema but function not found",
			exprStr:     "g(1)",
			searchPath:  []string{"sc2", "sc1"},
			expectedErr: "unknown function: g()",
		},
		{
			testName:         "explicit schema",
			exprStr:          "sc1.f(1)",
			searchPath:       []string{"sc2", "sc1"},
			expectedFuncOID:  100107,
			expectedFuncBody: "SELECT 2;",
		},
		{
			testName:         "explicit schema not in path",
			exprStr:          "sc1.f(1)",
			searchPath:       []string{"sc2"},
			expectedFuncOID:  100107,
			expectedFuncBody: "SELECT 2;",
		},
		{
			testName:         "implicit schema",
			exprStr:          "f(1)",
			searchPath:       []string{"sc2", "sc1"},
			expectedFuncOID:  100108,
			expectedFuncBody: "SELECT 3;",
		},
		{
			testName:         "implicit schema but unique signature",
			exprStr:          "f(1, 1)",
			searchPath:       []string{"sc2", "sc1"},
			expectedFuncOID:  100106,
			expectedFuncBody: "SELECT 1;",
		},
		{
			testName:         "implicit pg_catalog schema in path",
			exprStr:          "lower('HI')",
			searchPath:       []string{"sc1", "sc2"},
			expectedFuncBody: "",
			expectedFuncOID:  827,
			desiredType:      types.String,
		},
		{
			testName:         "explicit pg_catalog schema in path",
			exprStr:          "lower('HI')",
			searchPath:       []string{"sc1", "sc2", "pg_catalog"},
			expectedFuncOID:  100109,
			expectedFuncBody: "SELECT lower('HI':::STRING);",
			desiredType:      types.String,
		},
	}

	var m sessiondatapb.MigratableSession
	var sessionSerialized []byte
	tDB.QueryRow(t, "SELECT crdb_internal.serialize_session()").Scan(&sessionSerialized)
	require.NoError(t, protoutil.Unmarshal(sessionSerialized, &m))
	sd, err := sessiondata.UnmarshalNonLocal(m.SessionData)
	require.NoError(t, err)
	sd.SessionData = m.SessionData
	sd.LocalOnlySessionData = m.LocalOnlySessionData

	err = sqltestutils.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
		planner, cleanup := sql.NewInternalPlanner(
			"resolve-index", txn.KV(), username.NodeUserName(), &sql.MemoryMetrics{}, &execCfg, sd,
		)
		defer cleanup()
		ec := planner.(interface{ EvalContext() *eval.Context }).EvalContext()
		// Set "defaultdb" as current database.
		ec.SessionData().Database = "defaultdb"

		semaCtx := tree.MakeSemaContext(planner)

		for _, tc := range testCases {
			t.Run(tc.testName, func(t *testing.T) {
				expr, err := parser.ParseExpr(tc.exprStr)
				require.NoError(t, err)
				path := sessiondata.MakeSearchPath(tc.searchPath)
				semaCtx.SearchPath = &path
				desired := types.Int
				if tc.desiredType != nil {
					desired = tc.desiredType
				}
				typeChecked, err := tree.TypeCheck(ctx, expr, &semaCtx, desired)
				if tc.expectedErr != "" {
					require.Regexp(t, tc.expectedErr, err.Error())
					return
				}
				require.NoError(t, err)
				funcExpr := typeChecked.(*tree.FuncExpr)
				require.NotNil(t, funcExpr.ResolvedOverload())
				require.Equal(t, tc.expectedFuncBody, funcExpr.ResolvedOverload().Body)
				if tc.expectedFuncBody != "" {
					require.Equal(t, funcExpr.ResolvedOverload().Type, tree.UDFRoutine)
				} else {
					require.NotEqual(t, funcExpr.ResolvedOverload().Type, tree.UDFRoutine)
				}
				require.False(t, funcExpr.ResolvedOverload().UDFContainsOnlySignature)
				require.Equal(t, tc.expectedFuncOID, int(funcExpr.ResolvedOverload().Oid))
				require.Equal(t, funcExpr.ResolvedOverload().Type == tree.UDFRoutine,
					funcdesc.IsOIDUserDefinedFunc(funcExpr.ResolvedOverload().Oid))
				require.Equal(t, tc.expectedFuncBody, funcExpr.ResolvedOverload().Body)
			})
		}
		return nil
	})
	require.NoError(t, err)
}
