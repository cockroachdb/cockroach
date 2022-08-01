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
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
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
CREATE FUNCTION f(a notmyworkday) RETURNS INT IMMUTABLE LANGUAGE SQL AS $$
  SELECT a FROM t;
  SELECT b FROM t@t_idx_b;
  SELECT c FROM t@t_idx_c;
  SELECT a FROM v;
  SELECT nextval('sq1');
$$;
CREATE FUNCTION f() RETURNS VOID IMMUTABLE LANGUAGE SQL AS $$ SELECT 1 $$;`)

	var sessionData sessiondatapb.SessionData
	{
		var sessionSerialized []byte
		tDB.QueryRow(t, "SELECT crdb_internal.serialize_session()").Scan(&sessionSerialized)
		require.NoError(t, protoutil.Unmarshal(sessionSerialized, &sessionData))
	}

	err := sql.TestingDescsTxn(ctx, s, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
		execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
		planner, cleanup := sql.NewInternalPlanner(
			"resolve-index", txn, username.RootUserName(), &sql.MemoryMetrics{}, &execCfg, sessionData,
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
		funcDef, err := funcResolver.ResolveFunction(ctx, &fname, &path)
		require.NoError(t, err)
		require.Equal(t, 2, len(funcDef.Overloads))

		// Verify Function Signature looks good
		sort.Slice(funcDef.Overloads, func(i, j int) bool {
			return funcDef.Overloads[i].Overload.Oid < funcDef.Overloads[j].Overload.Oid
		})
		require.Equal(t, 100110, int(funcDef.Overloads[0].Oid))
		require.True(t, funcDef.Overloads[0].UDFContainsOnlySignature)
		require.True(t, funcDef.Overloads[0].IsUDF)
		require.Equal(t, 1, len(funcDef.Overloads[0].Types.Types()))
		// TODO(Chengxiong): assert user types are hydrated when hydration is added.
		require.Equal(t, types.EnumFamily, funcDef.Overloads[0].Types.Types()[0].Family())
		require.Equal(t, types.Int, funcDef.Overloads[0].ReturnType([]tree.TypedExpr{}))

		require.Equal(t, 100111, int(funcDef.Overloads[1].Oid))
		require.True(t, funcDef.Overloads[1].UDFContainsOnlySignature)
		require.True(t, funcDef.Overloads[1].IsUDF)
		require.Equal(t, 0, len(funcDef.Overloads[1].Types.Types()))
		require.Equal(t, types.Void, funcDef.Overloads[1].ReturnType([]tree.TypedExpr{}))

		overload, err := funcResolver.ResolveFunctionByOID(ctx, funcDef.Overloads[0].Oid)
		require.NoError(t, err)
		require.Equal(t, `SELECT a FROM defaultdb.public.t;
SELECT b FROM defaultdb.public.t@t_idx_b;
SELECT c FROM defaultdb.public.t@t_idx_c;
SELECT a FROM defaultdb.public.v;
SELECT nextval(105:::REGCLASS);`, overload.Body)
		require.True(t, overload.IsUDF)
		require.False(t, overload.UDFContainsOnlySignature)
		require.Equal(t, 1, len(overload.Types.Types()))
		// TODO(Chengxiong): assert user types are hydrated when hydration is added.
		require.Equal(t, types.EnumFamily, overload.Types.Types()[0].Family())
		require.Equal(t, types.Int, overload.ReturnType([]tree.TypedExpr{}))

		overload, err = funcResolver.ResolveFunctionByOID(ctx, funcDef.Overloads[1].Oid)
		require.NoError(t, err)
		require.Equal(t, `SELECT 1;`, overload.Body)
		require.True(t, overload.IsUDF)
		require.False(t, overload.UDFContainsOnlySignature)
		require.Equal(t, 0, len(overload.Types.Types()))
		require.Equal(t, types.Void, overload.ReturnType([]tree.TypedExpr{}))

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
CREATE FUNCTION sc1.f() RETURNS INT IMMUTABLE LANGUAGE SQL AS $$ SELECT 1 $$;
CREATE FUNCTION sc2.f() RETURNS INT IMMUTABLE LANGUAGE SQL AS $$ SELECT 2 $$;
CREATE FUNCTION sc1.lower() RETURNS INT IMMUTABLE LANGUAGE SQL AS $$ SELECT 3 $$;
`,
	)

	testCases := []struct {
		testName       string
		funName        tree.UnresolvedName
		searchPath     []string
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
			expectedBody:   []string{"SELECT 2;"},
			expectedSchema: []string{"sc2"},
		},
		{
			testName:       "use functions from search path",
			funName:        tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{"f", "", "", ""}},
			searchPath:     []string{"sc1", "sc2"},
			expectedBody:   []string{"SELECT 1;", "SELECT 2;"},
			expectedSchema: []string{"sc1", "sc2"},
		},
		{
			testName:    "unsupported builtin function",
			funName:     tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{"querytree", "", "", ""}},
			searchPath:  []string{"sc1", "sc2"},
			expectedErr: "querytree(): unimplemented: this function is not yet supported",
		},
		// TODO(Chengxiong): add test case for builtin function names when builtin
		// OIDs are changed to fixed IDs.
	}

	var sessionData sessiondatapb.SessionData
	{
		var sessionSerialized []byte
		tDB.QueryRow(t, "SELECT crdb_internal.serialize_session()").Scan(&sessionSerialized)
		require.NoError(t, protoutil.Unmarshal(sessionSerialized, &sessionData))
	}

	err := sql.TestingDescsTxn(ctx, s, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
		execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
		planner, cleanup := sql.NewInternalPlanner(
			"resolve-index", txn, username.RootUserName(), &sql.MemoryMetrics{}, &execCfg, sessionData,
		)
		defer cleanup()
		ec := planner.(interface{ EvalContext() *eval.Context }).EvalContext()
		// Set "defaultdb" as current database.
		ec.SessionData().Database = "defaultdb"

		funcResolver := planner.(tree.FunctionReferenceResolver)

		for _, tc := range testCases {
			path := sessiondata.MakeSearchPath(tc.searchPath)
			funcDef, err := funcResolver.ResolveFunction(ctx, &tc.funName, &path)
			if tc.expectedErr != "" {
				require.Equal(t, tc.expectedErr, err.Error())
				continue
			}
			require.NoError(t, err)

			require.Equal(t, len(tc.expectedBody), len(funcDef.Overloads))
			bodies := make([]string, len(funcDef.Overloads))
			schemas := make([]string, len(funcDef.Overloads))
			for i, o := range funcDef.Overloads {
				overload, err := funcResolver.ResolveFunctionByOID(ctx, o.Oid)
				require.NoError(t, err)
				bodies[i] = overload.Body
				schemas[i] = o.Schema
			}
			require.Equal(t, tc.expectedBody, bodies)
			require.Equal(t, tc.expectedSchema, schemas)
		}
		return nil
	})
	require.NoError(t, err)
}
