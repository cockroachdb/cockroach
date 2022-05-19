// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package queryvalidator_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/queryvalidator"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestValidator(t *testing.T) {
	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tDB := sqlutils.MakeSQLRunner(sqlDB)

	tDB.Exec(t, `
CREATE TABLE t (a INT PRIMARY KEY, b string);`,
	)

	var sessionData sessiondatapb.SessionData
	{
		var sessionSerialized []byte
		tDB.QueryRow(t, "SELECT crdb_internal.serialize_session()").Scan(&sessionSerialized)
		require.NoError(t, protoutil.Unmarshal(sessionSerialized, &sessionData))
	}

	err := sql.TestingDescsTxn(ctx, s, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
		execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
		planner, cleanup := sql.NewInternalPlanner(
			"resolve-index", txn, username.RootUserName(), &sql.MemoryMetrics{},
			&execCfg, sessionData, sql.WithDescCollection(col),
		)
		defer cleanup()

		catalog := sql.NewOptCatalog(planner)
		f := sql.NewNormFactory(planner)
		ec := planner.(interface{ EvalContext() *eval.Context }).EvalContext()
		sc := planner.(interface{ SemaCtx() *tree.SemaContext }).SemaCtx()

		bldFactory := func(stmt tree.Statement) (*memo.Memo, error) {
			bld := optbuilder.New(ctx, sc, ec, catalog, f, stmt)
			err := bld.Build()
			if err != nil {
				return nil, err
			}
			return f.Memo(), nil
		}

		validator := queryvalidator.New(bldFactory)

		stmts, err := parser.Parse("SELECT upper(a::string), b::int FROM defaultdb.public.t")
		require.NoError(t, err)
		err = validator.Validate(stmts[0].AST, []*types.T{types.String, types.Int})
		require.NoError(t, err)

		stmts, err = parser.Parse("SELECT row(a, b) FROM defaultdb.public.t")
		require.NoError(t, err)
		err = validator.Validate(stmts[0].AST, []*types.T{types.MakeTuple([]*types.T{types.Int, types.String})})
		require.NoError(t, err)

		stmts, err = parser.Parse("SELECT 'hello'")
		require.NoError(t, err)
		err = validator.Validate(stmts[0].AST, []*types.T{types.String})
		require.NoError(t, err)

		return nil
	})

	require.NoError(t, err)
}
