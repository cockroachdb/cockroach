// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/redact"
)

// sqlFnInfo stores information about a tree.SQLClass function, which is a
// function that executes a SQL statement as a side effect of the function call.
// See the comment above tree.SQLClass for more info.
type sqlFnInfo struct {
	*tree.FuncExpr

	def  memo.FunctionPrivate
	args memo.ScalarListExpr
}

// Walk is part of the tree.Expr interface.
func (s *sqlFnInfo) Walk(v tree.Visitor) tree.Expr {
	return s
}

// TypeCheck is part of the tree.Expr interface.
func (s *sqlFnInfo) TypeCheck(
	ctx context.Context, semaCtx *tree.SemaContext, desired *types.T,
) (tree.TypedExpr, error) {
	if _, err := s.FuncExpr.TypeCheck(ctx, semaCtx, desired); err != nil {
		return nil, err
	}
	return s, nil
}

// buildSQLFn builds a SQL statement from the given tree.SQLClass function and
// hoists it into a CTE. It then builds the original function as a scalar
// expression and returns it.
func (b *Builder) buildSQLFn(
	info *sqlFnInfo, inScope, outScope *scope, outCol *scopeColumn, colRefs *opt.ColSet,
) opt.ScalarExpr {
	// Get the arguments to the function.
	exprs := make(tree.Datums, len(info.args))
	for i := range exprs {
		if !memo.CanExtractConstDatum(info.args[i]) {
			panic(unimplemented.NewWithIssuef(49448, "non-constant argument passed to %s\n",
				redact.Safe(info.def.Name),
			))
		}
		exprs[i] = memo.ExtractConstDatum(info.args[i])
		if exprs[i] == tree.DNull && !info.def.Properties.NullableArgs {
			return b.factory.ConstructNull(info.ResolvedType())
		}
	}

	// Get the SQL statement and parse it.
	sql, err := info.def.Overload.SQLFn(b.evalCtx, exprs)
	if err != nil {
		panic(err)
	}
	stmt, err := parser.ParseOne(sql)
	if err != nil {
		panic(err)
	}

	// Build the SQL statement and hoist it into a CTE.
	b.semaCtx.Annotations = tree.MakeAnnotations(stmt.NumAnnotations)
	emptyScope := b.allocScope()
	innerScope := b.buildStmt(stmt.AST, nil /* desiredTypes */, emptyScope)

	id := b.factory.Memo().NextWithID()
	b.factory.Metadata().AddWithBinding(id, innerScope.expr)
	cte := &cteSource{
		name:         tree.AliasClause{},
		cols:         innerScope.makePresentationWithHiddenCols(),
		originalExpr: stmt.AST,
		expr:         innerScope.expr,
		id:           id,
	}
	b.addCTE(cte)

	// Build and return the original function.
	return b.buildScalar(info.FuncExpr, inScope, outScope, outCol, colRefs)
}
