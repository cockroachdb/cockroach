// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func init() {
	// Install the interceptor that implements the ExprFmtHideScalars functionality.
	memo.ScalarFmtInterceptor = fmtInterceptor
}

// fmtInterceptor is a function suitable for memo.ScalarFmtInterceptor. It detects
// if an expression tree contains only scalar expressions; if so, it tries to
// execbuild them and print the SQL expressions.
func fmtInterceptor(f *memo.ExprFmtCtx, scalar opt.ScalarExpr) string {
	if !onlyScalars(scalar) {
		return ""
	}

	// Let the filters node show up; we will apply the code on each filter.
	if scalar.Op() == opt.FiltersOp {
		return ""
	}

	// Build the scalar expression and format it as a single string.
	bld := New(nil /* factory */, f.Memo, nil /* catalog */, scalar, nil /* evalCtx */, false /* allowAutoCommit */)
	expr, err := bld.BuildScalar()
	if err != nil {
		// Not all scalar operators are supported (e.g. Projections).
		return ""
	}
	fmtCtx := tree.NewFmtCtx(
		tree.FmtSimple,
		tree.FmtIndexedVarFormat(func(ctx *tree.FmtCtx, idx int) {
			ctx.WriteString(f.ColumnString(opt.ColumnID(idx + 1)))
		}),
	)
	expr.Format(fmtCtx)
	return fmtCtx.String()
}

func onlyScalars(expr opt.Expr) bool {
	if !opt.IsScalarOp(expr) {
		return false
	}
	for i, n := 0, expr.ChildCount(); i < n; i++ {
		if !onlyScalars(expr.Child(i)) {
			return false
		}
	}
	return true
}
