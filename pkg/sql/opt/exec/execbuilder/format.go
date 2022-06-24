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
	// An AssignmentCastExpr is built as a crdb_internal.assignment_cast
	// function call by execbuilder. Formatting it as such would be confusing in
	// an opt tree, because it would look like a FunctionExpr. So we print the
	// full nodes instead.
	//
	// Also, print the full nodes if the scalar contains a routine.
	if !onlyScalarsWithoutOps(scalar, []opt.Operator{opt.AssignmentCastOp, opt.RoutineOp}) {
		return ""
	}

	switch scalar.Op() {
	case opt.FiltersOp:
		// Let the filters node show up; we will apply the code on each filter.
		return ""
	}

	// Build the scalar expression and format it as a single string.
	bld := New(
		nil, /* factory */
		nil, /* optimizer */
		f.Memo,
		nil, /* catalog */
		scalar,
		nil,   /* evalCtx */
		false, /* allowAutoCommit */
	)
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

func onlyScalarsWithoutOps(expr opt.Expr, ops []opt.Operator) bool {
	includedInOps := func(op opt.Operator) bool {
		for i := range ops {
			if op == ops[i] {
				return true
			}
		}
		return false
	}
	if !opt.IsScalarOp(expr) || includedInOps(expr.Op()) {
		return false
	}
	for i, n := 0, expr.ChildCount(); i < n; i++ {
		if !onlyScalarsWithoutOps(expr.Child(i), ops) {
			return false
		}
	}
	return true
}
