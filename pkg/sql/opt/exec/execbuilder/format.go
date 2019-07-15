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
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

func init() {
	// Install the interceptor that implements the ExprFmtHideScalars functionality.
	memo.ExprFmtInterceptor = fmtInterceptor
}

// fmtInterceptor is a function suitable for memo.ExprFmtInterceptor. It detects
// if an expression tree contains only scalar expressions; if so, it tries to
// execbuild them and print the SQL expressions.
func fmtInterceptor(f *memo.ExprFmtCtx, tp treeprinter.Node, nd opt.Expr) bool {
	if !f.HasFlags(memo.ExprFmtHideScalars) || !onlyScalars(nd) {
		return false
	}

	// Don't use this code on leaves.
	switch nd.Op() {
	case opt.FiltersItemOp, opt.ProjectionsItemOp, opt.AggregationsItemOp,
		opt.TupleOp, opt.ArrayOp:
		if nd.Child(0).ChildCount() == 0 {
			return false
		}
	default:
		if nd.ChildCount() == 0 {
			return false
		}
	}

	// Let the filters node show up; we will apply the code on each filter.
	if nd.Op() == opt.FiltersOp {
		return false
	}

	// Build the scalar expression and format it as a single tree node.
	bld := New(nil /* factory */, f.Memo, nil /* catalog */, nd, nil /* evalCtx */)
	md := f.Memo.Metadata()
	ivh := tree.MakeIndexedVarHelper(nil /* container */, md.NumColumns())
	expr, err := bld.BuildScalar(&ivh)
	if err != nil {
		// Not all scalar operators are supported (e.g. Projections).
		return false
	}
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	fmtCtx.SetIndexedVarFormat(func(ctx *tree.FmtCtx, idx int) {
		fullyQualify := !f.HasFlags(memo.ExprFmtHideQualifications)
		alias := md.QualifiedAlias(opt.ColumnID(idx+1), fullyQualify, f.Catalog)
		ctx.WriteString(alias)
	})
	expr.Format(fmtCtx)
	f.Buffer.Reset()
	_, _ = fmtCtx.WriteTo(f.Buffer)
	fmtCtx.Close()
	f.FormatScalarProps(nd.(opt.ScalarExpr))
	tp.Child(f.Buffer.String())
	return true
}

func onlyScalars(nd opt.Expr) bool {
	if !opt.IsScalarOp(nd) {
		return false
	}
	for i, n := 0, nd.ChildCount(); i < n; i++ {
		if !onlyScalars(nd.Child(i)) {
			return false
		}
	}
	return true
}
