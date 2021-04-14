// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// srf represents an srf expression in an expression tree
// after it has been type-checked and added to the memo.
type srf struct {
	// The resolved function expression.
	*tree.FuncExpr

	// cols contains the output columns of the srf.
	cols []scopeColumn

	// fn is the top level function expression of the srf.
	fn opt.ScalarExpr
}

// Walk is part of the tree.Expr interface.
func (s *srf) Walk(v tree.Visitor) tree.Expr {
	return s
}

// TypeCheck is part of the tree.Expr interface.
func (s *srf) TypeCheck(
	_ context.Context, ctx *tree.SemaContext, desired *types.T,
) (tree.TypedExpr, error) {
	if ctx.Properties.Derived.SeenGenerator {
		// This error happens if this srf struct is nested inside a raw srf that
		// has not yet been replaced. This is possible since scope.replaceSRF first
		// calls f.Walk(s) on the external raw srf, which replaces any internal
		// raw srfs with srf structs. The next call to TypeCheck on the external
		// raw srf triggers this error.
		return nil, unimplemented.NewWithIssuef(26234, "nested set-returning functions")
	}

	return s, nil
}

// Eval is part of the tree.TypedExpr interface.
func (s *srf) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic(errors.AssertionFailedf("srf must be replaced before evaluation"))
}

var _ tree.Expr = &srf{}
var _ tree.TypedExpr = &srf{}

// buildZip builds a set of memo groups which represent a functional zip over
// the given expressions.
//
// Reminder, for context: the functional zip over iterators a,b,c
// returns tuples of values from a,b,c picked "simultaneously". NULLs
// are used when an iterator is "shorter" than another. For example:
//
//    zip([1,2,3], ['a','b']) = [(1,'a'), (2,'b'), (3, null)]
//
func (b *Builder) buildZip(exprs tree.Exprs, inScope *scope) (outScope *scope) {
	outScope = inScope.push()

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
	b.semaCtx.Properties.Require(exprKindFrom.String(),
		tree.RejectAggregates|tree.RejectWindowApplications|tree.RejectNestedGenerators)
	inScope.context = exprKindFrom

	// Build each of the provided expressions.
	zip := make(memo.ZipExpr, len(exprs))
	for i, expr := range exprs {
		// Output column names should exactly match the original expression, so we
		// have to determine the output column name before we perform type
		// checking. However, the alias may be overridden later below if the expression
		// is a function and specifically defines a return label.
		_, alias, err := tree.ComputeColNameInternal(b.semaCtx.SearchPath, expr)
		if err != nil {
			panic(err)
		}
		texpr := inScope.resolveType(expr, types.Any)

		var def *tree.FunctionDefinition
		if funcExpr, ok := texpr.(*tree.FuncExpr); ok {
			if def, err = funcExpr.Func.Resolve(b.semaCtx.SearchPath); err != nil {
				panic(err)
			}
		}

		var outCol *scopeColumn
		startCols := len(outScope.cols)
		if def == nil || def.Class != tree.GeneratorClass || b.shouldCreateDefaultColumn(texpr) {

			if def != nil && len(def.ReturnLabels) > 0 {
				// Override the computed alias with the one defined in the ReturnLabels. This
				// satisfies a Postgres quirk where some json functions use different labels
				// when used in a from clause.
				alias = def.ReturnLabels[0]
			}
			outCol = outScope.addColumn(scopeColName(tree.Name(alias)), texpr)
		}

		scalar := b.buildScalar(texpr, inScope, outScope, outCol, nil)
		cols := make(opt.ColList, len(outScope.cols)-startCols)
		for j := startCols; j < len(outScope.cols); j++ {
			cols[j-startCols] = outScope.cols[j].id
		}
		zip[i] = b.factory.ConstructZipItem(scalar, cols)
	}

	// Construct the zip as a ProjectSet with empty input.
	input := b.factory.ConstructValues(memo.ScalarListWithEmptyTuple, &memo.ValuesPrivate{
		Cols: opt.ColList{},
		ID:   b.factory.Metadata().NextUniqueID(),
	})
	outScope.expr = b.factory.ConstructProjectSet(input, zip)
	if len(outScope.cols) == 1 {
		outScope.singleSRFColumn = true
	}
	return outScope
}

// finishBuildGeneratorFunction finishes building a set-generating function
// (SRF) such as generate_series() or unnest(). It synthesizes new columns in
// outScope for each of the SRF's output columns.
func (b *Builder) finishBuildGeneratorFunction(
	f *tree.FuncExpr, fn opt.ScalarExpr, inScope, outScope *scope, outCol *scopeColumn,
) (out opt.ScalarExpr) {
	// Add scope columns.
	if outCol != nil {
		// Single-column return type.
		b.populateSynthesizedColumn(outCol, fn)
	} else {
		// Multi-column return type. Use the tuple labels in the SRF's return type
		// as column aliases.
		typ := f.ResolvedType()
		for i := range typ.TupleContents() {
			b.synthesizeColumn(outScope, scopeColName(tree.Name(typ.TupleLabels()[i])), typ.TupleContents()[i], nil, fn)
		}
	}

	return fn
}

// buildProjectSet builds a ProjectSet, which is a lateral cross join
// between the given input expression and a functional zip constructed from the
// given srfs.
//
// This function is called at most once per SELECT clause, and updates
// inScope.expr if at least one SRF was discovered in the SELECT list. The
// ProjectSet is necessary in case some of the SRFs depend on the input.
// For example, consider this query:
//
//   SELECT generate_series(t.a, t.a + 1) FROM t
//
// In this case, the inputs to generate_series depend on table t, so during
// execution, generate_series will be called once for each row of t.
func (b *Builder) buildProjectSet(inScope *scope) {
	if len(inScope.srfs) == 0 {
		return
	}

	// Get the output columns and function expressions of the zip.
	zip := make(memo.ZipExpr, len(inScope.srfs))
	for i, srf := range inScope.srfs {
		cols := make(opt.ColList, len(srf.cols))
		for j := range srf.cols {
			cols[j] = srf.cols[j].id
		}
		zip[i] = b.factory.ConstructZipItem(srf.fn, cols)
	}

	inScope.expr = b.factory.ConstructProjectSet(inScope.expr, zip)
}
