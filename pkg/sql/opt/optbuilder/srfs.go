// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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
	if ctx.Properties.IsSet(tree.RejectGenerators) {
		// srf replacement can happen before type-checking, so we need to check
		// invalid usage here.
		return nil, tree.NewInvalidFunctionUsageError(tree.GeneratorClass, ctx.TypeCheckContext())
	}
	if ctx.Properties.Ancestors.Has(tree.ConditionalAncestor) {
		return nil, tree.NewInvalidFunctionUsageError(tree.GeneratorClass, "conditional expressions")
	}
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
func (s *srf) Eval(_ context.Context, _ tree.ExprEvaluator) (tree.Datum, error) {
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
//	zip([1,2,3], ['a','b']) = [(1,'a'), (2,'b'), (3, null)]
func (b *Builder) buildZip(exprs tree.Exprs, inScope *scope) (outScope *scope) {
	outScope = inScope.push()

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
	b.semaCtx.Properties.Require(exprKindFrom.String(),
		tree.RejectAggregates|tree.RejectWindowApplications|
			tree.RejectNestedGenerators|tree.RejectProcedures)
	inScope.context = exprKindFrom

	// Build each of the provided expressions.
	zip := make(memo.ZipExpr, 0, len(exprs))
	var outCols opt.ColSet
	for _, expr := range exprs {
		// Output column names should exactly match the original expression, so we
		// have to determine the output column name before we perform type
		// checking. However, the alias may be overridden later below if the expression
		// is a function and specifically defines a return label.
		_, alias, err := tree.ComputeColNameInternal(b.ctx, b.semaCtx.SearchPath, expr, b.semaCtx.FunctionResolver)
		if err != nil {
			panic(err)
		}
		texpr := inScope.resolveType(expr, types.AnyElement)

		var def *tree.ResolvedFunctionDefinition
		funcExpr, ok := texpr.(*tree.FuncExpr)
		if ok {
			if def, err = funcExpr.Func.Resolve(
				b.ctx, b.semaCtx.SearchPath, b.semaCtx.FunctionResolver,
			); err != nil {
				panic(err)
			}
		}

		var outCol *scopeColumn
		startCols := len(outScope.cols)

		isRecordReturningUDF := def != nil && funcExpr.ResolvedOverload().Type == tree.UDFRoutine &&
			texpr.ResolvedType().Family() == types.TupleFamily && b.insideDataSource
		var scalar opt.ScalarExpr
		_, isScopedColumn := texpr.(*scopeColumn)

		if def == nil || (funcExpr.ResolvedOverload().Class != tree.GeneratorClass && !isRecordReturningUDF) || (b.shouldCreateDefaultColumn(texpr) && !isRecordReturningUDF) {
			if def != nil && len(funcExpr.ResolvedOverload().ReturnLabels) > 0 {
				// Override the computed alias with the one defined in the ReturnLabels. This
				// satisfies a Postgres quirk where some json functions use different labels
				// when used in a from clause.
				alias = funcExpr.ResolvedOverload().ReturnLabels[0]
			}
			outCol = outScope.addColumn(scopeColName(tree.Name(alias)), texpr)
		}

		if isScopedColumn {
			// Function `buildScalar` treats a `scopeColumn` as a passthrough column
			// for projection when a non-nil `outScope` is passed in, resulting in no
			// scalar expression being built, but project set does not have
			// passthrough columns and must build a new scalar. Handle this case by
			// passing a nil `outScope` to `buildScalar`.
			scalar = b.buildScalar(texpr, inScope, nil, nil, nil)

			// Update the output column in outScope to refer to a new column ID.
			// We need to use an out column which doesn't overlap with outer columns.
			// Pre-existing function `populateSynthesizedColumn` does the necessary
			// steps for us.
			b.populateSynthesizedColumn(&outScope.cols[len(outScope.cols)-1], scalar)
		} else {
			scalar = b.buildScalar(texpr, inScope, outScope, outCol, nil)
		}
		numExpectedOutputCols := len(outScope.cols) - startCols
		cols := make(opt.ColList, 0, numExpectedOutputCols)
		for j := startCols; j < len(outScope.cols); j++ {
			// If a newly-added outScope column has already been added in a zip
			// expression, don't add it again.
			if outCols.Contains(outScope.cols[j].id) {
				continue
			}
			cols = append(cols, outScope.cols[j].id)

			// Record that this output column has been added.
			outCols.Add(outScope.cols[j].id)
		}
		// Only add the zip expression if it has output columns which haven't
		// already been built.
		if len(cols) != 0 {
			if len(cols) != numExpectedOutputCols {
				// Building more columns than were just added to outScope could cause
				// problems for the execution engine. Catch this case.
				panic(errors.AssertionFailedf("zip expression builds more columns than added to outScope"))
			}
			zip = append(zip, b.factory.ConstructZipItem(scalar, cols))
		}
	}

	// Construct the zip as a ProjectSet with empty input.
	input := b.factory.ConstructNoColsRow()
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
	rTyp := f.ResolvedType()
	b.validateGeneratorFunctionReturnType(f.ResolvedOverload(), rTyp, inScope)

	// Add scope columns.
	if outCol != nil {
		// Single-column return type.
		b.populateSynthesizedColumn(outCol, fn)
	} else {
		// Multi-column return type. Note that we already reconciled the function's
		// return type with the column definition list (if it exists).
		for i := range rTyp.TupleContents() {
			colName := scopeColName(tree.Name(rTyp.TupleLabels()[i]))
			b.synthesizeColumn(outScope, colName, rTyp.TupleContents()[i], nil, fn)
		}
	}
	return fn
}

// validateGeneratorFunctionReturnType checks for various errors that result
// from the presence or absence of a column definition list and its
// compatibility with the actual function return type. This logic mirrors that
// in postgres.
func (b *Builder) validateGeneratorFunctionReturnType(
	overload *tree.Overload, rTyp *types.T, inScope *scope,
) {
	lastAlias := inScope.alias
	hasColumnDefinitionList := false
	if lastAlias != nil {
		for _, c := range lastAlias.Cols {
			if c.Type != nil {
				hasColumnDefinitionList = true
				break
			}
		}
	}

	// Validate the column definition list against the concrete return type of the
	// function.
	if hasColumnDefinitionList {
		if !overload.ReturnsRecordType {
			// Non RECORD-return type with a column definition list is not permitted.
			for _, param := range overload.RoutineParams {
				if param.IsOutParam() {
					panic(pgerror.New(pgcode.Syntax,
						"a column definition list is redundant for a function with OUT parameters",
					))
				}
			}
			if rTyp.Family() == types.TupleFamily {
				panic(pgerror.New(pgcode.Syntax,
					"a column definition list is redundant for a function returning a named composite type",
				))
			} else {
				panic(pgerror.Newf(pgcode.Syntax,
					"a column definition list is only allowed for functions returning \"record\"",
				))
			}
		}
		if len(rTyp.TupleContents()) != len(lastAlias.Cols) {
			switch overload.Language {
			case tree.RoutineLangSQL:
				err := pgerror.New(pgcode.DatatypeMismatch,
					"function return row and query-specified return row do not match",
				)
				panic(errors.WithDetailf(err,
					"Returned row contains %d attributes, but query expects %d.",
					len(rTyp.TupleContents()), len(lastAlias.Cols),
				))
			case tree.RoutineLangPLpgSQL:
				err := pgerror.New(pgcode.DatatypeMismatch,
					"returned record type does not match expected record type",
				)
				panic(errors.WithDetailf(err,
					"Number of returned columns (%d) does not match expected column count (%d).",
					len(rTyp.TupleContents()), len(lastAlias.Cols),
				))
			default:
				panic(errors.AssertionFailedf(
					"unexpected language: %s", redact.SafeString(overload.Language),
				))
			}
		}
	} else if overload.ReturnsRecordType {
		panic(pgerror.New(pgcode.Syntax,
			"a column definition list is required for functions returning \"record\"",
		))
	}

	// Verify that the function return type can be assignment-casted to the
	// column definition list type.
	if hasColumnDefinitionList {
		colDefListTypes := b.getColumnDefinitionListTypes(inScope)
		for i := range colDefListTypes.TupleContents() {
			colTyp, defTyp := rTyp.TupleContents()[i], colDefListTypes.TupleContents()[i]
			if !colTyp.Identical(defTyp) && !cast.ValidCast(colTyp, defTyp, cast.ContextAssignment) {
				panic(errors.WithDetailf(pgerror.New(pgcode.InvalidFunctionDefinition,
					"return type mismatch in function declared to return record",
				), "Final statement returns %v instead of %v at column %d.",
					colTyp.SQLStringForError(), defTyp.SQLStringForError(), i+1))
			}
		}
	}
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
//	SELECT generate_series(t.a, t.a + 1) FROM t
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
