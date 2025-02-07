// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// buildScalar builds a set of memo groups that represent the given scalar
// expression. If outScope is not nil, then this is a projection context, and
// the resulting memo group will be projected as the output column outCol.
// Otherwise, the memo group is part of a larger expression that is not bound
// to a column.
//
// colRefs is the set of columns referenced so far by the scalar expression
// being built. If not nil, it is updated with any columns seen in
// finishBuildScalarRef.
//
// See Builder.buildStmt for a description of the remaining input and return
// values.
func (b *Builder) buildScalar(
	scalar tree.TypedExpr, inScope, outScope *scope, outCol *scopeColumn, colRefs *opt.ColSet,
) (out opt.ScalarExpr) {
	// If we are in a grouping context and this expression corresponds to a
	// GROUP BY expression, return a reference to the GROUP BY column.
	// Note that GROUP BY columns cannot be reused inside an aggregate input
	// expression (when inAgg=true) because the aggregate input expressions and
	// grouping expressions are built as part of the same projection.
	inGroupingContext := inScope.inGroupingContext() && !inScope.inAgg &&
		!inScope.groupby.buildingGroupingCols
	if inGroupingContext {
		// TODO(rytaft): This currently regenerates a string for each subexpression.
		// Change this to generate the string once for the top-level expression and
		// check the relevant slice for this subexpression.
		if col, ok := inScope.groupby.groupStrs[symbolicExprStr(scalar)]; ok {
			// We pass aggOutScope as the input scope because it contains all of
			// the aggregates and grouping columns that are available for projection.
			// finishBuildScalarRef wraps projected columns in a variable expression
			// with a new column ID if they are not contained in the input scope, so
			// passing in aggOutScope ensures we don't create new column IDs when not
			// necessary.
			return b.finishBuildScalarRef(col, inScope.groupby.aggOutScope, outScope, outCol, colRefs)
		}
	}

	switch t := scalar.(type) {
	case *scopeColumn:
		if inGroupingContext {
			// Non-grouping column was referenced. Note that a column that is part
			// of a larger grouping expression would have been detected by the
			// groupStrs checking code above.
			// Normally this would be a "column must appear in the GROUP BY clause"
			// error. The only cases where we allow this (for compatibility with
			// Postgres) is when this column is an outer column (and therefore
			// effectively constant) or it is part of a table and we are already
			// grouping on the entire PK of that table.
			g := inScope.groupby
			if !inScope.isOuterColumn(t.id) {
				if !b.allowImplicitGroupingColumn(t.id, g) {
					panic(newGroupingError(t.name.ReferenceName()))
				}
				// We add a new grouping column; these show up both in aggInScope and
				// aggOutScope. We only do this when the column is not an outer column;
				// otherwise, we may inadvertently convert a ScalarGroupBy to a GroupBy.
				//
				// Note that normalization rules will trim down the list of grouping
				// columns based on FDs, so this is only for the purposes of building a
				// valid operator.
				aggInCol := g.aggInScope.addColumn(scopeColName(""), t)
				b.finishBuildScalarRef(t, inScope, g.aggInScope, aggInCol, nil)
				g.groupStrs[symbolicExprStr(t)] = aggInCol

				g.aggOutScope.appendColumn(aggInCol)
			}

			return b.finishBuildScalarRef(t, g.aggOutScope, outScope, outCol, colRefs)
		}

		return b.finishBuildScalarRef(t, inScope, outScope, outCol, colRefs)

	case *aggregateInfo:
		var aggOutScope *scope
		if inScope.groupby != nil {
			aggOutScope = inScope.groupby.aggOutScope
		}
		return b.finishBuildScalarRef(t.col, aggOutScope, outScope, outCol, colRefs)

	case *windowInfo:
		return b.finishBuildScalarRef(t.col, inScope, outScope, outCol, colRefs)

	case *tree.AndExpr:
		left := b.buildScalar(reType(t.TypedLeft(), types.Bool), inScope, nil, nil, colRefs)
		right := b.buildScalar(reType(t.TypedRight(), types.Bool), inScope, nil, nil, colRefs)
		out = b.factory.ConstructAnd(left, right)

	case *tree.Array:
		els := make(memo.ScalarListExpr, len(t.Exprs))
		arrayType := t.ResolvedType()
		elementType := arrayType.ArrayContents()
		if err := types.CheckArrayElementType(elementType); err != nil {
			panic(err)
		}
		for i := range t.Exprs {
			texpr := t.Exprs[i].(tree.TypedExpr)
			els[i] = b.buildScalar(texpr, inScope, nil, nil, colRefs)
		}
		out = b.factory.ConstructArray(els, arrayType)

	case *tree.CollateExpr:
		in := b.buildScalar(t.Expr.(tree.TypedExpr), inScope, nil, nil, colRefs)
		out = b.factory.ConstructCollate(in, lex.NormalizeLocaleName(t.Locale))

	case *tree.ArrayFlatten:
		s := t.Subquery.(*subquery)

		inCol := s.cols[0].id

		// This looks kind of arbitrary and strange, because it is:
		// We cannot array_agg over some types, but we can only decorrelate via array_agg.
		// Thus, we reject a query that is correlated and over a type that we can't array_agg.
		typ := b.factory.Metadata().ColumnMeta(inCol).Type
		if !s.outerCols.Empty() && !memo.AggregateOverloadExists(opt.ArrayAggOp, typ) {
			panic(unimplementedWithIssueDetailf(35710, "", "can't execute a correlated ARRAY(...) over %s", typ))
		}

		if err := types.CheckArrayElementType(typ); err != nil {
			panic(err)
		}

		// Perform correctness checks on the outer cols, update colRefs and
		// b.subquery.outerCols.
		b.checkSubqueryOuterCols(s.outerCols, inGroupingContext, inScope, colRefs)

		subqueryPrivate := memo.SubqueryPrivate{
			OriginalExpr: s.Subquery,
			Ordering:     s.ordering,
			RequestedCol: inCol,
			WithinUDF:    b.insideUDF,
		}
		out = b.factory.ConstructArrayFlatten(s.node, &subqueryPrivate)

	case *tree.IndirectionExpr:
		if len(t.Indirection) != 1 && t.Expr.(tree.TypedExpr).ResolvedType().Family() == types.ArrayFamily {
			panic(unimplementedWithIssueDetailf(32552, "ind", "multidimensional indexing is not supported"))
		}

		out = b.buildScalar(t.Expr.(tree.TypedExpr), inScope, nil, nil, colRefs)

		for _, subscript := range t.Indirection {
			if subscript.Slice {
				panic(unimplementedWithIssueDetailf(32551, "", "array slicing is not supported"))
			}

			out = b.factory.ConstructIndirection(
				out,
				b.buildScalar(subscript.Begin.(tree.TypedExpr), inScope, nil, nil, colRefs),
			)
		}

	case *tree.IfErrExpr:
		cond := b.buildScalar(t.Cond.(tree.TypedExpr), inScope, nil, nil, colRefs)

		orElse := memo.EmptyScalarListExpr
		if t.Else != nil {
			orElse = memo.ScalarListExpr{
				b.buildScalar(t.Else.(tree.TypedExpr), inScope, nil, nil, colRefs),
			}
		}

		errCode := memo.EmptyScalarListExpr
		if t.ErrCode != nil {
			errCode = memo.ScalarListExpr{
				b.buildScalar(t.ErrCode.(tree.TypedExpr), inScope, nil, nil, colRefs),
			}
		}

		out = b.factory.ConstructIfErr(cond, orElse, errCode)

	case *tree.BinaryExpr:
		// It's possible for an overload to be selected that expects different
		// types than the TypedExpr arguments return:
		//
		//   ARRAY[1, 2] || NULL
		//
		// This is a tricky case, because the type checker selects []int as the
		// type of the right argument, but then types it as unknown. This causes
		// issues for the execbuilder, which doesn't have enough information to
		// select the right overload. The solution is to wrap any mismatched
		// arguments with a CastExpr that preserves the static type.

		left := t.TypedLeft()
		if left.ResolvedType().Family() == types.UnknownFamily {
			left = reType(left, t.ResolvedBinOp().LeftType)
		}
		right := t.TypedRight()
		if right.ResolvedType().Family() == types.UnknownFamily {
			right = reType(right, t.ResolvedBinOp().RightType)
		}
		out = b.constructBinary(
			treebin.MakeBinaryOperator(t.Operator.Symbol),
			b.buildScalar(left, inScope, nil, nil, colRefs),
			b.buildScalar(right, inScope, nil, nil, colRefs),
			t.ResolvedType(),
		)

	case *tree.CaseExpr:
		valType := t.ResolvedType()
		var input opt.ScalarExpr
		if t.Expr != nil {
			texpr := t.Expr.(tree.TypedExpr)
			input = b.buildScalar(texpr, inScope, nil, nil, colRefs)
		} else {
			input = memo.TrueSingleton
		}

		whens := make(memo.ScalarListExpr, 0, len(t.Whens)+1)
		for i := range t.Whens {
			condExpr := t.Whens[i].Cond.(tree.TypedExpr)
			cond := b.buildScalar(condExpr, inScope, nil, nil, colRefs)
			// TODO(mgartner): Rather than use WithoutTypeModifiers here,
			// consider typing the CaseExpr without a type modifier.
			valExpr, ok := eval.ReType(t.Whens[i].Val.(tree.TypedExpr), valType.WithoutTypeModifiers())
			if !ok {
				panic(pgerror.Newf(
					pgcode.DatatypeMismatch,
					"CASE WHEN types %s and %s cannot be matched",
					t.Whens[i].Val.(tree.TypedExpr).ResolvedType(), valType,
				))
			}
			val := b.buildScalar(valExpr, inScope, nil, nil, colRefs)
			whens = append(whens, b.factory.ConstructWhen(cond, val))
		}
		// Add the ELSE expression to the end of whens as a raw scalar expression.
		var orElse opt.ScalarExpr
		if t.Else != nil {
			elseExpr, ok := eval.ReType(t.Else.(tree.TypedExpr), valType.WithoutTypeModifiers())
			if !ok {
				panic(pgerror.Newf(
					pgcode.DatatypeMismatch,
					"CASE ELSE type %s cannot be matched to WHEN type %s",
					t.Else.(tree.TypedExpr).ResolvedType(), valType,
				))
			}
			orElse = b.buildScalar(elseExpr, inScope, nil, nil, colRefs)
		} else {
			orElse = b.factory.ConstructNull(valType)
		}
		out = b.factory.ConstructCase(input, whens, orElse)

	case *tree.CastExpr:
		texpr := t.Expr.(tree.TypedExpr)
		arg := b.buildScalar(texpr, inScope, nil, nil, colRefs)
		out = b.factory.ConstructCast(arg, t.ResolvedType())

	case *tree.CoalesceExpr:
		args := make(memo.ScalarListExpr, len(t.Exprs))
		typ := t.ResolvedType()
		for i := range args {
			// The type of the CoalesceExpr might be different than the inputs (e.g.
			// when they are NULL). Force all inputs to be the same type, so that we
			// build coalesce operator with the correct type.
			expr, ok := eval.ReType(t.TypedExprAt(i), typ.WithoutTypeModifiers())
			if !ok {
				panic(pgerror.Newf(
					pgcode.DatatypeMismatch,
					"COALESCE types %s and %s cannot be matched",
					t.TypedExprAt(i).ResolvedType(), typ,
				))
			}
			args[i] = b.buildScalar(expr, inScope, nil, nil, colRefs)
		}
		out = b.factory.ConstructCoalesce(args)

	case *tree.ColumnAccessExpr:
		input := b.buildScalar(t.Expr.(tree.TypedExpr), inScope, nil, nil, colRefs)
		out = b.factory.ConstructColumnAccess(input, memo.TupleOrdinal(t.ColIndex))

	case *tree.ComparisonExpr:
		if sub, ok := t.Right.(*subquery); ok && sub.isMultiRow() {
			out, _ = b.buildMultiRowSubquery(t, inScope, colRefs)
			// Perform correctness checks on the outer cols, update colRefs and
			// b.subquery.outerCols.
			b.checkSubqueryOuterCols(sub.outerCols, inGroupingContext, inScope, colRefs)
		} else if b.hasSubOperator(t) {
			// Cases where the RHS is a multi-row subquery were handled above, so this
			// only handles explicit tuples and arrays.
			out = b.buildAnyScalar(t, inScope, colRefs)
		} else {
			left := b.buildScalar(t.TypedLeft(), inScope, nil, nil, colRefs)
			right := b.buildScalar(t.TypedRight(), inScope, nil, nil, colRefs)
			out = b.constructComparison(t, left, right)
		}

	case *tree.DTuple:
		els := make(memo.ScalarListExpr, len(t.D))
		for i := range t.D {
			els[i] = b.buildScalar(t.D[i], inScope, nil, nil, colRefs)
		}
		out = b.factory.ConstructTuple(els, t.ResolvedType())

	case *tree.FuncExpr:
		return b.buildFunction(t, inScope, outScope, outCol, colRefs)

	case *tree.IfExpr:
		valType := t.ResolvedType()
		input := b.buildScalar(t.Cond.(tree.TypedExpr), inScope, nil, nil, colRefs)
		// Re-typing the True expression should always succeed because they
		// are given the same type during type-checking.
		ifTrueExpr := reType(t.True.(tree.TypedExpr), valType)
		ifTrue := b.buildScalar(ifTrueExpr, inScope, nil, nil, colRefs)
		whens := memo.ScalarListExpr{b.factory.ConstructWhen(memo.TrueSingleton, ifTrue)}
		orElseExpr, ok := eval.ReType(t.Else.(tree.TypedExpr), valType.WithoutTypeModifiers())
		if !ok {
			panic(pgerror.Newf(
				pgcode.DatatypeMismatch,
				"IF types %s and %s cannot be matched",
				t.Else.(tree.TypedExpr).ResolvedType(), valType,
			))
		}
		orElse := b.buildScalar(orElseExpr, inScope, nil, nil, colRefs)
		out = b.factory.ConstructCase(input, whens, orElse)

	case *tree.IndexedVar:
		// TODO(mgartner): Disallow ordinal column references completely in
		// v23.2.
		if !b.evalCtx.SessionData().AllowOrdinalColumnReferences {
			panic(errors.WithHintf(
				pgerror.Newf(pgcode.WarningDeprecatedFeature, "invalid syntax @%d", t.Idx+1),
				"ordinal column references have been deprecated. "+
					"Use `SET allow_ordinal_column_references=true` to allow them",
			))
		}
		if t.Idx < 0 || t.Idx >= len(inScope.cols) {
			panic(pgerror.Newf(pgcode.UndefinedColumn,
				"invalid column ordinal: @%d", t.Idx+1))
		}
		out = b.factory.ConstructVariable(inScope.cols[t.Idx].id)

	case *tree.NotExpr:
		input := b.buildScalar(reType(t.TypedInnerExpr(), types.Bool), inScope, nil, nil, colRefs)
		out = b.factory.ConstructNot(input)

	case *tree.IsNullExpr:
		input := b.buildScalar(t.TypedInnerExpr(), inScope, nil, nil, colRefs)
		if t.TypedInnerExpr().ResolvedType().Family() == types.TupleFamily {
			out = b.factory.ConstructIsTupleNull(input)
		} else {
			out = b.factory.ConstructIs(input, memo.NullSingleton)
		}

	case *tree.IsNotNullExpr:
		input := b.buildScalar(t.TypedInnerExpr(), inScope, nil, nil, colRefs)
		if t.TypedInnerExpr().ResolvedType().Family() == types.TupleFamily {
			out = b.factory.ConstructIsTupleNotNull(input)
		} else {
			out = b.factory.ConstructIsNot(input, memo.NullSingleton)
		}

	case *tree.NullIfExpr:
		valType := t.ResolvedType()
		// Ensure that the type of the first expression matches the resolved type
		// of the NULLIF expression so that type inference will be correct in the
		// CASE expression constructed below. For example, the type of
		// NULLIF(NULL, 0) should be int.
		expr1 := reType(t.Expr1.(tree.TypedExpr), valType)
		input := b.buildScalar(expr1, inScope, nil, nil, colRefs)
		cond := b.buildScalar(t.Expr2.(tree.TypedExpr), inScope, nil, nil, colRefs)
		whens := memo.ScalarListExpr{
			b.factory.ConstructWhen(cond, b.factory.ConstructNull(valType)),
		}
		out = b.factory.ConstructCase(input, whens, input)

	case *tree.OrExpr:
		left := b.buildScalar(reType(t.TypedLeft(), types.Bool), inScope, nil, nil, colRefs)
		right := b.buildScalar(reType(t.TypedRight(), types.Bool), inScope, nil, nil, colRefs)
		out = b.factory.ConstructOr(left, right)

	case *tree.ParenExpr:
		// Treat ParenExpr as if it wasn't present.
		return b.buildScalar(t.TypedInnerExpr(), inScope, outScope, outCol, colRefs)

	case *tree.Placeholder:
		if !b.KeepPlaceholders && b.evalCtx.HasPlaceholders() {
			b.HadPlaceholders = true
			// Replace placeholders with their value.
			d, err := eval.Expr(b.ctx, b.evalCtx, t)
			if err != nil {
				panic(err)
			}
			out = b.factory.ConstructConstVal(d, t.ResolvedType())
		} else {
			out = b.factory.ConstructPlaceholder(t)
		}

	case *tree.RangeCond:
		inputFrom := b.buildScalar(t.TypedLeftFrom(), inScope, nil, nil, colRefs)
		from := b.buildScalar(t.TypedFrom(), inScope, nil, nil, colRefs)
		inputTo := b.buildScalar(t.TypedLeftTo(), inScope, nil, nil, colRefs)
		to := b.buildScalar(t.TypedTo(), inScope, nil, nil, colRefs)
		out = b.buildRangeCond(t.Not, t.Symmetric, inputFrom, from, inputTo, to)

	case *sqlFnInfo:
		out = b.buildSQLFn(t, inScope, outScope, outCol, colRefs)

	case *srf:
		if len(t.cols) == 1 {
			if inGroupingContext {
				// Non-grouping column was referenced. Note that a column that is part
				// of a larger grouping expression would have been detected by the
				// groupStrs checking code above.
				panic(newGroupingError(t.cols[0].name.ReferenceName()))
			}
			return b.finishBuildScalarRef(&t.cols[0], inScope, outScope, outCol, colRefs)
		}
		els := make(memo.ScalarListExpr, len(t.cols))
		for i := range t.cols {
			els[i] = b.buildScalar(&t.cols[i], inScope, nil, nil, colRefs)
		}
		out = b.factory.ConstructTuple(els, t.ResolvedType())

	case *subquery:
		out, _ = b.buildSingleRowSubquery(t, inScope)
		// Perform correctness checks on the outer cols, update colRefs and
		// b.subquery.outerCols.
		b.checkSubqueryOuterCols(t.outerCols, inGroupingContext, inScope, colRefs)

	case *tree.Tuple:
		els := make(memo.ScalarListExpr, len(t.Exprs))
		for i := range t.Exprs {
			els[i] = b.buildScalar(t.Exprs[i].(tree.TypedExpr), inScope, nil, nil, colRefs)
		}
		out = b.factory.ConstructTuple(els, t.ResolvedType())

	case *tree.UnaryExpr:
		out = b.buildScalar(t.TypedInnerExpr(), inScope, nil, nil, colRefs)
		out = b.constructUnary(t.Operator, out, t.ResolvedType())

	case *tree.IsOfTypeExpr:
		// IsOfTypeExpr is a little strange because its value can be determined
		// statically just from the type of the expression.
		actualType := t.Expr.(tree.TypedExpr).ResolvedType()

		found := false
		for _, typ := range t.ResolvedTypes() {
			if actualType.Equivalent(typ) {
				found = true
				break
			}
		}

		if found != t.Not {
			out = b.factory.ConstructTrue()
		} else {
			out = b.factory.ConstructFalse()
		}

	// NB: this is the exception to the sorting of the case statements. The
	// tree.Datum case needs to occur after *tree.Placeholder which implements
	// Datum.
	case tree.Datum:
		out = b.factory.ConstructConstVal(t, t.ResolvedType())

	default:
		panic(unimplemented.Newf(fmt.Sprintf("optbuilder.%T", scalar), "not yet implemented: scalar expression: %T", scalar))
	}

	return b.finishBuildScalar(scalar, out, outScope, outCol)
}

func (b *Builder) hasSubOperator(t *tree.ComparisonExpr) bool {
	return t.Operator.Symbol == treecmp.Any || t.Operator.Symbol == treecmp.All || t.Operator.Symbol == treecmp.Some
}

func (b *Builder) buildAnyScalar(
	t *tree.ComparisonExpr, inScope *scope, colRefs *opt.ColSet,
) opt.ScalarExpr {
	left := b.buildScalar(t.TypedLeft(), inScope, nil, nil, colRefs)
	right := b.buildScalar(t.TypedRight(), inScope, nil, nil, colRefs)

	subop := opt.ComparisonOpMap[t.SubOperator.Symbol]

	if t.Operator.Symbol == treecmp.All {
		subop = opt.NegateOpMap[subop]
	}

	out := b.factory.ConstructAnyScalar(left, right, subop)
	if t.Operator.Symbol == treecmp.All {
		out = b.factory.ConstructNot(out)
	}
	return out
}

// buildFunction builds a set of memo groups that represent a function
// expression.
//
// f        The given function expression.
// outCol   The output column of the function being built.
// colRefs  The set of columns referenced so far by the scalar expression
//
//	being built. If not nil, it is updated with any columns seen in
//	finishBuildScalarRef.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildFunction(
	f *tree.FuncExpr, inScope, outScope *scope, outCol *scopeColumn, colRefs *opt.ColSet,
) (out opt.ScalarExpr) {
	if f.WindowDef != nil {
		if inScope.inAgg {
			panic(sqlerrors.NewWindowInAggError())
		}
	}

	def, err := f.Func.Resolve(b.ctx, b.semaCtx.SearchPath, b.semaCtx.FunctionResolver)
	if err != nil {
		panic(err)
	}

	overload := f.ResolvedOverload()
	if overload.HasSQLBody() {
		return b.buildUDF(f, def, inScope, outScope, outCol, colRefs)
	}
	b.factory.Metadata().AddBuiltin(f.Func.ReferenceByName)

	if overload.Class == tree.AggregateClass {
		panic(errors.AssertionFailedf("aggregate function should have been replaced"))
	}

	if overload.Class == tree.WindowClass {
		panic(errors.AssertionFailedf("window function should have been replaced"))
	}

	args := make(memo.ScalarListExpr, len(f.Exprs))
	for i, pexpr := range f.Exprs {
		args[i] = b.buildScalar(pexpr.(tree.TypedExpr), inScope, nil, nil, colRefs)
	}

	// Construct a private FuncOpDef that refers to a resolved function overload.
	out = b.factory.ConstructFunction(args, &memo.FunctionPrivate{
		Name:       def.Name,
		Typ:        f.ResolvedType(),
		Properties: &overload.FunctionProperties,
		Overload:   overload,
	})

	if overload.Class == tree.GeneratorClass {
		if overload.ReturnsRecordType {
			if colDefListTypes := b.getColumnDefinitionListTypes(inScope); colDefListTypes != nil {
				// Use the types from the column definition list to determine the
				// function return type.
				f.SetTypeAnnotation(colDefListTypes)
			}
		}
		return b.finishBuildGeneratorFunction(f, out, inScope, outScope, outCol)
	}

	// Add a dependency on sequences that are used as a string argument.
	if b.trackSchemaDeps {
		seqIdentifier, err := seqexpr.GetSequenceFromFunc(f)
		if err != nil {
			panic(err)
		}
		if seqIdentifier != nil {
			var ds cat.DataSource
			if seqIdentifier.IsByID() {
				flags := cat.Flags{
					AvoidDescriptorCaches: b.insideViewDef || b.insideFuncDef || b.insideTriggerDef,
				}
				ds, _, err = b.catalog.ResolveDataSourceByID(b.ctx, flags, cat.StableID(seqIdentifier.SeqID))
				if err != nil {
					panic(err)
				}
			} else {
				tn, err := parser.ParseQualifiedTableName(seqIdentifier.SeqName)
				if err != nil {
					panic(err)
				}
				ds, _, _ = b.resolveDataSource(tn, privilege.SELECT)
			}
			b.schemaDeps = append(b.schemaDeps, opt.SchemaDep{
				DataSource: ds,
			})
		}
	}

	return b.finishBuildScalar(f, out, outScope, outCol)
}

// getColumnDefinitionListTypes returns a composite type representing the column
// definition list for the current scope, if any. If one doesn't exist,
// getColumnDefinitionListTypes returns nil.
func (b *Builder) getColumnDefinitionListTypes(inScope *scope) *types.T {
	alias := inScope.alias
	if alias == nil || len(alias.Cols) == 0 || alias.Cols[0].Type == nil {
		return nil
	}
	contents := make([]*types.T, len(alias.Cols))
	labels := make([]string, len(alias.Cols))
	for i, c := range alias.Cols {
		defTyp, err := tree.ResolveType(b.ctx, c.Type, b.semaCtx.TypeResolver)
		if err != nil {
			panic(err)
		}
		contents[i] = defTyp
		labels[i] = string(c.Name)
	}
	return types.MakeLabeledTuple(contents, labels)
}

// buildRangeCond builds a RANGE clause as a simpler expression. Examples:
// x BETWEEN a AND b                ->  x >= a AND x <= b
// x NOT BETWEEN a AND b            ->  NOT (x >= a AND x <= b)
// x BETWEEN SYMMETRIC a AND b      ->  (x >= a AND x <= b) OR (x >= b AND x <= a)
// x NOT BETWEEN SYMMETRIC a AND b  ->  NOT ((x >= a AND x <= b) OR (x >= b AND x <= a))
//
// Note that x can be typed differently in the expressions (x >= a) and (x <= b)
// because a and b can have different types; the function takes both "variants"
// of x.
//
// Note that these expressions are subject to normalization rules (which can
// push down the negation).
// TODO(radu): this doesn't work when the expressions have side-effects.
func (b *Builder) buildRangeCond(
	not bool, symmetric bool, inputFrom, from, inputTo, to opt.ScalarExpr,
) opt.ScalarExpr {
	// Build "input >= from AND input <= to".
	out := b.factory.ConstructAnd(
		b.factory.ConstructGe(inputFrom, from),
		b.factory.ConstructLe(inputTo, to),
	)

	if symmetric {
		// Build "(input >= from AND input <= to) OR (input >= to AND input <= from)".
		lhs := out
		rhs := b.factory.ConstructAnd(
			b.factory.ConstructGe(inputTo, to),
			b.factory.ConstructLe(inputFrom, from),
		)
		out = b.factory.ConstructOr(lhs, rhs)
	}

	if not {
		out = b.factory.ConstructNot(out)
	}
	return out
}

// checkSubqueryOuterCols uses the subquery outer columns to update the given
// set of column references and the set of outer columns for any enclosing
// subuqery. It also performs the following checks:
//  1. If aggregates are not allowed in the current context (e.g., if we
//     are building the WHERE clause), it checks that the subquery does not
//     reference any aggregates from this scope.
//  2. If this is a grouping context, it checks that any outer columns from
//     the given subquery that reference inScope are either aggregate or
//     grouping columns in inScope.
func (b *Builder) checkSubqueryOuterCols(
	subqueryOuterCols opt.ColSet, inGroupingContext bool, inScope *scope, colRefs *opt.ColSet,
) {
	if subqueryOuterCols.Empty() {
		return
	}

	// Register the use of correlation to telemetry.
	// Note: we don't blindly increment the counter every time this
	// method is called, to avoid double counting the same query.
	if !b.isCorrelated {
		b.isCorrelated = true
		telemetry.Inc(sqltelemetry.CorrelatedSubqueryUseCounter)
	}

	var inScopeCols opt.ColSet
	if b.subquery != nil || inGroupingContext {
		// Only calculate the set of inScope columns if it will be used below.
		inScopeCols = inScope.colSet()
	}

	if colRefs != nil {
		colRefs.UnionWith(subqueryOuterCols)
	}
	if b.subquery != nil {
		b.subquery.outerCols.UnionWith(subqueryOuterCols.Difference(inScopeCols))
	}

	// Check 1 (see function comment).
	if b.semaCtx.Properties.IsSet(tree.RejectAggregates) && inScope.groupby != nil {
		aggCols := inScope.groupby.aggregateResultCols()
		for i := range aggCols {
			if subqueryOuterCols.Contains(aggCols[i].id) {
				panic(tree.NewInvalidFunctionUsageError(tree.AggregateClass, inScope.context.String()))
			}
		}
	}

	// Check 2 (see function comment).
	if inGroupingContext {
		subqueryOuterCols.IntersectionWith(inScopeCols)
		if !subqueryOuterCols.Empty() &&
			!subqueryOuterCols.SubsetOf(inScope.groupby.aggOutScope.colSet()) {
			subqueryOuterCols.DifferenceWith(inScope.groupby.aggOutScope.colSet())
			colID, _ := subqueryOuterCols.Next(0)
			col := inScope.getColumn(colID)
			name := col.name.ReferenceName()
			panic(pgerror.Newf(
				pgcode.Grouping,
				"subquery uses ungrouped column \"%s\" from outer query",
				tree.ErrString(&name)))
		}
	}
}

func (b *Builder) constructComparison(
	cmp *tree.ComparisonExpr, left, right opt.ScalarExpr,
) opt.ScalarExpr {
	switch cmp.Operator.Symbol {
	case treecmp.EQ:
		return b.factory.ConstructEq(left, right)
	case treecmp.LT:
		return b.factory.ConstructLt(left, right)
	case treecmp.GT:
		return b.factory.ConstructGt(left, right)
	case treecmp.LE:
		return b.factory.ConstructLe(left, right)
	case treecmp.GE:
		return b.factory.ConstructGe(left, right)
	case treecmp.NE:
		return b.factory.ConstructNe(left, right)
	case treecmp.In:
		return b.factory.ConstructIn(left, right)
	case treecmp.NotIn:
		return b.factory.ConstructNotIn(left, right)
	case treecmp.Like:
		return b.factory.ConstructLike(left, right)
	case treecmp.NotLike:
		return b.factory.ConstructNotLike(left, right)
	case treecmp.ILike:
		return b.factory.ConstructILike(left, right)
	case treecmp.NotILike:
		return b.factory.ConstructNotILike(left, right)
	case treecmp.SimilarTo:
		return b.factory.ConstructSimilarTo(left, right)
	case treecmp.NotSimilarTo:
		return b.factory.ConstructNotSimilarTo(left, right)
	case treecmp.RegMatch:
		leftFam, rightFam := cmp.Op.LeftType.Family(), cmp.Op.RightType.Family()
		if (leftFam == types.GeometryFamily || leftFam == types.Box2DFamily) &&
			(rightFam == types.GeometryFamily || rightFam == types.Box2DFamily) {
			// The ~ operator means "covers" when used with geometry or bounding box
			// operands.
			return b.factory.ConstructBBoxCovers(left, right)
		}
		return b.factory.ConstructRegMatch(left, right)
	case treecmp.NotRegMatch:
		return b.factory.ConstructNotRegMatch(left, right)
	case treecmp.RegIMatch:
		return b.factory.ConstructRegIMatch(left, right)
	case treecmp.NotRegIMatch:
		return b.factory.ConstructNotRegIMatch(left, right)
	case treecmp.IsDistinctFrom:
		return b.factory.ConstructIsNot(left, right)
	case treecmp.IsNotDistinctFrom:
		return b.factory.ConstructIs(left, right)
	case treecmp.Contains:
		return b.factory.ConstructContains(left, right)
	case treecmp.ContainedBy:
		return b.factory.ConstructContainedBy(left, right)
	case treecmp.JSONExists:
		return b.factory.ConstructJsonExists(left, right)
	case treecmp.JSONAllExists:
		return b.factory.ConstructJsonAllExists(left, right)
	case treecmp.JSONSomeExists:
		return b.factory.ConstructJsonSomeExists(left, right)
	case treecmp.Overlaps:
		leftFam, rightFam := cmp.Op.LeftType.Family(), cmp.Op.RightType.Family()
		if (leftFam == types.GeometryFamily || leftFam == types.Box2DFamily) &&
			(rightFam == types.GeometryFamily || rightFam == types.Box2DFamily) {
			// The && operator means "intersects" when used with geometry or bounding
			// box operands.
			return b.factory.ConstructBBoxIntersects(left, right)
		}
		return b.factory.ConstructOverlaps(left, right)
	case treecmp.TSMatches:
		return b.factory.ConstructTSMatches(left, right)
	}
	panic(errors.AssertionFailedf("unhandled comparison operator: %s", redact.Safe(cmp.Operator)))
}

func (b *Builder) constructBinary(
	bin treebin.BinaryOperator, left, right opt.ScalarExpr, typ *types.T,
) opt.ScalarExpr {
	switch bin.Symbol {
	case treebin.Bitand:
		return b.factory.ConstructBitand(left, right)
	case treebin.Bitor:
		return b.factory.ConstructBitor(left, right)
	case treebin.Bitxor:
		return b.factory.ConstructBitxor(left, right)
	case treebin.Plus:
		return b.factory.ConstructPlus(left, right)
	case treebin.Minus:
		return b.factory.ConstructMinus(left, right)
	case treebin.Mult:
		return b.factory.ConstructMult(left, right)
	case treebin.Div:
		return b.factory.ConstructDiv(left, right)
	case treebin.FloorDiv:
		return b.factory.ConstructFloorDiv(left, right)
	case treebin.Mod:
		return b.factory.ConstructMod(left, right)
	case treebin.Pow:
		return b.factory.ConstructPow(left, right)
	case treebin.Concat:
		return b.factory.ConstructConcat(left, right)
	case treebin.LShift:
		return b.factory.ConstructLShift(left, right)
	case treebin.RShift:
		return b.factory.ConstructRShift(left, right)
	case treebin.JSONFetchText:
		return b.factory.ConstructFetchText(left, right)
	case treebin.JSONFetchVal:
		return b.factory.ConstructFetchVal(left, right)
	case treebin.JSONFetchValPath:
		return b.factory.ConstructFetchValPath(left, right)
	case treebin.JSONFetchTextPath:
		return b.factory.ConstructFetchTextPath(left, right)
	case treebin.Distance:
		return b.factory.ConstructVectorDistance(left, right)
	case treebin.CosDistance:
		return b.factory.ConstructVectorCosDistance(left, right)
	case treebin.NegInnerProduct:
		return b.factory.ConstructVectorNegInnerProduct(left, right)
	}
	panic(errors.AssertionFailedf("unhandled binary operator: %s", redact.Safe(bin)))
}

func (b *Builder) constructUnary(
	un tree.UnaryOperator, input opt.ScalarExpr, typ *types.T,
) opt.ScalarExpr {
	switch un.Symbol {
	case tree.UnaryPlus:
		return b.factory.ConstructUnaryPlus(input)
	case tree.UnaryMinus:
		return b.factory.ConstructUnaryMinus(input)
	case tree.UnaryComplement:
		return b.factory.ConstructUnaryComplement(input)
	case tree.UnarySqrt:
		return b.factory.ConstructUnarySqrt(input)
	case tree.UnaryCbrt:
		return b.factory.ConstructUnaryCbrt(input)
	}
	panic(errors.AssertionFailedf("unhandled unary operator: %s", redact.Safe(un)))
}

// ScalarBuilder is a specialized variant of Builder that can be used to create
// a scalar from a TypedExpr. This is used to build scalar expressions for
// testing. It is also used temporarily to interface with the old planning code.
//
// TypedExprs can refer to columns in the current scope using IndexedVars (@1,
// @2, etc). When we build a scalar, we have to provide information about these
// columns.
// TODO(mgartner): Ordinal column references are deprecated.
type ScalarBuilder struct {
	Builder
	scope scope
}

// NewScalar creates a new ScalarBuilder. The columns in the metadata are accessible
// from scalar expressions via IndexedVars.
func NewScalar(
	ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, factory *norm.Factory,
) *ScalarBuilder {
	md := factory.Metadata()
	sb := &ScalarBuilder{
		Builder: Builder{
			factory: factory,
			ctx:     ctx,
			semaCtx: semaCtx,
			evalCtx: evalCtx,
		},
	}
	sb.scope.builder = &sb.Builder

	// Put all the columns in the current scope.
	sb.scope.cols = make([]scopeColumn, 0, md.NumColumns())
	for colID := opt.ColumnID(1); int(colID) <= md.NumColumns(); colID++ {
		colMeta := md.ColumnMeta(colID)
		var alias tree.TableName
		if colMeta.Table > 0 {
			alias = md.TableMeta(colMeta.Table).Alias
		}
		sb.scope.cols = append(sb.scope.cols, scopeColumn{
			name:  scopeColName(tree.Name(colMeta.Alias)),
			typ:   colMeta.Type,
			id:    colID,
			table: alias,
		})
	}

	return sb
}

// Build a memo structure from a TypedExpr: the root group represents a scalar
// expression equivalent to expr.
func (sb *ScalarBuilder) Build(expr tree.Expr) (_ opt.ScalarExpr, err error) {
	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate errors without adding lots of checks
			// for `if err != nil` throughout the construction code. This is only
			// possible because the code does not update shared state and does not
			// manipulate locks.
			if ok, e := errorutil.ShouldCatch(r); ok {
				err = e
			} else {
				panic(r)
			}
		}
	}()

	typedExpr := sb.scope.resolveType(expr, types.AnyElement)
	scalar := sb.buildScalar(typedExpr, &sb.scope, nil, nil, nil)
	return scalar, nil
}

// reType is similar to tree.ReType, except that it panics with an internal
// error if the expression cannot be re-typed. This should only be used when
// re-typing is expected to always be successful. For example, it is used to
// re-type the left and right children of an OrExpr to booleans, which should
// always succeed during the optbuild phase because type-checking has already
// validated the types of the children.
func reType(expr tree.TypedExpr, typ *types.T) tree.TypedExpr {
	retypedExpr, ok := eval.ReType(expr, typ)
	if !ok {
		panic(errors.AssertionFailedf(
			"expected successful retype from %s to %s",
			expr.ResolvedType(), typ,
		))
	}
	return retypedExpr
}
