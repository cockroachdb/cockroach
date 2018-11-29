// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package optbuilder

// This file has builder code specific to aggregations (queries with GROUP BY,
// HAVING, or aggregate functions).
//
// We build such queries using three operators:
//
//  - a pre-projection: a ProjectOp which generates the columns needed for
//    the aggregation:
//      - group by expressions
//      - arguments to aggregation functions
//
//  - the aggregation: a GroupByOp which has the pre-projection as the
//    input and produces columns with the results of the aggregation
//    functions. The group by columns are also passed through.
//
//  - a post-projection: calculates expressions using the results of the
//    aggregations; this is analogous to the ProjectOp that we would use for a
//    no-aggregation Select.
//
// For example:
//   SELECT 1 + MIN(v*2) FROM kv GROUP BY k+3
//
//   pre-projection:  k+3 (as col1), v*2 (as col2)
//   aggregation:     group by col1, calculate MIN(col2) (as col3)
//   post-projection: 1 + col3

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// groupby information stored in scopes.
type groupby struct {
	// See buildAggregation for a description of these scopes.
	aggInScope  *scope
	aggOutScope *scope

	// aggs is used by aggOutScope (see buildAggregation); it contains information
	// about aggregate functions encountered.
	aggs []aggregateInfo

	// groupStrs contains a string representation of each GROUP BY expression
	// using symbolic notation. These strings are used to determine if SELECT
	// and HAVING expressions contain sub-expressions matching a GROUP BY
	// expression. This enables queries such as:
	//   SELECT x+y FROM t GROUP BY x+y
	// but not:
	//   SELECT x+y FROM t GROUP BY y+x
	//
	// Each string maps to the grouping column in an aggOutScope scope that
	// projects that expression.
	groupStrs groupByStrSet

	// inAgg is true within the body of an aggregate function. inAgg is used
	// to ensure that nested aggregates are disallowed.
	inAgg bool

	// buildingGroupingCols is true while the grouping columns are being built.
	// It is used to ensure that the builder does not throw a grouping error
	// prematurely.
	buildingGroupingCols bool
}

// aggregateInfo stores information about an aggregation function call.
type aggregateInfo struct {
	*tree.FuncExpr

	def      memo.FunctionPrivate
	distinct bool
	args     memo.ScalarListExpr

	// col is the output column of the aggregation.
	col *scopeColumn

	// colRefs contains the set of columns referenced by the arguments of the
	// aggregation. It is used to determine the appropriate scope for this
	// aggregation.
	colRefs opt.ColSet
}

// Walk is part of the tree.Expr interface.
func (a *aggregateInfo) Walk(v tree.Visitor) tree.Expr {
	return a
}

// TypeCheck is part of the tree.Expr interface.
func (a *aggregateInfo) TypeCheck(ctx *tree.SemaContext, desired types.T) (tree.TypedExpr, error) {
	if _, err := a.FuncExpr.TypeCheck(ctx, desired); err != nil {
		return nil, err
	}
	return a, nil
}

// Eval is part of the tree.TypedExpr interface.
func (a *aggregateInfo) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic("aggregateInfo must be replaced before evaluation")
}

var _ tree.Expr = &aggregateInfo{}
var _ tree.TypedExpr = &aggregateInfo{}

func (b *Builder) needsAggregation(sel *tree.SelectClause, scope *scope) bool {
	// We have an aggregation if:
	//  - we have a GROUP BY, or
	//  - we have a HAVING clause, or
	//  - we have aggregate functions in the SELECT, DISTINCT ON and/or ORDER BY expressions.
	if len(sel.GroupBy) > 0 || sel.Having != nil || scope.hasAggregates() {
		return true
	}

	return false
}

func (b *Builder) constructGroupBy(
	input memo.RelExpr, groupingColSet opt.ColSet, aggCols []scopeColumn, ordering opt.Ordering,
) memo.RelExpr {
	aggs := make(memo.AggregationsExpr, 0, len(aggCols))

	// Deduplicate the columns; we don't need to produce the same aggregation
	// multiple times.
	colSet := opt.ColSet{}
	for i := range aggCols {
		if id, scalar := aggCols[i].id, aggCols[i].scalar; !colSet.Contains(int(id)) {
			if scalar == nil {
				// A "pass through" column (i.e. a VariableOp) is not legal as an
				// aggregation.
				panic("variable as aggregation")
			}
			aggs = append(aggs, memo.AggregationsItem{
				Agg:        scalar,
				ColPrivate: memo.ColPrivate{Col: id},
			})
			colSet.Add(int(id))
		}
	}

	private := memo.GroupingPrivate{GroupingCols: groupingColSet}

	// The ordering of the GROUP BY is inherited from the input. This ordering is
	// only useful for intra-group ordering (for order-sensitive aggregations like
	// ARRAY_AGG). So we add the grouping columns as optional columns.
	private.Ordering.FromOrderingWithOptCols(ordering, groupingColSet)

	if groupingColSet.Empty() {
		return b.factory.ConstructScalarGroupBy(input, aggs, &private)
	}
	return b.factory.ConstructGroupBy(input, aggs, &private)
}

// buildAggregation builds the pre-projection and the aggregation operators.
// Returns the output scope for the aggregation operation.
func (b *Builder) buildAggregation(
	sel *tree.SelectClause,
	havingExpr tree.TypedExpr,
	fromScope, projectionsScope, orderByScope, distinctOnScope *scope,
) (outScope *scope) {
	// We use two scopes:
	//   - aggInScope contains columns that are used as input by the
	//     GroupBy operator, specifically:
	//      - columns for the arguments to aggregate functions
	//      - grouping columns (from the GROUP BY)
	//     The grouping columns always come second.
	//
	//   - aggOutScope contains columns that are produced by the GroupBy operator,
	//     specifically:
	//      - columns for the results of the aggregate functions.
	//      - the grouping columns
	//
	// For example:
	//
	//   SELECT 1 + MIN(v*2) FROM kv GROUP BY k+3
	//
	//   aggInScope:   v*2 (as col2), k+3 (as col1)
	//   aggOutScope:  MIN(col2) (as col3), k+3 (as col1)
	//
	// Any aggregate functions which contain column references to this scope
	// trigger the creation of new grouping columns in the grouping scope. In
	// addition, if an aggregate function contains no column references, then the
	// aggregate will be added to the "nearest" grouping scope. For example:
	//   SELECT MAX(1) FROM t1
	//
	// TODO(radu): we aren't really using these as scopes, just as scopeColumn
	// containers. Perhaps separate the scopeColumn in a separate structure and
	// pass that instead of scopes.
	if fromScope.groupby.aggInScope == nil {
		fromScope.groupby.aggInScope = fromScope.replace()
	}
	if fromScope.groupby.aggOutScope == nil {
		fromScope.groupby.aggOutScope = fromScope.replace()
	}
	aggInScope := fromScope.groupby.aggInScope
	aggOutScope := fromScope.groupby.aggOutScope

	// The "from" columns are visible to any grouping expressions.
	b.buildGroupingList(sel.GroupBy, sel.Exprs, fromScope, aggInScope)
	groupingsLen := len(fromScope.groupby.groupStrs)

	// Copy the grouping columns to the aggOutScope.
	groupingCols := aggInScope.getGroupingCols(groupingsLen)
	aggOutScope.appendColumns(groupingCols)

	var having opt.ScalarExpr
	if sel.Having != nil {
		// Any "grouping" columns are visible to both the "having" and "projection"
		// expressions.
		having = b.buildHaving(havingExpr, fromScope)
	}

	b.buildProjectionList(fromScope, projectionsScope)
	b.buildOrderBy(fromScope, projectionsScope, orderByScope)
	b.buildDistinctOnArgs(fromScope, projectionsScope, distinctOnScope)
	if len(fromScope.srfs) > 0 {
		fromScope.expr = b.constructProjectSet(fromScope.expr, fromScope.srfs)
	}

	aggInfos := aggOutScope.groupby.aggs

	// Construct the aggregation operators.
	haveOrderingSensitiveAgg := false
	aggCols := aggOutScope.getAggregateCols()
	argCols := aggInScope.getAggregateArgCols(groupingsLen)
	var fromCols opt.ColSet
	if b.subquery != nil {
		// Only calculate the set of fromScope columns if it will be used below.
		fromCols = fromScope.colSet()
	}
	for i, agg := range aggInfos {
		// Aggregate functions will never have more than 3 operands.
		var args [memo.MaxAggChildren]opt.ScalarExpr
		for j := range agg.args {
			colID := argCols[0].id
			argCols = argCols[1:]
			args[j] = b.factory.ConstructVariable(colID)
			if agg.distinct {
				// Wrap the argument with AggDistinct.
				args[j] = b.factory.ConstructAggDistinct(args[j])
			}
		}

		aggCols[i].scalar = b.constructAggregate(agg.def.Name, args)
		if opt.AggregateIsOrderingSensitive(aggCols[i].scalar.Op()) {
			haveOrderingSensitiveAgg = true
		}

		if b.subquery != nil {
			// Update the subquery with any outer columns from the aggregate
			// arguments. The outer columns were not added in finishBuildScalarRef
			// because at the time the arguments were built, we did not know which
			// was the appropriate scope for the aggregation. (buildAggregateFunction
			// ensures that finishBuildScalarRef doesn't add the outer columns by
			// temporarily setting b.subquery to nil. See buildAggregateFunction
			// for more details.)
			b.subquery.outerCols.UnionWith(agg.colRefs.Difference(fromCols))
		}
	}

	if haveOrderingSensitiveAgg {
		aggInScope.copyOrdering(fromScope)
	}

	// Construct the pre-projection, which renders the grouping columns and the
	// aggregate arguments, as well as any additional order by columns.
	b.constructProjectForScope(fromScope, aggInScope)

	var groupingColSet opt.ColSet
	for i := range groupingCols {
		groupingColSet.Add(int(groupingCols[i].id))
	}

	aggOutScope.expr = b.constructGroupBy(
		aggInScope.expr.(memo.RelExpr),
		groupingColSet,
		aggCols,
		aggInScope.ordering,
	)

	// Wrap with having filter if it exists.
	if having != nil {
		input := aggOutScope.expr.(memo.RelExpr)
		filters := memo.FiltersExpr{{Condition: having}}
		aggOutScope.expr = b.factory.ConstructSelect(input, filters)
	}
	return aggOutScope
}

// analyzeHaving analyzes the having clause and returns it as a typed
// expression. inScope contains the name bindings that are visible for this
// HAVING clause (e.g., passed in from an enclosing statement).
func (b *Builder) analyzeHaving(having *tree.Where, inScope *scope) tree.TypedExpr {
	if having == nil {
		return nil
	}

	// We need to save and restore the previous value of the field in semaCtx
	// in case we are recursively called within a subquery context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
	b.semaCtx.Properties.Require("HAVING", tree.RejectWindowApplications|tree.RejectGenerators)
	inScope.context = "HAVING"
	return inScope.resolveAndRequireType(having.Expr, types.Bool)
}

// buildHaving builds a set of memo groups that represent the given HAVING
// clause. inScope contains the name bindings that are visible for this HAVING
// clause (e.g., passed in from an enclosing statement).
//
// The return value corresponds to the top-level memo group ID for this
// HAVING clause.
func (b *Builder) buildHaving(having tree.TypedExpr, inScope *scope) opt.ScalarExpr {
	return b.buildScalar(having, inScope, nil, nil, nil)
}

// buildGroupingList builds a set of memo groups that represent a list of
// GROUP BY expressions.
//
// groupBy  The given GROUP BY expressions.
// selects  The select expressions are needed in case one of the GROUP BY
//          expressions is an index into to the select list. For example,
//              SELECT count(*), k FROM t GROUP BY 2
//          indicates that the grouping is on the second select expression, k.
//
// See Builder.buildStmt for a description of the remaining input values.
func (b *Builder) buildGroupingList(
	groupBy tree.GroupBy, selects tree.SelectExprs, inScope *scope, outScope *scope,
) {
	inScope.groupby.groupStrs = make(groupByStrSet, len(groupBy))
	if outScope.cols == nil {
		outScope.cols = make([]scopeColumn, 0, len(groupBy))
	}

	inScope.startBuildingGroupingCols()
	for _, e := range groupBy {
		b.buildGrouping(e, selects, inScope, outScope)
	}
	inScope.endBuildingGroupingCols()
}

// buildGrouping builds a set of memo groups that represent a GROUP BY
// expression.
//
// groupBy  The given GROUP BY expression.
// selects  The select expressions are needed in case the GROUP BY expression
//          is an index into to the select list.
//
// See Builder.buildStmt for a description of the remaining input values.
func (b *Builder) buildGrouping(
	groupBy tree.Expr, selects tree.SelectExprs, inScope, outScope *scope,
) {
	// Unwrap parenthesized expressions like "((a))" to "a".
	groupBy = tree.StripParens(groupBy)

	// Check whether the GROUP BY clause refers to a column in the SELECT list
	// by index, e.g. `SELECT a, SUM(b) FROM y GROUP BY 1`.
	col := colIndex(len(selects), groupBy, "GROUP BY")
	label := ""
	if col != -1 {
		groupBy = selects[col].Expr
		label = string(selects[col].As)
	}

	// We need to save and restore the previous value of the field in semaCtx
	// in case we are recursively called within a subquery context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)

	// Make sure the GROUP BY columns have no special functions.
	b.semaCtx.Properties.Require("GROUP BY", tree.RejectSpecial)
	inScope.context = "GROUP BY"

	// Resolve types, expand stars, and flatten tuples.
	exprs := b.expandStarAndResolveType(groupBy, inScope)
	exprs = flattenTuples(exprs)

	// Finally, build each of the GROUP BY columns.
	for _, e := range exprs {
		// Save a representation of the GROUP BY expression for validation of the
		// SELECT and HAVING expressions. This enables queries such as:
		//   SELECT x+y FROM t GROUP BY x+y
		col := b.addColumn(outScope, label, e)
		b.buildScalar(e, inScope, outScope, col, nil)
		inScope.groupby.groupStrs[symbolicExprStr(e)] = col
	}
}

// buildAggregateFunction is called when we are building a function which is an
// aggregate. Any non-trivial parameters (i.e. not column reference) to the
// aggregate function are extracted and added to aggInScope. The aggregate
// function expression itself is added to aggOutScope. For example:
//
//   SELECT SUM(x+1) FROM xy
//   =>
//   aggInScope : x+1 AS column1
//   aggOutScope: SUM(column1)
//
// buildAggregateFunction returns a pointer to the aggregateInfo containing
// the function definition, fully built arguments, and the aggregate output
// column.
func (b *Builder) buildAggregateFunction(
	f *tree.FuncExpr, def *memo.FunctionPrivate, inScope *scope,
) *aggregateInfo {
	if len(f.Exprs) > 1 {
		// TODO: #10495
		panic(builderError{pgerror.UnimplementedWithIssueError(
			10495,
			"aggregate functions with multiple arguments are not supported yet"),
		})
	}

	tempScope := inScope.startAggFunc()
	tempScopeColsBefore := len(tempScope.cols)

	info := aggregateInfo{
		FuncExpr: f,
		def:      *def,
		distinct: (f.Type == tree.DistinctFuncType),
		args:     make(memo.ScalarListExpr, len(f.Exprs)),
	}

	// Temporarily set b.subquery to nil so we don't add outer columns to the
	// wrong scope.
	subq := b.subquery
	b.subquery = nil
	defer func() { b.subquery = subq }()

	for i, pexpr := range f.Exprs {
		// This synthesizes a new tempScope column, unless the argument is a
		// simple VariableOp.
		texpr := pexpr.(tree.TypedExpr)
		col := b.addColumn(tempScope, "" /* label */, texpr)
		b.buildScalar(texpr, inScope, tempScope, col, &info.colRefs)
		if col.scalar != nil {
			info.args[i] = col.scalar
		} else {
			info.args[i] = b.factory.ConstructVariable(col.id)
		}
	}

	// Find the appropriate aggregation scopes for this aggregate now that we
	// know which columns it references. If necessary, we'll move the columns
	// for the arguments from tempScope to aggInScope below.
	aggInScope, aggOutScope := inScope.endAggFunc(info.colRefs)

	// If we already have the same aggregation, reuse it. Otherwise add it
	// to the list of aggregates that need to be computed by the groupby
	// expression and synthesize a column for the aggregation result.
	info.col = aggOutScope.findAggregate(info)
	if info.col == nil {
		// Use 0 as the group for now; it will be filled in later by the
		// buildAggregation method.
		info.col = b.synthesizeColumn(aggOutScope, def.Name, f.ResolvedType(), f, nil /* scalar */)

		// Move the columns for the aggregate input expressions to the correct scope.
		if aggInScope != tempScope {
			aggInScope.cols = append(aggInScope.cols, tempScope.cols[tempScopeColsBefore:]...)
			tempScope.cols = tempScope.cols[:tempScopeColsBefore]
		}

		// Add the aggregate to the list of aggregates that need to be computed by
		// the groupby expression.
		aggOutScope.groupby.aggs = append(aggOutScope.groupby.aggs, info)
	} else {
		// Undo the adding of the args.
		// TODO(radu): is there a cleaner way to do this?
		tempScope.cols = tempScope.cols[:tempScopeColsBefore]
	}

	return &info
}

func (b *Builder) constructAggregate(
	name string, args [memo.MaxAggChildren]opt.ScalarExpr,
) opt.ScalarExpr {
	switch name {
	case "array_agg":
		return b.factory.ConstructArrayAgg(args[0])
	case "avg":
		return b.factory.ConstructAvg(args[0])
	case "bool_and":
		return b.factory.ConstructBoolAnd(args[0])
	case "bool_or":
		return b.factory.ConstructBoolOr(args[0])
	case "concat_agg":
		return b.factory.ConstructConcatAgg(args[0])
	case "count":
		return b.factory.ConstructCount(args[0])
	case "count_rows":
		return b.factory.ConstructCountRows()
	case "max":
		return b.factory.ConstructMax(args[0])
	case "min":
		return b.factory.ConstructMin(args[0])
	case "sum_int":
		return b.factory.ConstructSumInt(args[0])
	case "sum":
		return b.factory.ConstructSum(args[0])
	case "sqrdiff":
		return b.factory.ConstructSqrDiff(args[0])
	case "variance":
		return b.factory.ConstructVariance(args[0])
	case "stddev":
		return b.factory.ConstructStdDev(args[0])
	case "xor_agg":
		return b.factory.ConstructXorAgg(args[0])
	case "json_agg":
		return b.factory.ConstructJsonAgg(args[0])
	case "jsonb_agg":
		return b.factory.ConstructJsonbAgg(args[0])
	}
	panic(fmt.Sprintf("unhandled aggregate: %s", name))
}

func isAggregate(def *tree.FunctionDefinition) bool {
	return def.Class == tree.AggregateClass
}

func isGenerator(def *tree.FunctionDefinition) bool {
	return def.Class == tree.GeneratorClass
}

func newGroupingError(name *tree.Name) error {
	return pgerror.NewErrorf(pgerror.CodeGroupingError,
		"column \"%s\" must appear in the GROUP BY clause or be used in an aggregate function",
		tree.ErrString(name),
	)
}
