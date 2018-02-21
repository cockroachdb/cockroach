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

	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
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

	// groupings contains all group by expressions that were extracted from the
	// query and which will become columns in this scope.
	groupings []opt.GroupID

	// groupStrs contains a string representation of each GROUP BY expression
	// using symbolic notation. These strings are used to determine if SELECT
	// and HAVING expressions contain sub-expressions matching a GROUP BY
	// expression. This enables queries such as:
	//    SELECT x+y FROM t GROUP BY x+y
	// but not:
	//    SELECT x+y FROM t GROUP BY y+x
	groupStrs groupByStrSet

	// inAgg is true within the body of an aggregate function. inAgg is used
	// to ensure that nested aggregates are disallowed.
	inAgg bool

	// varsUsed is only utilized when groupingsScope is not nil.
	// It keeps track of variables that are encountered by the builder that are:
	//   (1) not explicit GROUP BY columns,
	//   (2) not part of a sub-expression that matches a GROUP BY expression, and
	//   (3) not contained in an aggregate.
	//
	// varsUsed is a slice rather than a set because the builder appends
	// variables found in each sub-expression, and variables from individual
	// sub-expresssions may be removed if they are found to match a GROUP BY
	// expression. If any variables remain in varsUsed when the builder is
	// done building a SELECT expression or HAVING clause, the builder throws
	// an error.
	//
	// For example, consider this query:
	//   SELECT COUNT(*), v/(k+v) FROM t.kv GROUP BY k+v
	// When building the expression v/(k+v), varsUsed will contain the variables
	// shown in brackets after each of the following expressions is built (in
	// order of completed recursive calls):
	//
	//  1.   Build v [v]
	//          \
	//  2.       \  Build k [v, k]
	//            \    \
	//  3.         \    \  Build v [v, k, v]
	//              \    \   /
	//  4.           \  Build (k+v) [v]  <- truncate varsUsed since (k+v) matches
	//                \   /                 a GROUP BY expression
	//  5.          Build v/(k+v) [v] <- error - build is complete and varsUsed
	//                                   is not empty
	varsUsed []opt.ColumnIndex
}

// aggregateInfo stores information about an aggregation function call.
type aggregateInfo struct {
	def  opt.FuncDef
	args []opt.GroupID
}

func (b *Builder) needsAggregation(sel *tree.SelectClause) bool {
	// We have an aggregation if:
	//  - we have a GROUP BY, or
	//  - we have a HAVING clause, or
	//  - we have aggregate functions in the select expressions.
	return len(sel.GroupBy) > 0 || sel.Having != nil || b.hasAggregates(sel.Exprs)
}

// hasAggregates determines if any of the given select expressions contain an
// aggregate function.
func (b *Builder) hasAggregates(selects tree.SelectExprs) bool {
	exprTransformCtx := transform.ExprTransformContext{}
	for _, sel := range selects {
		// TODO(rytaft): This function does not recurse into subqueries, so this
		// will be incorrect for correlated subqueries.
		if exprTransformCtx.AggregateInExpr(sel.Expr, b.semaCtx.SearchPath) {
			return true
		}
	}

	return false
}

// buildAggregation builds the pre-projection and the aggregation operators.
// Returns:
//  - the group with the aggregation operator and the corresponding scope
//  - post-projections with corresponding scope.
func (b *Builder) buildAggregation(
	sel *tree.SelectClause, fromGroup opt.GroupID, fromScope *scope,
) (outGroup opt.GroupID, outScope *scope, projections []opt.GroupID, projectionsScope *scope) {
	// We use two scopes:
	//   - aggInScope contains columns that are used as input by the
	//     GroupBy operator, specifically:
	//      - grouping columns (from the GROUP BY)
	//      - columns for the arguments to aggregate functions
	//     The grouping columns always come first.
	//
	//   - aggOutScope contains columns that are produced by the GroupBy operator,
	//     specifically:
	//       - the grouping columns
	//       - columns for the results of the aggregate functions.
	//
	// For example:
	//
	//   SELECT 1 + MIN(v*2) FROM kv GROUP BY k+3
	//
	//   aggInScope:   k+3 (as col1), v*2 (as col2)
	//   aggOutScope:  k+3 (as col1), MIN(col2) (as col3)
	//
	// Any aggregate functions which contain column references to this scope
	// trigger the creation of new grouping columns in the grouping scope. In
	// addition, if an aggregate function contains no column references, then the
	// aggregate will be added to the "nearest" grouping scope. For example:
	//   SELECT MAX(1) FROM t1
	//
	// TODO(radu): we aren't really using these as scopes, just as columnProps
	// containers. Perhaps separate the columnProps in a separate structure and
	// pass that instead of scopes.
	aggInScope := fromScope.replace()
	aggOutScope := fromScope.replace()

	// The "from" columns are visible to any grouping expressions.
	groupings := b.buildGroupingList(sel.GroupBy, sel.Exprs, fromScope, aggInScope)

	// Copy the grouping columns to the aggOutScope.
	aggOutScope.appendColumns(aggInScope)

	fromScope.groupby.groupings = groupings
	fromScope.groupby.aggInScope = aggInScope
	fromScope.groupby.aggOutScope = aggOutScope

	var having opt.GroupID
	if sel.Having != nil {
		// Any "grouping" columns are visible to both the "having" and "projection"
		// expressions. The build has the side effect of extracting aggregation
		// columns.
		having = b.buildHaving(sel.Having.Expr, fromScope)
	}

	projectionsScope = fromScope.replace()
	// This is where the magic happens. When this call reaches an aggregate
	// function that refers to variables in fromScope, buildAggregateFunction is
	// called which adds columns to the aggInScope and aggOutScope.
	projections = b.buildProjectionList(sel.Exprs, fromScope, projectionsScope)

	aggInfos := aggOutScope.groupby.aggs

	// Construct the pre-projection, which renders the grouping columns and the
	// aggregate arguments.
	outGroup = b.buildPreProjection(fromGroup, fromScope, aggInfos, aggInScope, groupings)

	// Construct the aggregation. We represent the aggregations as Function
	// operators with Variable arguments; construct those now.
	aggExprs := make([]opt.GroupID, len(aggInfos))
	argIdx := 0
	for i, agg := range aggInfos {
		argList := make([]opt.GroupID, len(agg.args))
		for j := range agg.args {
			colIndex := aggInScope.cols[argIdx].index
			argIdx++
			argList[j] = b.factory.ConstructVariable(b.factory.InternPrivate(colIndex))
		}
		aggExprs[i] = b.factory.ConstructFunction(
			b.factory.InternList(argList),
			b.factory.InternPrivate(agg.def),
		)
	}

	aggList := b.constructList(opt.AggregationsOp, aggExprs, aggOutScope.getAggregateCols())
	var groupingColSet opt.ColSet
	for i := range groupings {
		groupingColSet.Add(int(aggInScope.cols[i].index))
	}
	outGroup = b.factory.ConstructGroupBy(outGroup, aggList, b.factory.InternPrivate(&groupingColSet))

	// Wrap with having filter if it exists.
	if having != 0 {
		outGroup = b.factory.ConstructSelect(outGroup, having)
	}
	return outGroup, aggOutScope, projections, projectionsScope
}

// buildPreProjection constructs the projection before a GroupBy. This
// projection renders:
//  - grouping columns
//  - arguments to aggregate functions
func (b *Builder) buildPreProjection(
	fromGroup opt.GroupID,
	fromScope *scope,
	aggInfos []aggregateInfo,
	aggInScope *scope,
	groupings []opt.GroupID,
) (outGroup opt.GroupID) {
	// Don't add an unnecessary "pass-through" projection.
	if fromScope.hasSameColumns(aggInScope) {
		return fromGroup
	}

	preProjGroups := make([]opt.GroupID, 0, len(aggInScope.cols))
	preProjGroups = append(preProjGroups, groupings...)

	// Add the aggregate function arguments.
	for _, agg := range aggInfos {
		preProjGroups = append(preProjGroups, agg.args...)
	}
	// The aggInScope columns should match up with the grouping columns and the
	// arguments.
	if len(preProjGroups) != len(aggInScope.cols) {
		panic(fmt.Sprintf(
			"mismatch between aggregates and input scope columns: %d groups; cols: %v",
			len(preProjGroups), aggInScope.cols,
		))
	}

	preProjList := b.constructList(opt.ProjectionsOp, preProjGroups, aggInScope.cols)
	return b.factory.ConstructProject(fromGroup, preProjList)
}

// buildHaving builds a set of memo groups that represent the given HAVING
// clause. inScope contains the name bindings that are visible for this HAVING
// clause (e.g., passed in from an enclosing statement).
//
// The return value corresponds to the top-level memo group ID for this
// HAVING clause.
func (b *Builder) buildHaving(having tree.Expr, inScope *scope) opt.GroupID {
	out := b.buildScalar(inScope.resolveType(having, types.Bool), inScope)
	if len(inScope.groupby.varsUsed) > 0 {
		i := inScope.groupby.varsUsed[0]
		col := b.colMap[i]
		panic(groupingError(col.String()))
	}
	return out
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
// The first return value `groupings` is an ordered list of top-level memo
// groups corresponding to each GROUP BY expression. See Builder.buildStmt
// above for a description of the remaining input and return values.
func (b *Builder) buildGroupingList(
	groupBy tree.GroupBy, selects tree.SelectExprs, inScope *scope, outScope *scope,
) (groupings []opt.GroupID) {

	groupings = make([]opt.GroupID, 0, len(groupBy))
	inScope.groupby.groupStrs = make(groupByStrSet, len(groupBy))
	for _, e := range groupBy {
		subset := b.buildGrouping(e, selects, inScope, outScope)
		groupings = append(groupings, subset...)
	}

	return groupings
}

// buildGrouping builds a set of memo groups that represent a GROUP BY
// expression.
//
// groupBy  The given GROUP BY expression.
// selects  The select expressions are needed in case the GROUP BY expression
//          is an index into to the select list.
//
// The return value is an ordered list of top-level memo groups corresponding
// to the expression. The list generally consists of a single memo group except
// in the case of "*", where the expression is expanded to multiple columns.
//
// See Builder.buildStmt above for a description of the remaining input values
// (outScope is passed as a parameter here rather than a return value because
// the newly bound variables are appended to a growing list to be returned by
// buildGroupingList).
func (b *Builder) buildGrouping(
	groupBy tree.Expr, selects tree.SelectExprs, inScope, outScope *scope,
) []opt.GroupID {
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

	// Resolve types, expand stars, and flatten tuples.
	exprs := b.expandStarAndResolveType(groupBy, inScope)
	exprs = flattenTuples(exprs)

	// Finally, build each of the GROUP BY columns.
	out := make([]opt.GroupID, 0, len(exprs))
	for _, e := range exprs {
		// Save a representation of the GROUP BY expression for validation of the
		// SELECT and HAVING expressions. This enables queries such as:
		//    SELECT x+y FROM t GROUP BY x+y
		inScope.groupby.groupStrs[symbolicExprStr(e)] = exists
		out = append(out, b.buildScalarProjection(e, label, inScope, outScope))
	}
	return out
}

// buildAggregateFunction is called when we are building a function which is an
// aggregate.
func (b *Builder) buildAggregateFunction(
	f *tree.FuncExpr, funcDef opt.FuncDef, label string, inScope *scope,
) (out opt.GroupID, col *columnProps) {
	aggInScope, aggOutScope := inScope.startAggFunc()

	info := aggregateInfo{
		def:  funcDef,
		args: make([]opt.GroupID, len(f.Exprs)),
	}
	aggInScopeColsBefore := len(aggInScope.cols)
	for i, pexpr := range f.Exprs {
		// This synthesizes a new aggInScope column, unless the argument is a simple
		// VariableOp.
		info.args[i] = b.buildScalarProjection(
			pexpr.(tree.TypedExpr), "" /* label */, inScope, aggInScope,
		)
	}

	inScope.endAggFunc()

	// If we already have the same aggregation, reuse it. Otherwise add it
	// to the list of aggregates that need to be computed by the groupby
	// expression and synthesize a column for the aggregation result.
	col = aggOutScope.findAggregate(info)
	if col == nil {
		col = b.synthesizeColumn(aggOutScope, label, f.ResolvedType())

		// Add the aggregate to the list of aggregates that need to be computed by
		// the groupby expression.
		aggOutScope.groupby.aggs = append(aggOutScope.groupby.aggs, info)
	} else {
		// Undo the adding of the args.
		// TODO(radu): is there a cleaner way to do this?
		aggInScope.cols = aggInScope.cols[:aggInScopeColsBefore]
	}

	// Replace the function call with a reference to the column.
	return b.factory.ConstructVariable(b.factory.InternPrivate(col.index)), col
}
