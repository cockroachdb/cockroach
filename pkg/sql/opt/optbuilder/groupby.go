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

	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
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
	groupings []memo.GroupID

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
	varsUsed []opt.ColumnID
}

// aggregateInfo stores information about an aggregation function call.
type aggregateInfo struct {
	def  memo.FuncOpDef
	args []memo.GroupID
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
//  - the output scope for the aggregation operation.
//  - the output scope the post-projections.
func (b *Builder) buildAggregation(
	sel *tree.SelectClause, orderBy tree.OrderBy, fromScope *scope,
) (outScope *scope, projectionsScope *scope) {
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
	// TODO(radu): we aren't really using these as scopes, just as scopeColumn
	// containers. Perhaps separate the scopeColumn in a separate structure and
	// pass that instead of scopes.
	aggInScope := fromScope.replace()
	aggOutScope := fromScope.replace()

	// The "from" columns are visible to any grouping expressions.
	b.buildGroupingList(sel.GroupBy, sel.Exprs, fromScope, aggInScope)
	groupings := fromScope.groupby.groupings

	// Copy the grouping columns to the aggOutScope.
	aggOutScope.appendColumns(aggInScope)

	fromScope.groupby.aggInScope = aggInScope
	fromScope.groupby.aggOutScope = aggOutScope

	var having memo.GroupID
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
	b.buildProjectionList(sel.Exprs, fromScope, projectionsScope)

	// Any additional columns or aggregates in the ORDER BY clause (if it exists)
	// will be added here.
	b.buildOrderBy(orderBy, fromScope, projectionsScope)

	aggInfos := aggOutScope.groupby.aggs

	// Construct the pre-projection, which renders the grouping columns and the
	// aggregate arguments.
	b.constructProject(fromScope, aggInScope)

	// Construct the aggregation. We represent the aggregations as Function
	// operators with Variable arguments; construct those now.
	aggCols := aggOutScope.getAggregateCols()
	argCols := aggInScope.cols[len(groupings):]
	for i, agg := range aggInfos {
		argList := make([]memo.GroupID, len(agg.args))
		for j := range agg.args {
			colID := argCols[0].id
			argCols = argCols[1:]
			argList[j] = b.factory.ConstructVariable(b.factory.InternColumnID(colID))
		}
		def := agg.def
		aggCols[i].group = b.factory.ConstructFunction(
			b.factory.InternList(argList),
			b.factory.InternFuncOpDef(&def),
		)
	}

	aggList := b.constructList(opt.AggregationsOp, aggCols)
	var groupingColSet opt.ColSet
	for i := range groupings {
		groupingColSet.Add(int(aggInScope.cols[i].id))
	}
	aggOutScope.group = b.factory.ConstructGroupBy(aggInScope.group, aggList, b.factory.InternColSet(groupingColSet))

	// Wrap with having filter if it exists.
	if having != 0 {
		aggOutScope.group = b.factory.ConstructSelect(aggOutScope.group, having)
	}
	return aggOutScope, projectionsScope
}

// buildHaving builds a set of memo groups that represent the given HAVING
// clause. inScope contains the name bindings that are visible for this HAVING
// clause (e.g., passed in from an enclosing statement).
//
// The return value corresponds to the top-level memo group ID for this
// HAVING clause.
func (b *Builder) buildHaving(having tree.Expr, inScope *scope) memo.GroupID {
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
// See Builder.buildStmt for a description of the remaining input values.
func (b *Builder) buildGroupingList(
	groupBy tree.GroupBy, selects tree.SelectExprs, inScope *scope, outScope *scope,
) {

	inScope.groupby.groupings = make([]memo.GroupID, 0, len(groupBy))
	inScope.groupby.groupStrs = make(groupByStrSet, len(groupBy))
	for _, e := range groupBy {
		b.buildGrouping(e, selects, inScope, outScope)
	}
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

	// Make sure the GROUP BY columns have no aggregates or window functions.
	b.assertNoAggregationOrWindowing(groupBy, "GROUP BY")

	// Resolve types, expand stars, and flatten tuples.
	exprs := b.expandStarAndResolveType(groupBy, inScope)
	exprs = flattenTuples(exprs)

	// Finally, build each of the GROUP BY columns.
	for _, e := range exprs {
		// Save a representation of the GROUP BY expression for validation of the
		// SELECT and HAVING expressions. This enables queries such as:
		//    SELECT x+y FROM t GROUP BY x+y
		inScope.groupby.groupStrs[symbolicExprStr(e)] = exists
		col := b.buildScalarProjection(e, label, inScope, outScope)
		inScope.groupby.groupings = append(inScope.groupby.groupings, col.group)
	}
}

// buildAggregateFunction is called when we are building a function which is an
// aggregate.
func (b *Builder) buildAggregateFunction(
	f *tree.FuncExpr, funcDef memo.FuncOpDef, label string, inScope *scope,
) *scopeColumn {
	if len(f.Exprs) > 1 {
		// TODO: #10495
		panic(builderError{
			pgerror.UnimplementedWithIssueError(10495, "aggregate functions with multiple arguments are not supported yet"),
		})
	}

	aggInScope, aggOutScope := inScope.startAggFunc()

	info := aggregateInfo{
		def:  funcDef,
		args: make([]memo.GroupID, len(f.Exprs)),
	}
	aggInScopeColsBefore := len(aggInScope.cols)
	for i, pexpr := range f.Exprs {
		b.assertNoAggregationOrWindowing(pexpr, fmt.Sprintf("the argument of %s()", &f.Func))

		// This synthesizes a new aggInScope column, unless the argument is a simple
		// VariableOp.
		col := b.buildScalarProjection(
			pexpr.(tree.TypedExpr), "" /* label */, inScope, aggInScope,
		)
		info.args[i] = col.group
	}

	inScope.endAggFunc()

	// If we already have the same aggregation, reuse it. Otherwise add it
	// to the list of aggregates that need to be computed by the groupby
	// expression and synthesize a column for the aggregation result.
	col := aggOutScope.findAggregate(info)
	if col == nil {
		col = b.synthesizeColumn(aggOutScope, label, f.ResolvedType(), f, 0 /* group */)

		// Add the aggregate to the list of aggregates that need to be computed by
		// the groupby expression.
		aggOutScope.groupby.aggs = append(aggOutScope.groupby.aggs, info)
	} else {
		// Undo the adding of the args.
		// TODO(radu): is there a cleaner way to do this?
		aggInScope.cols = aggInScope.cols[:aggInScopeColsBefore]
	}

	// Replace the function call with a reference to the column.
	col.group = b.factory.ConstructVariable(b.factory.InternColumnID(col.id))
	return col
}

func isAggregate(def *tree.FunctionDefinition) bool {
	_, ok := builtins.Aggregates[strings.ToLower(def.Name)]
	return ok
}

func groupingError(colName string) error {
	return errorf("column \"%s\" must appear in the GROUP BY clause or be used in an aggregate function", colName)
}
