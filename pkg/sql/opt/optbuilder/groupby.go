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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
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

	// groupings contains all group by expressions that were extracted from the
	// query and which will become columns in this scope.
	groupings []memo.GroupID

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
}

// aggregateInfo stores information about an aggregation function call.
type aggregateInfo struct {
	def      memo.FuncOpDef
	distinct bool
	args     []memo.GroupID
}

func (b *Builder) needsAggregation(sel *tree.SelectClause, orderBy tree.OrderBy) bool {
	// We have an aggregation if:
	//  - we have a GROUP BY, or
	//  - we have a HAVING clause, or
	//  - we have aggregate functions in the SELECT, DISTINCT ON and/or ORDER BY expressions.
	if len(sel.GroupBy) > 0 || sel.Having != nil {
		return true
	}

	for _, sel := range sel.Exprs {
		// TODO(rytaft): This function does not recurse into subqueries, so this
		// will be incorrect for correlated subqueries.
		if b.exprTransformCtx.AggregateInExpr(sel.Expr, b.semaCtx.SearchPath) {
			return true
		}
	}

	for _, on := range sel.DistinctOn {
		if b.exprTransformCtx.AggregateInExpr(on, b.semaCtx.SearchPath) {
			return true
		}
	}

	for _, ob := range orderBy {
		if b.exprTransformCtx.AggregateInExpr(ob.Expr, b.semaCtx.SearchPath) {
			return true
		}
	}

	return false
}

func (b *Builder) constructGroupBy(
	input memo.GroupID, groupingColSet opt.ColSet, aggCols []scopeColumn, ordering opt.Ordering,
) memo.GroupID {
	colList := make(opt.ColList, 0, len(aggCols))
	groupList := make([]memo.GroupID, 0, len(aggCols))

	// Deduplicate the columns; we don't need to produce the same aggregation
	// multiple times.
	colSet := opt.ColSet{}
	for i := range aggCols {
		if id, group := aggCols[i].id, aggCols[i].group; !colSet.Contains(int(id)) {
			if group == 0 {
				// A "pass through" column (i.e. a VariableOp) is not legal as an
				// aggregation.
				panic("variable as aggregation")
			}
			colList = append(colList, id)
			groupList = append(groupList, group)
			colSet.Add(int(id))
		}
	}

	aggs := b.factory.ConstructAggregations(
		b.factory.InternList(groupList),
		b.factory.InternColList(colList),
	)
	def := memo.GroupByDef{GroupingCols: groupingColSet}
	// The ordering of the GROUP BY is inherited from the input. This ordering is
	// only useful for intra-group ordering (for order-sensitive aggregations like
	// ARRAY_AGG). So we add the grouping columns as optional columns.
	def.Ordering.FromOrderingWithOptCols(ordering, groupingColSet)
	if groupingColSet.Empty() {
		return b.factory.ConstructScalarGroupBy(input, aggs, b.factory.InternGroupByDef(&def))
	}
	return b.factory.ConstructGroupBy(input, aggs, b.factory.InternGroupByDef(&def))
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

	// Any additional columns or aggregates in the ORDER BY and DISTINCT ON
	// clauses (if they exist) will be added here.
	b.buildOrderBy(orderBy, fromScope, projectionsScope)
	b.buildDistinctOnArgs(sel.DistinctOn, fromScope, projectionsScope)

	aggInfos := aggOutScope.groupby.aggs

	// Copy the ordering from fromScope to aggInScope if needed.
	// TODO(justin): we should have a whitelist somewhere of ordering-sensitive
	// aggregations and only propagate the ordering if we have one here.
	if len(aggOutScope.getAggregateCols()) > 0 {
		aggInScope.copyOrdering(fromScope)
	}

	// Construct the pre-projection, which renders the grouping columns and the
	// aggregate arguments, as well as any additional order by columns.
	b.constructProjectForScope(fromScope, aggInScope)

	// Construct the aggregation operators.
	aggCols := aggOutScope.getAggregateCols()
	argCols := aggInScope.cols[len(groupings):]
	for i, agg := range aggInfos {
		argList := make([]memo.GroupID, len(agg.args))
		for j := range agg.args {
			colID := argCols[0].id
			argCols = argCols[1:]
			argList[j] = b.factory.ConstructVariable(b.factory.InternColumnID(colID))
			if agg.distinct {
				// Wrap the argument with AggDistinct.
				argList[j] = b.factory.ConstructAggDistinct(argList[j])
			}
		}
		aggCols[i].group = constructAggLookup[agg.def.Name](b.factory, argList)
	}

	var groupingColSet opt.ColSet
	for i := range groupings {
		groupingColSet.Add(int(aggInScope.cols[i].id))
	}

	aggOutScope.group = b.constructGroupBy(
		aggInScope.group,
		groupingColSet,
		aggCols,
		aggInScope.ordering,
	)

	// Wrap with having filter if it exists.
	if having != 0 {
		// Wrap the filter in a FiltersOp.
		having = b.factory.ConstructFilters(b.factory.InternList([]memo.GroupID{having}))
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
	// We need to save and restore the previous value of the field in semaCtx
	// in case we are recursively called within a subquery context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
	b.semaCtx.Properties.Require("HAVING", tree.RejectWindowApplications|tree.RejectGenerators)
	return b.buildScalar(inScope.resolveAndRequireType(having, types.Bool, "HAVING"), inScope)
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

	// We need to save and restore the previous value of the field in semaCtx
	// in case we are recursively called within a subquery context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)

	// Make sure the GROUP BY columns have no special functions.
	b.semaCtx.Properties.Require("GROUP BY", tree.RejectSpecial)

	// Resolve types, expand stars, and flatten tuples.
	exprs := b.expandStarAndResolveType(groupBy, inScope)
	exprs = flattenTuples(exprs)

	// Finally, build each of the GROUP BY columns.
	for _, e := range exprs {
		// Save a representation of the GROUP BY expression for validation of the
		// SELECT and HAVING expressions. This enables queries such as:
		//   SELECT x+y FROM t GROUP BY x+y
		col := b.buildScalarProjection(e, label, inScope, outScope)
		inScope.groupby.groupStrs[symbolicExprStr(e)] = col
		inScope.groupby.groupings = append(inScope.groupby.groupings, col.group)
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
// If outScope is nil, then buildAggregateFunction returns a Variable reference
// to the aggOutScope column. Otherwise, it adds that column to the outScope
// projection, and returns 0.
func (b *Builder) buildAggregateFunction(
	f *tree.FuncExpr, funcDef memo.FuncOpDef, label string, inScope, outScope *scope,
) (out memo.GroupID) {
	if len(f.Exprs) > 1 {
		// TODO: #10495
		panic(builderError{pgerror.UnimplementedWithIssueError(
			10495,
			"aggregate functions with multiple arguments are not supported yet"),
		})
	}
	if f.Filter != nil {
		panic(unimplementedf("aggregates with FILTER are not supported yet"))
	}

	aggInScope, aggOutScope := inScope.startAggFunc()

	info := aggregateInfo{
		def:      funcDef,
		distinct: (f.Type == tree.DistinctFuncType),
		args:     make([]memo.GroupID, len(f.Exprs)),
	}
	aggInScopeColsBefore := len(aggInScope.cols)
	for i, pexpr := range f.Exprs {
		// This synthesizes a new aggInScope column, unless the argument is a simple
		// VariableOp.
		col := b.buildScalarProjection(
			pexpr.(tree.TypedExpr), "" /* label */, inScope, aggInScope,
		)
		info.args[i] = col.group
		if info.args[i] == 0 {
			info.args[i] = b.factory.ConstructVariable(b.factory.InternColumnID(col.id))
		}
	}

	inScope.endAggFunc()

	// If we already have the same aggregation, reuse it. Otherwise add it
	// to the list of aggregates that need to be computed by the groupby
	// expression and synthesize a column for the aggregation result.
	col := aggOutScope.findAggregate(info)
	if col == nil {
		// Use 0 as the group for now; it will be filled in later by the
		// buildAggregation method.
		col = b.synthesizeColumn(aggOutScope, label, f.ResolvedType(), f, 0 /* group */)

		// Add the aggregate to the list of aggregates that need to be computed by
		// the groupby expression.
		aggOutScope.groupby.aggs = append(aggOutScope.groupby.aggs, info)
	} else {
		// Undo the adding of the args.
		// TODO(radu): is there a cleaner way to do this?
		aggInScope.cols = aggInScope.cols[:aggInScopeColsBefore]

		// Update the column name if a label is provided.
		if label != "" {
			col.name = tree.Name(label)
		}
	}

	// Either wrap the column in a Variable expression if it's part of a larger
	// expression, or else add it to the outScope if it needs to be projected.
	return b.finishBuildScalarRef(col, label, aggOutScope, outScope)
}

func isAggregate(def *tree.FunctionDefinition) bool {
	return def.Class == tree.AggregateClass
}

func isGenerator(def *tree.FunctionDefinition) bool {
	return def.Class == tree.GeneratorClass
}

var constructAggLookup = map[string]func(f *norm.Factory, argList []memo.GroupID) memo.GroupID{
	"array_agg": func(f *norm.Factory, argList []memo.GroupID) memo.GroupID {
		return f.ConstructArrayAgg(argList[0])
	},
	"avg": func(f *norm.Factory, argList []memo.GroupID) memo.GroupID {
		return f.ConstructAvg(argList[0])
	},
	"bool_and": func(f *norm.Factory, argList []memo.GroupID) memo.GroupID {
		return f.ConstructBoolAnd(argList[0])
	},
	"bool_or": func(f *norm.Factory, argList []memo.GroupID) memo.GroupID {
		return f.ConstructBoolOr(argList[0])
	},
	"concat_agg": func(f *norm.Factory, argList []memo.GroupID) memo.GroupID {
		return f.ConstructConcatAgg(argList[0])
	},
	"count": func(f *norm.Factory, argList []memo.GroupID) memo.GroupID {
		return f.ConstructCount(argList[0])
	},
	"count_rows": func(f *norm.Factory, argList []memo.GroupID) memo.GroupID {
		return f.ConstructCountRows()
	},
	"max": func(f *norm.Factory, argList []memo.GroupID) memo.GroupID {
		return f.ConstructMax(argList[0])
	},
	"min": func(f *norm.Factory, argList []memo.GroupID) memo.GroupID {
		return f.ConstructMin(argList[0])
	},
	"sum_int": func(f *norm.Factory, argList []memo.GroupID) memo.GroupID {
		return f.ConstructSumInt(argList[0])
	},
	"sum": func(f *norm.Factory, argList []memo.GroupID) memo.GroupID {
		return f.ConstructSum(argList[0])
	},
	"sqrdiff": func(f *norm.Factory, argList []memo.GroupID) memo.GroupID {
		return f.ConstructSqrDiff(argList[0])
	},
	"variance": func(f *norm.Factory, argList []memo.GroupID) memo.GroupID {
		return f.ConstructVariance(argList[0])
	},
	"stddev": func(f *norm.Factory, argList []memo.GroupID) memo.GroupID {
		return f.ConstructStdDev(argList[0])
	},
	"xor_agg": func(f *norm.Factory, argList []memo.GroupID) memo.GroupID {
		return f.ConstructXorAgg(argList[0])
	},
	"json_agg": func(f *norm.Factory, argList []memo.GroupID) memo.GroupID {
		return f.ConstructJsonAgg(argList[0])
	},
	"jsonb_agg": func(f *norm.Factory, argList []memo.GroupID) memo.GroupID {
		return f.ConstructJsonbAgg(argList[0])
	},
}
