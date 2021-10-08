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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// groupby information stored in scopes.
type groupby struct {
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
	// containers. Perhaps separate the necessary functionality in a separate
	// structure and pass that instead of scopes.
	aggInScope  *scope
	aggOutScope *scope

	// aggs contains information about aggregate functions encountered.
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

	// buildingGroupingCols is true while the grouping columns are being built.
	// It is used to ensure that the builder does not throw a grouping error
	// prematurely.
	buildingGroupingCols bool
}

// groupByStrSet is a set of stringified GROUP BY expressions that map to the
// grouping column in an aggOutScope scope that projects that expression. It
// is used to enforce scoping rules, since any non-aggregate, variable
// expression in the SELECT list must be a GROUP BY expression or be composed
// of GROUP BY expressions. For example, this query is legal:
//
//   SELECT COUNT(*), k + v FROM kv GROUP by k, v
//
// but this query is not:
//
//   SELECT COUNT(*), k + v FROM kv GROUP BY k - v
//
type groupByStrSet map[string]*scopeColumn

// hasNonCommutativeAggregates checks whether any of the aggregates are
// non-commutative or ordering sensitive.
func (g *groupby) hasNonCommutativeAggregates() bool {
	for i := range g.aggs {
		if !g.aggs[i].isCommutative() {
			return true
		}
	}
	return false
}

// groupingCols returns the columns in the aggInScope corresponding to grouping
// columns.
func (g *groupby) groupingCols() []scopeColumn {
	// Grouping cols are always clustered at the end of the column list.
	return g.aggInScope.cols[len(g.aggInScope.cols)-len(g.groupStrs):]
}

// getAggregateArgCols returns the columns in the aggInScope corresponding to
// arguments to aggregate functions. If the aggregate has a filter, the column
// corresponding to the filter's input will immediately follow the arguments.
func (g *groupby) aggregateArgCols() []scopeColumn {
	return g.aggInScope.cols[:len(g.aggInScope.cols)-len(g.groupStrs)]
}

// getAggregateResultCols returns the columns in the aggOutScope corresponding
// to aggregate functions.
func (g *groupby) aggregateResultCols() []scopeColumn {
	// Aggregates are always clustered at the beginning of the column list, in
	// the same order as s.groupby.aggs.
	return g.aggOutScope.cols[:len(g.aggs)]
}

// hasAggregates returns true if the enclosing scope has aggregate functions.
func (g *groupby) hasAggregates() bool {
	return len(g.aggs) > 0
}

// findAggregate finds the given aggregate among the bound variables
// in this scope. Returns nil if the aggregate is not found.
func (g *groupby) findAggregate(agg aggregateInfo) *scopeColumn {
	if g.aggs == nil {
		return nil
	}

	for i, a := range g.aggs {
		// Find an existing aggregate that uses the same function overload.
		if a.def.Overload == agg.def.Overload && a.distinct == agg.distinct && a.filter == agg.filter {
			// Now check that the arguments are identical.
			if len(a.args) == len(agg.args) {
				match := true
				for j, arg := range a.args {
					if arg != agg.args[j] {
						match = false
						break
					}
				}

				// If agg is ordering sensitive, check if the orders match as well.
				if match && !agg.isCommutative() {
					if len(a.OrderBy) != len(agg.OrderBy) {
						match = false
					} else {
						for j := range a.OrderBy {
							if !a.OrderBy[j].Equal(agg.OrderBy[j]) {
								match = false
								break
							}
						}
					}
				}

				if match {
					// Aggregate already exists, so return information about the
					// existing column that computes it.
					return &g.aggregateResultCols()[i]
				}
			}
		}
	}

	return nil
}

// aggregateInfo stores information about an aggregation function call.
type aggregateInfo struct {
	*tree.FuncExpr

	def      memo.FunctionPrivate
	distinct bool
	args     memo.ScalarListExpr
	filter   opt.ScalarExpr

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
func (a *aggregateInfo) TypeCheck(
	ctx context.Context, semaCtx *tree.SemaContext, desired *types.T,
) (tree.TypedExpr, error) {
	if _, err := a.FuncExpr.TypeCheck(ctx, semaCtx, desired); err != nil {
		return nil, err
	}
	return a, nil
}

// isOrderedSetAggregate returns true if the given aggregate operator is an
// ordered-set aggregate.
func (a aggregateInfo) isOrderedSetAggregate() bool {
	switch a.def.Name {
	case "percentile_disc_impl", "percentile_cont_impl":
		return true
	default:
		return false
	}
}

// isOrderingSensitive returns true if the given aggregate operator is
// ordering sensitive. That is, it can give different results based on the order
// values are fed to it.
func (a aggregateInfo) isOrderingSensitive() bool {
	if a.isOrderedSetAggregate() {
		return true
	}
	switch a.def.Name {
	case "array_agg", "concat_agg", "string_agg", "json_agg", "jsonb_agg", "json_object_agg", "jsonb_object_agg",
		"st_makeline", "st_collect", "st_memcollect":
		return true
	default:
		return false
	}
}

// isCommutative checks whether the aggregate is commutative. That is, if it is
// ordering insensitive or if no ordering is specified.
func (a aggregateInfo) isCommutative() bool {
	return a.OrderBy == nil || !a.isOrderingSensitive()
}

// Eval is part of the tree.TypedExpr interface.
func (a *aggregateInfo) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic(errors.AssertionFailedf("aggregateInfo must be replaced before evaluation"))
}

// ResolvedType is part of the tree.TypedExpr interface.
func (a *aggregateInfo) ResolvedType() *types.T {
	return a.col.typ
}

var _ tree.Expr = &aggregateInfo{}
var _ tree.TypedExpr = &aggregateInfo{}

func (b *Builder) needsAggregation(sel *tree.SelectClause, scope *scope) bool {
	// We have an aggregation if:
	//  - we have a GROUP BY, or
	//  - we have a HAVING clause, or
	//  - we have aggregate functions in the SELECT, DISTINCT ON and/or ORDER BY expressions.
	return len(sel.GroupBy) > 0 ||
		sel.Having != nil ||
		(scope.groupby != nil && scope.groupby.hasAggregates())
}

func (b *Builder) constructGroupBy(
	input memo.RelExpr, groupingColSet opt.ColSet, aggCols []scopeColumn, ordering opt.Ordering,
) memo.RelExpr {
	aggs := make(memo.AggregationsExpr, 0, len(aggCols))

	// Deduplicate the columns; we don't need to produce the same aggregation
	// multiple times.
	colSet := opt.ColSet{}
	for i := range aggCols {
		if id, scalar := aggCols[i].id, aggCols[i].scalar; !colSet.Contains(id) {
			if scalar == nil {
				// A "pass through" column (i.e. a VariableOp) is not legal as an
				// aggregation.
				panic(errors.AssertionFailedf("variable as aggregation"))
			}
			aggs = append(aggs, b.factory.ConstructAggregationsItem(scalar, id))
			colSet.Add(id)
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

// buildGroupingColumns builds the grouping columns and adds them to the
// groupby scopes that will be used to build the aggregation expression.
// Returns the slice of grouping columns.
func (b *Builder) buildGroupingColumns(sel *tree.SelectClause, projectionsScope, fromScope *scope) {
	if fromScope.groupby == nil {
		fromScope.initGrouping()
	}
	g := fromScope.groupby

	// The "from" columns are visible to any grouping expressions.
	b.buildGroupingList(sel.GroupBy, sel.Exprs, projectionsScope, fromScope)

	// Copy the grouping columns to the aggOutScope.
	g.aggOutScope.appendColumns(g.groupingCols())
}

// buildAggregation builds the aggregation operators and constructs the
// GroupBy expression. Returns the output scope for the aggregation operation.
func (b *Builder) buildAggregation(having opt.ScalarExpr, fromScope *scope) (outScope *scope) {
	g := fromScope.groupby

	groupingCols := g.groupingCols()

	// Build ColSet of grouping columns.
	var groupingColSet opt.ColSet
	for i := range groupingCols {
		groupingColSet.Add(groupingCols[i].id)
	}

	// If there are any aggregates that are ordering sensitive, build the
	// aggregations as window functions over each group.
	if g.hasNonCommutativeAggregates() {
		return b.buildAggregationAsWindow(groupingColSet, having, fromScope)
	}

	aggInfos := g.aggs

	// Construct the aggregation operators.
	haveOrderingSensitiveAgg := false
	aggCols := g.aggregateResultCols()
	argCols := g.aggregateArgCols()
	var fromCols opt.ColSet
	if b.subquery != nil {
		// Only calculate the set of fromScope columns if it will be used below.
		fromCols = fromScope.colSet()
	}
	for i, agg := range aggInfos {
		// First accumulate the arguments to the aggregate function. These are
		// always variables referencing columns in the GroupBy input expression,
		// except in the case of string_agg, where the second argument must be
		// a constant expression.
		args := make([]opt.ScalarExpr, 0, 2)
		for range agg.args {
			colID := argCols[0].id
			args = append(args, b.factory.ConstructVariable(colID))

			// Skip past argCols that have been handled. There may be variable
			// number of them, so need to set up for next aggregate function.
			argCols = argCols[1:]
		}

		// Construct the aggregate function from its name and arguments and store
		// it in the corresponding scope column.
		aggCols[i].scalar = b.constructAggregate(agg.def.Name, args)

		// Wrap the aggregate function with an AggDistinct operator if DISTINCT
		// was specified in the query.
		if agg.distinct {
			aggCols[i].scalar = b.factory.ConstructAggDistinct(aggCols[i].scalar)
		}

		// Wrap the aggregate function or the AggDistinct in an AggFilter operator
		// if FILTER (WHERE ...) was specified in the query.
		// TODO(justin): add a norm rule to push these filters below GroupBy where
		// possible.
		if agg.filter != nil {
			// Column containing filter expression is always after the argument
			// columns (which have already been processed).
			colID := argCols[0].id
			argCols = argCols[1:]
			variable := b.factory.ConstructVariable(colID)
			aggCols[i].scalar = b.factory.ConstructAggFilter(aggCols[i].scalar, variable)
		}

		if agg.isOrderingSensitive() {
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
		g.aggInScope.copyOrdering(fromScope)
	}

	// Construct the pre-projection, which renders the grouping columns and the
	// aggregate arguments, as well as any additional order by columns.
	b.constructProjectForScope(fromScope, g.aggInScope)

	g.aggOutScope.expr = b.constructGroupBy(
		g.aggInScope.expr.(memo.RelExpr),
		groupingColSet,
		aggCols,
		g.aggInScope.ordering,
	)

	// Wrap with having filter if it exists.
	if having != nil {
		input := g.aggOutScope.expr.(memo.RelExpr)
		filters := memo.FiltersExpr{b.factory.ConstructFiltersItem(having)}
		g.aggOutScope.expr = b.factory.ConstructSelect(input, filters)
	}

	return g.aggOutScope
}

// analyzeHaving analyzes the having clause and returns it as a typed
// expression. fromScope contains the name bindings that are visible for this
// HAVING clause (e.g., passed in from an enclosing statement).
func (b *Builder) analyzeHaving(having *tree.Where, fromScope *scope) tree.TypedExpr {
	if having == nil {
		return nil
	}

	// We need to save and restore the previous value of the field in semaCtx
	// in case we are recursively called within a subquery context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
	b.semaCtx.Properties.Require(
		exprKindHaving.String(), tree.RejectWindowApplications|tree.RejectGenerators,
	)
	fromScope.context = exprKindHaving
	return fromScope.resolveAndRequireType(having.Expr, types.Bool)
}

// buildHaving builds a set of memo groups that represent the given HAVING
// clause. fromScope contains the name bindings that are visible for this HAVING
// clause (e.g., passed in from an enclosing statement).
//
// The return value corresponds to the top-level memo group ID for this
// HAVING clause.
func (b *Builder) buildHaving(having tree.TypedExpr, fromScope *scope) opt.ScalarExpr {
	if having == nil {
		return nil
	}

	return b.buildScalar(having, fromScope, nil, nil, nil)
}

// buildGroupingList builds a set of memo groups that represent a list of
// GROUP BY expressions, adding the group-by expressions as columns to
// aggInScope and populating groupStrs.
//
// groupBy   The given GROUP BY expressions.
// selects   The select expressions are needed in case one of the GROUP BY
//           expressions is an index into to the select list. For example,
//               SELECT count(*), k FROM t GROUP BY 2
//           indicates that the grouping is on the second select expression, k.
// fromScope The scope for the input to the aggregation (the FROM clause).
func (b *Builder) buildGroupingList(
	groupBy tree.GroupBy, selects tree.SelectExprs, projectionsScope *scope, fromScope *scope,
) {
	g := fromScope.groupby
	g.groupStrs = make(groupByStrSet, len(groupBy))
	if g.aggInScope.cols == nil {
		g.aggInScope.cols = make([]scopeColumn, 0, len(groupBy))
	}

	// The buildingGroupingCols flag is used to ensure that a grouping error is
	// not called prematurely. For example:
	//   SELECT count(*), a FROM ab GROUP BY a
	// is legal, but
	//   SELECT count(*), b FROM ab GROUP BY a
	// will throw the error, `column "b" must appear in the GROUP BY clause or be
	// used in an aggregate function`. The builder cannot know whether there is
	// a grouping error until the grouping columns are fully built.
	g.buildingGroupingCols = true
	for _, e := range groupBy {
		b.buildGrouping(e, selects, projectionsScope, fromScope, g.aggInScope)
	}
	g.buildingGroupingCols = false
}

// buildGrouping builds a set of memo groups that represent a GROUP BY
// expression. The expression (or expressions, if we have a star) is added to
// groupStrs and to the aggInScope.
//
//
// groupBy          The given GROUP BY expression.
// selects          The select expressions are needed in case the GROUP BY
//                  expression is an index into to the select list.
// projectionsScope The scope that contains the columns for the SELECT targets
//                  (used when GROUP BY refers to a target by alias).
// fromScope        The scope for the input to the aggregation (the FROM
//                  clause).
// aggInScope       The scope that will contain the grouping expressions as well
//                  as the aggregate function arguments.
func (b *Builder) buildGrouping(
	groupBy tree.Expr, selects tree.SelectExprs, projectionsScope, fromScope, aggInScope *scope,
) {
	// Unwrap parenthesized expressions like "((a))" to "a".
	groupBy = tree.StripParens(groupBy)
	alias := ""

	// Comment below pasted from PostgreSQL (findTargetListEntrySQL92 in
	// src/backend/parser/parse_clause.c).
	//
	// Handle two special cases as mandated by the SQL92 spec:
	//
	// 1. Bare ColumnName (no qualifier or subscripts)
	//    For a bare identifier, we search for a matching column name
	//    in the existing target list.  Multiple matches are an error
	//    unless they refer to identical values; for example,
	//    we allow  SELECT a, a FROM table ORDER BY a
	//    but not   SELECT a AS b, b FROM table ORDER BY b
	//    If no match is found, we fall through and treat the identifier
	//    as an expression.
	//    For GROUP BY, it is incorrect to match the grouping item against
	//    targetlist entries: according to SQL92, an identifier in GROUP BY
	//    is a reference to a column name exposed by FROM, not to a target
	//    list column.  However, many implementations (including pre-7.0
	//    PostgreSQL) accept this anyway.  So for GROUP BY, we look first
	//    to see if the identifier matches any FROM column name, and only
	//    try for a targetlist name if it doesn't.  This ensures that we
	//    adhere to the spec in the case where the name could be both.
	//    DISTINCT ON isn't in the standard, so we can do what we like there;
	//    we choose to make it work like ORDER BY, on the rather flimsy
	//    grounds that ordinary DISTINCT works on targetlist entries.
	//
	// 2. IntegerConstant
	//    This means to use the n'th item in the existing target list.
	//    Note that it would make no sense to order/group/distinct by an
	//    actual constant, so this does not create a conflict with SQL99.
	//    GROUP BY column-number is not allowed by SQL92, but since
	//    the standard has no other behavior defined for this syntax,
	//    we may as well accept this common extension.

	// This function sets groupBy and alias in these special cases.
	func() {
		// Check whether the GROUP BY clause refers to a column in the SELECT list
		// by index, e.g. `SELECT a, SUM(b) FROM y GROUP BY 1` (case 2 above).
		if col := colIndex(len(selects), groupBy, "GROUP BY"); col != -1 {
			groupBy, alias = selects[col].Expr, string(selects[col].As)
			return
		}

		if name, ok := groupBy.(*tree.UnresolvedName); ok {
			if name.NumParts != 1 || name.Star {
				return
			}
			// Case 1 above.
			targetName := tree.Name(name.Parts[0])

			// We must prefer a match against a FROM-clause column (but ignore upper
			// scopes); in this case we let the general case below handle the reference.
			for i := range fromScope.cols {
				if fromScope.cols[i].name.MatchesReferenceName(targetName) {
					return
				}
			}
			// See if it matches exactly one of the target lists.
			var match *scopeColumn
			for i := range projectionsScope.cols {
				if col := &projectionsScope.cols[i]; col.name.MatchesReferenceName(targetName) {
					if match != nil {
						// Multiple matches are only allowed if they refer to identical
						// expressions.
						if match.getExprStr() != col.getExprStr() {
							panic(pgerror.Newf(pgcode.AmbiguousColumn, "GROUP BY %q is ambiguous", targetName))
						}
					}
					match = col
				}
			}
			if match != nil {
				groupBy, alias = match.expr, string(targetName)
			}
		}
	}()

	// We need to save and restore the previous value of the field in semaCtx
	// in case we are recursively called within a subquery context.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)

	// Make sure the GROUP BY columns have no special functions.
	b.semaCtx.Properties.Require(exprKindGroupBy.String(), tree.RejectSpecial)
	fromScope.context = exprKindGroupBy

	// Resolve types, expand stars, and flatten tuples.
	exprs := b.expandStarAndResolveType(groupBy, fromScope)
	exprs = flattenTuples(exprs)

	// Finally, build each of the GROUP BY columns.
	for _, e := range exprs {
		// If a grouping column has already been added, don't add it again.
		// GROUP BY a, a is semantically equivalent to GROUP BY a.
		exprStr := symbolicExprStr(e)
		if _, ok := fromScope.groupby.groupStrs[exprStr]; ok {
			continue
		}

		// Save a representation of the GROUP BY expression for validation of the
		// SELECT and HAVING expressions. This enables queries such as:
		//   SELECT x+y FROM t GROUP BY x+y
		col := aggInScope.addColumn(scopeColName(tree.Name(alias)), e)
		b.buildScalar(e, fromScope, aggInScope, col, nil)
		fromScope.groupby.groupStrs[exprStr] = col
	}
}

// buildAggArg builds a scalar expression which is used as an input in some form
// to an aggregate expression. The scopeColumn for the built expression will
// be added to tempScope.
func (b *Builder) buildAggArg(
	e tree.TypedExpr, info *aggregateInfo, tempScope, fromScope *scope,
) opt.ScalarExpr {
	// This synthesizes a new tempScope column, unless the argument is a
	// simple VariableOp.
	col := tempScope.addColumn(scopeColName(""), e)
	b.buildScalar(e, fromScope, tempScope, col, &info.colRefs)
	if col.scalar != nil {
		return col.scalar
	}
	return b.factory.ConstructVariable(col.id)
}

// translateAggName translates the aggregate name if needed. This is used
// to override the output column name of an aggregation.
// See isOrderedSetAggregate.
func translateAggName(name string) string {
	switch name {
	case "percentile_disc_impl":
		return "percentile_disc"
	case "percentile_cont_impl":
		return "percentile_cont"
	}
	return name
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
//
// tempScope is a temporary scope which is used for building the aggregate
// function arguments before the correct scope is determined.
func (b *Builder) buildAggregateFunction(
	f *tree.FuncExpr, def *memo.FunctionPrivate, tempScope, fromScope *scope,
) *aggregateInfo {
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
		info.args[i] = b.buildAggArg(pexpr.(tree.TypedExpr), &info, tempScope, fromScope)
	}

	// If we have a filter, add it to tempScope after all the arguments. We'll
	// later extract the column that gets added here in buildAggregation.
	if f.Filter != nil {
		info.filter = b.buildAggArg(f.Filter.(tree.TypedExpr), &info, tempScope, fromScope)
	}

	// If we have ORDER BY, add the ordering columns to the tempScope.
	if f.OrderBy != nil {
		for _, o := range f.OrderBy {
			b.buildAggArg(o.Expr.(tree.TypedExpr), &info, tempScope, fromScope)
		}
	}

	// Find the appropriate aggregation scopes for this aggregate now that we
	// know which columns it references. If necessary, we'll move the columns
	// for the arguments from tempScope to aggInScope below.
	g := fromScope.endAggFunc(info.colRefs)

	// If we already have the same aggregation, reuse it. Otherwise add it
	// to the list of aggregates that need to be computed by the groupby
	// expression and synthesize a column for the aggregation result.
	info.col = g.findAggregate(info)
	if info.col == nil {
		// Translate function name if needed.
		funcName := translateAggName(def.Name)

		// Use 0 as the group for now; it will be filled in later by the
		// buildAggregation method.
		info.col = b.synthesizeColumn(g.aggOutScope, scopeColName(tree.Name(funcName)), f.ResolvedType(), f, nil /* scalar */)

		// Move the columns for the aggregate input expressions to the correct scope.
		if g.aggInScope != tempScope {
			g.aggInScope.cols = append(g.aggInScope.cols, tempScope.cols[tempScopeColsBefore:]...)
			tempScope.cols = tempScope.cols[:tempScopeColsBefore]
		}

		// Add the aggregate to the list of aggregates that need to be computed by
		// the groupby expression.
		g.aggs = append(g.aggs, info)
	} else {
		// Undo the adding of the args.
		// TODO(radu): is there a cleaner way to do this?
		tempScope.cols = tempScope.cols[:tempScopeColsBefore]
	}

	return &info
}

func (b *Builder) constructWindowFn(name string, args []opt.ScalarExpr) opt.ScalarExpr {
	switch name {
	case "rank":
		return b.factory.ConstructRank()
	case "row_number":
		return b.factory.ConstructRowNumber()
	case "dense_rank":
		return b.factory.ConstructDenseRank()
	case "percent_rank":
		return b.factory.ConstructPercentRank()
	case "cume_dist":
		return b.factory.ConstructCumeDist()
	case "ntile":
		return b.factory.ConstructNtile(args[0])
	case "lag":
		return b.factory.ConstructLag(args[0], args[1], args[2])
	case "lead":
		return b.factory.ConstructLead(args[0], args[1], args[2])
	case "first_value":
		return b.factory.ConstructFirstValue(args[0])
	case "last_value":
		return b.factory.ConstructLastValue(args[0])
	case "nth_value":
		return b.factory.ConstructNthValue(args[0], args[1])
	default:
		return b.constructAggregate(name, args)
	}
}

func (b *Builder) constructAggregate(name string, args []opt.ScalarExpr) opt.ScalarExpr {
	switch name {
	case "array_agg":
		return b.factory.ConstructArrayAgg(args[0])
	case "avg":
		return b.factory.ConstructAvg(args[0])
	case "bit_and":
		return b.factory.ConstructBitAndAgg(args[0])
	case "bit_or":
		return b.factory.ConstructBitOrAgg(args[0])
	case "bool_and", "every":
		return b.factory.ConstructBoolAnd(args[0])
	case "bool_or":
		return b.factory.ConstructBoolOr(args[0])
	case "concat_agg":
		return b.factory.ConstructConcatAgg(args[0])
	case "corr":
		return b.factory.ConstructCorr(args[0], args[1])
	case "count":
		return b.factory.ConstructCount(args[0])
	case "count_rows":
		return b.factory.ConstructCountRows()
	case "covar_pop":
		return b.factory.ConstructCovarPop(args[0], args[1])
	case "covar_samp":
		return b.factory.ConstructCovarSamp(args[0], args[1])
	case "regr_avgx":
		return b.factory.ConstructRegressionAvgX(args[0], args[1])
	case "regr_avgy":
		return b.factory.ConstructRegressionAvgY(args[0], args[1])
	case "regr_intercept":
		return b.factory.ConstructRegressionIntercept(args[0], args[1])
	case "regr_r2":
		return b.factory.ConstructRegressionR2(args[0], args[1])
	case "regr_slope":
		return b.factory.ConstructRegressionSlope(args[0], args[1])
	case "regr_sxx":
		return b.factory.ConstructRegressionSXX(args[0], args[1])
	case "regr_sxy":
		return b.factory.ConstructRegressionSXY(args[0], args[1])
	case "regr_syy":
		return b.factory.ConstructRegressionSYY(args[0], args[1])
	case "regr_count":
		return b.factory.ConstructRegressionCount(args[0], args[1])
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
	case "variance", "var_samp":
		return b.factory.ConstructVariance(args[0])
	case "stddev", "stddev_samp":
		return b.factory.ConstructStdDev(args[0])
	case "stddev_pop":
		return b.factory.ConstructStdDevPop(args[0])
	case "var_pop":
		return b.factory.ConstructVarPop(args[0])
	case "st_makeline":
		return b.factory.ConstructSTMakeLine(args[0])
	case "st_collect", "st_memcollect":
		return b.factory.ConstructSTCollect(args[0])
	case "st_extent":
		return b.factory.ConstructSTExtent(args[0])
	case "st_union", "st_memunion":
		return b.factory.ConstructSTUnion(args[0])
	case "xor_agg":
		return b.factory.ConstructXorAgg(args[0])
	case "json_agg":
		return b.factory.ConstructJsonAgg(args[0])
	case "jsonb_agg":
		return b.factory.ConstructJsonbAgg(args[0])
	case "string_agg":
		return b.factory.ConstructStringAgg(args[0], args[1])
	case "percentile_disc_impl":
		return b.factory.ConstructPercentileDisc(args[0], args[1])
	case "percentile_cont_impl":
		return b.factory.ConstructPercentileCont(args[0], args[1])
	case "json_object_agg":
		return b.factory.ConstructJsonObjectAgg(args[0], args[1])
	case "jsonb_object_agg":
		return b.factory.ConstructJsonbObjectAgg(args[0], args[1])
	}

	panic(errors.AssertionFailedf("unhandled aggregate: %s", name))
}

func isAggregate(def *tree.FunctionDefinition) bool {
	return def.Class == tree.AggregateClass
}

func isWindow(def *tree.FunctionDefinition) bool {
	return def.Class == tree.WindowClass
}

func isGenerator(def *tree.FunctionDefinition) bool {
	return def.Class == tree.GeneratorClass
}

func isSQLFn(def *tree.FunctionDefinition) bool {
	return def.Class == tree.SQLClass
}

func newGroupingError(name tree.Name) error {
	return pgerror.Newf(pgcode.Grouping,
		"column \"%s\" must appear in the GROUP BY clause or be used in an aggregate function",
		tree.ErrString(&name),
	)
}

// allowImplicitGroupingColumn returns true if col is part of a table and the
// the groupby metadata indicates that we are grouping on the entire PK of that
// table. In that case, we can allow col as an "implicit" grouping column, even
// if it is not specified in the query.
func (b *Builder) allowImplicitGroupingColumn(colID opt.ColumnID, g *groupby) bool {
	md := b.factory.Metadata()
	colMeta := md.ColumnMeta(colID)
	if colMeta.Table == 0 {
		return false
	}
	// Get all the PK columns.
	tab := md.Table(colMeta.Table)
	var pkCols opt.ColSet
	if tab.IndexCount() == 0 {
		// Virtual tables have no indexes.
		return false
	}
	primaryIndex := tab.Index(cat.PrimaryIndex)
	for i := 0; i < primaryIndex.KeyColumnCount(); i++ {
		pkCols.Add(colMeta.Table.IndexColumnID(primaryIndex, i))
	}
	// Remove PK columns that are grouping cols and see if there's anything left.
	groupingCols := g.groupingCols()
	for i := range groupingCols {
		pkCols.Remove(groupingCols[i].id)
	}
	return pkCols.Empty()
}
