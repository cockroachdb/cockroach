// Copyright 2016 The Cockroach Authors.
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

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// This file contains the functions that perform filter propagation.
//
// The explanation below uses the following notations:
//    P, Q, R : logical predicates (formulas with boolean values)
//    T       : the tautology ("always true" predicate)
//    A, B    : data sources
//    t       : table
//
// Definitions:
//
//     "f" is abbreviation for "propagateFilters".
//     Performs filter propagation and returns the part
//     of the filter that was not propagated and could
//     not be attached at the current level.
//
//     f :: data source, predicate -> predicate, data source
//     (definition below)
//
//     Note: following the definition of f, if given T as predicate
//     argument f always returns T as remaining predicate result.
//
//     "t" is abbreviation for "triggerFilterPropagation".
//     Initiates filter propagation on the given data source.
//
//     t :: data source -> data source
//     t( A )     = B
//                  where _, B := f(A, T)
//
//     "w" is abbreviation for "propagateOrWrapFilters".
//     Calls f and wraps the results in a filterNode if
//     the remaining filter is not the tautology.
//
//     w :: data source, predicate -> data source
//     w( A, P )  = B                   if R = T
//                = [filter(R) FROM B]  otherwise
//                  where (R, B) := f(A, P)
//
// f (propagateFilters) knows how to attach an incoming filter to
// empty and scan nodes:
//
//     f( [<empty>], P )      = T, [<empty>]
//     f( [scan t // P], Q )  = T, [scan t // (P AND Q)]
//
// f combines an incoming filter with an existing filter and pushes
// forward:
//
//     f( [filter(P) FROM A], Q )  = f(A, (P AND Q))
//
// Filtering is distributive over UNION:
//
//     f( [union FROM A, B], P )  = T, [union FROM w(A, P), w(B, P)]
//
// f propagates filters "through" distinct and sort nodes:
//
//     f( [distinct FROM A], P )  = T, [distinct FROM w(B, P) ]
//
//     f( [sort FROM A], P )      = T, [sort FROM w(B, P) ]
//
// Some nodes "block" filter propagation entirely:
//
//     f( [limit FROM A], P )       = P, [limit FROM t(A)]
//     f( [ordinality FROM A], P )  = P, [ordinality FROM t(A)]
//
// Some nodes currently block filtering entirely, but could be
// optimized later to let some filters through (see inline comments
// below):
//
//     f( [group FROM A], P )   = P, [group FROM t(A)]
//     f( [window FROM A], P )  = P, [window FROM t(A)]
//
// f propagates filters through render nodes, though only for simple renders:
//
//     f( [render <colname> AS x FROM A], P )  = T, [render <colname> AS x FROM w(A, [P/x/<colname>])]
//     f( [render Expr(...) AS x FROM A], P )  = P, [render Expr(...) AS x FROM t(A)]
//
// (The notation [P/x/<colname>] means replace all occurrences of "x" in P by <colname>.
//
// f propagates filters through inner joins only:
//
//     f( [outerjoin(P) FROM A, B], Q )  = Q, [outerjoin(P) FROM t(A), t(B)]
//     f( [innerjoin(P) FROM A, B], Q )  = T, [innerjoin(combi(R)) FROM w(A, left(R)), w(B, right(R))
//                                         where:
//                                         R = (P AND Q)
//                                         left(R) is the part of R that only uses columns from A
//                                         right(R) is the part of R that only uses columns from B
//                                         combi(R) is the part of R that uses columns from both A and B
//
// (see the RFC for filter propagation over joins.)
//
// General implementation principles:
//
// Predicates are either:
// - free, meaning they can be evaluated independently of context;
//   this includes simple constants or calls to functions with only
//   free arguments.
// - dependent, meaning they use references (IndexedVars) to
//   values in the data source at the current level.
//
// Dependent predicates contain IndexedVars to refer to values at the
// current level.
// From the perspective of this algorithm, we can abstract and
// consider that IndexedVars are just positional indices to a virtual
// array of the values. We ignore in particular that their actual
// implementation is more complex (e.g. they carry also a
// "container"), because this complexity is fully inconsequential to
// the predicate transformations performed here.
//
// Because IndexedVars are indices, they need to be adjusted
// when propagating filters across nodes that augment or reduce
// the column set of their source. For example, take a join:
//
//    n := [join FROM A, B]
//
// the virtual array of values in join results is the (virtual)
// concatenation of value arrays from both operands (we ignore for the
// sake of this explanation merged columns that result from USING or
// NATURAL). So, for example, if (say) A has 2 columns and B has 3
// columns, and we are considering a filter predicate on n which
// contains an IndexedVar @4 (using base-1 indexing), then that
// IndexedVar is really relative to B, and *when propagated in the
// context of B* must be adjusted to @2.

// propagateFilters tries to combine the extraFilter with the filter
// at the current level (if any), propagate the result, re-attach the
// remainder filter at the current level (if possible) and returns the
// remaining filter.
//
// The reason why propagateFilter returns a remainingFilter instead of
// adding a new filterNode itself, and thus the reason why
// propagateFilter and propagateOrWrapFilter are two separate
// functions, has to do with the handling of renderNodes:
//
// - if two renderNodes are stacked onto each other (e.g. SELECT *
//   FROM (SELECT * FROM ...) ), we don't want to introduce a filter
//   in between them, because...
// - another optimization (pending #12763) seeks to combine adjacent
//   renderNodes, and...
// - we can't combine renderNodes beforehand either, because filter
//   propagation may eliminate some filterNodes and that may create
//   more opportunities to merge renderNodes.
//
// Perhaps the way #12763 will be addressed is to combine the two
// recursions that propagate filters and merge renderNodes, in which
// case perhaps propagateFilter and propagateOrWrapFilter can merge,
// but we are not there yet.
func (p *planner) propagateFilters(
	ctx context.Context, plan planNode, info *sqlbase.DataSourceInfo, extraFilter tree.TypedExpr,
) (newPlan planNode, remainingFilter tree.TypedExpr, err error) {
	remainingFilter = extraFilter
	switch n := plan.(type) {
	case *zeroNode:
		// There is no row (by definition), so all filters
		// are "already applied". Silently absorb any extra filter.
		return plan, tree.DBoolTrue, nil
	case *unaryNode:
		// TODO(knz): We could evaluate the filter here and transform the unaryNode
		// into an emptyNode, assuming the filter is not "row dependent" (cf.
		// resolveNames()).
	case *filterNode:
		newFilter := mergeConj(n.filter, extraFilter)
		newPlan, err = p.propagateOrWrapFilters(ctx, n.source.plan, n.source.info, newFilter)
		if err != nil {
			return plan, extraFilter, err
		}
		return newPlan, tree.DBoolTrue, nil

	case *scanNode:
		n.filter = mergeConj(n.filter, n.filterVars.Rebind(extraFilter, true, false))
		return plan, tree.DBoolTrue, nil

	case *renderNode:
		return p.addRenderFilter(ctx, n, extraFilter)

	case *joinNode:
		return p.addJoinFilter(ctx, n, extraFilter)

	case *indexJoinNode:
		panic("filter optimization must occur before index selection")

	case *distinctNode:
		// A distinct node can propagate a filter. Source filtering
		// reduces the amount of work.
		n.plan, err = p.propagateOrWrapFilters(ctx, n.plan, nil, extraFilter)
		if err != nil {
			return plan, extraFilter, err
		}
		return plan, tree.DBoolTrue, nil

	case *sortNode:
		// A sort node can propagate a filter, and source filtering
		// reduces the amount of work.
		n.plan, err = p.propagateOrWrapFilters(ctx, n.plan, nil, extraFilter)
		if err != nil {
			return plan, extraFilter, err
		}
		return plan, tree.DBoolTrue, nil

	case *unionNode:
		// Filtering is distributive over set operations.
		// Source filtering reduces the amount of work, so force propagation.
		newLeft, err := p.propagateOrWrapFilters(ctx, n.left, nil, extraFilter)
		if err != nil {
			return plan, extraFilter, err
		}

		newRight, err := p.propagateOrWrapFilters(ctx, n.right, nil, extraFilter)
		if err != nil {
			return plan, extraFilter, err
		}

		n.left = newLeft
		n.right = newRight
		return plan, tree.DBoolTrue, nil

	case *groupNode:
		return p.addGroupFilter(ctx, n, info, extraFilter)

	case *limitNode:
		if n.plan, err = p.triggerFilterPropagation(ctx, n.plan); err != nil {
			return plan, extraFilter, err
		}

	case *windowNode:
		if n.plan, err = p.triggerFilterPropagation(ctx, n.plan); err != nil {
			return plan, extraFilter, err
		}

	case *ordinalityNode:
		if n.source, err = p.triggerFilterPropagation(ctx, n.source); err != nil {
			return plan, extraFilter, err
		}

	case *spoolNode:
		if n.source, err = p.triggerFilterPropagation(ctx, n.source); err != nil {
			return plan, extraFilter, err
		}

	case *createTableNode:
		if n.n.As() {
			if n.sourcePlan, err = p.triggerFilterPropagation(ctx, n.sourcePlan); err != nil {
				return plan, extraFilter, err
			}
		}

	case *deleteNode:
		if n.source, err = p.triggerFilterPropagation(ctx, n.source); err != nil {
			return plan, extraFilter, err
		}

	case *rowCountNode:
		newSource, err := p.triggerFilterPropagation(ctx, n.source)
		if err != nil {
			return plan, extraFilter, err
		}
		n.source = newSource.(batchedPlanNode)

	case *serializeNode:
		newSource, err := p.triggerFilterPropagation(ctx, n.source)
		if err != nil {
			return plan, extraFilter, err
		}
		n.source = newSource.(batchedPlanNode)

	case *insertNode:
		if n.source, err = p.triggerFilterPropagation(ctx, n.source); err != nil {
			return plan, extraFilter, err
		}

	case *upsertNode:
		if n.source, err = p.triggerFilterPropagation(ctx, n.source); err != nil {
			return plan, extraFilter, err
		}

	case *updateNode:
		if n.source, err = p.triggerFilterPropagation(ctx, n.source); err != nil {
			return plan, extraFilter, err
		}

	case *explainDistSQLNode:
		if n.plan, err = p.triggerFilterPropagation(ctx, n.plan); err != nil {
			return plan, extraFilter, err
		}

	case *explainPlanNode:
		if n.optimized {
			if n.plan, err = p.triggerFilterPropagation(ctx, n.plan); err != nil {
				return plan, extraFilter, err
			}
		}

	case *showTraceReplicaNode:
		if n.plan, err = p.triggerFilterPropagation(ctx, n.plan); err != nil {
			return plan, extraFilter, err
		}

	case *delayedNode:
		if n.plan != nil {
			if n.plan, err = p.triggerFilterPropagation(ctx, n.plan); err != nil {
				return plan, extraFilter, err
			}
		}

	case *splitNode:
		if n.rows, err = p.triggerFilterPropagation(ctx, n.rows); err != nil {
			return plan, extraFilter, err
		}

	case *relocateNode:
		if n.rows, err = p.triggerFilterPropagation(ctx, n.rows); err != nil {
			return plan, extraFilter, err
		}

	case *cancelQueriesNode:
		if n.rows, err = p.triggerFilterPropagation(ctx, n.rows); err != nil {
			return plan, extraFilter, err
		}

	case *cancelSessionsNode:
		if n.rows, err = p.triggerFilterPropagation(ctx, n.rows); err != nil {
			return plan, extraFilter, err
		}

	case *controlJobsNode:
		if n.rows, err = p.triggerFilterPropagation(ctx, n.rows); err != nil {
			return plan, extraFilter, err
		}

	case *projectSetNode:
		// TODO(knz): we can propagate the part of the filter that applies
		// to the source columns.
		if n.source, err = p.triggerFilterPropagation(ctx, n.source); err != nil {
			return plan, extraFilter, err
		}

	case *alterIndexNode:
	case *alterTableNode:
	case *alterSequenceNode:
	case *alterUserSetPasswordNode:
	case *renameColumnNode:
	case *renameDatabaseNode:
	case *renameIndexNode:
	case *renameTableNode:
	case *scrubNode:
	case *truncateNode:
	case *commentOnTableNode:
	case *createDatabaseNode:
	case *createIndexNode:
	case *CreateUserNode:
	case *createViewNode:
	case *createSequenceNode:
	case *createStatsNode:
	case *dropDatabaseNode:
	case *dropIndexNode:
	case *dropTableNode:
	case *dropViewNode:
	case *dropSequenceNode:
	case *DropUserNode:
	case *hookFnNode:
	case *valuesNode:
	case *virtualTableNode:
	case *sequenceSelectNode:
	case *setVarNode:
	case *setClusterSettingNode:
	case *setZoneConfigNode:
	case *showZoneConfigNode:
	case *showRangesNode:
	case *showFingerprintsNode:
	case *showTraceNode:
	case *scatterNode:

	default:
		panic(fmt.Sprintf("unhandled node type: %T", plan))
	}

	return plan, remainingFilter, nil
}

// triggerFilterPropagation initiates filter propagation on the given plan.
func (p *planner) triggerFilterPropagation(ctx context.Context, plan planNode) (planNode, error) {
	newPlan, remainingFilter, err := p.propagateFilters(ctx, plan, nil, tree.DBoolTrue)
	if err != nil {
		return plan, err
	}

	if !isFilterTrue(remainingFilter) {
		panic(fmt.Sprintf("propagateFilters on \n%s\n spilled a non-trivial remaining filter: %s",
			planToString(ctx, plan, nil), remainingFilter))
	}

	return newPlan, nil
}

// propagateOrWrapFilters triggers filter propagation on the given
// node, and creates a new filterNode if there is any remaining filter
// after the propagation.
func (p *planner) propagateOrWrapFilters(
	ctx context.Context, plan planNode, info *sqlbase.DataSourceInfo, filter tree.TypedExpr,
) (planNode, error) {
	newPlan, remainingFilter, err := p.propagateFilters(ctx, plan, info, filter)
	if err != nil {
		return plan, err
	}

	// If there is no remaining filter, simply return the new plan.
	if isFilterTrue(remainingFilter) {
		return newPlan, nil
	}

	// Otherwise, wrap it using a new filterNode.
	if info == nil {
		info = sqlbase.NewSourceInfoForSingleTable(sqlbase.AnonymousTable, planColumns(newPlan))
	}
	f := &filterNode{
		source: planDataSource{plan: newPlan, info: info},
	}
	f.ivarHelper = tree.MakeIndexedVarHelper(f, len(info.SourceColumns))
	f.filter = f.ivarHelper.Rebind(remainingFilter,
		false /* helper is fresh, no reset needed */, false)
	return f, nil
}

// addGroupFilter attempts to add the extraFilter to the groupNode.
// The part of the filter that depends only on GROUP BY expressions is
// propagated to the source.
func (p *planner) addGroupFilter(
	ctx context.Context, g *groupNode, info *sqlbase.DataSourceInfo, extraFilter tree.TypedExpr,
) (planNode, tree.TypedExpr, error) {
	// innerFilter is the passed-through filter on the source planNode.
	var innerFilter tree.TypedExpr = tree.DBoolTrue

	if !isFilterTrue(extraFilter) {
		// The filter that's being added refers to the result expressions,
		// not the groupNode's source node. We need to detect which parts
		// of the filter refer to passed-through source columns ("IDENT
		// aggregations"), and renumber the indexed vars accordingly.
		convFunc := func(v tree.VariableExpr) (bool, tree.Expr) {
			if iv, ok := v.(*tree.IndexedVar); ok {
				if groupingCol, ok := g.aggIsGroupingColumn(iv.Idx); ok {
					return true, &tree.IndexedVar{Idx: groupingCol}
				}
			}
			return false, v
		}

		// Do the replacement proper.
		innerFilter, extraFilter = splitFilter(extraFilter, convFunc)
	}

	// Propagate the inner filter.
	newPlan, err := p.propagateOrWrapFilters(ctx, g.plan, nil /* info */, innerFilter)
	if err != nil {
		return g, extraFilter, err
	}

	// Attach what remains as the new source.
	g.plan = newPlan

	return g, extraFilter, nil
}

// addRenderFilter attempts to add the extraFilter to the renderNode.
// The filter is only propagated to the sub-plan if it is expressed
// using renders that are either simple datums or simple column
// references to the source.
func (p *planner) addRenderFilter(
	ctx context.Context, s *renderNode, extraFilter tree.TypedExpr,
) (planNode, tree.TypedExpr, error) {
	// innerFilter is the passed-through filter on the source planNode.
	var innerFilter tree.TypedExpr = tree.DBoolTrue

	if !isFilterTrue(extraFilter) {
		// The filter that's being added refers to the rendered
		// expressions, not the render's source node.

		// We propagate only those filters that use simple renders
		// of columns from the source.
		// For example given the query:
		//  SELECT *
		//    FROM (SELECT a*b AS c, a, b FROM t)
		//   WHERE a > 5 and b < 7 and c < 100 and a + b < 20
		//
		// we propagate the filter so that the query becomes equivalent
		// to:
		//  SELECT * FROM (SELECT a*b AS c, a, b
		//                   FROM (SELECT *
		//                           FROM t
		//                          WHERE a > 5 and b < 7 and a + b < 20) )
		//   WHERE c < 100
		//
		// More complex expressions remain outside of the renderNode, that
		// is we do not replace:
		//
		//   SELECT *
		//     FROM (SELECT a + 2 * b AS c FROM t)
		//    WHERE c > 123
		// by
		//   SELECT a + 2 * b AS c
		//    FROM (SELECT * FROM t
		//           WHERE a + 2 * b > 123)
		//
		// Because that would cause the (more complex) expression `a + 2 *
		// b` to be computed at both levels. This cannot be "simplified"
		// further by trying to factor the common sub-expression: `SELECT
		// c FROM (SELECT * FROM (SELECT a + 2 * b AS c FROM t) WHERE c >
		// 123)` contains the original query.
		//
		// To perform this propagation, we use splitFilter with a convFunc
		// which both detects which renders are simple enough to be
		// propagated, and replaces the IndexedVars in the original filter
		// to become relative to the renderNode's source columns.
		//
		// Note: we don't really care about the IndexedVars' container
		// here, as no filter will stay at this node -- either they
		// propagate down or spill upward as remaining filter.
		// See also the comment for propagateFilters() above.
		//
		convFunc := func(v tree.VariableExpr) (bool, tree.Expr) {
			if iv, ok := v.(*tree.IndexedVar); ok {
				renderExpr := s.render[iv.Idx]
				if d, ok := renderExpr.(tree.Datum); ok {
					// A standalone Datum is not complex, so it can be propagated further.
					return true, d
				}
				if rv, ok := renderExpr.(*tree.IndexedVar); ok {
					// A naked IndexedVar is not complex, so it can be propagated further.
					return true, rv
				}
			}
			return false, v
		}

		// Do the replacement proper.
		innerFilter, extraFilter = splitFilter(extraFilter, convFunc)
	}

	// Propagate the inner filter.
	newPlan, err := p.propagateOrWrapFilters(
		ctx, s.source.plan, s.source.info, innerFilter)
	if err != nil {
		return s, extraFilter, err
	}

	// Attach what remains as the new source.
	s.source.plan = newPlan

	return s, extraFilter, nil
}

// expandOnCond infers constraints (on equality columns) for one side of the
// join from constraints for the other side.
//
// The ON condition (which for inner joins includes any filters) can constrain
// equality columns from one side, and we want to push down those constraints
// to both sides.
//
// For example, for
//   SELECT * FROM a INNER JOIN b ON a.x = b.x AND a.x > 10
// we infer the condition b.x > 10, resulting in
//   SELECT * FROM a INNER JOIN b ON a.x = b.x AND a.x > 10 AND b.x > 10
//
// A common situation where we can get something like this is when using a
// merged column, which for inner joins is treated as an alias for the left
// column:
//   SELECT * FROM a INNER JOIN b USING(x) WHERE x > 10
// In this case `x > 10` becomes an additional `a.x > 10` ON condition; this
// function will add `b.x > 10`.
//
// The expanded expression can then be split as necessary.
func expandOnCond(n *joinNode, cond tree.TypedExpr, evalCtx *tree.EvalContext) tree.TypedExpr {
	if isFilterTrue(cond) || len(n.pred.leftEqualityIndices) == 0 {
		return cond
	}
	numLeft := len(n.left.info.SourceColumns)

	// We can't use splitFilter directly because that function removes all the
	// constraints we were able to use from the expression. We could use it
	// without using the remainder result, but we need to have some other way of
	// not keeping redundant constraints.
	//
	// We split the condition into conjuncts and use splitFilter on each
	// conjunct to convert it to constraints for each side (as much as
	// possible); if the constraint is consumed completely by either splitFilter
	// call, we remove the original conjunct and just keep the splitFilter
	// results.  Otherwise, we keep all three.
	//
	// Some examples (assuming left.x = right.x, left.y = right.y):
	//
	//      conjunct:           left.x > 10
	//      splitFilter left:   left.x > 10
	//      splitFilter right:  right.x > 10
	//      result:             left.x > 10 AND right.x > 10
	//
	//      conjunct:           left.x > right.v
	//      splitFilter left:   <empty>
	//      splitFilter right:  right.x > right.v
	//      result:             right.x > right.v
	//
	//      conjunct:           left.x = 10 OR right.x = 20
	//      splitFilter left:   left.x = 10 OR left.x = 20
	//      splitFilter right:  right.x = 10 OR right.x = 20
	//      result:             (left.x = 10 OR left.x = 20) AND (right.x = 10 OR right.x = 20)
	//
	//      conjunct:           left.v > right.w
	//      splitFilter left:   <empty>
	//      splitFilter right:  <empty>
	//      result:             left.v > right.w
	//
	//      conjunct:           left.x > right.y
	//      splitFilter left:   left.x > right.y
	//      splitFilter right   right.x > right.y
	//      result:             left.x > right.y AND right.x > right.y
	//
	// The final result is obtained by merging all the results for each
	// conjunction.

	var result tree.TypedExpr = tree.DBoolTrue

	andExprs := splitAndExpr(evalCtx, cond, nil)
	for _, e := range andExprs {
		convLeft := func(expr tree.VariableExpr) (bool, tree.Expr) {
			iv, ok := expr.(*tree.IndexedVar)
			if !ok {
				return false, expr
			}
			if iv.Idx < numLeft {
				// Variable from the left side.
				return true, expr
			}
			// Variable from the right side; see if it is an equality column.
			for i, c := range n.pred.rightEqualityIndices {
				if c == iv.Idx-numLeft {
					return true, n.pred.iVarHelper.IndexedVar(n.pred.leftEqualityIndices[i])
				}
			}
			return false, expr
		}
		leftFilter, leftRemainder := splitFilter(e, convLeft)

		convRight := func(expr tree.VariableExpr) (bool, tree.Expr) {
			iv, ok := expr.(*tree.IndexedVar)
			if !ok {
				return false, expr
			}
			if iv.Idx >= numLeft {
				// Variable from the right side.
				return true, expr
			}
			// Variable from the left side; see if it is an equality column.
			for i, c := range n.pred.leftEqualityIndices {
				if c == iv.Idx {
					return true, n.pred.iVarHelper.IndexedVar(numLeft + n.pred.rightEqualityIndices[i])
				}
			}
			return false, expr
		}
		rightFilter, rightRemainder := splitFilter(e, convRight)

		result = mergeConj(result, leftFilter)
		result = mergeConj(result, rightFilter)
		// Also retain the original expression, unless one of the splitFilter
		// calls consumed it completely.
		if !isFilterTrue(leftRemainder) && !isFilterTrue(rightRemainder) {
			result = mergeConj(result, e)
		}
	}
	return result
}

// remapLeftEqColConstraints takes an expression that applies to the left side
// of the join and returns any constraints that refer to left equality columns,
// remapped to refer to the corresponding columns on the right side.
func remapLeftEqColConstraints(n *joinNode, cond tree.TypedExpr) tree.TypedExpr {
	conv := func(expr tree.VariableExpr) (bool, tree.Expr) {
		if iv, ok := expr.(*tree.IndexedVar); ok {
			for i, c := range n.pred.leftEqualityIndices {
				if c == iv.Idx {
					return true, n.pred.iVarHelper.IndexedVar(n.pred.rightEqualityIndices[i])
				}
			}
		}
		return false, expr
	}
	result, _ := splitFilter(cond, conv)
	return result
}

// remapRightEqColConstraints takes an expression that applies to the right side
// of the join and returns any constraints that refer to right equality columns,
// remapped to refer to the corresponding columns on the left side.
func remapRightEqColConstraints(n *joinNode, cond tree.TypedExpr) tree.TypedExpr {
	conv := func(expr tree.VariableExpr) (bool, tree.Expr) {
		if iv, ok := expr.(*tree.IndexedVar); ok {
			for i, c := range n.pred.rightEqualityIndices {
				if c == iv.Idx {
					return true, n.pred.iVarHelper.IndexedVar(n.pred.leftEqualityIndices[i])
				}
			}
		}
		return false, expr
	}
	result, _ := splitFilter(cond, conv)
	return result
}

// splitJoinFilter splits a predicate over both sides of the join into
// three predicates: the part that definitely refers only to the left,
// the part that definitely refers only to the right, and the rest.
// In this process we shift the IndexedVars in the left and right
// filter expressions so that they are numbered starting from 0
// relative to their respective operand.
// rightBegin is the logical index of the first column in
// the right data source.
func splitJoinFilter(
	n *joinNode, rightBegin int, initialPred tree.TypedExpr,
) (tree.TypedExpr, tree.TypedExpr, tree.TypedExpr) {
	leftExpr, remainder := splitJoinFilterLeft(n, rightBegin, initialPred)
	rightExpr, combinedExpr := splitJoinFilterRight(n, rightBegin, remainder)
	return leftExpr, rightExpr, combinedExpr
}

func splitJoinFilterLeft(
	n *joinNode, rightBegin int, initialPred tree.TypedExpr,
) (tree.TypedExpr, tree.TypedExpr) {
	return splitFilter(initialPred,
		func(expr tree.VariableExpr) (bool, tree.Expr) {
			if iv, ok := expr.(*tree.IndexedVar); ok && iv.Idx < rightBegin {
				return true, expr
			}
			return false, expr
		})
}

func splitJoinFilterRight(
	n *joinNode, rightBegin int, initialPred tree.TypedExpr,
) (tree.TypedExpr, tree.TypedExpr) {
	return splitFilter(initialPred,
		func(expr tree.VariableExpr) (bool, tree.Expr) {
			if iv, ok := expr.(*tree.IndexedVar); ok && iv.Idx >= rightBegin {
				return true, n.pred.iVarHelper.IndexedVar(iv.Idx - rightBegin)
			}
			return false, expr
		})
}

// addJoinFilter propagates the given filter to a joinNode.
func (p *planner) addJoinFilter(
	ctx context.Context, n *joinNode, extraFilter tree.TypedExpr,
) (planNode, tree.TypedExpr, error) {

	// There are four steps to the transformation below:
	//  1. For inner joins, incorporate the extra filter into the ON condition.
	//  2. Extract any join equality constraints from the ON condition and
	//     append it to the equality indices of the join predicate.
	//  3. "Expand" the remaining ON condition with new constraints inferred based
	//     on the equality columns (see expandOnCond).
	//  4. Propagate the filter and ON condition depending on the join type.
	numLeft := len(n.left.info.SourceColumns)
	extraFilter = n.pred.iVarHelper.Rebind(
		extraFilter, true /* alsoReset */, false /* normalizeToNonNil */)

	onAndExprs := splitAndExpr(p.EvalContext(), n.pred.onCond, nil /* exprs */)

	// Step 1: for inner joins, incorporate the filter into the ON condition.
	if n.joinType == sqlbase.InnerJoin {
		// Split the filter into conjunctions and append them to onAndExprs.
		onAndExprs = splitAndExpr(p.EvalContext(), extraFilter, onAndExprs)
		extraFilter = nil
	}

	// Step 2: harvest any equality columns. We want to do this before the
	// expandOnCond call below, which uses the equality columns information.
	onCond := tree.TypedExpr(tree.DBoolTrue)
	for _, e := range onAndExprs {
		if e != tree.DBoolTrue && !n.pred.tryAddEqualityFilter(e, n.left.info, n.right.info) {
			onCond = mergeConj(onCond, e)
		}
	}

	// Step 3: expand the ON condition, in the hope that new inferred
	// constraints can be pushed down. This is not useful for FULL OUTER
	// joins, where nothing can be pushed down.
	if n.joinType != sqlbase.FullOuterJoin {
		onCond = expandOnCond(n, onCond, p.EvalContext())
	}

	// Step 4: propagate the filter and ON conditions as allowed by the join type.
	var propagateLeft, propagateRight, filterRemainder tree.TypedExpr
	switch n.joinType {
	case sqlbase.InnerJoin:
		// We transform:
		//   SELECT * FROM
		//          l JOIN r ON (onLeft AND onRight AND onCombined)
		//   WHERE (filterLeft AND filterRight AND filterCombined)
		// to:
		//   SELECT * FROM
		//          (SELECT * FROM l WHERE (onLeft AND filterLeft)
		//          JOIN
		//          (SELECT * from r WHERE (onRight AND filterRight)
		//          ON (onCombined AND filterCombined)
		propagateLeft, propagateRight, onCond = splitJoinFilter(n, numLeft, onCond)

	case sqlbase.LeftOuterJoin:
		// We transform:
		//   SELECT * FROM
		//          l LEFT OUTER JOIN r ON (onLeft AND onRight AND onCombined)
		//   WHERE (filterLeft AND filterRight AND filterCombined)
		// to:
		//   SELECT * FROM
		//          (SELECT * FROM l WHERE filterLeft)
		//          LEFT OUTER JOIN
		//          (SELECT * from r WHERE onRight)
		//          ON (onLeft AND onCombined)
		//   WHERE (filterRight AND filterCombined)

		// Extract filterLeft towards propagation on the left.
		// filterRemainder = filterRight AND filterCombined.
		propagateLeft, filterRemainder = splitJoinFilterLeft(n, numLeft, extraFilter)

		// Extract onRight towards propagation on the right.
		// onCond = onLeft AND onCombined.
		propagateRight, onCond = splitJoinFilterRight(n, numLeft, onCond)

		// For left joins, if we have any conditions on the left-hand equality
		// columns, we can remap these to the right-hand equality columns and
		// incorporate in the ON condition. For example:
		//
		//   SELECT * FROM l LEFT JOIN r USING (a) WHERE l.a = 1
		//
		// In this case, we can push down r.a = 1 to the right side.
		//
		// Note that this is what happens when a merged column name is used; for
		// outer left joins, `a` is an alias for the left side equality column and
		// the following query is equivalent to the one above:
		//   SELECT * FROM l LEFT JOIN r USING (a) WHERE a = 1
		propagateRight = mergeConj(propagateRight, remapLeftEqColConstraints(n, propagateLeft))

	case sqlbase.RightOuterJoin:
		// We transform:
		//   SELECT * FROM
		//          l RIGHT OUTER JOIN r ON (onLeft AND onRight AND onCombined)
		//   WHERE (filterLeft AND filterRight AND filterCombined)
		// to:
		//   SELECT * FROM
		//          (SELECT * FROM l WHERE onLeft)
		//          RIGHT OUTER JOIN
		//          (SELECT * from r WHERE filterRight)
		//          ON (onRight AND onCombined)
		//   WHERE (filterLeft AND filterCombined)

		// Extract filterRight towards propagation on the right.
		// filterRemainder = filterLeft AND filterCombined.
		propagateRight, filterRemainder = splitJoinFilterRight(n, numLeft, extraFilter)
		// Extract onLeft towards propagation on the left.
		// onCond = onRight AND onCombined.
		propagateLeft, onCond = splitJoinFilterLeft(n, numLeft, onCond)

		// Symmetric to the corresponding case above, for example:
		//   SELECT * FROM l RIGHT JOIN r USING (a) WHERE r.a = 1
		//
		// In this case, we can push down l.a = 1 to the left side.
		propagateLeft = mergeConj(propagateLeft, remapRightEqColConstraints(n, propagateRight))

	case sqlbase.FullOuterJoin:
		// Not much we can do for full outer joins.
		filterRemainder = extraFilter
	}

	// Propagate the left and right predicates to the left and right sides of the
	// join. The predicates must first be "shifted" i.e. their IndexedVars which
	// are relative to the join columns must be modified to become relative to the
	// operand's columns.
	propagate := func(ctx context.Context, pred tree.TypedExpr, side *planDataSource) error {
		newPlan, err := p.propagateOrWrapFilters(ctx, side.plan, side.info, pred)
		if err != nil {
			return err
		}
		side.plan = newPlan
		return nil
	}
	if err := propagate(ctx, propagateLeft, &n.left); err != nil {
		return n, extraFilter, err
	}
	if err := propagate(ctx, propagateRight, &n.right); err != nil {
		return n, extraFilter, err
	}
	n.pred.onCond = onCond

	return n, filterRemainder, nil
}

// mergeConj combines two predicates.
func mergeConj(left, right tree.TypedExpr) tree.TypedExpr {
	if isFilterTrue(left) {
		if right == tree.DBoolTrue {
			return nil
		}
		return right
	}
	if isFilterTrue(right) {
		return left
	}
	return tree.NewTypedAndExpr(left, right)
}

func isFilterTrue(expr tree.TypedExpr) bool {
	return expr == nil || expr == tree.DBoolTrue
}

// splitAndExpr flattens a tree of AND expressions, appending all of the child
// expressions as a list. Any non-AND expression is appended as a single element
// in the list.
//
//   a AND b AND c AND d -> [a, b, c, d]
func splitAndExpr(
	evalCtx *tree.EvalContext, e tree.TypedExpr, exprs tree.TypedExprs,
) tree.TypedExprs {
	switch t := e.(type) {
	case *tree.AndExpr:
		return splitAndExpr(evalCtx, t.TypedRight(), splitAndExpr(evalCtx, t.TypedLeft(), exprs))
	}
	return append(exprs, e)
}
