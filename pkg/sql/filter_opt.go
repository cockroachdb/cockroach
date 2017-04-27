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
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package sql

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
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
	ctx context.Context, plan planNode, info *dataSourceInfo, extraFilter parser.TypedExpr,
) (newPlan planNode, remainingFilter parser.TypedExpr, err error) {
	remainingFilter = extraFilter
	switch n := plan.(type) {
	case *emptyNode:
		if !n.results {
			// There is no row (by definition), so all filters
			// are "already applied". Silently absorb any extra filter.
			return plan, parser.DBoolTrue, nil
		}
		// TODO(knz): We could evaluate the filter here and set/reset
		// n.results accordingly, assuming the filter is not "row
		// dependent" (cf. resolveNames()).

	case *filterNode:
		newFilter := mergeConj(n.filter, extraFilter)
		newPlan, err = p.propagateOrWrapFilters(ctx, n.source.plan, n.source.info, newFilter)
		if err != nil {
			return plan, extraFilter, err
		}
		return newPlan, parser.DBoolTrue, nil

	case *scanNode:
		n.filter = mergeConj(n.filter, n.filterVars.Rebind(extraFilter, true, false))
		return plan, parser.DBoolTrue, nil

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
		return plan, parser.DBoolTrue, nil

	case *sortNode:
		// A sort node can propagate a filter, and source filtering
		// reduces the amount of work.
		n.plan, err = p.propagateOrWrapFilters(ctx, n.plan, nil, extraFilter)
		if err != nil {
			return plan, extraFilter, err
		}
		return plan, parser.DBoolTrue, nil

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
		return plan, parser.DBoolTrue, nil

	case *groupNode:
		// Filters can't propagate across aggregates:
		//   SELECT * FROM (SELECT MAX(x) AS m FROM foo GROUP BY y) WHERE m > 123
		// can only be filtered after the aggregation has occurred.
		// They can however propagate across non-aggregated GROUP BY columns:
		//   SELECT * FROM (SELECT MAX(x) AS m, y FROM foo GROUP BY y) WHERE y > 123
		// can be transformed to:
		//   SELECT MAX(x) AS m, y FROM (SELECT * FROM foo WHERE y > 123) GROUP BY y
		// However we don't do that yet.
		// For now, simply trigger optimization for the child node.
		//
		// TODO(knz): implement the aforementioned optimization.
		//
		if n.plan, err = p.triggerFilterPropagation(ctx, n.plan); err != nil {
			return plan, extraFilter, err
		}

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

	case *createTableNode:
		if n.n.As() {
			if n.sourcePlan, err = p.triggerFilterPropagation(ctx, n.sourcePlan); err != nil {
				return plan, extraFilter, err
			}
		}

	case *createViewNode:
		if n.sourcePlan, err = p.triggerFilterPropagation(ctx, n.sourcePlan); err != nil {
			return plan, extraFilter, err
		}

	case *deleteNode:
		if n.run.rows, err = p.triggerFilterPropagation(ctx, n.run.rows); err != nil {
			return plan, extraFilter, err
		}

	case *insertNode:
		if n.run.rows, err = p.triggerFilterPropagation(ctx, n.run.rows); err != nil {
			return plan, extraFilter, err
		}

	case *updateNode:
		if n.run.rows, err = p.triggerFilterPropagation(ctx, n.run.rows); err != nil {
			return plan, extraFilter, err
		}

	case *explainDebugNode:
		if n.plan, err = p.triggerFilterPropagation(ctx, n.plan); err != nil {
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

	case *explainTraceNode:
		if n.plan, err = p.triggerFilterPropagation(ctx, n.plan); err != nil {
			return plan, extraFilter, err
		}

	case *splitNode:
		if n.rows, err = p.triggerFilterPropagation(ctx, n.rows); err != nil {
			return plan, extraFilter, err
		}

	case *relocateNode:
		if n.rows, err = p.triggerFilterPropagation(ctx, n.rows); err != nil {
			return plan, extraFilter, err
		}

	case *alterTableNode:
	case *copyNode:
	case *createDatabaseNode:
	case *createIndexNode:
	case *createUserNode:
	case *delayedNode:
	case *dropDatabaseNode:
	case *dropIndexNode:
	case *dropTableNode:
	case *dropViewNode:
	case *hookFnNode:
	case *valueGenerator:
	case *valuesNode:
	case *showRangesNode:
	case *scatterNode:

	default:
		panic(fmt.Sprintf("unhandled node type: %T", plan))
	}

	return plan, remainingFilter, nil
}

// triggerFilterPropagation initiates filter propagation on the given plan.
func (p *planner) triggerFilterPropagation(ctx context.Context, plan planNode) (planNode, error) {
	newPlan, remainingFilter, err := p.propagateFilters(ctx, plan, nil, parser.DBoolTrue)
	if err != nil {
		return plan, err
	}

	if !isFilterTrue(remainingFilter) {
		panic(fmt.Sprintf("propagateFilters on \n%s\n spilled a non-trivial remaining filter: %s",
			planToString(ctx, plan), remainingFilter))
	}

	return newPlan, nil
}

// propagateOrWrapFilters triggers filter propagation on the given
// node, and creates a new filterNode if there is any remaining filter
// after the propagation.
func (p *planner) propagateOrWrapFilters(
	ctx context.Context, plan planNode, info *dataSourceInfo, filter parser.TypedExpr,
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
		info = newSourceInfoForSingleTable(anonymousTable, newPlan.Columns())
	}
	f := &filterNode{
		p:      p,
		source: planDataSource{plan: newPlan, info: info},
	}
	f.ivarHelper = parser.MakeIndexedVarHelper(f, len(info.sourceColumns))
	f.filter = f.ivarHelper.Rebind(remainingFilter,
		false /* helper is fresh, no reset needed */, false)
	return f, nil
}

// addRenderFilter attempts to add the extraFilter to the renderNode.
// The filter is only propagated to the sub-plan if it is expressed
// using renders that are either simple datums or simple column
// references to the source.
func (p *planner) addRenderFilter(
	ctx context.Context, s *renderNode, extraFilter parser.TypedExpr,
) (planNode, parser.TypedExpr, error) {
	// outerFilter is the filter expressed using values rendered by this renderNode.
	var outerFilter parser.TypedExpr = parser.DBoolTrue
	// innerFilter is the filter on the source planNode.
	var innerFilter parser.TypedExpr = parser.DBoolTrue

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
		convFunc := func(v parser.VariableExpr) (bool, parser.Expr) {
			if iv, ok := v.(*parser.IndexedVar); ok {
				renderExpr := s.render[iv.Idx]
				if d, ok := renderExpr.(parser.Datum); ok {
					// A standalone Datum is not complex, so it can be propagated further.
					return true, d
				}
				if rv, ok := renderExpr.(*parser.IndexedVar); ok {
					// A naked IndexedVar is not complex, so it can be propagated further.
					return true, rv
				}
			}
			return false, v
		}

		// Do the replacement proper.
		innerFilter, outerFilter = splitFilter(extraFilter, convFunc)
	}

	// Propagate the inner filter.
	newPlan, err := s.planner.propagateOrWrapFilters(
		ctx, s.source.plan, s.source.info, innerFilter)
	if err != nil {
		return s, extraFilter, err
	}

	// Attach what remains as the new source.
	s.source.plan = newPlan

	return s, outerFilter, nil
}

// addJoinFilter propagates the given filter to a joinNode.
func (p *planner) addJoinFilter(
	ctx context.Context, n *joinNode, extraFilter parser.TypedExpr,
) (planNode, parser.TypedExpr, error) {
	// TODO(knz): support outer joins.
	if n.joinType != joinTypeInner {
		// Outer joins not supported; simply trigger filter optimization in the sub-nodes.
		var err error
		if n.left.plan, err = p.triggerFilterPropagation(ctx, n.left.plan); err == nil {
			n.right.plan, err = p.triggerFilterPropagation(ctx, n.right.plan)
		}
		return n, extraFilter, err
	}

	// There are three steps to the transformation below:
	//
	// 1. if there are merged columns (i.e. USING/NATURAL), since the
	//    incoming extra filter may refer to them, transform the
	//    extra filter to only use the left and right columns.
	// 2. merge the existing ON predicate with the extra filter, yielding
	//    an "initial predicate".
	// 3. split the initial predicate into a left, right and combined parts.
	// 4. propagate the left and right part to the left and right join operands.
	// 5. use the combined part from step #3 to create a new ON predicate,
	//    possibly detecting new equality columns.

	// Reminder: the layout of the virtual data values for a join is:
	// [ merged columns ] [ columns from left ] [ columns from right]
	//
	// The merged columns result from USING or NATURAL; for example
	//      SELECT * FROM a JOIN b USING(x)
	// has columns: x (merged), a.*, b.*

	// leftBegin is the logical index of the first column after
	// the merged columns.
	leftBegin := n.pred.numMergedEqualityColumns
	// rightBegin is the logical index of the first column in
	// the right data source.
	rightBegin := leftBegin + len(n.left.info.sourceColumns)

	if leftBegin > 0 && !isFilterTrue(extraFilter) {
		// There are merged columns, and the incoming extra filter may be
		// referring to them.
		// To understand what's going on below consider the query:
		//
		// SELECT * FROM a JOIN b USING(x) WHERE f(a,b) AND g(a) AND g(b) AND h(x)
		//
		// What we want at the end (below) is this:
		//    SELECT x, ... FROM
		//          (SELECT * FROM a WHERE g(a) AND h(a.x))
		//     JOIN (SELECT * FROM b WHERE g(b) AND h(b.x))
		//       ON a.x = b.x AND f(a,b)
		//
		// So we need to replace h(x) which refers to the merged USING
		// column by h(x) on the source tables. But we can't simply
		// replace it by a single reference to *either* of the two source
		// tables (say, via h(a.x) in the example), because if we did then
		// the filter propagation would push it towards that source table
		// only (a in the example) -- and we want it propagated on both
		// sides!
		//
		// So what we do instead:
		// - we first split the expression to extract those expressions that
		//   refer to merged columns (notUsingMerged // perhapsUsingMerged):
		//   "f(a,b) AND g(a) AND g(b)" // "h(x)"
		// - we replace the part that refers to merged column by an AND
		//   of its substitution by references to the left and righ sources:
		//     "h(x)" -> "h(a.x) AND h(b.x)"
		// - we recombine all of them:
		//     f(a,b) AND g(a) AND g(b) AND h(a.x) AND h(b.x)
		// Then the rest of the optimization below can go forward, and
		// will take care of splitting the expressions among the left and
		// right operands.
		noMergedVars := func(expr parser.VariableExpr) (bool, parser.Expr) {
			if iv, ok := expr.(*parser.IndexedVar); ok && iv.Idx >= leftBegin {
				return true, expr
			}
			return false, expr
		}
		// Note: we use a negative condition here because splitFilter()
		// doesn't guarantee that its right return value doesn't contain
		// sub-expressions where the conversion function returns true.
		notUsingMerged, perhapsUsingMerged := splitFilter(extraFilter, noMergedVars)
		leftFilter := exprConvertVars(perhapsUsingMerged,
			func(expr parser.VariableExpr) (bool, parser.Expr) {
				if iv, ok := expr.(*parser.IndexedVar); ok && iv.Idx < leftBegin {
					newIv := n.pred.iVarHelper.IndexedVar(leftBegin + n.pred.leftEqualityIndices[iv.Idx])
					return true, newIv
				}
				return true, expr
			})
		rightFilter := exprConvertVars(perhapsUsingMerged,
			func(expr parser.VariableExpr) (bool, parser.Expr) {
				if iv, ok := expr.(*parser.IndexedVar); ok && iv.Idx < leftBegin {
					newIv := n.pred.iVarHelper.IndexedVar(rightBegin + n.pred.rightEqualityIndices[iv.Idx])
					return true, newIv
				}
				return true, expr
			})
		extraFilter = mergeConj(notUsingMerged, mergeConj(leftFilter, rightFilter))
	}

	// Merge the existing ON predicate with the extra filter.
	// We don't need to reset the helper here, as this will be done
	// later for the final predicate below.
	// Note: we assume here that neither extraFilter nor n.pred.onCond
	// can refer to the merged columns any more. For extraFilter
	// that's guaranteed by the code above. For n.pred.onCond, that's
	// proven by induction on the following two observations:
	// - initially, onCond cannot refer to merged columns because
	//   in SQL the USING/NATURAL syntax is mutually exclusive with ON
	// - at every subsequent round of filter optimization, changes to
	//   n.pred.onCond have been processed by the code above.
	initialPred := mergeConj(n.pred.iVarHelper.Rebind(extraFilter, false, false), n.pred.onCond)

	// Split the initial predicate into left, right and combined parts.
	// In this process we shift the IndexedVars in the left and right
	// filter expressions so that they are numbered starting from 0
	// relative to their respective operand.
	leftExpr, remainder := splitFilter(initialPred,
		func(expr parser.VariableExpr) (bool, parser.Expr) {
			if iv, ok := expr.(*parser.IndexedVar); ok && iv.Idx < rightBegin {
				return true, n.pred.iVarHelper.IndexedVar(iv.Idx - leftBegin)
			}
			return false, expr
		})
	rightExpr, combinedExpr := splitFilter(remainder,
		func(expr parser.VariableExpr) (bool, parser.Expr) {
			if iv, ok := expr.(*parser.IndexedVar); ok && iv.Idx >= rightBegin {
				return true, n.pred.iVarHelper.IndexedVar(iv.Idx - rightBegin)
			}
			return false, expr
		})

	// Propagate the left and right predicates to the left and right
	// sides of the join. The predicates must first be "shifted"
	// i.e. their IndexedVars which are relative to the join columns
	// must be modified to become relative to the operand's columns.

	propagate := func(ctx context.Context, pred parser.TypedExpr, side *planDataSource) error {
		newPlan, err := p.propagateOrWrapFilters(ctx, side.plan, side.info, pred)
		if err != nil {
			return err
		}
		side.plan = newPlan
		return nil
	}
	if err := propagate(ctx, leftExpr, &n.left); err != nil {
		return n, extraFilter, err
	}
	if err := propagate(ctx, rightExpr, &n.right); err != nil {
		return n, extraFilter, err
	}

	// Extract possibly new equality columns from the combined predicate, and
	// use the rest as new ON condition.
	var newCombinedExpr parser.TypedExpr = parser.DBoolTrue
	for _, e := range splitAndExpr(&p.evalCtx, combinedExpr, nil) {
		if e == parser.DBoolTrue {
			continue
		}
		if !n.pred.tryAddEqualityFilter(e, n.left.info, n.right.info) {
			newCombinedExpr = mergeConj(newCombinedExpr, e)
		}
	}
	combinedExpr = newCombinedExpr

	n.pred.onCond = n.pred.iVarHelper.Rebind(combinedExpr, true, false)

	return n, nil, nil
}

// mergeConj combines two predicates.
func mergeConj(left, right parser.TypedExpr) parser.TypedExpr {
	if isFilterTrue(left) {
		if right == parser.DBoolTrue {
			return nil
		}
		return right
	}
	if isFilterTrue(right) {
		return left
	}
	return parser.NewTypedAndExpr(left, right)
}

func isFilterTrue(expr parser.TypedExpr) bool {
	return expr == nil || expr == parser.DBoolTrue
}
