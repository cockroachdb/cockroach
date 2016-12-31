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
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

// propagateFilters is the recursive algorithm that supports triggerFilterPropagation().
//
// The explanation below uses the following notations:
//    P, Q, R : logical predicates (formulas with boolean values)
//    T       : the tautology ("always true" predicate)
//    A, B    : data sources
//    t       : table
//
//    f : abbreviation for propagateFilters
//    w : abbreviation for propagateOrWrapFilters
//    t : abbreviation for triggerFilterPropagation
//
// Definitions:
//
//     t( A )    -> f(A, T)
//
//     w( A, P ) -> B                   if R = T
//               -> [filter(R) FROM B]  otherwise
//                  where (R, B) := f(A, P)
//
// f (propagateFilters) knows how to attach an incoming filter to
// empty and scan nodes:
//
//     f( [<empty>], P )     -> T, [<empty>]
//     f( [scan t // P], Q ) -> T, [scan t // (P AND Q)]
//
// f combines an incoming filter with an existing filter and pushes
// forward:
//
//     f( [filter(P) FROM A], Q ) -> f(A, (P AND Q))
//
// Filtering is distributive over UNION:
//
//     f( [union FROM A, B], P ) -> T, [union FROM w(A, P), w(B, P)]
//
// f propagates filters "through" distinct and sort nodes:
//
//     f( [distinct FROM A], P ) -> R, [distinct FROM B]
//                                  where (R, B) := f(A, P)
//     f( [sort FROM A], P )     -> R, [sort FROM B]
//                                  where (R, B) := f(A, P)
//
// Some nodes "block" filtering entirely:
//
//     f( [limit FROM A], P )      -> P, [limit FROM t(A)]
//     f( [ordinality FROM A], P ) -> P, [ordinality FROM t(A)]
//
// Some nodes currently block filtering entirely, but could be
// optimized later to let some filters through (see inline comments
// below):
//
//     f( [group FROM A], P )  -> P, [group FROM t(A)]
//     f( [window FROM A], P ) -> P, [window FROM t(A)]
//
// f propagates filters through render nodes, though only for simple renders:
//
//     f( [render <colname> AS x FROM A], P ) -> T, [render <colname> AS x FROM w(A, [P/x/<colname>])]
//     f( [render Expr(...) AS x FROM A], P ) -> P, [render Expr(...) AS x FROM t(A)]
//
// (The notation [P/x/<colname>] means replace all occurrences of "x" in P by <colname>.
//
// f propagates filters through inner joins only:
//
//     f( [outerjoin(P) FROM A, B], Q ) -> Q, [outerjoin(P) FROM t(A), t(B)]
//     f( [innerjoin(P) FROM A, B], Q ) -> T, [innerjoin(combi(R)) FROM w(A, left(R)), w(B, right(R))
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
// - predicates are encoded as conjunctions (list of predicates that
//   semantically evaluates to the conjunction of its elements):
//   - T is the empty conjunction (`nil` in Go).
//   - "P AND Q" is implemented by merging the conjunctions.
// - the member expressions of a conjunctions are either:
//   - free, meaning they can be evaluated independently of context;
//     this includes simple constants or calls to functions with only
//     free arguments.
//   - dependent, meaning they use references (IndexedVars) to
//     values in the data source at the current level.
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
//
func (p *planner) propagateFilters(
	plan planNode, info *dataSourceInfo, extraFilter conjExpr,
) (newPlan planNode, remainingFilter conjExpr, err error) {
	var subPlan planNode
	remainingFilter = extraFilter

	switch n := plan.(type) {
	case *emptyNode:
		if !n.results {
			// There is no row (by definition), so all filters
			// are "already applied". Silently absorb any extra filter.
			return plan, nil, nil
		}
		// TODO(knz) We could evaluate the filter here and set/reset
		// n.results accordingly, assuming the filter is not "row
		// dependent" (cf. resolveNames()).

	case *filterNode:
		newFilter := mergeConj(exprToConj(n.filter), extraFilter)
		return p.propagateFilters(n.source.plan, n.source.info, newFilter)

	case *scanNode:
		n.filter = n.filterVars.Rebind(
			conjToExpr(mergeConj(exprToConj(n.filter), extraFilter)), true)

		return plan, nil, nil

	case *renderNode:
		return p.addRenderFilter(n, extraFilter)

	case *joinNode:
		return p.addJoinFilter(n, extraFilter)

	case *indexJoinNode:
		panic("filter optimization must occur before index selection")

	case *distinctNode:
		// A distinct node can propagate a filter. Source filtering
		// reduces the amount of work.
		subPlan, remainingFilter, err = p.propagateFilters(n.plan, info, extraFilter)
		if err != nil {
			return plan, extraFilter, err
		}
		n.plan = subPlan

	case *sortNode:
		// A sort node can propagate a filter, and source filter reduces
		// the amount of work.
		subPlan, remainingFilter, err = p.propagateFilters(n.plan, nil, extraFilter)
		if err != nil {
			return plan, extraFilter, err
		}
		n.plan = subPlan

	case *selectTopNode:
		if n.limit == nil && n.window == nil && n.group == nil {
			// We only know how to propagate the filter if there is no
			// intervening GROUP BY or LIMIT clause nor window functions.
			// (For GROUP BY, see below for details.)
			newPlan, remainingFilter, err = p.propagateFilters(n.source, nil, extraFilter)
			if err != nil {
				return plan, extraFilter, err
			}
			n.source = newPlan
		} else if n.source, err = p.triggerFilterPropagation(n.source); err != nil {
			return plan, extraFilter, err
		}

	case *unionNode:
		// Filtering is distributive over set operations.
		// Source filtering reduces the amount of work, so force propagation.
		newLeft, err := p.propagateOrWrapFilters(n.left, nil, extraFilter)
		if err != nil {
			return plan, extraFilter, err
		}

		newRight, err := p.propagateOrWrapFilters(n.right, nil, extraFilter)
		if err != nil {
			return plan, extraFilter, err
		}

		n.left = newLeft
		n.right = newRight
		return plan, nil, nil

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
		// TODO(knz) implement the aforementioned optimization.
		//
		if n.plan, err = p.triggerFilterPropagation(n.plan); err != nil {
			return plan, extraFilter, err
		}

	case *limitNode:
		if n.plan, err = p.triggerFilterPropagation(n.plan); err != nil {
			return plan, extraFilter, err
		}

	case *windowNode:
		if n.plan, err = p.triggerFilterPropagation(n.plan); err != nil {
			return plan, extraFilter, err
		}

	case *ordinalityNode:
		if n.source, err = p.triggerFilterPropagation(n.source); err != nil {
			return plan, extraFilter, err
		}

	case *createTableNode:
		if n.n.As() {
			if n.sourcePlan, err = p.triggerFilterPropagation(n.sourcePlan); err != nil {
				return plan, extraFilter, err
			}
		}

	case *createViewNode:
		if n.sourcePlan, err = p.triggerFilterPropagation(n.sourcePlan); err != nil {
			return plan, extraFilter, err
		}

	case *deleteNode:
		if n.run.rows, err = p.triggerFilterPropagation(n.run.rows); err != nil {
			return plan, extraFilter, err
		}

	case *insertNode:
		if n.run.rows, err = p.triggerFilterPropagation(n.run.rows); err != nil {
			return plan, extraFilter, err
		}

	case *updateNode:
		if n.run.rows, err = p.triggerFilterPropagation(n.run.rows); err != nil {
			return plan, extraFilter, err
		}

	case *explainDebugNode:
		if n.plan, err = p.triggerFilterPropagation(n.plan); err != nil {
			return plan, extraFilter, err
		}

	case *explainPlanNode:
		if n.optimized {
			if n.plan, err = p.triggerFilterPropagation(n.plan); err != nil {
				return plan, extraFilter, err
			}
		}

	case *explainTraceNode:
		if n.plan, err = p.triggerFilterPropagation(n.plan); err != nil {
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
	case *splitNode:
	case *valueGenerator:
	case *valuesNode:

	default:
		panic(fmt.Sprintf("unhandled node type: %T", plan))
	}

	return plan, remainingFilter, nil
}

// triggerFilterPropagation initiates filter propagation on the given plan.
func (p *planner) triggerFilterPropagation(plan planNode) (planNode, error) {
	newPlan, remainingFilter, err := p.propagateFilters(plan, nil, nil)
	if err != nil {
		return plan, err
	}

	if len(remainingFilter) > 0 {
		panic(fmt.Sprintf("propagateFilters on \n%s\n spilled a non-nil remaining filter: %s",
			planToString(plan), remainingFilter))
	}

	return newPlan, nil
}

// propagateOrWrapFilters triggers filter propagation on the given
// node, and creates a new filterNode if there is any remaining filter
// after the propagation.
func (p *planner) propagateOrWrapFilters(
	plan planNode, info *dataSourceInfo, filter conjExpr,
) (planNode, error) {
	newPlan, remainingFilter, err := p.propagateFilters(plan, info, filter)
	if err != nil {
		return plan, err
	}

	// If there is no remaining filter, simply return the new plan.
	if len(remainingFilter) == 0 {
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
	f.filter = f.ivarHelper.Rebind(conjToExpr(remainingFilter),
		false /* helper is fresh, no reset needed */)
	return f, nil
}

// addRenderFilter attempts to add the extraFilter to the renderNode.
// The filter is only propagated to the sub-plan if it is expressed
// using renders that are either simple datums or simple column
// references to the source.
func (p *planner) addRenderFilter(s *renderNode, extraFilter conjExpr) (planNode, conjExpr, error) {
	// outerFilter is the filter expressed using values rendered by this renderNode.
	var outerFilter conjExpr
	// innerFilter is the filter on the source planNode.
	var innerFilter conjExpr

	if extraFilter != nil {
		// The filter that's being added refers to the rendered
		// expressions, not the render's source node.

		// We propagate only those filters that use simple renders
		// of columns from the source, that is, we do not replace:
		//
		//   SELECT a + 2 * b AS c FROM t WHERE c > 123
		// by
		//   SELECT a + 2 * b AS c FROM (SELECT * FROM t WHERE a + 2 * b > 123)
		//
		// Because that would cause the (more complex) expression `a + 2 *
		// b` to be computed at both levels. This cannot be "simplified"
		// further by trying to factor the common sub-expression: `SELECT
		// c FROM (SELECT * FROM (SELECT a + 2 * b AS c FROM t) WHERE c >
		// 123)` contains the original query.
		//
		// To perform this propagation, we apply the following convFunc to
		// all the filter expressions. This replaces IndexedVars in the
		// filter that are relative to this renderNode's render list by
		// IndexedVars that are relative to the renderNode's source
		// columns. It also sets complexRenderEncountered to true if any
		// of the IndexedVars refers to non-trivial expressions.
		//
		// Note: we don't really care about the IndexedVars' container
		// here, as no filter will stay at this node -- either they
		// propagate down or spill upward as remaining filter.
		// See also the comment for propagateFilters() above.
		//
		var complexRenderEncountered bool
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
				// Otherwise, say we can't propagate further.
				complexRenderEncountered = true
			}
			return true, v
		}

		// Do the replacement proper.
		for _, f := range extraFilter {
			complexRenderEncountered = false
			newExpr := exprConvertVars(f.expr, convFunc)

			if complexRenderEncountered {
				// Complex expressions encountered: keep the filter on the outer side.
				outerFilter = append(outerFilter, f)
			} else {
				// Only simple column references: propagate the filter.
				innerFilter = append(innerFilter, exprToConjPredicate(newExpr))
			}
		}
	}

	// Propagate the inner filter.
	newPlan, err := s.planner.propagateOrWrapFilters(
		s.source.plan, s.source.info, innerFilter)
	if err != nil {
		return s, extraFilter, err
	}

	// Attach what remains as the new source.
	s.source.plan = newPlan

	return s, outerFilter, nil
}

// addJoinFilter propagates the given filter to a joinNode.
func (p *planner) addJoinFilter(n *joinNode, extraFilter conjExpr) (planNode, conjExpr, error) {
	// TODO(knz) support outer joins.
	if n.joinType != joinTypeInner {
		// Outer joins not supported; simply trigger filter optimization in the sub-nodes.
		var err error
		if n.left.plan, err = p.triggerFilterPropagation(n.left.plan); err == nil {
			n.right.plan, err = p.triggerFilterPropagation(n.right.plan)
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
	// has columns: x, a.*, b.*
	//
	// mergedBoundary is the logical index of the first column after
	// the merged columns.
	mergedBoundary := n.pred.numMergedEqualityColumns
	// sourceBoundary is the logical index of the first column in
	// the right data source.
	sourceBoundary := mergedBoundary + len(n.left.info.sourceColumns)

	if mergedBoundary > 0 && len(extraFilter) > 0 {
		newExtraFilter := make(conjExpr, 0, len(extraFilter))
		// There are merged columns, and the incoming extra filter may
		// be referring to them. We need to transform the extra filter
		// so that it only refers to either left or right columns.
		rebaseFunc := func(expr parser.VariableExpr) (bool, parser.Expr) {
			if iv, ok := expr.(*parser.IndexedVar); ok {
				if iv.Idx < mergedBoundary {
					// The expression is referring to one of the merged equality columns.
					// Substitute it with the appropriate COALESCE expression.
					return true, parser.NewTypedCoalesceExpr(
						n.pred.iVarHelper.IndexedVar(mergedBoundary+n.pred.leftEqualityIndices[iv.Idx]),
						n.pred.iVarHelper.IndexedVar(sourceBoundary+n.pred.rightEqualityIndices[iv.Idx]))
				}
			}
			return true, expr
		}
		for i := range extraFilter {
			newExpr := exprConvertVars(extraFilter[i].expr, rebaseFunc)
			newExtraFilter = append(newExtraFilter, exprToConjPredicate(newExpr))
		}
		extraFilter = newExtraFilter
	}

	// Merge the existing ON predicate with the extra filter.
	initialPred := mergeConj(extraFilter, exprToConj(n.pred.onCond))

	// Split the initial predicate into left, right and combined parts.
	var hasLeft, hasRight bool
	analyzeFunc := func(expr parser.VariableExpr) (bool, parser.Expr) {
		if iv, ok := expr.(*parser.IndexedVar); ok {
			if iv.Idx < sourceBoundary {
				hasLeft = true
			} else {
				hasRight = true
			}
		}
		return true, expr
	}
	var combinedExpr, leftExpr, rightExpr conjExpr
	for _, f := range initialPred {
		hasLeft = false
		hasRight = false
		// We don't kneed to reset the helper here, as this will be done
		// later for the final predicate below.
		f.expr = n.pred.iVarHelper.Rebind(f.expr, false)
		exprCheckVars(f.expr, analyzeFunc)
		if hasLeft && !hasRight {
			leftExpr = append(leftExpr, f)
		} else if hasRight && !hasLeft {
			rightExpr = append(rightExpr, f)
		} else if hasRight && hasLeft {
			combinedExpr = append(combinedExpr, f)
		} else {
			// This is a free expression.
			// Note: we arrive here if f.expr is either a constant
			// expression, which can be evaluated right away, or a
			// row-dependent expression, which needs to be preserved. We
			// don't optimize for constants yet and just leave the entire
			// thing in the combined predicate.
			combinedExpr = append(combinedExpr, f)
		}
	}

	// Propagate the left and right predicates to the left and right
	// sides of the join. The predicates must first be "shifted"
	// i.e. their IndexedVars which are relative to the join columns
	// must be modified to become relative to the operand's columns.

	propagate := func(pred conjExpr, offset int, side *planDataSource) error {
		pred = shiftConj(n.pred.iVarHelper, pred, offset)
		newPlan, err := p.propagateOrWrapFilters(side.plan, side.info, pred)
		if err != nil {
			return err
		}
		side.plan = newPlan
		return nil
	}
	if err := propagate(leftExpr, mergedBoundary, &n.left); err != nil {
		return n, extraFilter, err
	}
	if err := propagate(rightExpr, sourceBoundary, &n.right); err != nil {
		return n, extraFilter, err
	}

	// Extract possibly new equality columns from the combined predicate, and
	// use the rest as new ON condition.
	var newOnCond conjExpr
	for _, f := range combinedExpr {
		if !n.pred.tryAddEqualityFilter(f.expr, n.left.info, n.right.info) {
			newOnCond = append(newOnCond, f)
		}
	}
	n.pred.onCond = n.pred.iVarHelper.Rebind(conjToExpr(newOnCond), true)

	return n, nil, nil
}

// conjExpr represents a conjunction of multiple sub-predicates.
type conjExpr []conjPredicate

// conjPredicate represents a single predicate in a conjunction.
type conjPredicate struct {
	// expr is the expression containing the predicate itself.
	expr parser.TypedExpr

	// key is a representation of the expression suitable for "fast"
	// structural comparisons. Currently we use the result of
	// pretty-printing the expression with column references printed as
	// ordinal references.
	key string
}

// exprToConjPredicate converts an expression into a predicate
// suitable for a conjExpr.
func exprToConjPredicate(expr parser.TypedExpr) conjPredicate {
	return conjPredicate{
		expr: expr,
		key:  parser.AsStringWithFlags(expr, parser.FmtSymbolicVars),
	}
}

func (c conjExpr) String() string {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, f := range c {
		if i > 0 {
			buf.WriteString(", ")
		}
		f.expr.Format(&buf, parser.FmtSymbolicVars)
		buf.WriteString(" // ")
		fmt.Fprintf(&buf, "%q", f.key)
	}
	buf.WriteByte(']')
	return buf.String()
}

// conjToExpr converts a conjunction of the form `[a, b, c]` to an
// expression of the form `a AND b AND c`.
func conjToExpr(filter conjExpr) parser.TypedExpr {
	switch len(filter) {
	case 0:
		// A nil filter is equivalent to true in all planNodes.
		return nil
	case 1:
		return filter[0].expr
	default:
		return parser.NewTypedAndExpr(filter[0].expr, conjToExpr(filter[1:]))
	}
}

// exprToConj converts an expression of the form `a AND b AND c` to a
// conjunction `[a, b, c]`.
func exprToConj(filter parser.TypedExpr) conjExpr {
	if filter == nil {
		return nil
	}
	if e, ok := filter.(*parser.ParenExpr); ok {
		return exprToConj(e.Expr.(parser.TypedExpr))
	}
	if e, ok := filter.(*parser.AndExpr); ok {
		leftConj := exprToConj(e.Left.(parser.TypedExpr))
		rightConj := exprToConj(e.Right.(parser.TypedExpr))
		return mergeConj(leftConj, rightConj)
	}
	return conjExpr{exprToConjPredicate(filter)}
}

// mergeConj computes the union of two conjunctions. It also ensures
// that the union of [a, b] and [c, a] is [a, b, c]
// and not [a, b, c, a].
func mergeConj(left, right conjExpr) conjExpr {
	res := left
	for _, r := range right {
		dup := false
		for _, l := range left {
			if r.key == l.key {
				dup = true
				continue
			}
		}
		if !dup {
			res = append(res, r)
		}
	}
	return res
}

// shiftConj renumbers the IndexedVars in the given filter by shifting
// their index to the left by the given offset. For example a shift of
// "@3+@5" by 2 results in the expression "@1+@3".
func shiftConj(ivarHelper parser.IndexedVarHelper, filter conjExpr, offset int) conjExpr {
	shiftFunc := func(expr parser.VariableExpr) (bool, parser.Expr) {
		if iv, ok := expr.(*parser.IndexedVar); ok {
			return true, ivarHelper.IndexedVar(iv.Idx - offset)
		}
		return true, expr
	}
	for i, f := range filter {
		filter[i].expr = exprConvertVars(f.expr, shiftFunc)
		filter[i].key = parser.AsStringWithFlags(filter[i].expr, parser.FmtSymbolicVars)
	}
	return filter
}
