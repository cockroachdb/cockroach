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

// optimizeFilters performs filter propagation on the given plan.
func (p *planner) optimizeFilters(plan planNode) (planNode, error) {
	newPlan, remainingFilter, err := p.addFilter(plan, nil, nil, false)
	if err != nil {
		return plan, err
	}

	if len(remainingFilter) > 0 {
		panic(fmt.Sprintf("addFilter on \n%s\n spilled a non-nil remaining filter: %s",
			planToString(plan), remainingFilter))
	}

	return newPlan, nil
}

// addFilter is the recursive algorithm that supports optimizeFilters().
//
// Its function is to try to add the supplied extra filter to the
// given logical plan. The extra filter is expressed as a conjunction
// of predicates.
//
// In some cases the recursion into sub-plans is unable to "accept"
// the entire conjunction and will spill a "remaining filter" with the
// predicates that were not absorbed.
//
// If the autoWrap argument is set to true, a new filterNode is created
// to wrap the plan and filter using the remaining predicates.
// Otherwise, the remaining filter is returned.
func (p *planner) addFilter(
	plan planNode, info *dataSourceInfo, extraFilter conjExpr, autoWrap bool,
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
		// n.results accordingly.

		// Fallthrough: wrap or spill.

	case *filterNode:
		newFilter := mergeConj(exprToConj(n.filter), extraFilter)
		subPlan, remainingFilter, err = p.addFilter(
			n.source.plan, n.source.info, newFilter, false,
		)
		if err != nil {
			return plan, extraFilter, err
		}
		if len(remainingFilter) > 0 {
			n.ivarHelper.Reset()
			n.filter = n.ivarHelper.Rebind(conjToExpr(remainingFilter))
			n.source.plan = subPlan
		} else {
			// No filter remaining, silently kill the filter node.
			plan = subPlan
		}

		return plan, nil, nil

	case *scanNode:
		n.filter = n.filterVars.Rebind(
			conjToExpr(mergeConj(exprToConj(n.filter), extraFilter)))

		return plan, nil, nil

	case *selectNode:
		subPlan, remainingFilter, err = p.addSelectFilter(n, extraFilter)
		if err != nil {
			return plan, extraFilter, err
		}
		plan = subPlan

		// Fallthrough: wrap or spill.

	case *joinNode:
		subPlan, remainingFilter, err = p.addJoinFilter(n, extraFilter)
		if err != nil {
			return plan, extraFilter, err
		}
		plan = subPlan

		// Fallthrough: wrap or spill.

	case *indexJoinNode:
		// An index join propagates the filter on its table source.
		//
		// This doesn't sound smart -- perhaps we could do a different
		// index join with knowledge of a new filter --, but it is too
		// late to be smart at this point. We probably shouldn't be here
		// anyway: addFilter() should be run before expandPlan() which
		// does index selection, not after. Before expandPlan(), there is
		// no indexJoinNode in the tree.
		subPlan, remainingFilter, err = p.addFilter(n.table, info, extraFilter, false)
		if err != nil {
			return plan, extraFilter, err
		}
		n.table = subPlan.(*scanNode)

		// Fallthrough: wrap or spill.

	case *distinctNode:
		// A distinct node can propagate a filter. Source filtering
		// reduces the amount of work.
		subPlan, remainingFilter, err = p.addFilter(n.plan, info, extraFilter, false)
		if err != nil {
			return plan, extraFilter, err
		}
		n.plan = subPlan

		// Fallthrough: wrap or spill.

	case *sortNode:
		// A sort node can propagate a filter, and source filter reduces
		// the amount of work.
		subPlan, remainingFilter, err = p.addFilter(n.plan, nil, extraFilter, false)
		if err != nil {
			return plan, extraFilter, err
		}
		n.plan = subPlan

		// Fallthrough: wrap or spill.

	case *selectTopNode:
		if n.limit == nil && n.window == nil && n.group == nil {
			// We only know how to propagate the filter if there is no
			// intervening GROUP BY or LIMIT clause nor window functions.
			newPlan, remainingFilter, err = p.addFilter(n.source, nil, extraFilter, false)
			if err != nil {
				return plan, extraFilter, err
			}
			n.source = newPlan
		} else if n.source, err = p.optimizeFilters(n.source); err != nil {
			return plan, extraFilter, err
		}

		// Fallthrough: wrap or spill.

	case *unionNode:
		// Filtering is distributive over set operations.
		// Source filtering reduces the amount of work, so force propagation.
		newLeft, remainderLeft, err := p.addFilter(n.left, nil, extraFilter, true)
		if err != nil {
			return plan, extraFilter, err
		}
		if len(remainderLeft) > 0 {
			panic(fmt.Sprintf("left union node\n%q\nrefused wrapping %s from %s",
				planToString(n.left), remainderLeft, extraFilter))
		}

		newRight, remainderRight, err := p.addFilter(n.right, nil, extraFilter, true)
		if err != nil {
			return plan, extraFilter, err
		}
		if len(remainderRight) > 0 {
			panic(fmt.Sprintf("right union node\n%q\nrefused wrapping %s from %s",
				planToString(n.right), remainderRight, extraFilter))
		}

		n.left = newLeft
		n.right = newRight
		return plan, nil, nil

	case *groupNode:
		if n.plan, err = p.optimizeFilters(n.plan); err != nil {
			return plan, extraFilter, err
		}

	case *limitNode:
		if n.plan, err = p.optimizeFilters(n.plan); err != nil {
			return plan, extraFilter, err
		}

	case *windowNode:
		if n.plan, err = p.optimizeFilters(n.plan); err != nil {
			return plan, extraFilter, err
		}

	case *ordinalityNode:
		if n.source, err = p.optimizeFilters(n.source); err != nil {
			return plan, extraFilter, err
		}

	case *createTableNode:
		if n.n.As() {
			if n.sourcePlan, err = p.optimizeFilters(n.sourcePlan); err != nil {
				return plan, extraFilter, err
			}
		}

	case *createViewNode:
		if n.sourcePlan, err = p.optimizeFilters(n.sourcePlan); err != nil {
			return plan, extraFilter, err
		}

	case *deleteNode:
		if n.run.rows, err = p.optimizeFilters(n.run.rows); err != nil {
			return plan, extraFilter, err
		}

	case *insertNode:
		if n.run.rows, err = p.optimizeFilters(n.run.rows); err != nil {
			return plan, extraFilter, err
		}

	case *updateNode:
		if n.run.rows, err = p.optimizeFilters(n.run.rows); err != nil {
			return plan, extraFilter, err
		}

	case *explainDebugNode:
		if n.plan, err = p.optimizeFilters(n.plan); err != nil {
			return plan, extraFilter, err
		}

	case *explainPlanNode:
		if n.optimized {
			if n.plan, err = p.optimizeFilters(n.plan); err != nil {
				return plan, extraFilter, err
			}
		}

	case *explainTraceNode:
		if n.plan, err = p.optimizeFilters(n.plan); err != nil {
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

	if autoWrap {
		// Note: wrapFilter() is a no-op if remainingFilter is empty.
		return p.wrapFilter(plan, info, remainingFilter), nil, nil
	}
	return plan, remainingFilter, nil
}

// addSelectFilter attempts to add the extraFilter to the selectNode.
// The filter is only propagated to the sub-plan if it is expressed
// using renders that are either simple datums or simple column
// references to the source.
func (p *planner) addSelectFilter(s *selectNode, extraFilter conjExpr) (planNode, conjExpr, error) {
	// outerFilter is the filter expressed using values rendered by this selectNode.
	var outerFilter conjExpr
	// innerFilter is the filter on the source planNode.
	var innerFilter conjExpr

	if extraFilter != nil {
		// The filter that's being added refers to the rendered
		// expressions, not the select's source node.

		// We want to propagate only those filters that use simple renders
		// of columns from the source.
		//
		// To do this, we apply the following convFunc to all the filter
		// expressions. This replaces IndexedVars in the filter that are
		// relative to this selectNode's render list by IndexedVars that
		// are relative to the selectNode's source columns. It also sets
		// complexRenderEncountered to true if any of the IndexedVars
		// refers to non-trivial expressions.
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
	newPlan, remainingInner, err := s.planner.addFilter(
		s.source.plan, s.source.info, innerFilter, false)
	if err != nil {
		return s, extraFilter, err
	}
	if len(remainingInner) > 0 {
		panic(fmt.Sprintf("select node\n%s\nrefused wrapping %s from %s",
			planToString(s), remainingInner, innerFilter))
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
		if n.left.plan, err = p.optimizeFilters(n.left.plan); err == nil {
			n.right.plan, err = p.optimizeFilters(n.right.plan)
		}
		return n, extraFilter, err
	}

	mergedBoundary := n.pred.numMergedEqualityColumns
	sourceBoundary := mergedBoundary + len(n.left.info.sourceColumns)

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
		f.expr = n.pred.iVarHelper.Rebind(f.expr)
		exprCheckVars(f.expr, analyzeFunc)
		if hasLeft && !hasRight {
			leftExpr = append(leftExpr, f)
		} else if hasRight && !hasLeft {
			rightExpr = append(rightExpr, f)
		} else if hasRight && hasLeft {
			combinedExpr = append(combinedExpr, f)
		} else {
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
		newPlan, remainingFilter, err := p.addFilter(side.plan, side.info, pred, true)
		if err != nil {
			return err
		}
		if len(remainingFilter) > 0 {
			panic(fmt.Sprintf("join operand\n%s\nrefused wrapping %s from %s", planToString(side.plan), remainingFilter, pred))
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
	n.pred.iVarHelper.Reset()
	n.pred.onCond = n.pred.iVarHelper.Rebind(conjToExpr(newOnCond))

	return n, nil, nil
}

// wrapFilter adds a filterNode around the given data source.
func (p *planner) wrapFilter(plan planNode, info *dataSourceInfo, filter conjExpr) planNode {
	if len(filter) == 0 {
		return plan
	}
	if info == nil {
		info = newSourceInfoForSingleTable(anonymousTable, plan.Columns())
	}
	f := &filterNode{
		p:      p,
		source: planDataSource{plan: plan, info: info},
	}
	f.ivarHelper = parser.MakeIndexedVarHelper(f, len(info.sourceColumns))
	f.filter = f.ivarHelper.Rebind(conjToExpr(filter))
	return f
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
