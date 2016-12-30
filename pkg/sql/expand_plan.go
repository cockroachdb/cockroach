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
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// expandPlan finalizes type checking of placeholders and expands
// the query plan to its final form, including index selection and
// expansion of sub-queries. Returns an error if the initialization
// fails.
func (p *planner) expandPlan(plan planNode) error {
	if plan == nil {
		return nil
	}

	switch n := plan.(type) {
	case *createTableNode:
		return p.expandPlan(n.sourcePlan)

	case *updateNode:
		// TODO(knz) eliminate this once #12599 is fixed.
		return n.run.expandEditNodePlan(&n.editNodeBase, &n.tw)

	case *insertNode:
		// TODO(knz) eliminate this once #12599 is fixed.
		return n.run.expandEditNodePlan(&n.editNodeBase, n.tw)

	case *deleteNode:
		// TODO(knz) eliminate this once #12599 is fixed.
		return n.run.expandEditNodePlan(&n.editNodeBase, &n.tw)

	case *createViewNode:
		return p.expandPlan(n.sourcePlan)

	case *explainDebugNode:
		if err := p.expandPlan(n.plan); err != nil {
			return err
		}
		n.plan.MarkDebug(explainDebug)

	case *explainTraceNode:
		if err := p.expandPlan(n.plan); err != nil {
			return err
		}
		n.plan.MarkDebug(explainDebug)

	case *explainPlanNode:
		if n.expanded {
			if err := p.expandPlan(n.plan); err != nil {
				return err
			}
			// Trigger limit hint propagation, which would otherwise only occur
			// during the plan's Start() phase. This may trigger additional
			// optimizations (eg. in sortNode) which the user of EXPLAIN will be
			// interested in.
			n.plan.SetLimitHint(math.MaxInt64, true)
		}

	case *indexJoinNode:
		if err := p.expandPlan(n.table); err != nil {
			return err
		}
		return p.expandPlan(n.index)

	case *unionNode:
		if err := p.expandPlan(n.right); err != nil {
			return err
		}
		return p.expandPlan(n.left)

	case *joinNode:
		if err := p.expandPlan(n.left.plan); err != nil {
			return err
		}
		return p.expandPlan(n.right.plan)

	case *ordinalityNode:
		if err := p.expandPlan(n.source); err != nil {
			return err
		}

		// We are going to "optimize" the ordering. We had an ordering
		// initially from the source, but expandPlan() may have caused it to
		// change. So here retrieve the ordering of the source again.
		origOrdering := n.source.Ordering()

		if len(origOrdering.ordering) > 0 {
			// TODO(knz/radu) we basically have two simultaneous orderings.
			// What we really want is something that orderingInfo cannot
			// currently express: that the rows are ordered by a set of
			// columns AND at the same time they are also ordered by a
			// different set of columns. However since ordinalityNode is
			// currently the only case where this happens we consider it's not
			// worth the hassle and just use the source ordering.
			n.ordering = origOrdering
		} else {
			// No ordering defined in the source, so create a new one.
			n.ordering.exactMatchCols = origOrdering.exactMatchCols
			n.ordering.ordering = sqlbase.ColumnOrdering{
				sqlbase.ColumnOrderInfo{
					ColIdx:    len(n.columns) - 1,
					Direction: encoding.Ascending,
				},
			}
			n.ordering.unique = true
		}

	case *limitNode, *sortNode, *windowNode, *groupNode, *distinctNode:
		panic(fmt.Sprintf("expandPlan for %T must be handled by selectTopNode", plan))

	case *selectNode:
		return p.expandSelectNode(n)

	case *selectTopNode:
		return p.expandSelectTopNode(n)

	case *delayedNode:
		v, err := n.constructor(p)
		if err != nil {
			return err
		}
		n.plan = v
		return p.expandPlan(n.plan)

	case *valuesNode:
	case *scanNode:
	case *alterTableNode:
	case *copyNode:
	case *createDatabaseNode:
	case *createIndexNode:
	case *createUserNode:
	case *dropDatabaseNode:
	case *dropIndexNode:
	case *dropTableNode:
	case *dropViewNode:
	case *emptyNode:
	case *hookFnNode:
	case *splitNode:
	case *valueGenerator:

	default:
		panic(fmt.Sprintf("unhandled node type: %T", plan))
	}
	return nil
}

func (p *planner) expandSelectTopNode(n *selectTopNode) error {
	if n.plan != nil {
		// Plan is already expanded. No-op.
		return nil
	}

	if err := p.expandPlan(n.source); err != nil {
		return err
	}
	n.plan = n.source

	if n.group != nil {
		if len(n.group.desiredOrdering) > 0 {
			match := computeOrderingMatch(n.group.desiredOrdering, n.plan.Ordering(), false)
			if match == len(n.group.desiredOrdering) {
				// We have a single MIN/MAX function and the underlying plan's
				// ordering matches the function. We only need to retrieve one row.
				n.plan.SetLimitHint(1, false /* !soft */)
				n.group.needOnlyOneRow = true
			}
		}
		n.group.plan = n.plan
		n.plan = n.group
	}

	if n.window != nil {
		n.window.plan = n.plan
		n.plan = n.window
	}

	if n.sort != nil {
		// Check to see if the requested ordering is compatible with the existing
		// ordering.
		existingOrdering := n.plan.Ordering()
		match := computeOrderingMatch(n.sort.ordering, existingOrdering, false)
		if match < len(n.sort.ordering) {
			n.sort.needSort = true
			n.sort.plan = n.plan
			n.plan = n.sort
		} else if len(n.sort.columns) < len(n.plan.Columns()) {
			// No sorting required, but we have to strip off the extra render
			// expressions we added.
			n.sort.plan = n.plan
			n.plan = n.sort
		} else {
			// Sort node fully disappears.
			n.sort = nil
		}
	}

	if n.distinct != nil {
		ordering := n.plan.Ordering()
		if !ordering.isEmpty() {
			n.distinct.columnsInOrder = make([]bool, len(n.plan.Columns()))
			for colIdx := range ordering.exactMatchCols {
				if colIdx >= len(n.distinct.columnsInOrder) {
					// If the exact-match column is not part of the output, we can safely ignore it.
					continue
				}
				n.distinct.columnsInOrder[colIdx] = true
			}
			for _, c := range ordering.ordering {
				if c.ColIdx >= len(n.distinct.columnsInOrder) {
					// Cannot use sort order. This happens when the
					// columns used for sorting are not part of the output.
					// e.g. SELECT a FROM t ORDER BY c.
					n.distinct.columnsInOrder = nil
					break
				}
				n.distinct.columnsInOrder[c.ColIdx] = true
			}
		}
		n.distinct.plan = n.plan
		n.plan = n.distinct
	}

	if n.limit != nil {
		n.limit.plan = n.plan
		n.plan = n.limit
	}
	return nil
}

func (p *planner) expandSelectNode(s *selectNode) error {
	// Get the ordering for index selection (if any).
	var ordering sqlbase.ColumnOrdering
	var grouping bool

	if s.top.group != nil {
		ordering = s.top.group.desiredOrdering
		grouping = true
	} else if s.top.sort != nil {
		ordering = s.top.sort.ordering
	}

	// Estimate the limit parameters. We can't full eval them just yet,
	// because evaluation requires running potential sub-queries, which
	// cannot occur during expandPlan.
	limitCount, limitOffset := s.top.limit.estimateLimit()

	if scan, ok := s.source.plan.(*scanNode); ok {
		// Compute a filter expression for the scan node.
		convFunc := func(expr parser.VariableExpr) (bool, parser.VariableExpr) {
			ivar := expr.(*parser.IndexedVar)
			s.ivarHelper.AssertSameContainer(ivar)
			return true, scan.filterVars.IndexedVar(ivar.Idx)
		}

		scan.filter, s.filter = splitFilter(s.filter, convFunc)
		if s.filter != nil {
			// Right now we support only one table, so the entire expression
			// should be converted.
			panic(fmt.Sprintf("residual filter `%s` (scan filter `%s`)", s.filter, scan.filter))
		}

		var analyzeOrdering analyzeOrderingFn
		if ordering != nil {
			analyzeOrdering = func(indexOrdering orderingInfo) (matchingCols, totalCols int) {
				selOrder := s.computeOrdering(indexOrdering)
				return computeOrderingMatch(ordering, selOrder, false), len(ordering)
			}
		}

		// If we have a reasonable limit, prefer an order matching index even if
		// it is not covering - unless we are grouping, in which case the limit
		// applies to the grouping results and not to the rows we scan.
		var preferOrderMatchingIndex bool
		if !grouping && len(ordering) > 0 && limitCount <= 1000-limitOffset {
			preferOrderMatchingIndex = true
		}

		plan, err := selectIndex(scan, analyzeOrdering, preferOrderMatchingIndex)
		if err != nil {
			return err
		}

		// Update s.source.info with the new plan.
		s.source.plan = plan
	}

	// Expand the source node. We need to do this before computing the
	// ordering, since expansion may modify the ordering.
	if err := p.expandPlan(s.source.plan); err != nil {
		return err
	}

	s.ordering = s.computeOrdering(s.source.plan.Ordering())

	return nil
}
