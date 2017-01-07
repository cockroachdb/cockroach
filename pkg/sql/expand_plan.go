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
func (p *planner) expandPlan(plan planNode) (planNode, error) {
	if plan == nil {
		return nil, nil
	}

	var err error
	switch n := plan.(type) {
	case *createTableNode:
		n.sourcePlan, err = p.expandPlan(n.sourcePlan)

	case *updateNode:
		n.run.rows, err = p.expandPlan(n.run.rows)

	case *insertNode:
		n.run.rows, err = p.expandPlan(n.run.rows)

	case *deleteNode:
		n.run.rows, err = p.expandPlan(n.run.rows)

	case *createViewNode:
		n.sourcePlan, err = p.expandPlan(n.sourcePlan)

	case *explainDebugNode:
		n.plan, err = p.expandPlan(n.plan)
		if err != nil {
			return plan, err
		}
		n.plan.MarkDebug(explainDebug)

	case *explainTraceNode:
		n.plan, err = p.expandPlan(n.plan)
		if err != nil {
			return plan, err
		}
		n.plan.MarkDebug(explainDebug)

	case *explainPlanNode:
		if n.expanded {
			n.plan, err = p.expandPlan(n.plan)
			if err != nil {
				return plan, err
			}
			// Trigger limit hint propagation, which would otherwise only occur
			// during the plan's Start() phase. This may trigger additional
			// optimizations (eg. in sortNode) which the user of EXPLAIN will be
			// interested in.
			n.plan.SetLimitHint(math.MaxInt64, true)
		}

	case *indexJoinNode:
		// We ignore the return value because we know the scanNode is preserved.
		_, err = p.expandPlan(n.table)
		if err != nil {
			return plan, err
		}
		_, err = p.expandPlan(n.index)

	case *unionNode:
		n.right, err = p.expandPlan(n.right)
		if err != nil {
			return plan, err
		}
		n.left, err = p.expandPlan(n.left)

	case *filterNode:
		n.source.plan, err = p.expandPlan(n.source.plan)

	case *joinNode:
		n.left.plan, err = p.expandPlan(n.left.plan)
		if err != nil {
			return plan, err
		}
		n.right.plan, err = p.expandPlan(n.right.plan)

	case *ordinalityNode:
		n.source, err = p.expandPlan(n.source)
		if err != nil {
			return plan, err
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

	case *renderNode:
		plan, err = p.expandSelectNode(n)

	case *selectTopNode:
		plan, err = p.expandSelectTopNode(n)

	case *delayedNode:
		v, err := n.constructor(p)
		if err != nil {
			return plan, err
		}
		n.plan = v
		n.plan, err = p.expandPlan(n.plan)

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
	return plan, err
}

func (p *planner) expandSelectTopNode(n *selectTopNode) (planNode, error) {
	if n.plan != nil {
		// Plan is already expanded. Just elide.
		return n.plan, nil
	}

	var err error
	n.source, err = p.expandPlan(n.source)
	if err != nil {
		return n, err
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

	// Everything's expanded, so elide the remaining selectTopNode.
	return n.plan, nil
}

func (p *planner) expandSelectNode(s *renderNode) (planNode, error) {
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

	maybeScanNode := s.source.plan
	var whereFilter parser.TypedExpr
	where, ok := maybeScanNode.(*filterNode)
	if ok {
		whereFilter = where.filter
		maybeScanNode = where.source.plan
	}

	if scan, ok := maybeScanNode.(*scanNode); ok {
		if whereFilter != nil {
			// Migrate the filter to the scan node.
			// (Will be done soon by filter propagation.)
			scan.filter = scan.filterVars.Rebind(whereFilter)
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
			return s, err
		}

		// Update s.source.info with the new plan. This removes the
		// filterNode, if any.
		s.source.plan = plan
	}

	// Expand the source node. We need to do this before computing the
	// ordering, since expansion may modify the ordering.
	var err error
	s.source.plan, err = p.expandPlan(s.source.plan)
	if err != nil {
		return s, err
	}

	// Elide the render node if it renders its source as-is.

	sourceCols := s.source.plan.Columns()
	if len(s.columns) == len(sourceCols) && s.source.info.viewDesc == nil {
		// 1) we don't drop renderNodes which also interface to a view, because
		// CREATE VIEW needs it.
		// TODO(knz) make this optimization conditional on a flag, which can
		// be set to false by CREATE VIEW.
		//
		// 2) we don't drop renderNodes which have a different number of
		// columns than their sources, because some nodes currently assume
		// the number of source columns doesn't change between
		// instantiation and Start() (e.g. groupNode).
		// TODO(knz) investigate this further and enable the optimization fully.

		foundNonTrivialRender := false
		for i, e := range s.render {
			if s.columns[i].omitted {
				continue
			}
			if iv, ok := e.(*parser.IndexedVar); ok && i < len(sourceCols) &&
				(iv.Idx == i && sourceCols[i].Name == s.columns[i].Name) {
				continue
			}
			foundNonTrivialRender = true
			break
		}
		if !foundNonTrivialRender {
			// Nothing special rendered, remove the render node entirely.
			return s.source.plan, nil
		}
	}

	s.ordering = s.computeOrdering(s.source.plan.Ordering())

	return s, nil
}
