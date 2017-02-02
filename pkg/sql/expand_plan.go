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
	params := expandParameters{p: p}
	return doExpandPlan(params.clear(), plan)
}

// expandParameters propagates the known row limit and desired ordering at
// a given level to the levels under it (upstream).
type expandParameters struct {
	p               *planner
	numRowsHint     int64
	desiredOrdering sqlbase.ColumnOrdering
}

func (p expandParameters) clear() expandParameters {
	p.numRowsHint = math.MaxInt64
	p.desiredOrdering = nil
	return p
}

// doExpandPlan is the algorithm that supports expandPlan().
func doExpandPlan(params expandParameters, plan planNode) (planNode, error) {
	if plan == nil {
		return nil, nil
	}

	var err error
	switch n := plan.(type) {
	case *createTableNode:
		n.sourcePlan, err = doExpandPlan(params.clear(), n.sourcePlan)

	case *updateNode:
		n.run.rows, err = doExpandPlan(params.clear(), n.run.rows)

	case *insertNode:
		n.run.rows, err = doExpandPlan(params.clear(), n.run.rows)

	case *deleteNode:
		n.run.rows, err = doExpandPlan(params.clear(), n.run.rows)

	case *createViewNode:
		n.sourcePlan, err = doExpandPlan(params.clear(), n.sourcePlan)

	case *explainDebugNode:
		n.plan, err = doExpandPlan(params.clear(), n.plan)
		if err != nil {
			return plan, err
		}
		n.plan.MarkDebug(explainDebug)

	case *explainTraceNode:
		n.plan, err = doExpandPlan(params.clear(), n.plan)
		if err != nil {
			return plan, err
		}
		n.plan.MarkDebug(explainDebug)

	case *explainPlanNode:
		if n.expanded {
			n.plan, err = doExpandPlan(params.clear(), n.plan)
			if err != nil {
				return plan, err
			}
			// Trigger limit hint propagation, which would otherwise only occur
			// during the plan's Start() phase. This may trigger additional
			// optimizations (eg. in sortNode) which the user of EXPLAIN will be
			// interested in.
			setUnlimited(n.plan)
		}

	case *indexJoinNode:
		// We ignore the return value because we know the scanNode is preserved.
		_, err = doExpandPlan(params, n.index)
		if err != nil {
			return plan, err
		}

		// The row limit and desired ordering, if any, only propagates on
		// the index side.
		_, err = doExpandPlan(params.clear(), n.table)

	case *unionNode:
		n.right, err = doExpandPlan(params, n.right)
		if err != nil {
			return plan, err
		}
		n.left, err = doExpandPlan(params, n.left)

	case *filterNode:
		n.source.plan, err = doExpandPlan(params, n.source.plan)

	case *joinNode:
		params = params.clear()
		n.left.plan, err = doExpandPlan(params, n.left.plan)
		if err != nil {
			return plan, err
		}
		n.right.plan, err = doExpandPlan(params, n.right.plan)

	case *ordinalityNode:
		// If there's a desired ordering on the ordinality column, drop it.
		if len(params.desiredOrdering) > 0 {
			newDesired := make(sqlbase.ColumnOrdering, 0, len(params.desiredOrdering))
			for _, ordInfo := range params.desiredOrdering {
				if ordInfo.ColIdx == len(n.columns)-1 {
					break
				}
				newDesired = append(newDesired, ordInfo)
			}
			params.desiredOrdering = newDesired
		}
		n.source, err = doExpandPlan(params, n.source)
		if err != nil {
			return plan, err
		}

		// We are going to "optimize" the ordering. We had an ordering
		// initially from the source, but expand() may have caused it to
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

	case *limitNode:
		// Estimate the limit parameters. We can't full eval them just yet,
		// because evaluation requires running potential sub-queries, which
		// cannot occur during expand.
		n.estimateLimit()
		params.numRowsHint = getLimit(n.count, n.offset)
		n.plan, err = doExpandPlan(params, n.plan)

	case *groupNode:
		params.desiredOrdering = n.desiredOrdering
		// Under a group node, there may be arbitrarily more rows
		// than those required by the context.
		params.numRowsHint = math.MaxInt64
		n.plan, err = doExpandPlan(params, n.plan)

	case *windowNode:
		// Under a window node, there may be arbitrarily more rows
		// than those required by the context.
		params.numRowsHint = math.MaxInt64
		n.plan, err = doExpandPlan(params, n.plan)

	case *sortNode:
		params.desiredOrdering = n.ordering
		n.plan, err = doExpandPlan(params, n.plan)
		if err != nil {
			return plan, err
		}

		// Check to see if the requested ordering is compatible with the existing
		// ordering.
		existingOrdering := n.plan.Ordering()
		match := computeOrderingMatch(n.ordering, existingOrdering, false)
		if match < len(n.ordering) {
			n.needSort = true
		} else if len(n.columns) < len(n.plan.Columns()) {
			// No sorting required, but we have to strip off the extra render
			// expressions we added. So keep the sort node.
		} else {
			// Sort node fully disappears.
			plan = n.plan
		}

	case *distinctNode:
		// TODO(radu/knz) perhaps we can propagate the DISTINCT
		// clause as desired ordering/exact match for the source node.
		n.plan, err = doExpandPlan(params, n.plan)
		if err != nil {
			return plan, err
		}

		ordering := n.plan.Ordering()
		if !ordering.isEmpty() {
			n.columnsInOrder = make([]bool, len(n.plan.Columns()))
			for colIdx := range ordering.exactMatchCols {
				if colIdx >= len(n.columnsInOrder) {
					// If the exact-match column is not part of the output, we can safely ignore it.
					continue
				}
				n.columnsInOrder[colIdx] = true
			}
			for _, c := range ordering.ordering {
				if c.ColIdx >= len(n.columnsInOrder) {
					// Cannot use sort order. This happens when the
					// columns used for sorting are not part of the output.
					// e.g. SELECT a FROM t ORDER BY c.
					n.columnsInOrder = nil
					break
				}
				n.columnsInOrder[c.ColIdx] = true
			}
		}

	case *scanNode:
		plan, err = expandScanNode(params, n)

	case *renderNode:
		plan, err = expandRenderNode(params, n)

	case *delayedNode:
		n.plan, err = n.constructor(params.p)
		if err == nil {
			n.plan, err = doExpandPlan(params, n.plan)
		}

	case *valuesNode:
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

func expandScanNode(params expandParameters, s *scanNode) (planNode, error) {
	var analyzeOrdering analyzeOrderingFn
	if len(params.desiredOrdering) > 0 {
		analyzeOrdering = func(indexOrdering orderingInfo) (matchingCols, totalCols int) {
			match := computeOrderingMatch(params.desiredOrdering, indexOrdering, false)
			return match, len(params.desiredOrdering)
		}
	}

	// If we have a reasonable limit, prefer an order matching index even if
	// it is not covering.
	var preferOrderMatchingIndex bool
	if len(params.desiredOrdering) > 0 && params.numRowsHint <= 1000 {
		preferOrderMatchingIndex = true
	}

	plan, err := params.p.selectIndex(s, analyzeOrdering, preferOrderMatchingIndex)
	if err != nil {
		return s, err
	}
	return plan, nil
}

func expandRenderNode(params expandParameters, r *renderNode) (planNode, error) {
	params.desiredOrdering = translateOrdering(params.desiredOrdering, r)

	var err error
	r.source.plan, err = doExpandPlan(params, r.source.plan)
	if err != nil {
		return r, err
	}

	// Elide the render node if it renders its source as-is.

	sourceCols := r.source.plan.Columns()
	if len(r.columns) == len(sourceCols) && r.source.info.viewDesc == nil {
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
		for i, e := range r.render {
			if r.columns[i].omitted {
				continue
			}
			if iv, ok := e.(*parser.IndexedVar); ok && i < len(sourceCols) &&
				(iv.Idx == i && sourceCols[i].Name == r.columns[i].Name) {
				continue
			}
			foundNonTrivialRender = true
			break
		}
		if !foundNonTrivialRender {
			// Nothing special rendered, remove the render node entirely.
			return r.source.plan, nil
		}
	}

	r.ordering = r.computeOrdering(r.source.plan.Ordering())

	return r, nil
}

// translateOrdering modifies a desired ordering on the output of the
// renderNode to a desired ordering on its its input.
//
// For example, it translates a desired ordering [@2 asc, @1 desc] for
// a render node that renders [@4, @3, @2] into a desired ordering [@3
// asc, @4 desc].
func translateOrdering(desiredDown sqlbase.ColumnOrdering, r *renderNode) sqlbase.ColumnOrdering {
	var desiredUp sqlbase.ColumnOrdering

	for _, colOrder := range desiredDown {
		rendered := r.render[colOrder.ColIdx]
		if _, ok := rendered.(parser.Datum); ok {
			// Simple constants do not participate in ordering. Just ignore.
			continue
		}
		if v, ok := rendered.(*parser.IndexedVar); ok {
			// This is a simple render, so we can propagate the desired ordering.
			// However take care of avoiding duplicate ordering requests in
			// case there is more than one render for the same source column.
			duplicate := false
			for _, desiredOrderCol := range desiredUp {
				if desiredOrderCol.ColIdx == v.Idx {
					duplicate = true
					break
				}
			}
			if !duplicate {
				desiredUp = append(desiredUp,
					sqlbase.ColumnOrderInfo{ColIdx: v.Idx, Direction: colOrder.Direction})
			}
			continue
		}
		// Anything else and we can't propagate the desired order.
		break
	}

	return desiredUp
}
