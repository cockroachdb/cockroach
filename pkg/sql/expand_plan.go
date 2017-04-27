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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// expandPlan finalizes type checking of placeholders and expands
// the query plan to its final form, including index selection and
// expansion of sub-queries. Returns an error if the initialization
// fails.
func (p *planner) expandPlan(ctx context.Context, plan planNode) (planNode, error) {
	var err error
	plan, err = doExpandPlan(ctx, p, noParams, plan)
	if err != nil {
		return plan, err
	}
	plan = simplifyOrderings(plan, nil)
	return plan, nil
}

// expandParameters propagates the known row limit and desired ordering at
// a given level to the levels under it (upstream).
type expandParameters struct {
	numRowsHint     int64
	desiredOrdering sqlbase.ColumnOrdering
}

var noParams = expandParameters{numRowsHint: math.MaxInt64, desiredOrdering: nil}

// doExpandPlan is the algorithm that supports expandPlan().
func doExpandPlan(
	ctx context.Context, p *planner, params expandParameters, plan planNode,
) (planNode, error) {
	var err error
	switch n := plan.(type) {
	case *createTableNode:
		n.sourcePlan, err = doExpandPlan(ctx, p, noParams, n.sourcePlan)

	case *updateNode:
		n.run.rows, err = doExpandPlan(ctx, p, noParams, n.run.rows)

	case *insertNode:
		n.run.rows, err = doExpandPlan(ctx, p, noParams, n.run.rows)

	case *deleteNode:
		n.run.rows, err = doExpandPlan(ctx, p, noParams, n.run.rows)

	case *createViewNode:
		n.sourcePlan, err = doExpandPlan(ctx, p, noParams, n.sourcePlan)

	case *explainDebugNode:
		n.plan, err = doExpandPlan(ctx, p, noParams, n.plan)
		if err != nil {
			return plan, err
		}
		n.plan.MarkDebug(explainDebug)

	case *explainDistSQLNode:
		n.plan, err = doExpandPlan(ctx, p, noParams, n.plan)
		if err != nil {
			return plan, err
		}

	case *explainTraceNode:
		n.plan, err = doExpandPlan(ctx, p, noParams, n.plan)
		if err != nil {
			return plan, err
		}
		n.plan.MarkDebug(explainDebug)

	case *explainPlanNode:
		if n.expanded {
			n.plan, err = doExpandPlan(ctx, p, noParams, n.plan)
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
		_, err = doExpandPlan(ctx, p, params, n.index)
		if err != nil {
			return plan, err
		}

		// The row limit and desired ordering, if any, only propagates on
		// the index side.
		_, err = doExpandPlan(ctx, p, noParams, n.table)

	case *unionNode:
		n.right, err = doExpandPlan(ctx, p, params, n.right)
		if err != nil {
			return plan, err
		}
		n.left, err = doExpandPlan(ctx, p, params, n.left)

	case *filterNode:
		n.source.plan, err = doExpandPlan(ctx, p, params, n.source.plan)

	case *joinNode:
		n.left.plan, err = doExpandPlan(ctx, p, noParams, n.left.plan)
		if err != nil {
			return plan, err
		}
		n.right.plan, err = doExpandPlan(ctx, p, noParams, n.right.plan)

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
		n.source, err = doExpandPlan(ctx, p, params, n.source)
		if err != nil {
			return plan, err
		}

		// We are going to "optimize" the ordering. We had an ordering
		// initially from the source, but expand() may have caused it to
		// change. So here retrieve the ordering of the source again.
		origOrdering := n.source.Ordering()

		if len(origOrdering.ordering) > 0 {
			// TODO(knz/radu): we basically have two simultaneous orderings.
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
		n.plan, err = doExpandPlan(ctx, p, params, n.plan)

	case *groupNode:
		params.desiredOrdering = n.desiredOrdering
		// Under a group node, there may be arbitrarily more rows
		// than those required by the context.
		params.numRowsHint = math.MaxInt64
		n.plan, err = doExpandPlan(ctx, p, params, n.plan)

		if len(n.desiredOrdering) > 0 {
			match := n.plan.Ordering().computeMatch(n.desiredOrdering)
			if match == len(n.desiredOrdering) {
				// We have a single MIN/MAX function and the underlying plan's
				// ordering matches the function. We only need to retrieve one row.
				// See desiredAggregateOrdering.
				n.needOnlyOneRow = true
			}
		}

	case *windowNode:
		n.plan, err = doExpandPlan(ctx, p, noParams, n.plan)

	case *sortNode:
		if !n.ordering.IsPrefixOf(params.desiredOrdering) {
			params.desiredOrdering = n.ordering
		}
		n.plan, err = doExpandPlan(ctx, p, params, n.plan)
		if err != nil {
			return plan, err
		}

		// Check to see if the requested ordering is compatible with the existing
		// ordering.
		match := n.plan.Ordering().computeMatch(n.ordering)
		n.needSort = (match < len(n.ordering))

	case *distinctNode:
		// TODO(radu/knz): perhaps we can propagate the DISTINCT
		// clause as desired ordering/exact match for the source node.
		n.plan, err = doExpandPlan(ctx, p, params, n.plan)
		if err != nil {
			return plan, err
		}

		ordering := n.plan.Ordering()
		if !ordering.isEmpty() {
			n.columnsInOrder = make([]bool, len(n.plan.Columns()))
			for colIdx := range ordering.exactMatchCols {
				n.columnsInOrder[colIdx] = true
			}
			for _, c := range ordering.ordering {
				n.columnsInOrder[c.ColIdx] = true
			}
		}

	case *scanNode:
		plan, err = expandScanNode(ctx, p, params, n)

	case *renderNode:
		plan, err = expandRenderNode(ctx, p, params, n)

	case *delayedNode:
		n.plan, err = n.constructor(ctx, p)
		if err == nil {
			n.plan, err = doExpandPlan(ctx, p, params, n.plan)
		}

	case *splitNode:
		n.rows, err = doExpandPlan(ctx, p, noParams, n.rows)

	case *relocateNode:
		n.rows, err = doExpandPlan(ctx, p, noParams, n.rows)

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
	case *valueGenerator:
	case *showRangesNode:
	case *scatterNode:
	case nil:

	default:
		panic(fmt.Sprintf("unhandled node type: %T", plan))
	}
	return plan, err
}

func expandScanNode(
	ctx context.Context, p *planner, params expandParameters, s *scanNode,
) (planNode, error) {
	var analyzeOrdering analyzeOrderingFn
	if len(params.desiredOrdering) > 0 {
		analyzeOrdering = func(indexOrdering orderingInfo) (matchingCols, totalCols int) {
			match := indexOrdering.computeMatch(params.desiredOrdering)
			return match, len(params.desiredOrdering)
		}
	}

	// If we have a reasonable limit, prefer an order matching index even if
	// it is not covering.
	var preferOrderMatchingIndex bool
	if len(params.desiredOrdering) > 0 && params.numRowsHint <= 1000 {
		preferOrderMatchingIndex = true
	}

	plan, err := p.selectIndex(ctx, s, analyzeOrdering, preferOrderMatchingIndex)
	if err != nil {
		return s, err
	}
	return plan, nil
}

func expandRenderNode(
	ctx context.Context, p *planner, params expandParameters, r *renderNode,
) (planNode, error) {
	params.desiredOrdering = translateOrdering(params.desiredOrdering, r)

	var err error
	r.source.plan, err = doExpandPlan(ctx, p, params, r.source.plan)
	if err != nil {
		return r, err
	}

	// Elide the render node if it renders its source as-is.

	sourceCols := r.source.plan.Columns()
	if len(r.columns) == len(sourceCols) && r.source.info.viewDesc == nil {
		// 1) we don't drop renderNodes which also interface to a view, because
		// CREATE VIEW needs it.
		// TODO(knz): make this optimization conditional on a flag, which can
		// be set to false by CREATE VIEW.
		//
		// 2) we don't drop renderNodes which have a different number of
		// columns than their sources, because some nodes currently assume
		// the number of source columns doesn't change between
		// instantiation and Start() (e.g. groupNode).
		// TODO(knz): investigate this further and enable the optimization fully.

		foundNonTrivialRender := false
		for i, e := range r.render {
			if r.columns[i].Omitted {
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

// simplifyOrderings reduces the Ordering() guarantee of each node in the plan
// to that which is actually used by the parent(s). It also performs sortNode
// elision when possible.
//
// Simplification of orderings is useful for DistSQL, where maintaining
// orderings between parallel streams is not free.
//
// This determination cannot be done directly as part of the doExpandPlan
// recursion (using desiredOrdering) because some nodes (distinctNode) make use
// of whatever ordering the underlying node happens to provide.
func simplifyOrderings(plan planNode, usefulOrdering sqlbase.ColumnOrdering) planNode {
	if plan == nil {
		return nil
	}

	switch n := plan.(type) {
	case *createTableNode:
		n.sourcePlan = simplifyOrderings(n.sourcePlan, nil)

	case *updateNode:
		n.run.rows = simplifyOrderings(n.run.rows, nil)

	case *insertNode:
		n.run.rows = simplifyOrderings(n.run.rows, nil)

	case *deleteNode:
		n.run.rows = simplifyOrderings(n.run.rows, nil)

	case *createViewNode:
		n.sourcePlan = simplifyOrderings(n.sourcePlan, nil)

	case *explainDebugNode:
		n.plan = simplifyOrderings(n.plan, nil)

	case *explainDistSQLNode:
		n.plan = simplifyOrderings(n.plan, nil)

	case *explainTraceNode:
		n.plan = simplifyOrderings(n.plan, nil)

	case *explainPlanNode:
		if n.expanded {
			n.plan = simplifyOrderings(n.plan, nil)
		}

	case *indexJoinNode:
		n.index.ordering.trim(usefulOrdering)
		n.table.ordering = orderingInfo{}

	case *unionNode:
		n.right = simplifyOrderings(n.right, nil)
		n.left = simplifyOrderings(n.left, nil)

	case *filterNode:
		n.source.plan = simplifyOrderings(n.source.plan, usefulOrdering)

	case *joinNode:
		n.left.plan = simplifyOrderings(n.left.plan, nil)
		n.right.plan = simplifyOrderings(n.right.plan, nil)

	case *ordinalityNode:
		// The ordinality node either passes through the source ordering, or if
		// there is none it creates an ordering on the ordinality column (see the
		// corresponding code in doExpandPlan).
		// TODO(radu): better encapsulate this code in ordinalityNode (#13594).
		if len(n.ordering.ordering) == 1 && n.ordering.ordering[0].ColIdx == len(n.columns)-1 {
			n.source = simplifyOrderings(n.source, nil)
		} else {
			n.source = simplifyOrderings(n.source, n.ordering.ordering)
		}

	case *limitNode:
		n.plan = simplifyOrderings(n.plan, usefulOrdering)

	case *groupNode:
		if n.needOnlyOneRow {
			n.plan = simplifyOrderings(n.plan, n.desiredOrdering)
		} else {
			n.plan = simplifyOrderings(n.plan, nil)
		}

	case *windowNode:
		n.plan = simplifyOrderings(n.plan, nil)

	case *sortNode:
		if n.needSort {
			// We could pass no ordering below, but a partial ordering can speed up
			// the sort (and save memory), at least for DistSQL.
			n.plan = simplifyOrderings(n.plan, n.ordering)
		} else {
			exactMatchCols := n.plan.Ordering().exactMatchCols
			// Normally we would pass n.ordering; but n.ordering could be a prefix of
			// the useful ordering. Check for this, ignoring any exact match columns.
			sortOrder := make(sqlbase.ColumnOrdering, 0, len(n.ordering))
			for _, c := range n.ordering {
				if _, ok := exactMatchCols[c.ColIdx]; !ok {
					sortOrder = append(sortOrder, c)
				}
			}
			givenOrder := make(sqlbase.ColumnOrdering, 0, len(usefulOrdering))
			for _, c := range usefulOrdering {
				if _, ok := exactMatchCols[c.ColIdx]; !ok {
					givenOrder = append(givenOrder, c)
				}
			}
			if sortOrder.IsPrefixOf(givenOrder) {
				n.plan = simplifyOrderings(n.plan, givenOrder)
			} else {
				n.plan = simplifyOrderings(n.plan, sortOrder)
			}
		}

		if !n.needSort {
			if len(n.columns) < len(n.plan.Columns()) {
				// No sorting required, but we have to strip off the extra render
				// expressions we added. So keep the sort node.
				// TODO(radu): replace with a renderNode
			} else {
				// Sort node fully disappears.
				plan = n.plan
			}
		}

	case *distinctNode:
		// distinctNode uses whatever order the underlying node presents (regardless
		// of any ordering requirement on distinctNode itself).
		n.plan = simplifyOrderings(n.plan, n.plan.Ordering().ordering)

	case *scanNode:
		n.ordering.trim(usefulOrdering)

	case *renderNode:
		n.source.plan = simplifyOrderings(n.source.plan, translateOrdering(usefulOrdering, n))
		// Recompute r.ordering using the source's simplified ordering.
		// TODO(radu): in some cases there may be multiple possible n.orderings for
		// a given source plan ordering; we should pass usefulOrdering to help make
		// that choice (#13709).
		n.ordering = n.computeOrdering(n.source.plan.Ordering())

	case *delayedNode:
		n.plan = simplifyOrderings(n.plan, usefulOrdering)

	case *splitNode:
		n.rows = simplifyOrderings(n.rows, nil)

	case *relocateNode:
		n.rows = simplifyOrderings(n.rows, nil)

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
	case *valueGenerator:
	case *showRangesNode:
	case *scatterNode:

	default:
		panic(fmt.Sprintf("unhandled node type: %T", plan))
	}
	return plan
}
