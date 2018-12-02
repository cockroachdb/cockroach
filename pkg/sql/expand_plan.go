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
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// expandPlan finalizes type checking of placeholders and expands
// the query plan to its final form, including index selection and
// expansion of sub-queries. Returns an error if the initialization
// fails.
func (p *planner) expandPlan(ctx context.Context, plan planNode) (planNode, error) {
	var err error
	topParams := noParamsBase
	topParams.atTop = true
	plan, err = doExpandPlan(ctx, p, topParams, plan)
	if err != nil {
		return plan, err
	}
	plan = p.simplifyOrderings(plan, nil)

	if p.autoCommit {
		if ac, ok := plan.(autoCommitNode); ok {
			ac.enableAutoCommit()
		}
	}

	return plan, nil
}

// expandParameters propagates the known row limit and desired ordering at
// a given level to the levels under it (upstream).
type expandParameters struct {
	numRowsHint     int64
	desiredOrdering sqlbase.ColumnOrdering

	// spooledResults is set to true if one of the parents of the
	// current plan either already provides spooling (e.g. upsertNode)
	// or has required spooling (which means doExpandPlan will
	// eventually add a spool). This is used to elide the insertion of a
	// spool.
	spooledResults bool

	// atTop is set to true on the top-level call to doExpandPlan. Further
	// recursive call set it to false. Used to elide the insertion of a spool
	// for top-level nodes.
	atTop bool
}

var noParamsBase = expandParameters{numRowsHint: math.MaxInt64, desiredOrdering: nil}

// doExpandPlan is the algorithm that supports expandPlan().
func doExpandPlan(
	ctx context.Context, p *planner, params expandParameters, plan planNode,
) (planNode, error) {
	// atTop remembers we're at the top level.
	atTop := params.atTop

	// needSpool will indicate at the end of the recursion whether
	// a new spool stage is needed.
	needSpool := false

	// Determine what to do.
	if _, ok := plan.(planNodeRequireSpool); ok {
		// parentSpooled indicates that a parent node has already
		// established the results will be spooled (i.e. accumulated at the
		// start of execution).
		parentSpooled := params.spooledResults

		// At the top level, we ignore the spool requirement. If a parent
		// is already spooled, we don't need to add a spool.
		if !params.atTop && !parentSpooled {
			// If the node requires a spool but we are already spooled, we
			// won't need a new spool.
			needSpool = true
			// Although we're not spooled yet, needSpool will ensure we
			// become spooled. Tell this to the children nodes.
			params.spooledResults = true
		}
	} else if _, ok := plan.(planNodeSpooled); ok {
		// Propagate this knowledge to the children nodes.
		params.spooledResults = true
	}
	params.atTop = false
	// Every recursion using noParams still wants to know about the
	// current spooling status.
	noParams := noParamsBase
	noParams.spooledResults = params.spooledResults

	var err error
	switch n := plan.(type) {
	case *createTableNode:
		n.sourcePlan, err = doExpandPlan(ctx, p, noParams, n.sourcePlan)

	case *updateNode:
		n.source, err = doExpandPlan(ctx, p, noParams, n.source)

	case *insertNode:
		n.source, err = doExpandPlan(ctx, p, noParams, n.source)

	case *upsertNode:
		n.source, err = doExpandPlan(ctx, p, noParams, n.source)

	case *deleteNode:
		// If the source of the delete is a scan node (optionally with a render on
		// top), mark it as such. Note that this parallels the logic in
		// canDeleteFast.
		maybeScan := n.source
		if sel, ok := maybeScan.(*renderNode); ok {
			maybeScan = sel.source.plan
		}
		scan, ok := maybeScan.(*scanNode)
		if ok {
			scan.isDeleteSource = true
		}

		n.source, err = doExpandPlan(ctx, p, noParams, n.source)

	case *rowCountNode:
		var newPlan planNode
		newPlan, err = doExpandPlan(ctx, p, noParams, n.source)
		n.source = newPlan.(batchedPlanNode)

	case *serializeNode:
		var newPlan planNode
		newPlan, err = doExpandPlan(ctx, p, noParams, n.source)
		n.source = newPlan.(batchedPlanNode)

	case *explainDistSQLNode:
		// EXPLAIN only shows the structure of the plan, and wants to do
		// so "as if" plan was at the top level w.r.t spool semantics.
		explainParams := noParamsBase
		explainParams.atTop = true
		n.plan, err = doExpandPlan(ctx, p, explainParams, n.plan)

	case *showTraceReplicaNode:
		n.plan, err = doExpandPlan(ctx, p, noParams, n.plan)

	case *explainPlanNode:
		// EXPLAIN only shows the structure of the plan, and wants to do
		// so "as if" plan was at the top level w.r.t spool semantics.
		explainParams := noParamsBase
		explainParams.atTop = true
		if n.expanded {
			n.plan, err = doExpandPlan(ctx, p, explainParams, n.plan)
			if err != nil {
				return plan, err
			}
			// Trigger limit hint propagation, which would otherwise only occur
			// during the plan's Start() phase. This may trigger additional
			// optimizations (eg. in sortNode) which the user of EXPLAIN will be
			// interested in.
			p.setUnlimited(n.plan)
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
		plan, err = expandFilterNode(ctx, p, params, n)

	case *joinNode:
		n.left.plan, err = doExpandPlan(ctx, p, noParams, n.left.plan)
		if err != nil {
			return plan, err
		}
		n.right.plan, err = doExpandPlan(ctx, p, noParams, n.right.plan)
		if err != nil {
			return plan, err
		}

		n.mergeJoinOrdering = computeMergeJoinOrdering(
			planPhysicalProps(n.left.plan),
			planPhysicalProps(n.right.plan),
			n.pred.leftEqualityIndices,
			n.pred.rightEqualityIndices,
		)
		n.props = n.joinOrdering()

	case *ordinalityNode:
		// There may be too many columns in the required ordering. Filter them.
		params.desiredOrdering = n.restrictOrdering(params.desiredOrdering)

		n.source, err = doExpandPlan(ctx, p, params, n.source)
		if err != nil {
			return plan, err
		}

		// The source ordering may have been updated. Update the
		// ordinality ordering accordingly.
		n.optimizeOrdering()

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
			match := planPhysicalProps(n.plan).computeMatch(n.desiredOrdering)
			if match == len(n.desiredOrdering) {
				// We have a single MIN/MAX function and the underlying plan's
				// ordering matches the function. We only need to retrieve one row.
				// See desiredAggregateOrdering.
				n.needOnlyOneRow = true
			}
		}

		// Project the props of the GROUP BY columns, as they're retained as-is.
		groupColProjMap := make([]int, len(n.funcs))
		for i := range n.funcs {
			if groupingCol, ok := n.aggIsGroupingColumn(i); ok {
				groupColProjMap[i] = groupingCol
			} else {
				groupColProjMap[i] = -1
			}
		}
		childProps := planPhysicalProps(n.plan)
		n.props = childProps.project(groupColProjMap)

		// The GROUP BY columns form a weak key.
		var groupColSet util.FastIntSet
		for i, c := range groupColProjMap {
			if c == -1 {
				continue
			}
			groupColSet.Add(i)
		}
		if !groupColSet.Empty() {
			n.props.addWeakKey(groupColSet)
		}

		groupColProps := planPhysicalProps(n.plan)
		groupColProps = groupColProps.project(n.groupCols)
		n.orderedGroupCols = make([]int, len(groupColProps.ordering))
		for i, o := range groupColProps.ordering {
			n.orderedGroupCols[i] = o.ColIdx
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

		if s, ok := n.plan.(*sortNode); ok {
			// (... ORDER BY x) ORDER BY y -> keep the outer sort
			elideDoubleSort(n, s)
		}

		// Check to see if the requested ordering is compatible with the existing
		// ordering.
		match := planPhysicalProps(n.plan).computeMatch(n.ordering)
		n.needSort = (match < len(n.ordering))

	case *distinctNode:
		plan, err = expandDistinctNode(ctx, p, params, n)

	case *scanNode:
		plan, err = expandScanNode(ctx, p, params, n)

	case *renderNode:
		plan, err = expandRenderNode(ctx, p, params, n)

	case *delayedNode:
		var newPlan planNode
		newPlan, err = n.constructor(ctx, p)
		if err != nil {
			return plan, err
		}
		newPlan, err = doExpandPlan(ctx, p, params, newPlan)
		if err != nil {
			return plan, err
		}
		plan = newPlan

	case *splitNode:
		n.rows, err = doExpandPlan(ctx, p, noParams, n.rows)

	case *relocateNode:
		n.rows, err = doExpandPlan(ctx, p, noParams, n.rows)

	case *cancelQueriesNode:
		n.rows, err = doExpandPlan(ctx, p, noParams, n.rows)

	case *cancelSessionsNode:
		n.rows, err = doExpandPlan(ctx, p, noParams, n.rows)

	case *controlJobsNode:
		n.rows, err = doExpandPlan(ctx, p, noParams, n.rows)

	case *projectSetNode:
		n.source, err = doExpandPlan(ctx, p, noParams, n.source)

	case *valuesNode:
	case *virtualTableNode:
	case *alterIndexNode:
	case *alterTableNode:
	case *alterSequenceNode:
	case *alterUserSetPasswordNode:
	case *commentOnTableNode:
	case *renameColumnNode:
	case *renameDatabaseNode:
	case *renameIndexNode:
	case *renameTableNode:
	case *scrubNode:
	case *truncateNode:
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
	case *zeroNode:
	case *unaryNode:
	case *hookFnNode:
		for i := range n.subplans {
			n.subplans[i], err = doExpandPlan(ctx, p, noParams, n.subplans[i])
			if err != nil {
				break
			}
		}
	case *sequenceSelectNode:
	case *setVarNode:
	case *setClusterSettingNode:
	case *setZoneConfigNode:
	case *showZoneConfigNode:
	case *showRangesNode:
	case *showFingerprintsNode:
	case *showTraceNode:
	case *scatterNode:
	case nil:

	default:
		panic(fmt.Sprintf("unhandled node type: %T", plan))
	}

	if atTop || needSpool {
		// Peel whatever spooling layers we have added prior to some elision above.
		for {
			if s, ok := plan.(*spoolNode); ok {
				plan = s.source
			} else {
				break
			}
		}
	}
	// If we need a spool, add it now.
	if needSpool {
		// The parent of this node does not provide spooling yet, but
		// spooling is required. Add it.
		plan = p.makeSpool(plan)
	}

	return plan, err
}

// elideDoubleSort removes the source sortNode because it is
// redundant.
func elideDoubleSort(parent, source *sortNode) {
	parent.plan = source.plan
	// Propagate renamed columns
	mutSourceCols := planMutableColumns(parent.plan)
	for i, col := range parent.columns {
		mutSourceCols[i].Name = col.Name
	}
}

func expandFilterNode(
	ctx context.Context, p *planner, params expandParameters, n *filterNode,
) (planNode, error) {
	var err error
	n.source.plan, err = doExpandPlan(ctx, p, params, n.source.plan)
	if err != nil {
		return n, err
	}

	// If there's a spool, pull it up.
	if spool, ok := n.source.plan.(*spoolNode); ok {
		n.source.plan = spool.source
		return p.makeSpool(n), nil
	}

	return n, nil
}

func expandDistinctNode(
	ctx context.Context, p *planner, params expandParameters, d *distinctNode,
) (planNode, error) {
	// TODO(radu/knz): perhaps we can propagate the DISTINCT
	// clause as desired ordering for the source node.
	var err error
	d.plan, err = doExpandPlan(ctx, p, params, d.plan)
	if err != nil {
		return d, err
	}

	// If there's a spool, we'll pull it up before returning below.
	respool := func(plan planNode) planNode { return plan }
	if spool, ok := d.plan.(*spoolNode); ok {
		respool = p.makeSpool
		d.plan = spool.source
	}

	// We use the physical properties of the distinctNode but projected
	// to the OnExprs (since the other columns are irrelevant to the
	// bookkeeping below).
	distinctOnPp := d.projectChildPropsToOnExprs()

	for _, k := range distinctOnPp.weakKeys {
		// If there is a strong key on the DISTINCT ON columns, then we
		// can elide the distinct node.
		// Since distinctNode does not project columns, this is fine
		// (it has a parent renderNode).
		if k.SubsetOf(distinctOnPp.notNullCols) {
			return respool(d.plan), nil
		}
	}

	if !distinctOnPp.isEmpty() {
		// distinctNode uses ordering to optimize "distinctification".
		// If the columns are sorted in a certain direction and the column
		// values "change", no subsequent rows can possibly have the same
		// column values again. We can thus clear out our bookkeeping.
		// This needs to be planColumns(n.plan) and not planColumns(n) since
		// distinctNode is "distinctifying" on the child plan's output rows.
		d.columnsInOrder = util.FastIntSet{}
		for i, numCols := 0, len(planColumns(d.plan)); i < numCols; i++ {
			group := distinctOnPp.eqGroups.Find(i)
			if distinctOnPp.constantCols.Contains(group) {
				d.columnsInOrder.Add(i)
				continue
			}
			for _, g := range distinctOnPp.ordering {
				if g.ColIdx == group {
					d.columnsInOrder.Add(i)
					break
				}
			}
		}
	}

	return respool(d), nil
}

func expandScanNode(
	ctx context.Context, p *planner, params expandParameters, s *scanNode,
) (planNode, error) {
	var analyzeOrdering analyzeOrderingFn
	if len(params.desiredOrdering) > 0 {
		analyzeOrdering = func(indexProps physicalProps) (matchingCols, totalCols int) {
			match := indexProps.computeMatch(params.desiredOrdering)
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

	// If there's a spool, we'll pull it up before returning below.
	respool := func(plan planNode) planNode { return plan }
	if spool, ok := r.source.plan.(*spoolNode); ok {
		respool = p.makeSpool
		r.source.plan = spool.source
	}

	// Elide the render node if it renders its source as-is.
	sourceCols := planColumns(r.source.plan)
	if len(r.columns) == len(sourceCols) {
		// We don't drop renderNodes which have a different number of
		// columns than their sources, because some nodes currently assume
		// the number of source columns doesn't change between
		// instantiation and Start() (e.g. groupNode).
		// TODO(knz): investigate this further and enable the optimization fully.
		// TODO(radu): once this is investigated, we should look into coalescing
		// renderNodes (at least if the parent node is just a projection).

		needRename := false
		foundNonTrivialRender := false
		for i, e := range r.render {
			if r.columns[i].Omitted {
				continue
			}
			if iv, ok := e.(*tree.IndexedVar); ok && i < len(sourceCols) && iv.Idx == i {
				if sourceCols[i].Name != r.columns[i].Name {
					// Pass-through with rename: SELECT k AS x, v AS y FROM kv ...
					// We'll want to push the demanded names "x" and "y" to the
					// source.
					needRename = true
				}
				continue
			}
			foundNonTrivialRender = true
			break
		}
		if !foundNonTrivialRender {
			// Nothing special rendered, remove the render node entirely.
			if needRename {
				// If the render was renaming some columns, propagate the
				// requested names.
				mutSourceCols := planMutableColumns(r.source.plan)
				for i, col := range r.columns {
					mutSourceCols[i].Name = col.Name
				}
			}
			return respool(r.source.plan), nil
		}
	}

	p.computePhysicalPropsForRender(r, planPhysicalProps(r.source.plan))
	return respool(r), nil
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
		if _, ok := rendered.(tree.Datum); ok {
			// Simple constants do not participate in ordering. Just ignore.
			continue
		}
		if v, ok := rendered.(*tree.IndexedVar); ok {
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

func translateGroupOrdering(
	desiredDown sqlbase.ColumnOrdering, g *groupNode,
) sqlbase.ColumnOrdering {
	var desiredUp sqlbase.ColumnOrdering

	for _, colOrder := range desiredDown {
		groupingCol, ok := g.aggIsGroupingColumn(colOrder.ColIdx)
		if !ok {
			// We cannot maintain the rest of the ordering since it uses a
			// non-identity aggregate function.
			break
		}
		// For identity (i.e., GROUP BY) columns, we can propagate the ordering.
		desiredUp = append(desiredUp, sqlbase.ColumnOrderInfo{
			ColIdx: groupingCol, Direction: colOrder.Direction,
		})
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
func (p *planner) simplifyOrderings(plan planNode, usefulOrdering sqlbase.ColumnOrdering) planNode {
	if plan == nil {
		return nil
	}

	switch n := plan.(type) {
	case *createTableNode:
		n.sourcePlan = p.simplifyOrderings(n.sourcePlan, nil)

	case *updateNode:
		n.source = p.simplifyOrderings(n.source, nil)

	case *insertNode:
		n.source = p.simplifyOrderings(n.source, nil)

	case *upsertNode:
		n.source = p.simplifyOrderings(n.source, nil)

	case *deleteNode:
		n.source = p.simplifyOrderings(n.source, nil)

	case *rowCountNode:
		n.source = p.simplifyOrderings(n.source, nil).(batchedPlanNode)

	case *serializeNode:
		n.source = p.simplifyOrderings(n.source, nil).(batchedPlanNode)

	case *explainDistSQLNode:
		n.plan = p.simplifyOrderings(n.plan, nil)

	case *showTraceReplicaNode:
		n.plan = p.simplifyOrderings(n.plan, nil)

	case *explainPlanNode:
		if n.expanded {
			n.plan = p.simplifyOrderings(n.plan, nil)
		}

	case *projectSetNode:
		// We propagate down any ordering constraint relative to the
		// source. We don't propagate orderings expressed over the SRF
		// results.
		var desiredUp sqlbase.ColumnOrdering
		for _, colOrder := range usefulOrdering {
			if colOrder.ColIdx >= n.numColsInSource {
				break
			}
			desiredUp = append(desiredUp, colOrder)
		}
		n.source = p.simplifyOrderings(n.source, desiredUp)
		n.computePhysicalProps()

	case *indexJoinNode:
		// Passing through usefulOrdering here is fine because indexJoinNodes
		// produced by the heuristic planner always have the same schema as the
		// underlying table.
		n.index.props.trim(usefulOrdering)
		n.props.trim(usefulOrdering)
		n.table.props = physicalProps{}

	case *unionNode:
		n.right = p.simplifyOrderings(n.right, nil)
		n.left = p.simplifyOrderings(n.left, nil)

	case *filterNode:
		n.source.plan = p.simplifyOrderings(n.source.plan, usefulOrdering)
		n.computePhysicalProps(p.EvalContext())

	case *joinNode:
		// In DistSQL, we may take advantage of matching orderings on equality
		// columns and use merge joins. Preserve the orderings in that case.
		var usefulLeft, usefulRight sqlbase.ColumnOrdering
		if len(n.mergeJoinOrdering) > 0 {
			usefulLeft = make(sqlbase.ColumnOrdering, len(n.mergeJoinOrdering))
			usefulRight = make(sqlbase.ColumnOrdering, len(n.mergeJoinOrdering))
			for i, mergedCol := range n.mergeJoinOrdering {
				usefulLeft[i].ColIdx = n.pred.leftEqualityIndices[mergedCol.ColIdx]
				usefulRight[i].ColIdx = n.pred.rightEqualityIndices[mergedCol.ColIdx]
				usefulLeft[i].Direction = mergedCol.Direction
				usefulRight[i].Direction = mergedCol.Direction
			}
		}

		n.props.trim(usefulOrdering)

		n.left.plan = p.simplifyOrderings(n.left.plan, usefulLeft)
		n.right.plan = p.simplifyOrderings(n.right.plan, usefulRight)

	case *ordinalityNode:
		n.props.trim(usefulOrdering)
		n.source = p.simplifyOrderings(n.source, n.restrictOrdering(usefulOrdering))

	case *limitNode:
		n.plan = p.simplifyOrderings(n.plan, usefulOrdering)

	case *spoolNode:
		n.source = p.simplifyOrderings(n.source, usefulOrdering)

	case *groupNode:
		if n.needOnlyOneRow {
			n.plan = p.simplifyOrderings(n.plan, n.desiredOrdering)
		} else {
			// Keep only the ordering required by the groupNode.
			n.plan = p.simplifyOrderings(n.plan, translateGroupOrdering(n.props.ordering, n))
		}
		n.props.trim(usefulOrdering)

	case *windowNode:
		n.plan = p.simplifyOrderings(n.plan, nil)

	case *sortNode:
		if n.needSort {
			// We could pass no ordering below, but a partial ordering can speed up
			// the sort (and save memory), at least for DistSQL.
			n.plan = p.simplifyOrderings(n.plan, n.ordering)
		} else {
			constantCols := planPhysicalProps(n.plan).constantCols
			// Normally we would pass n.ordering; but n.ordering could be a prefix of
			// the useful ordering. Check for this, ignoring any constant columns.
			sortOrder := make(sqlbase.ColumnOrdering, 0, len(n.ordering))
			for _, c := range n.ordering {
				if !constantCols.Contains(c.ColIdx) {
					sortOrder = append(sortOrder, c)
				}
			}
			givenOrder := make(sqlbase.ColumnOrdering, 0, len(usefulOrdering))
			for _, c := range usefulOrdering {
				if !constantCols.Contains(c.ColIdx) {
					givenOrder = append(givenOrder, c)
				}
			}
			if sortOrder.IsPrefixOf(givenOrder) {
				n.plan = p.simplifyOrderings(n.plan, givenOrder)
			} else {
				n.plan = p.simplifyOrderings(n.plan, sortOrder)
			}
		}

		if !n.needSort {
			if len(n.columns) < len(planColumns(n.plan)) {
				// No sorting required, but we have to strip off the extra render
				// expressions we added. So keep the sort node.
				// TODO(radu): replace with a renderNode
			} else {
				// Sort node fully disappears.
				// Just be sure to propagate the column names.
				mutSourceCols := planMutableColumns(n.plan)
				for i, col := range n.columns {
					mutSourceCols[i].Name = col.Name
				}
				plan = n.plan
			}
		}

	case *distinctNode:
		// distinctNode uses the ordering computed from its source but
		// trimmed to the DISTINCT ON columns (if applicable).
		// Any useful ordering pertains only to the columns
		// we're distinctifying on.
		sourceOrdering := n.projectChildPropsToOnExprs()
		n.plan = p.simplifyOrderings(n.plan, sourceOrdering.ordering)

	case *scanNode:
		n.props.trim(usefulOrdering)

	case *renderNode:
		n.source.plan = p.simplifyOrderings(n.source.plan, translateOrdering(usefulOrdering, n))
		// Recompute r.ordering using the source's simplified ordering.
		// TODO(radu): in some cases there may be multiple possible n.orderings for
		// a given source plan ordering; we should pass usefulOrdering to help make
		// that choice (#13709).
		p.computePhysicalPropsForRender(n, planPhysicalProps(n.source.plan))

	case *delayedNode:
		n.plan = p.simplifyOrderings(n.plan, usefulOrdering)

	case *splitNode:
		n.rows = p.simplifyOrderings(n.rows, nil)

	case *relocateNode:
		n.rows = p.simplifyOrderings(n.rows, nil)

	case *cancelQueriesNode:
		n.rows = p.simplifyOrderings(n.rows, nil)

	case *cancelSessionsNode:
		n.rows = p.simplifyOrderings(n.rows, nil)

	case *controlJobsNode:
		n.rows = p.simplifyOrderings(n.rows, nil)

	case *valuesNode:
	case *virtualTableNode:
	case *alterIndexNode:
	case *alterTableNode:
	case *alterSequenceNode:
	case *alterUserSetPasswordNode:
	case *commentOnTableNode:
	case *renameColumnNode:
	case *renameDatabaseNode:
	case *renameIndexNode:
	case *renameTableNode:
	case *scrubNode:
	case *truncateNode:
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
	case *zeroNode:
	case *unaryNode:
	case *hookFnNode:
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
	return plan
}
