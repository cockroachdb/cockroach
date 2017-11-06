// Copyright 2017 The Cockroach Authors.
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
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

func (dsp *DistSQLPlanner) createPlanForJoin(
	planCtx *planningCtx, n *joinNode,
) (physicalPlan, error) {
	// A hint was provided that the merge join can be performed as an
	// interleave join.
	if planInterleaveJoins.Get(&dsp.st.SV) && n.joinHint == joinHintInterleave && n.joinType == joinTypeInner {
		// TODO(richardwu): We currently only do an interleave join on
		// all equality columns. This can be relaxed once a hybrid
		// hash-merge join is implemented (see comment below for merge
		// joins).
		if len(n.mergeJoinOrdering) == len(n.pred.leftEqualityIndices) {
			return dsp.createPlanForInterleaveJoin(planCtx, n)
		}
	}

	// Outline of the planning process for joins:
	//
	//  - We create physicalPlans for the left and right side. Each plan has a set
	//    of output routers with result that will serve as input for the join.
	//
	//  - We merge the list of processors and streams into a single plan. We keep
	//    track of the output routers for the left and right results.
	//
	//  - We add a set of joiner processors (say K of them).
	//
	//  - We configure the left and right output routers to send results to
	//    these joiners, distributing rows by hash (on the join equality columns).
	//    We are thus breaking up all input rows into K buckets such that rows
	//    that match on the equality columns end up in the same bucket. If there
	//    are no equality columns, we cannot distribute rows so we use a single
	//    joiner.
	//
	//  - The routers of the joiner processors are the result routers of the plan.

	leftPlan, err := dsp.createPlanForNode(planCtx, n.left.plan)
	if err != nil {
		return physicalPlan{}, err
	}
	rightPlan, err := dsp.createPlanForNode(planCtx, n.right.plan)
	if err != nil {
		return physicalPlan{}, err
	}

	var p physicalPlan
	var leftRouters, rightRouters []distsqlplan.ProcessorIdx
	p.PhysicalPlan, leftRouters, rightRouters = distsqlplan.MergePlans(
		&leftPlan.PhysicalPlan, &rightPlan.PhysicalPlan,
	)

	// Nodes where we will run the join processors.
	var nodes []roachpb.NodeID

	// We initialize these properties of the joiner. They will then be used to
	// fill in the processor spec. See descriptions for HashJoinerSpec.
	var joinType distsqlrun.JoinType
	var leftEqCols, rightEqCols []uint32
	var leftMergeOrd, rightMergeOrd distsqlrun.Ordering

	switch n.joinType {
	case joinTypeInner:
		joinType = distsqlrun.JoinType_INNER
	case joinTypeFullOuter:
		joinType = distsqlrun.JoinType_FULL_OUTER
	case joinTypeRightOuter:
		joinType = distsqlrun.JoinType_RIGHT_OUTER
	case joinTypeLeftOuter:
		joinType = distsqlrun.JoinType_LEFT_OUTER
	default:
		panic(fmt.Sprintf("invalid join type %d", n.joinType))
	}

	// Figure out the left and right types.
	leftTypes := leftPlan.ResultTypes
	rightTypes := rightPlan.ResultTypes

	// Set up the output columns.
	if numEq := len(n.pred.leftEqualityIndices); numEq != 0 {
		// TODO(radu): for now we run a join processor on every node that produces
		// data for either source. In the future we should be smarter here.
		seen := make(map[roachpb.NodeID]struct{})
		for _, pIdx := range leftRouters {
			n := p.Processors[pIdx].Node
			if _, ok := seen[n]; !ok {
				seen[n] = struct{}{}
				nodes = append(nodes, n)
			}
		}
		for _, pIdx := range rightRouters {
			n := p.Processors[pIdx].Node
			if _, ok := seen[n]; !ok {
				seen[n] = struct{}{}
				nodes = append(nodes, n)
			}
		}

		// Set up the equality columns.
		leftEqCols = eqCols(n.pred.leftEqualityIndices, leftPlan.planToStreamColMap)
		rightEqCols = eqCols(n.pred.rightEqualityIndices, rightPlan.planToStreamColMap)

		if planMergeJoins.Get(&dsp.st.SV) && len(n.mergeJoinOrdering) > 0 &&
			joinType == distsqlrun.JoinType_INNER {
			// TODO(radu): we currently only use merge joins when we have an ordering on
			// all equality columns. We should relax this by either:
			//  - implementing a hybrid hash/merge processor which implements merge
			//    logic on the columns we have an ordering on, and within each merge
			//    group uses a hashmap on the remaining columns
			//  - or: adding a sort processor to complete the order
			if len(n.mergeJoinOrdering) == len(n.pred.leftEqualityIndices) {
				// Excellent! We can use the merge joiner.
				leftMergeOrd = distsqlOrdering(n.mergeJoinOrdering, leftEqCols)
				rightMergeOrd = distsqlOrdering(n.mergeJoinOrdering, rightEqCols)
			}
		}
	} else {
		// Without column equality, we cannot distribute the join. Run a
		// single processor.
		nodes = []roachpb.NodeID{dsp.nodeDesc.NodeID}

		// If either side has a single stream, put the processor on that node. We
		// prefer the left side because that is processed first by the hash joiner.
		if len(leftRouters) == 1 {
			nodes[0] = p.Processors[leftRouters[0]].Node
		} else if len(rightRouters) == 1 {
			nodes[0] = p.Processors[rightRouters[0]].Node
		}
	}

	post, joinToStreamColMap := joinOutColumns(n, leftPlan, rightPlan)
	onExpr := remapOnExpr(n, leftPlan, rightPlan)

	// Create the Core spec.
	var core distsqlrun.ProcessorCoreUnion
	if leftMergeOrd.Columns == nil {
		core.HashJoiner = &distsqlrun.HashJoinerSpec{
			LeftEqColumns:  leftEqCols,
			RightEqColumns: rightEqCols,
			OnExpr:         onExpr,
			Type:           joinType,
		}
	} else {
		core.MergeJoiner = &distsqlrun.MergeJoinerSpec{
			LeftOrdering:  leftMergeOrd,
			RightOrdering: rightMergeOrd,
			OnExpr:        onExpr,
			Type:          joinType,
		}
	}

	pIdxStart := distsqlplan.ProcessorIdx(len(p.Processors))
	stageID := p.NewStageID()

	// Each node has a join processor.
	for _, n := range nodes {
		proc := distsqlplan.Processor{
			Node: n,
			Spec: distsqlrun.ProcessorSpec{
				Input: []distsqlrun.InputSyncSpec{
					{ColumnTypes: leftTypes},
					{ColumnTypes: rightTypes},
				},
				Core:    core,
				Post:    post,
				Output:  []distsqlrun.OutputRouterSpec{{Type: distsqlrun.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}
		p.Processors = append(p.Processors, proc)
	}

	if len(nodes) > 1 {
		// Parallel hash or merge join: we distribute rows (by hash of
		// equality columns) to len(nodes) join processors.

		// Set up the left routers.
		for _, resultProc := range leftRouters {
			p.Processors[resultProc].Spec.Output[0] = distsqlrun.OutputRouterSpec{
				Type:        distsqlrun.OutputRouterSpec_BY_HASH,
				HashColumns: leftEqCols,
			}
		}
		// Set up the right routers.
		for _, resultProc := range rightRouters {
			p.Processors[resultProc].Spec.Output[0] = distsqlrun.OutputRouterSpec{
				Type:        distsqlrun.OutputRouterSpec_BY_HASH,
				HashColumns: rightEqCols,
			}
		}
	}
	p.ResultRouters = p.ResultRouters[:0]

	// Connect the left and right routers to the output joiners. Each joiner
	// corresponds to a hash bucket.
	for bucket := 0; bucket < len(nodes); bucket++ {
		pIdx := pIdxStart + distsqlplan.ProcessorIdx(bucket)

		// Connect left routers to the processor's first input. Currently the join
		// node doesn't care about the orderings of the left and right results.
		p.MergeResultStreams(leftRouters, bucket, leftMergeOrd, pIdx, 0)
		// Connect right routers to the processor's second input.
		p.MergeResultStreams(rightRouters, bucket, rightMergeOrd, pIdx, 1)

		p.ResultRouters = append(p.ResultRouters, pIdx)
	}

	p.planToStreamColMap = joinToStreamColMap
	p.ResultTypes = getTypesForPlanResult(n, joinToStreamColMap)

	// Joiners may guarantee an ordering to outputs, so we ensure that
	// ordering is propagated through the input synchronizer of the next stage.
	// We can propagate the ordering from either side, we use the left side here.
	p.SetMergeOrdering(dsp.convertOrdering(n.props, p.planToStreamColMap))
	return p, nil
}

func (dsp *DistSQLPlanner) createPlanForInterleaveJoin(
	planCtx *planningCtx, n *joinNode,
) (physicalPlan, error) {
	leftScan, leftOk := n.left.plan.(*scanNode)
	rightScan, rightOk := n.right.plan.(*scanNode)
	if !leftOk || !rightOk {
		panic("cannot plan interleave join with non-scan left and right nodes")
	}

	if leftScan.reverse != rightScan.reverse {
		panic("cannot plan interleave join with opposite scan directions")
	}

	if len(n.mergeJoinOrdering) <= 0 {
		panic("cannot plan interleave join with no equality join columns")
	}

	if len(n.mergeJoinOrdering) != len(n.pred.leftEqualityIndices) {
		panic("cannot plan interleave join on a subset of equality join columns")
	}

	// We iterate through each table and collate their metadata for
	// the InterleaveReaderJoinerSpec.
	tables := make([]distsqlrun.InterleaveReaderJoinerTable, 2)
	plans := make([]physicalPlan, 2)
	var totalLimitHint int64
	for i, t := range []struct {
		scan      *scanNode
		eqIndices []int
	}{
		{
			scan:      leftScan,
			eqIndices: n.pred.leftEqualityIndices,
		},
		{
			scan:      rightScan,
			eqIndices: n.pred.rightEqualityIndices,
		},
	} {
		// We don't really need to initialize a full-on plan to
		// retrieve the metadata for each table reader, but this turns
		// out to be very useful to compute ordering and remapping the
		// onCond and columns.
		var err error
		if plans[i], err = dsp.createTableReaders(planCtx, t.scan, nil); err != nil {
			return physicalPlan{}, err
		}

		eqCols := eqCols(t.eqIndices, plans[i].planToStreamColMap)
		ordering := distsqlOrdering(n.mergeJoinOrdering, eqCols)

		// Doesn't matter which processor we choose since the metadata
		// for TableReader is independent of node/processor instance.
		tr := plans[i].Processors[0].Spec.Core.TableReader

		tables[i] = distsqlrun.InterleaveReaderJoinerTable{
			Desc:     tr.Table,
			IndexIdx: tr.IndexIdx,
			Post:     plans[i].GetLastStagePost(),
			Ordering: ordering,
		}

		if tr.LimitHint >= math.MaxInt64-totalLimitHint {
			totalLimitHint = math.MaxInt64
		} else {
			totalLimitHint += tr.LimitHint
		}
	}

	// We need to combine the two set of spans from the left and right side
	// since we have one reading processor (InterleaveReaderJoiner).
	var spans []roachpb.Span
	var joinType distsqlrun.JoinType
	switch n.joinType {
	case joinTypeInner:
		// We can take the intersection of the two set of spans for
		// inner joins because we never need rows from either table
		// that do not satisfy the ON condition.
		// Since equality columns map 1-1 with the columns of the
		// interleave prefix, we need only scan interleaves together.
		// Spans for interleaved tables/indexes always refer to their
		// root ancestor and thus incorporate all interleaves of the
		// ancestors between the root and itself.
		spans = roachpb.IntersectSpans(append(leftScan.spans, rightScan.spans...))
		joinType = distsqlrun.JoinType_INNER
	default:
		// TODO(richardwu): For outer joins, we will need to take the
		// union of spans and propagate scan.origFilters down into the
		// spec. See comment above spans in InterleaveReaderJoinerSpec.
		panic("can only plan inner joins with interleave joins")
	}

	post, joinToStreamColMap := joinOutColumns(n, plans[0], plans[1])
	onExpr := remapOnExpr(n, plans[0], plans[1])

	// We partition the combined spans to their data nodes (best effort).
	spanPartitions, err := dsp.partitionSpans(planCtx, spans)
	if err != nil {
		return physicalPlan{}, err
	}
	// For a given parent row, we need to ensure each
	// InterleaveReaderJoiner processor reads all children rows. If
	// children rows for a given parent row is partitioned into a different
	// node, then we will miss joining those children rows to the parent
	// row.
	// We thus fix the spans such that the end key always includes the last
	// children.
	if spanPartitions, err = fixInterleavePartitions(n, spanPartitions); err != nil {
		return physicalPlan{}, err
	}

	var p physicalPlan
	stageID := p.NewStageID()

	// We provision a separate InterleaveReaderJoiner per node that has
	// rows from both tables.
	p.ResultRouters = make([]distsqlplan.ProcessorIdx, len(spanPartitions))
	for _, sp := range spanPartitions {
		irj := &distsqlrun.InterleaveReaderJoinerSpec{
			Tables: tables,
			// We previously checked that both scans are in the
			// same direction.
			Reverse:   leftScan.reverse,
			LimitHint: totalLimitHint,
			OnExpr:    onExpr,
			Type:      joinType,
		}

		irj.Spans = make([]distsqlrun.TableReaderSpan, len(sp.spans))
		for i, span := range sp.spans {
			irj.Spans[i].Span = span
		}

		proc := distsqlplan.Processor{
			Node: sp.node,
			Spec: distsqlrun.ProcessorSpec{
				Core:    distsqlrun.ProcessorCoreUnion{InterleaveReaderJoiner: irj},
				Post:    post,
				Output:  []distsqlrun.OutputRouterSpec{{Type: distsqlrun.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}

		p.Processors = append(p.Processors, proc)
	}

	p.planToStreamColMap = joinToStreamColMap
	p.ResultTypes = getTypesForPlanResult(n, joinToStreamColMap)

	p.SetMergeOrdering(dsp.convertOrdering(n.props, p.planToStreamColMap))
	return p, nil
}

func joinOutColumns(n *joinNode, leftPlan, rightPlan physicalPlan) (post distsqlrun.PostProcessSpec, joinToStreamColMap []int) {
	joinToStreamColMap = makePlanToStreamColMap(len(n.columns))
	post.Projection = true

	// addOutCol appends to post.OutputColumns and returns the index
	// in the slice of the added column.
	addOutCol := func(col uint32) int {
		idx := len(post.OutputColumns)
		post.OutputColumns = append(post.OutputColumns, col)
		return idx
	}

	// The join columns are in two groups:
	//  - the columns on the left side (numLeftCols)
	//  - the columns on the right side (numRightCols)
	joinCol := 0
	for i := 0; i < n.pred.numLeftCols; i++ {
		if !n.columns[joinCol].Omitted {
			joinToStreamColMap[joinCol] = addOutCol(uint32(leftPlan.planToStreamColMap[i]))
		}
		joinCol++
	}
	for i := 0; i < n.pred.numRightCols; i++ {
		if !n.columns[joinCol].Omitted {
			joinToStreamColMap[joinCol] = addOutCol(
				uint32(rightPlan.planToStreamColMap[i] + len(leftPlan.ResultTypes)),
			)
		}
		joinCol++
	}

	return post, joinToStreamColMap
}

// We have to remap ordinal references in the on condition (which refer to the
// join columns as described above) to values that make sense in the joiner (0
// to N-1 for the left input columns, N to N+M-1 for the right input columns).
func remapOnExpr(n *joinNode, leftPlan, rightPlan physicalPlan) distsqlrun.Expression {
	if n.pred.onCond == nil {
		return distsqlrun.Expression{}
	}

	joinColMap := make([]int, len(n.columns))
	idx := 0
	for i := 0; i < n.pred.numLeftCols; i++ {
		joinColMap[idx] = leftPlan.planToStreamColMap[i]
		idx++
	}
	for i := 0; i < n.pred.numRightCols; i++ {
		joinColMap[idx] = rightPlan.planToStreamColMap[i] + len(leftPlan.ResultTypes)
		idx++
	}

	return distsqlplan.MakeExpression(n.pred.onCond, joinColMap)
}

func eqCols(eqIndices, planToColMap []int) []uint32 {
	eqCols := make([]uint32, len(eqIndices))
	for i, planCol := range eqIndices {
		eqCols[i] = uint32(planToColMap[planCol])
	}

	return eqCols
}

func distsqlOrdering(mergeJoinOrdering sqlbase.ColumnOrdering, eqCols []uint32) distsqlrun.Ordering {
	var ord distsqlrun.Ordering
	ord.Columns = make([]distsqlrun.Ordering_Column, len(mergeJoinOrdering))
	for i, c := range mergeJoinOrdering {
		ord.Columns[i].ColIdx = eqCols[c.ColIdx]
		dir := distsqlrun.Ordering_Column_ASC
		if c.Direction == encoding.Descending {
			dir = distsqlrun.Ordering_Column_DESC
		}
		ord.Columns[i].Direction = dir
	}

	return ord
}

func fixInterleavePartitions(n *joinNode, partitions []spanPartition) ([]spanPartition, error) {
	ancestor, descendant := n.interleaveNodes()
	if ancestor == nil || descendant == nil {
		panic("cannot fix interleave join spans since there is no interleave relation")
	}

	// Given a span with the start and end keys:
	// StartKey: /parent/1/2/#/child/2
	// EndKey:   /parent/1/42/#/child/4
	// We'd like to fix the EndKey such that the span contains all
	// children keys for the given parent ID 42. The end result we want is
	// StartKey: /parent/1/2/#/child/2
	// EndKey:   /parent/1/43
	// To do this, we must truncate up to the prefix /parent/1/42 to invoke
	// encoding.PrefixEnd().
	// To calculate how long this prefix is, we take the general encoding of a descendant EndKey
	//   /<1st-tableid>/<1st-indexid>/<1st-index-columns>/#/<2nd-tableid>/<2nd-indexid>/2nd-index-columns/#/...
	// and note that the number of times we need to invoke encoding.PeekLength()
	// (for each value in the key) to figure out the total length is
	//   3 * count(interleave ancestors) + sum(shared_prefix_len) - 1
	nAncestors := 0
	sharedPrefixLen := 0
	for _, descAncestor := range descendant.index.Interleave.Ancestors {
		nAncestors++
		sharedPrefixLen += int(descAncestor.SharedPrefixLen)
		if descAncestor.TableID == ancestor.desc.ID && descAncestor.IndexID == ancestor.index.ID {
			break
		}
	}

	for i := range partitions {
		for j := range partitions[i].spans {
			span := &partitions[i].spans[j]
			endKey := span.EndKey
			// See the above formula for computing the number of times
			// to invoke encoding.PeekLength().
			prefixLen := 0
			for i := 0; i < 3*nAncestors+sharedPrefixLen-1; i++ {
				// It's possible for the span key to not
				// contain all the shared fields (e.g. when no
				// filter is specified, the key will be in the
				// form /tableid/indexid instead of
				// /tableid/indexid/col1).
				// This is fine since we only care about keys
				// which restrict within the child's column
				// fields. Any keys shorter than the maximum
				// prefix length is guaranteed to include the
				// all relevant, interleaved children rows.
				if len(endKey) == 0 {
					break
				}
				len, err := encoding.PeekLength(endKey)
				if err != nil {
					return nil, err
				}
				prefixLen += len
				endKey = endKey[len:]
			}

			// Truncate the end key up to the prefix length.
			span.EndKey = span.EndKey[:prefixLen]
		}
	}

	return partitions, nil
}
