// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// joinPlanningInfo is a utility struct that contains the information needed to
// perform the physical planning of hash and merge joins.
type joinPlanningInfo struct {
	leftPlan, rightPlan *PhysicalPlan
	joinType            descpb.JoinType
	joinResultTypes     []*types.T
	onExpr              execinfrapb.Expression
	post                execinfrapb.PostProcessSpec
	joinToStreamColMap  []int
	// leftEqCols and rightEqCols are the indices of equality columns. These
	// are only used when planning a hash join.
	leftEqCols, rightEqCols             []uint32
	leftEqColsAreKey, rightEqColsAreKey bool
	// leftMergeOrd and rightMergeOrd are the orderings on both inputs to a
	// merge join. They must be of the same length, and if the length is 0,
	// then a hash join is planned.
	leftMergeOrd, rightMergeOrd                 execinfrapb.Ordering
	leftPlanDistribution, rightPlanDistribution physicalplan.PlanDistribution
}

// makeCoreSpec creates a processor core for hash and merge joins based on the
// join planning information. Merge ordering fields of info determine which
// kind of join is being planned.
func (info *joinPlanningInfo) makeCoreSpec() execinfrapb.ProcessorCoreUnion {
	var core execinfrapb.ProcessorCoreUnion
	if len(info.leftMergeOrd.Columns) != len(info.rightMergeOrd.Columns) {
		panic(errors.AssertionFailedf(
			"unexpectedly different merge join ordering lengths: left %d, right %d",
			len(info.leftMergeOrd.Columns), len(info.rightMergeOrd.Columns),
		))
	}
	if len(info.leftMergeOrd.Columns) == 0 {
		// There is no required ordering on the columns, so we plan a hash join.
		core.HashJoiner = &execinfrapb.HashJoinerSpec{
			LeftEqColumns:        info.leftEqCols,
			RightEqColumns:       info.rightEqCols,
			OnExpr:               info.onExpr,
			Type:                 info.joinType,
			LeftEqColumnsAreKey:  info.leftEqColsAreKey,
			RightEqColumnsAreKey: info.rightEqColsAreKey,
		}
	} else {
		core.MergeJoiner = &execinfrapb.MergeJoinerSpec{
			LeftOrdering:         info.leftMergeOrd,
			RightOrdering:        info.rightMergeOrd,
			OnExpr:               info.onExpr,
			Type:                 info.joinType,
			LeftEqColumnsAreKey:  info.leftEqColsAreKey,
			RightEqColumnsAreKey: info.rightEqColsAreKey,
		}
	}
	return core
}

// joinPlanningHelper is a utility struct that helps with the physical planning
// of joins.
type joinPlanningHelper struct {
	// numLeftOutCols and numRightOutCols store the number of columns that need
	// to be included in the output of the join from each of the sides.
	numLeftOutCols, numRightOutCols int
	// numAllLeftCols stores the width of the rows coming from the left side.
	// Note that it includes all of the left "out" columns and might include
	// other "internal" columns that are needed to merge the streams for the
	// left input.
	numAllLeftCols                                  int
	leftPlanToStreamColMap, rightPlanToStreamColMap []int
}

func (h *joinPlanningHelper) joinOutColumns(
	joinType descpb.JoinType, columns colinfo.ResultColumns,
) (post execinfrapb.PostProcessSpec, joinToStreamColMap []int) {
	joinToStreamColMap = makePlanToStreamColMap(len(columns))
	post.Projection = true

	// addOutCol appends to post.OutputColumns and returns the index
	// in the slice of the added column.
	addOutCol := func(col uint32) int {
		idx := len(post.OutputColumns)
		post.OutputColumns = append(post.OutputColumns, col)
		return idx
	}

	// The join columns are in two groups:
	//  - the columns on the left side (numLeftOutCols)
	//  - the columns on the right side (numRightOutCols)
	var numLeftOutCols int
	var numAllLeftCols int
	if joinType.ShouldIncludeLeftColsInOutput() {
		numLeftOutCols = h.numLeftOutCols
		numAllLeftCols = h.numAllLeftCols
		for i := 0; i < h.numLeftOutCols; i++ {
			joinToStreamColMap[i] = addOutCol(uint32(h.leftPlanToStreamColMap[i]))
		}
	}

	if joinType.ShouldIncludeRightColsInOutput() {
		for i := 0; i < h.numRightOutCols; i++ {
			joinToStreamColMap[numLeftOutCols+i] = addOutCol(
				uint32(numAllLeftCols + h.rightPlanToStreamColMap[i]),
			)
		}
	}

	return post, joinToStreamColMap
}

// remapOnExpr remaps ordinal references in the ON condition (which refer to the
// join columns as described above) to values that make sense in the joiner (0
// to N-1 for the left input columns, N to N+M-1 for the right input columns).
func (h *joinPlanningHelper) remapOnExpr(
	planCtx *PlanningCtx, onCond tree.TypedExpr,
) (execinfrapb.Expression, error) {
	if onCond == nil {
		return execinfrapb.Expression{}, nil
	}

	joinColMap := make([]int, h.numLeftOutCols+h.numRightOutCols)
	idx := 0
	leftCols := 0
	for i := 0; i < h.numLeftOutCols; i++ {
		joinColMap[idx] = h.leftPlanToStreamColMap[i]
		if h.leftPlanToStreamColMap[i] != -1 {
			leftCols++
		}
		idx++
	}
	for i := 0; i < h.numRightOutCols; i++ {
		joinColMap[idx] = leftCols + h.rightPlanToStreamColMap[i]
		idx++
	}

	return physicalplan.MakeExpression(onCond, planCtx, joinColMap)
}

// eqCols produces a slice of ordinal references for the plan columns specified
// in eqIndices using planToColMap.
// That is: eqIndices contains a slice of plan column indexes and planToColMap
// maps the plan column indexes to the ordinal references (index of the
// intermediate row produced).
func eqCols(eqIndices []exec.NodeColumnOrdinal, planToColMap []int) []uint32 {
	eqCols := make([]uint32, len(eqIndices))
	for i, planCol := range eqIndices {
		eqCols[i] = uint32(planToColMap[planCol])
	}

	return eqCols
}

// distsqlOrdering converts the ordering specified by mergeJoinOrdering in
// terms of the index of eqCols to the ordinal references provided by eqCols.
func distsqlOrdering(
	mergeJoinOrdering colinfo.ColumnOrdering, eqCols []uint32,
) execinfrapb.Ordering {
	var ord execinfrapb.Ordering
	ord.Columns = make([]execinfrapb.Ordering_Column, len(mergeJoinOrdering))
	for i, c := range mergeJoinOrdering {
		ord.Columns[i].ColIdx = eqCols[c.ColIdx]
		dir := execinfrapb.Ordering_Column_ASC
		if c.Direction == encoding.Descending {
			dir = execinfrapb.Ordering_Column_DESC
		}
		ord.Columns[i].Direction = dir
	}

	return ord
}

func distsqlSetOpJoinType(setOpType tree.UnionType) descpb.JoinType {
	switch setOpType {
	case tree.ExceptOp:
		return descpb.ExceptAllJoin
	case tree.IntersectOp:
		return descpb.IntersectAllJoin
	default:
		panic(errors.AssertionFailedf("set op type %v unsupported by joins", setOpType))
	}
}

// getNodesOfRouters returns all nodes that routers are put on.
func getNodesOfRouters(
	routers []physicalplan.ProcessorIdx, processors []physicalplan.Processor,
) (nodes []roachpb.NodeID) {
	seen := make(map[roachpb.NodeID]struct{})
	for _, pIdx := range routers {
		n := processors[pIdx].Node
		if _, ok := seen[n]; !ok {
			seen[n] = struct{}{}
			nodes = append(nodes, n)
		}
	}
	return nodes
}

func findJoinProcessorNodes(
	leftRouters, rightRouters []physicalplan.ProcessorIdx, processors []physicalplan.Processor,
) (nodes []roachpb.NodeID) {
	// TODO(radu): for now we run a join processor on every node that produces
	// data for either source. In the future we should be smarter here.
	return getNodesOfRouters(append(leftRouters, rightRouters...), processors)
}
