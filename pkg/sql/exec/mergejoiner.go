// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// group is an ADT representing a contiguous set of rows that match on their
// equality columns.
type group struct {
	rowStartIdx int
	rowEndIdx   int
	// numRepeats is used when expanding each group into a cross product in the
	// build phase.
	numRepeats int
	// toBuild is used in the build phase to determine the right output count.
	// This field should stay in sync with the builder over time.
	toBuild int
	// nullGroup indicates whether the output corresponding to the group should
	// consist of all nulls.
	nullGroup bool
	// unmatched indicates that the rows in the group do not have matching rows
	// from the other side (i.e. other side's group will be a null group).
	// NOTE: during the probing phase, the assumption is that such group will
	// consist of a single row.
	unmatched bool
}

// mjBuilderState contains all the state required to execute the build phase.
type mjBuilderState struct {
	// Fields to hold the input sources batches from which to build the cross
	// products.
	lBatch coldata.Batch
	rBatch coldata.Batch

	// Fields to identify the groups in the input sources.
	lGroups []group
	rGroups []group

	// outCount keeps record of the current number of rows in the output.
	outCount uint16
	// outFinished is used to determine if the builder is finished outputting
	// the groups from input.
	outFinished bool

	// Cross product materialization state.
	left  mjBuilderCrossProductState
	right mjBuilderCrossProductState
}

// mjBuilderCrossProductState is used to keep track of builder state within the
// loops to materialize the cross product. Useful for picking up where we left
// off.
type mjBuilderCrossProductState struct {
	colIdx         int
	groupsIdx      int
	curSrcStartIdx int
	numRepeatsIdx  int
}

// mjProberState contains all the state required to execute in the probing
// phase.
type mjProberState struct {
	// Fields to save the "working" batches to state in between outputs.
	lBatch  coldata.Batch
	rBatch  coldata.Batch
	lIdx    int
	lLength int
	rIdx    int
	rLength int

	// Local buffer for the last left and right groups which is used when the
	// group ends with a batch and the group on each side needs to be saved to
	// state in order to be able to continue it in the next batch.
	lBufferedGroup *mjBufferedGroup
	rBufferedGroup *mjBufferedGroup
}

// mjState represents the state of the merge joiner.
type mjState int

const (
	// mjEntry is the entry state of the merge joiner where all the batches and
	// indices are properly set, regardless if Next was called the first time or
	// the 1000th time. This state also routes into the correct state based on
	// the prober state after setup.
	mjEntry mjState = iota

	// mjSourceFinished is the state in which one of the input sources has no
	// more available batches, thus signaling that the joiner should begin
	// wrapping up execution by outputting any remaining groups in state.
	mjSourceFinished

	// mjFinishBufferedGroup is the state in which the previous state resulted in
	// a group that ended with a batch. Such a group was buffered, and this state
	// finishes that group and builds the output.
	mjFinishBufferedGroup

	// mjProbe is the main probing state in which the groups for the current
	// batch are determined.
	mjProbe

	// mjBuild is the state in which the groups determined by the probing states
	// are built, i.e. materialized to the output member by creating the cross
	// product.
	mjBuild
)

type mergeJoinInput struct {
	// eqCols specify the indices of the source table equality columns during the
	// merge join.
	eqCols []uint32

	// outCols specify the indices of the columns that should be outputted by the
	// merge joiner.
	outCols []uint32

	// directions specifies the ordering direction of each column. Note that each
	// direction corresponds to an equality column at the same location, i.e. the
	// direction of eqCols[x] is encoded at directions[x], or
	// len(eqCols) == len(directions).
	directions []distsqlpb.Ordering_Column_Direction

	// sourceTypes specify the types of the input columns of the source table for
	// the merge joiner.
	sourceTypes []types.T

	// The distincter is used in the finishGroup phase, and is used only to
	// determine where the current group ends, in the case that the group ended
	// with a batch.
	distincterInput feedOperator
	distincter      Operator
	distinctOutput  []bool

	// source specifies the input operator to the merge join.
	source Operator
}

// feedOperator is used to feed the distincter with input by manually setting
// the next batch.
type feedOperator struct {
	batch coldata.Batch
}

func (feedOperator) Init() {}

func (o *feedOperator) Next(context.Context) coldata.Batch {
	return o.batch
}

var _ Operator = &feedOperator{}

// mergeJoinOp is an operator that implements sort-merge join. It performs a
// merge on the left and right input sources, based on the equality columns,
// assuming both inputs are in sorted order.

// The merge join operator uses a probe and build approach to generate the
// join. What this means is that instead of going through and expanding the
// cross product row by row, the operator performs two passes.
// The first pass generates a list of groups of matching rows based on the
// equality columns (where a "group" represents a contiguous set of rows that
// match on the equality columns).
// The second pass is where the groups and their associated cross products are
// materialized into the full output.

// Two buffers are used, one for the group on the left table and one for the
// group on the right table. These buffers are only used if the group ends with
// a batch, to make sure that we don't miss any cross product entries while
// expanding the groups (leftGroups and rightGroups) when a group spans
// multiple batches.

// NewMergeJoinOp returns a new merge join operator with the given spec.
func NewMergeJoinOp(
	joinType sqlbase.JoinType,
	left Operator,
	right Operator,
	leftOutCols []uint32,
	rightOutCols []uint32,
	leftTypes []types.T,
	rightTypes []types.T,
	leftOrdering []distsqlpb.Ordering_Column,
	rightOrdering []distsqlpb.Ordering_Column,
) (Operator, error) {
	switch joinType {
	case sqlbase.JoinType_INNER:
		return newMergeJoinInnerOp(left, right, leftOutCols, rightOutCols, leftTypes, rightTypes, leftOrdering, rightOrdering)
	case sqlbase.JoinType_LEFT_OUTER:
		return newMergeJoinLeftOuterOp(left, right, leftOutCols, rightOutCols, leftTypes, rightTypes, leftOrdering, rightOrdering)
	case sqlbase.JoinType_RIGHT_OUTER:
		return newMergeJoinRightOuterOp(left, right, leftOutCols, rightOutCols, leftTypes, rightTypes, leftOrdering, rightOrdering)
	case sqlbase.JoinType_FULL_OUTER:
		return newMergeJoinFullOuterOp(left, right, leftOutCols, rightOutCols, leftTypes, rightTypes, leftOrdering, rightOrdering)
	case sqlbase.JoinType_LEFT_SEMI:
		return newMergeJoinLeftSemiOp(left, right, leftOutCols, rightOutCols, leftTypes, rightTypes, leftOrdering, rightOrdering)
	default:
		panic("unsupported join type")
	}
}

// Const declarations for the merge joiner cross product (MJCP) zero state.
const (
	zeroMJCPcolIdx    = 0
	zeroMJCPgroupsIdx = 0
	// The sentinel value for curSrcStartIdx is -1, as this:
	// a) indicates that a src has not been started
	// b) panics if the sentinel isn't checked
	zeroMJCPcurSrcStartIdx = -1
	zeroMJCPnumRepeatsIdx  = 0
)

// Package level struct for easy access to the MJCP zero state.
var zeroMJBuilderState = mjBuilderCrossProductState{
	colIdx:         zeroMJCPcolIdx,
	groupsIdx:      zeroMJCPgroupsIdx,
	curSrcStartIdx: zeroMJCPcurSrcStartIdx,
	numRepeatsIdx:  zeroMJCPnumRepeatsIdx,
}

func (s *mjBuilderCrossProductState) reset() {
	s.setBuilderColumnState(zeroMJBuilderState)
	s.colIdx = zeroMJCPcolIdx
}

func (s *mjBuilderCrossProductState) setBuilderColumnState(target mjBuilderCrossProductState) {
	s.groupsIdx = target.groupsIdx
	s.curSrcStartIdx = target.curSrcStartIdx
	s.numRepeatsIdx = target.numRepeatsIdx
}
