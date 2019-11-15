// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
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
	directions []execinfrapb.Ordering_Column_Direction

	// sourceTypes specify the types of the input columns of the source table for
	// the merge joiner.
	sourceTypes []coltypes.T

	// The distincter is used in the finishGroup phase, and is used only to
	// determine where the current group ends, in the case that the group ended
	// with a batch.
	distincterInput *feedOperator
	distincter      Operator
	distinctOutput  []bool

	// source specifies the input operator to the merge join.
	source Operator
}

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

// NewMergeJoinOp returns a new merge join operator with the given spec that
// implements sort-merge join. It performs a merge on the left and right input
// sources, based on the equality columns, assuming both inputs are in sorted
// order.
func NewMergeJoinOp(
	allocator *Allocator,
	joinType sqlbase.JoinType,
	left Operator,
	right Operator,
	leftOutCols []uint32,
	rightOutCols []uint32,
	leftTypes []coltypes.T,
	rightTypes []coltypes.T,
	leftOrdering []execinfrapb.Ordering_Column,
	rightOrdering []execinfrapb.Ordering_Column,
	filterConstructor func(Operator) (Operator, error),
	filterOnlyOnLeft bool,
) (Operator, error) {
	base, err := newMergeJoinBase(
		allocator,
		left,
		right,
		leftOutCols,
		rightOutCols,
		leftTypes,
		rightTypes,
		leftOrdering,
		rightOrdering,
		filterConstructor,
		filterOnlyOnLeft,
	)
	if filterConstructor != nil {
		switch joinType {
		case sqlbase.JoinType_INNER:
			execerror.VectorizedInternalPanic("INNER JOIN with ON expression is handled differently")
		case sqlbase.JoinType_LEFT_SEMI:
			return &mergeJoinLeftSemiWithOnExprOp{base}, err
		case sqlbase.JoinType_LEFT_ANTI:
			return &mergeJoinLeftAntiWithOnExprOp{base}, err
		default:
			execerror.VectorizedInternalPanic(
				fmt.Sprintf("unsupported join type %s with ON expression", joinType.String()),
			)
		}
	}
	switch joinType {
	case sqlbase.JoinType_INNER:
		return &mergeJoinInnerOp{base}, err
	case sqlbase.JoinType_LEFT_OUTER:
		return &mergeJoinLeftOuterOp{base}, err
	case sqlbase.JoinType_RIGHT_OUTER:
		return &mergeJoinRightOuterOp{base}, err
	case sqlbase.JoinType_FULL_OUTER:
		return &mergeJoinFullOuterOp{base}, err
	case sqlbase.JoinType_LEFT_SEMI:
		return &mergeJoinLeftSemiOp{base}, err
	case sqlbase.JoinType_LEFT_ANTI:
		return &mergeJoinLeftAntiOp{base}, err
	default:
		execerror.VectorizedInternalPanic("unsupported join type")
	}
	// This code is unreachable, but the compiler cannot infer that.
	return nil, nil
}

// Const declarations for the merge joiner cross product (MJCP) zero state.
const (
	zeroMJCPGroupsIdx = 0
	// The sentinel value for curSrcStartIdx is -1, as this:
	// a) indicates that a src has not been started
	// b) panics if the sentinel isn't checked
	zeroMJCPCurSrcStartIdx = -1
	zeroMJCPNumRepeatsIdx  = 0
)

// Package level struct for easy access to the MJCP zero state.
var zeroMJBuilderState = mjBuilderCrossProductState{
	groupsIdx:      zeroMJCPGroupsIdx,
	curSrcStartIdx: zeroMJCPCurSrcStartIdx,
	numRepeatsIdx:  zeroMJCPNumRepeatsIdx,
}

func (s *mjBuilderCrossProductState) reset() {
	s.setBuilderColumnState(zeroMJBuilderState)
}

func (s *mjBuilderCrossProductState) setBuilderColumnState(target mjBuilderCrossProductState) {
	s.groupsIdx = target.groupsIdx
	s.curSrcStartIdx = target.curSrcStartIdx
	s.numRepeatsIdx = target.numRepeatsIdx
}

func newMergeJoinBase(
	allocator *Allocator,
	left Operator,
	right Operator,
	leftOutCols []uint32,
	rightOutCols []uint32,
	leftTypes []coltypes.T,
	rightTypes []coltypes.T,
	leftOrdering []execinfrapb.Ordering_Column,
	rightOrdering []execinfrapb.Ordering_Column,
	filterConstructor func(Operator) (Operator, error),
	filterOnlyOnLeft bool,
) (mergeJoinBase, error) {
	lEqCols := make([]uint32, len(leftOrdering))
	lDirections := make([]execinfrapb.Ordering_Column_Direction, len(leftOrdering))
	for i, c := range leftOrdering {
		lEqCols[i] = c.ColIdx
		lDirections[i] = c.Direction
	}

	rEqCols := make([]uint32, len(rightOrdering))
	rDirections := make([]execinfrapb.Ordering_Column_Direction, len(rightOrdering))
	for i, c := range rightOrdering {
		rEqCols[i] = c.ColIdx
		rDirections[i] = c.Direction
	}

	base := mergeJoinBase{
		twoInputNode: newTwoInputNode(left, right),
		allocator:    allocator,
		left: mergeJoinInput{
			source:      left,
			outCols:     leftOutCols,
			sourceTypes: leftTypes,
			eqCols:      lEqCols,
			directions:  lDirections,
		},
		right: mergeJoinInput{
			source:      right,
			outCols:     rightOutCols,
			sourceTypes: rightTypes,
			eqCols:      rEqCols,
			directions:  rDirections,
		},
	}
	var err error
	base.left.distincterInput = &feedOperator{}
	base.left.distincter, base.left.distinctOutput, err = OrderedDistinctColsToOperators(
		base.left.distincterInput, lEqCols, leftTypes)
	if err != nil {
		return base, err
	}
	base.right.distincterInput = &feedOperator{}
	base.right.distincter, base.right.distinctOutput, err = OrderedDistinctColsToOperators(
		base.right.distincterInput, rEqCols, rightTypes)
	if err != nil {
		return base, err
	}
	if filterConstructor != nil {
		base.filter, err = newJoinerFilter(
			base.allocator,
			leftTypes,
			rightTypes,
			filterConstructor,
			filterOnlyOnLeft,
		)
	}
	return base, err
}

// mergeJoinBase extracts the common logic between all merge join operators.
type mergeJoinBase struct {
	twoInputNode

	allocator *Allocator
	left      mergeJoinInput
	right     mergeJoinInput

	// Output buffer definition.
	output          coldata.Batch
	outputBatchSize uint16
	// outputReady is a flag to indicate that merge joiner is ready to emit an
	// output batch.
	outputReady bool

	// Local buffer for the "working" repeated groups.
	groups circularGroupsBuffer

	state        mjState
	proberState  mjProberState
	builderState mjBuilderState

	filter *joinerFilter
}

func (o *mergeJoinBase) getOutColTypes() []coltypes.T {
	outColTypes := make([]coltypes.T, 0, len(o.left.outCols)+len(o.right.outCols))
	for _, leftOutCol := range o.left.outCols {
		outColTypes = append(outColTypes, o.left.sourceTypes[leftOutCol])
	}
	for _, rightOutCol := range o.right.outCols {
		outColTypes = append(outColTypes, o.right.sourceTypes[rightOutCol])
	}
	return outColTypes
}

func (o *mergeJoinBase) InternalMemoryUsage() int {
	const sizeOfGroup = int(unsafe.Sizeof(group{}))
	return 8 * int(coldata.BatchSize()) * sizeOfGroup // o.groups
}

func (o *mergeJoinBase) Init() {
	o.initWithOutputBatchSize(coldata.BatchSize())
}

func (o *mergeJoinBase) initWithOutputBatchSize(outBatchSize uint16) {
	o.output = o.allocator.NewMemBatchWithSize(o.getOutColTypes(), int(outBatchSize))
	o.left.source.Init()
	o.right.source.Init()
	o.outputBatchSize = outBatchSize
	// If there are no output columns, then the operator is for a COUNT query,
	// in which case we treat the output batch size as the max uint16.
	if o.output.Width() == 0 {
		o.outputBatchSize = 1<<16 - 1
	}

	o.proberState.lBufferedGroup = newMJBufferedGroup(o.allocator, o.left.sourceTypes)
	o.proberState.rBufferedGroup = newMJBufferedGroup(o.allocator, o.right.sourceTypes)

	o.builderState.lGroups = make([]group, 1)
	o.builderState.rGroups = make([]group, 1)

	o.groups = makeGroupsBuffer(int(coldata.BatchSize()))
	o.resetBuilderCrossProductState()

	if o.filter != nil {
		o.filter.Init()
	}
}

func (o *mergeJoinBase) resetBuilderCrossProductState() {
	o.builderState.left.reset()
	o.builderState.right.reset()
}

// appendToBufferedGroup appends all the tuples from batch that are part of the
// same group as the ones in the buffered group that corresponds to the input
// source. This needs to happen when a group starts at the end of an input
// batch and can continue into the following batches.
func (o *mergeJoinBase) appendToBufferedGroup(
	input *mergeJoinInput, batch coldata.Batch, sel []uint16, groupStartIdx int, groupLength int,
) {
	bufferedGroup := o.proberState.lBufferedGroup
	if input == &o.right {
		bufferedGroup = o.proberState.rBufferedGroup
	}
	destStartIdx := bufferedGroup.length
	groupEndIdx := groupStartIdx + groupLength
	for cIdx, cType := range input.sourceTypes {
		o.allocator.Append(
			bufferedGroup.ColVec(cIdx),
			coldata.SliceArgs{
				ColType:     cType,
				Src:         batch.ColVec(cIdx),
				Sel:         sel,
				DestIdx:     destStartIdx,
				SrcStartIdx: uint64(groupStartIdx),
				SrcEndIdx:   uint64(groupEndIdx),
			},
		)
	}

	// We've added groupLength number of tuples to bufferedGroup, so we need to
	// adjust its length.
	bufferedGroup.length += uint64(groupLength)
	for _, v := range bufferedGroup.colVecs {
		if v.Type() == coltypes.Bytes {
			v.Bytes().UpdateOffsetsToBeNonDecreasing(bufferedGroup.length)
		}
	}
}

// setBuilderSourceToBatch sets the builder state to use groups from the
// circular group buffer and the batches from input. This happens when we have
// groups that are fully contained within a single input batch from each of the
// sources.
func (o *mergeJoinBase) setBuilderSourceToBatch() {
	o.builderState.lGroups, o.builderState.rGroups = o.groups.getGroups()
	o.builderState.lBatch = o.proberState.lBatch
	o.builderState.rBatch = o.proberState.rBatch
}

// initProberState sets the batches, lengths, and current indices to the right
// locations given the last iteration of the operator.
func (o *mergeJoinBase) initProberState(ctx context.Context) {
	// If this is the first batch or we're done with the current batch, get the
	// next batch.
	if o.proberState.lBatch == nil || (o.proberState.lLength != 0 && o.proberState.lIdx == o.proberState.lLength) {
		o.proberState.lIdx, o.proberState.lBatch = 0, o.left.source.Next(ctx)
		o.proberState.lLength = int(o.proberState.lBatch.Length())
	}
	if o.proberState.rBatch == nil || (o.proberState.rLength != 0 && o.proberState.rIdx == o.proberState.rLength) {
		o.proberState.rIdx, o.proberState.rBatch = 0, o.right.source.Next(ctx)
		o.proberState.rLength = int(o.proberState.rBatch.Length())
	}
	if o.proberState.lBufferedGroup.needToReset {
		o.proberState.lBufferedGroup.reset()
	}
	if o.proberState.rBufferedGroup.needToReset {
		o.proberState.rBufferedGroup.reset()
	}
}

// nonEmptyBufferedGroup returns true if there is a buffered group that needs
// to be finished.
func (o *mergeJoinBase) nonEmptyBufferedGroup() bool {
	return o.proberState.lBufferedGroup.length > 0 || o.proberState.rBufferedGroup.length > 0
}

// sourceFinished returns true if either of input sources has no more rows.
func (o *mergeJoinBase) sourceFinished() bool {
	return o.proberState.lLength == 0 || o.proberState.rLength == 0
}

// completeBufferedGroup extends the buffered group corresponding to input.
// First, we check that the first row in batch is still part of the same group.
// If this is the case, we use the Distinct operator to find the first
// occurrence in batch (or subsequent batches) that doesn't match the current
// group.
// NOTE: we will be buffering all batches until we find such non-matching tuple
// (or until we exhaust the input).
// TODO(yuzefovich): this can be refactored so that only the right side does
// unbounded buffering.
// SIDE EFFECT: can append to the buffered group corresponding to the source.
func (o *mergeJoinBase) completeBufferedGroup(
	ctx context.Context, input *mergeJoinInput, batch coldata.Batch, rowIdx int,
) (_ coldata.Batch, idx int, batchLength int) {
	batchLength = int(batch.Length())
	if o.isBufferedGroupFinished(input, batch, rowIdx) {
		return batch, rowIdx, batchLength
	}

	isBufferedGroupComplete := false
	input.distincter.(resetter).reset()
	// Ignore the first row of the distincter in the first pass since we already
	// know that we are in the same group and, thus, the row is not distinct,
	// regardless of what the distincter outputs.
	loopStartIndex := 1
	var sel []uint16
	for !isBufferedGroupComplete {
		// Note that we're not resetting the distincter on every loop iteration
		// because if we're doing the second, third, etc, iteration, then all the
		// previous iterations had only the matching tuples to the buffered group,
		// so the distincter - in a sense - compares the incoming tuples to the
		// first tuple of the first iteration (which we know is the same group).
		input.distincterInput.batch = batch
		input.distincter.Next(ctx)

		sel = batch.Selection()
		var groupLength int
		if sel != nil {
			for groupLength = loopStartIndex; groupLength < batchLength; groupLength++ {
				if input.distinctOutput[sel[groupLength]] {
					// We found the beginning of a new group!
					isBufferedGroupComplete = true
					break
				}
			}
		} else {
			for groupLength = loopStartIndex; groupLength < batchLength; groupLength++ {
				if input.distinctOutput[groupLength] {
					// We found the beginning of a new group!
					isBufferedGroupComplete = true
					break
				}
			}
		}

		// Zero out the distinct output for the next pass.
		copy(input.distinctOutput, zeroBoolColumn)
		loopStartIndex = 0

		// Buffer all the tuples that are part of the buffered group.
		o.appendToBufferedGroup(input, batch, sel, rowIdx, groupLength)
		rowIdx += groupLength

		if !isBufferedGroupComplete {
			// The buffered group is still not complete which means that we have
			// just appended all the tuples from batch to it, so we need to get a
			// fresh batch from the input.
			rowIdx, batch = 0, input.source.Next(ctx)
			batchLength = int(batch.Length())
			if batchLength == 0 {
				// The input has been exhausted, so the buffered group is now complete.
				isBufferedGroupComplete = true
			}
		}
	}

	return batch, rowIdx, batchLength
}

// finishProbe completes the buffered groups on both sides of the input.
func (o *mergeJoinBase) finishProbe(ctx context.Context) {
	o.proberState.lBatch, o.proberState.lIdx, o.proberState.lLength = o.completeBufferedGroup(
		ctx,
		&o.left,
		o.proberState.lBatch,
		o.proberState.lIdx,
	)
	o.proberState.rBatch, o.proberState.rIdx, o.proberState.rLength = o.completeBufferedGroup(
		ctx,
		&o.right,
		o.proberState.rBatch,
		o.proberState.rIdx,
	)
}
