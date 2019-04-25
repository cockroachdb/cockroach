// Copyright 2019 The Cockroach Authors.
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

package exec

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// group is an ADT representing a contiguous section of values.
type group struct {
	rowStartIdx int
	rowEndIdx   int
	// numRepeats is used when expanding each group into a cross product in the build phase.
	numRepeats int
	// toBuild is used in the build phase to determine the right output count. This field should
	// stay in sync with the builder over time.
	toBuild int
}

// mjBuilderState contains all the state required to execute in the build phase.
type mjBuilderState struct {
	// Fields to hold the input sources batches from which to build the cross products.
	lBatch coldata.Batch
	rBatch coldata.Batch

	// Fields to identify the groups in the input sources.
	lGroups   []group
	rGroups   []group
	groupsLen int

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
// loops to materialize the cross product. Useful for picking up where we left off.
type mjBuilderCrossProductState struct {
	colIdx         int
	groupsIdx      int
	curSrcStartIdx int
	numRepeatsIdx  int
	finished       bool
}

// mjProberState contains all the state required to execute in the probing phases.
type mjProberState struct {
	// Fields to save the "working" batches to state in between outputs.
	lBatch  coldata.Batch
	rBatch  coldata.Batch
	lIdx    int
	lLength int
	rIdx    int
	rLength int

	// Local buffer for the last left and right groups.
	// Used when the group ends with a batch and the group on each side needs to be saved to state
	// in order to be able to continue it in the next batch.
	lGroup       coldata.Batch
	lGroupEndIdx int
	rGroup       coldata.Batch
	rGroupEndIdx int

	// inputDone is a flag to indicate whether the merge joiner has reached the end
	// of input, and thus should wrap up execution.
	inputDone bool
}

// mjState represents the state of the merge joiner.
type mjState int

const (
	// mjEntry is the entry state of the merge joiner where all the batches and indices
	// are properly set, regardless if Next was called the first time or the 1000th time.
	// This state also routes into the correct state based on the prober state after setup.
	mjEntry mjState = iota

	// mjSourceFinished is the state in which one of the input sources has no more available
	// batches, thus signaling that the joiner should begin wrapping up execution by outputting
	// any remaining groups in state.
	mjSourceFinished

	// mjFinishGroup is the state in which the previous state resulted in a group that ended
	// with a batch. This state finishes that group and builds the output.
	mjFinishGroup

	// mjProbe is the main probing state in which the groups for the current batch are determined.
	mjProbe

	// mjBuild is the state in which the groups determined by the probing states are built, ie
	// materialized to the output member by creating the cross product.
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
	// direction corresponds to an equality column at the same location, ie the
	// direction of eqCols[x] is encoded at directions[x], or len(eqCols) == len(directions).
	directions []distsqlpb.Ordering_Column_Direction

	// sourceTypes specify the types of the input columns of the source table for
	// the merge joiner.
	sourceTypes []types.T

	// The distincter is used in the finishGroup phase, and is used only to determine
	// where the current group ends, in the case that the group ended with a batch.
	distincterInput feedOperator
	distincter      Operator
	distinctOutput  []bool

	// source specifies the input operator to the merge join.
	source Operator
}

// feedOperator is used to feed the distincter with input by manually setting the
// next batch.
type feedOperator struct {
	bat coldata.Batch
}

func (feedOperator) Init() {}

func (o *feedOperator) Next(context.Context) coldata.Batch {
	return o.bat
}

var _ Operator = &feedOperator{}

// mergeJoinOp is an operator that implements sort-merge join.
// It performs a merge on the left and right input sources, based on the equality
// columns, assuming both inputs are in sorted order.

// The merge join operator uses a probe and build approach to generate the join.
// What this means is that instead of going through and expanding the cross product
// row by row, the operator performs two passes.
// The first pass generates a list of groups of matching rows based on the equality
// column. A group is an ADT representing a contiguous set of rows that match on their
// equality columns. This group is represented as the starting row ordinal, the ending
// row ordinal, and the number of times this group is expanded/repeated.
// The second pass is where the groups and their associated cross products are
// materialized into the full output.

// TODO(georgeutsin): Add outer joins functionality and templating to support different equality types

// Two buffers are used, one for the group on the left table and one for the group on the
// right table. These buffers are only used if the group ends with a batch, to make sure
// that we don't miss any cross product entries while expanding the groups
// (leftGroups and rightGroups) when a group spans multiple batches.
type mergeJoinOp struct {
	left  mergeJoinInput
	right mergeJoinInput

	// Output buffer definition.
	output          coldata.Batch
	outputBatchSize uint16
	outputCeil      uint16

	// Local buffer for the "working" repeated groups.
	groups circularGroupsBuffer

	state        mjState
	proberState  mjProberState
	builderState mjBuilderState
}

var _ Operator = &mergeJoinOp{}

// NewMergeJoinOp returns a new merge join operator with the given spec.
func NewMergeJoinOp(
	left Operator,
	right Operator,
	leftOutCols []uint32,
	rightOutCols []uint32,
	leftTypes []types.T,
	rightTypes []types.T,
	leftOrdering []distsqlpb.Ordering_Column,
	rightOrdering []distsqlpb.Ordering_Column,
) (Operator, error) {
	lEqCols := make([]uint32, len(leftOrdering))
	lDirections := make([]distsqlpb.Ordering_Column_Direction, len(leftOrdering))
	for i, c := range leftOrdering {
		lEqCols[i] = c.ColIdx
		lDirections[i] = c.Direction
	}

	rEqCols := make([]uint32, len(rightOrdering))
	rDirections := make([]distsqlpb.Ordering_Column_Direction, len(rightOrdering))
	for i, c := range rightOrdering {
		rEqCols[i] = c.ColIdx
		rDirections[i] = c.Direction
	}

	c := &mergeJoinOp{
		left:  mergeJoinInput{source: left, outCols: leftOutCols, sourceTypes: leftTypes, eqCols: lEqCols, directions: lDirections},
		right: mergeJoinInput{source: right, outCols: rightOutCols, sourceTypes: rightTypes, eqCols: rEqCols, directions: rDirections},
	}

	var err error
	c.left.distincterInput = feedOperator{}
	c.left.distincter, c.left.distinctOutput, err = orderedDistinctColsToOperators(
		&c.left.distincterInput, lEqCols, leftTypes)
	if err != nil {
		return nil, err
	}
	c.right.distincterInput = feedOperator{}
	c.right.distincter, c.right.distinctOutput, err = orderedDistinctColsToOperators(
		&c.right.distincterInput, rEqCols, rightTypes)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (o *mergeJoinOp) Init() {
	o.initWithBatchSize(coldata.BatchSize)
}

func (o *mergeJoinOp) initWithBatchSize(outBatchSize uint16) {
	outColTypes := make([]types.T, len(o.left.sourceTypes)+len(o.right.sourceTypes))
	copy(outColTypes, o.left.sourceTypes)
	copy(outColTypes[len(o.left.sourceTypes):], o.right.sourceTypes)

	o.output = coldata.NewMemBatchWithSize(outColTypes, int(outBatchSize))
	o.left.source.Init()
	o.right.source.Init()
	o.outputBatchSize = outBatchSize
	o.outputCeil = o.outputBatchSize
	// If there are no output columns, then the operator is for a COUNT query,
	// in which case the ceiling for output is the max uint16.
	if o.output.Width() == 0 {
		o.outputCeil = 1<<16 - 1
	}

	o.proberState.lGroup = coldata.NewMemBatchWithSize(o.left.sourceTypes, coldata.BatchSize)
	o.proberState.rGroup = coldata.NewMemBatchWithSize(o.right.sourceTypes, coldata.BatchSize)

	o.builderState.lGroups = make([]group, 1)
	o.builderState.rGroups = make([]group, 1)

	o.groups = makeGroupsBuffer(coldata.BatchSize)
	o.proberState.inputDone = false
	o.resetBuilderCrossProductState()
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
	// Default the state of the builder to finished.
	zeroMJCPfinished = true
)

// Package level struct for easy access to the MJCP zero state.
var zeroMJBuilderState = mjBuilderCrossProductState{
	colIdx:         zeroMJCPcolIdx,
	groupsIdx:      zeroMJCPgroupsIdx,
	curSrcStartIdx: zeroMJCPcurSrcStartIdx,
	numRepeatsIdx:  zeroMJCPnumRepeatsIdx,
	finished:       zeroMJCPfinished,
}

func (o *mergeJoinOp) resetBuilderCrossProductState() {
	o.builderState.left.reset()
	o.builderState.right.reset()
}

func (s *mjBuilderCrossProductState) reset() {
	s.setBuilderColumnState(zeroMJBuilderState)
	s.colIdx = zeroMJCPcolIdx
	s.finished = zeroMJCPfinished
}

func (s *mjBuilderCrossProductState) setBuilderColumnState(target mjBuilderCrossProductState) {
	s.groupsIdx = target.groupsIdx
	s.curSrcStartIdx = target.curSrcStartIdx
	s.numRepeatsIdx = target.numRepeatsIdx
}

// getBatch takes a mergeJoinInput and returns either the next batch (from source),
// or the saved batch from state (if it exists).
func (o *mergeJoinOp) getBatch(ctx context.Context, input *mergeJoinInput) coldata.Batch {
	batch := o.proberState.lBatch

	if input == &o.right {
		batch = o.proberState.rBatch
	}

	if batch != nil {
		return batch
	}

	n := input.source.Next(ctx)
	return n
}

// calculateOutputCount uses the toBuild field of each group and the global output ceiling
// to determine the output count. Note that as soon as a group is materialized fully to output,
// its toBuild field is set to 0.
func (o *mergeJoinOp) calculateOutputCount(groups []group, groupsLen int) uint16 {
	count := int(o.builderState.outCount)

	for i := 0; i < groupsLen; i++ {
		count += groups[i].toBuild
		groups[i].toBuild = 0
		if count > int(o.outputCeil) {
			groups[i].toBuild = count - int(o.outputCeil)
			count = int(o.outputCeil)
			return uint16(count)
		}
	}
	o.builderState.outFinished = true
	return uint16(count)
}

// completeGroup extends the group in state given the source input.
// First we check that the first row in bat is still part of the same group.
// If this is the case, we use the Distinct operator to find the first occurrence
// in bat (or subsequent batches) that doesn't match the current group.
// SIDE EFFECT: extends the group in state corresponding to the source.
func (o *mergeJoinOp) completeGroup(
	ctx context.Context, input *mergeJoinInput, bat coldata.Batch, rowIdx int,
) (idx int, batch coldata.Batch, length int) {
	length = int(bat.Length())
	sel := bat.Selection()
	savedGroup := o.proberState.lGroup
	savedGroupIdx := &o.proberState.lGroupEndIdx
	if input == &o.right {
		savedGroup = o.proberState.rGroup
		savedGroupIdx = &o.proberState.rGroupEndIdx
	}

	if o.isGroupFinished(input, savedGroup, *savedGroupIdx, bat, rowIdx, sel) {
		return rowIdx, bat, length
	}

	isGroupComplete := false
	input.distincter.(resetter).reset()
	// Ignore the first row of the distincter in the first pass, since we already
	// know that we are in the same group and thus the row is not distinct,
	// regardless of what the distincter outputs.
	loopStartIndex := 1
	for !isGroupComplete {
		input.distincterInput.bat = bat
		input.distincter.Next(ctx)

		var groupLength int
		if sel != nil {
			for groupLength = loopStartIndex; groupLength < length; groupLength++ {
				if input.distinctOutput[sel[groupLength]] {
					// We found the beginning of a new group!
					isGroupComplete = true
					break
				}
			}
		} else {
			for groupLength = loopStartIndex; groupLength < length; groupLength++ {
				if input.distinctOutput[groupLength] {
					// We found the beginning of a new group!
					isGroupComplete = true
					break
				}
			}
		}

		// Zero out the distinct output for the next pass.
		copy(input.distinctOutput, zeroBoolVec)
		loopStartIndex = 0

		// Save the group to state.
		o.saveGroupToState(rowIdx, groupLength, bat, sel, input, savedGroup, savedGroupIdx)
		rowIdx += groupLength

		if !isGroupComplete {
			rowIdx, bat = 0, input.source.Next(ctx)
			length = int(bat.Length())
			if length == 0 {
				// The group is complete if there are no more batches left.
				break
			}
		}
	}

	return rowIdx, bat, length
}

// saveGroupToState puts each column of the batch in a group into state, to be able to build
// output from this set of rows later.
// SIDE EFFECT: increments destStartIdx by the groupLength.
func (o *mergeJoinOp) saveGroupToState(
	idx int,
	groupLength int,
	bat coldata.Batch,
	sel []uint16,
	src *mergeJoinInput,
	destBatch coldata.Batch,
	destStartIdx *int,
) {
	endIdx := idx + groupLength
	if sel != nil {
		for cIdx, cType := range src.sourceTypes {
			destBatch.ColVec(cIdx).AppendSliceWithSel(bat.ColVec(cIdx), cType, uint64(*destStartIdx), uint16(idx), uint16(endIdx), sel)
		}
	} else {
		for cIdx, cType := range src.sourceTypes {
			destBatch.ColVec(cIdx).AppendSlice(bat.ColVec(cIdx), cType, uint64(*destStartIdx), uint16(idx), uint16(endIdx))
		}
	}

	*destStartIdx += groupLength
}

// setBuilderSourceToBatch sets the builder state to use groups from the circular
// group buffer, and the batches from input.
func (o *mergeJoinOp) setBuilderSourceToBatch() {
	o.builderState.lGroups = o.groups.getLGroups()
	o.builderState.rGroups = o.groups.getRGroups()
	o.builderState.groupsLen = o.groups.getBufferLen()
	o.builderState.lBatch = o.proberState.lBatch
	o.builderState.rBatch = o.proberState.rBatch
}

// probe is where we generate the groups slices that are used in the build phase.
// We do this by first assuming that every row in both batches contributes to the
// cross product. Then, with every equality column, we filter out the rows that
// don't contribute to the cross product (ie they don't have a matching row on
// the other side in the case of an inner join), and set the correct cardinality.
// Note that in this phase, we do this for every group, except the last group in
// the batch.
func (o *mergeJoinOp) probe() {
	o.groups.reset(o.proberState.lIdx, o.proberState.lLength, o.proberState.rIdx, o.proberState.rLength)
	lSel := o.proberState.lBatch.Selection()
	rSel := o.proberState.rBatch.Selection()
	if lSel != nil {
		if rSel != nil {
			o.probeBodyLSeltrueRSeltrue()
		} else {
			o.probeBodyLSeltrueRSelfalse()
		}
	} else {
		if rSel != nil {
			o.probeBodyLSelfalseRSeltrue()
		} else {
			o.probeBodyLSelfalseRSelfalse()
		}
	}
}

// finishProbe completes the groups on both sides of the input.
func (o *mergeJoinOp) finishProbe(ctx context.Context) {
	o.proberState.lIdx, o.proberState.lBatch, o.proberState.lLength = o.completeGroup(ctx, &o.left, o.proberState.lBatch, o.proberState.lIdx)
	o.proberState.rIdx, o.proberState.rBatch, o.proberState.rLength = o.completeGroup(ctx, &o.right, o.proberState.rBatch, o.proberState.rIdx)
}

// setBuilderSourceToGroupBuffer sets the builder state to use the group that
// ended with a batch, with its source being the buffered rows in state.
func (o *mergeJoinOp) setBuilderSourceToGroupBuffer() {
	// The capacity of builder state lGroups and rGroups is always at least 1
	// given the init.
	o.builderState.lGroups = o.builderState.lGroups[:1]
	o.builderState.lGroups[0] = group{
		rowStartIdx: 0,
		rowEndIdx:   o.proberState.lGroupEndIdx,
		numRepeats:  o.proberState.rGroupEndIdx,
		toBuild:     o.proberState.lGroupEndIdx * o.proberState.rGroupEndIdx,
	}
	o.builderState.rGroups = o.builderState.rGroups[:1]
	o.builderState.rGroups[0] = group{
		rowStartIdx: 0,
		rowEndIdx:   o.proberState.rGroupEndIdx,
		numRepeats:  o.proberState.lGroupEndIdx,
		toBuild:     o.proberState.rGroupEndIdx * o.proberState.lGroupEndIdx,
	}
	o.builderState.groupsLen = 1

	o.builderState.lBatch = o.proberState.lGroup
	o.builderState.rBatch = o.proberState.rGroup

	o.proberState.lGroupEndIdx = 0
	o.proberState.rGroupEndIdx = 0
}

// build creates the cross product, and writes it to the output member.
func (o *mergeJoinOp) build() {
	if o.output.Width() != 0 {
		outStartIdx := o.builderState.outCount
		o.buildLeftGroups(o.builderState.lGroups, o.builderState.groupsLen, 0 /* colOffset */, &o.left, o.builderState.lBatch, outStartIdx)
		o.buildRightGroups(o.builderState.rGroups, o.builderState.groupsLen, len(o.left.sourceTypes), &o.right, o.builderState.rBatch, outStartIdx)
	}
	o.builderState.outCount = o.calculateOutputCount(o.builderState.lGroups, o.builderState.groupsLen)
}

// initProberState sets the batches, lengths, and current indices to
// the right locations given the last iteration of the operator.
func (o *mergeJoinOp) initProberState(ctx context.Context) {
	// If this isn't the first batch and we're done with the current batch, get the next batch.
	if o.proberState.lLength != 0 && o.proberState.lIdx == o.proberState.lLength {
		o.proberState.lIdx, o.proberState.lBatch = 0, o.left.source.Next(ctx)
	}
	if o.proberState.rLength != 0 && o.proberState.rIdx == o.proberState.rLength {
		o.proberState.rIdx, o.proberState.rBatch = 0, o.right.source.Next(ctx)
	}

	o.proberState.lBatch = o.getBatch(ctx, &o.left)
	o.proberState.rBatch = o.getBatch(ctx, &o.right)
	o.proberState.lLength = int(o.proberState.lBatch.Length())
	o.proberState.rLength = int(o.proberState.rBatch.Length())
}

// groupNeedsToBeFinished is syntactic sugar for the state machine, as it wraps the boolean
// logic to determine if there is a group in state that needs to be finished.
func (o *mergeJoinOp) groupNeedsToBeFinished() bool {
	return o.proberState.lGroupEndIdx > 0 || o.proberState.rGroupEndIdx > 0
}

// sourceFinished is syntactic sugar for the state machine, as it wraps the boolean logic
// to determine if either input source has no more rows.
func (o *mergeJoinOp) sourceFinished() bool {
	// TODO (georgeutsin): update this logic to be able to support joins other than INNER.
	return o.proberState.lLength == 0 || o.proberState.rLength == 0
}

func (o *mergeJoinOp) Next(ctx context.Context) coldata.Batch {
	for {
		switch o.state {
		case mjEntry:
			o.initProberState(ctx)

			if o.groupNeedsToBeFinished() {
				o.state = mjFinishGroup
				break
			}

			if o.sourceFinished() {
				o.state = mjSourceFinished
				break
			}

			o.state = mjProbe
		case mjSourceFinished:
			o.setBuilderSourceToGroupBuffer()
			o.proberState.inputDone = true
			o.state = mjBuild
		case mjFinishGroup:
			o.finishProbe(ctx)
			o.setBuilderSourceToGroupBuffer()
			o.state = mjBuild
		case mjProbe:
			o.probe()
			o.setBuilderSourceToBatch()
			o.state = mjBuild
		case mjBuild:
			o.build()

			// If both builders had output and they didn't finish at the same time, panic.
			if len(o.left.outCols) > 0 && len(o.right.outCols) > 0 && o.builderState.left.finished != o.builderState.right.finished {
				panic("unexpected builder state, both left and right should finish at the same time")
			}

			if o.builderState.left.finished && o.builderState.right.finished && o.builderState.outFinished {
				o.state = mjEntry
				o.builderState.outFinished = false
			}

			if o.proberState.inputDone || o.builderState.outCount == o.outputCeil {
				o.output.SetLength(o.builderState.outCount)
				// Reset builder out count.
				o.builderState.outCount = uint16(0)
				return o.output
			}
		default:
			panic(fmt.Sprintf("unexpected merge joiner state in Next: %v", o.state))
		}
	}
}
