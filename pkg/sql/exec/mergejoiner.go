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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// group is an ADT representing a contiguous section of values, and
// numRepeats is used when expanding each group into a cross product in the build phase.
type group struct {
	rowStartIdx int
	rowEndIdx   int
	numRepeats  int
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
	// mjSetup is the entry state of the merge joiner where all the batches and indices
	// are properly set, regardless if Next was called the first time or the 1000th time.
	// This state also routes into the correct state based on the prober state after setup.
	mjSetup mjState = iota

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

	// sourceTypes specify the types of the input columns of the source table for
	// the merge joiner.
	sourceTypes []types.T

	// source specifies the input operator to the merge join.
	source Operator
}

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

	// Member to keep track of count overflow, in the case that calculateOutputCount
	// returns a number that doesn't fit into a uint16.
	countOverflow uint64

	// Output buffer definition.
	output          coldata.Batch
	outputBatchSize uint16

	// Local buffer for the "working" repeated groups.
	groups circularGroupsBuffer

	state        mjState
	proberState  mjProberState
	builderState mjBuilderState
}

// NewMergeJoinOp returns a new merge join operator with the given spec.
func NewMergeJoinOp(
	left Operator,
	right Operator,
	leftOutCols []uint32,
	rightOutCols []uint32,
	leftTypes []types.T,
	rightTypes []types.T,
	leftEqCols []uint32,
	rightEqCols []uint32,
) Operator {
	c := &mergeJoinOp{
		left:  mergeJoinInput{source: left, outCols: leftOutCols, sourceTypes: leftTypes, eqCols: leftEqCols},
		right: mergeJoinInput{source: right, outCols: rightOutCols, sourceTypes: rightTypes, eqCols: rightEqCols},
	}
	return c
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
func (o *mergeJoinOp) getBatch(input *mergeJoinInput) coldata.Batch {
	batch := o.proberState.lBatch

	if input == &o.right {
		batch = o.proberState.rBatch
	}

	if batch != nil {
		return batch
	}

	n := input.source.Next()
	return n
}

// getValForIdx returns the value for comparison given a slice and an index.
// TODO(georgeutsin): template this to work for all types.
func getValForIdx(keys []int64, idx int, sel []uint16) int64 {
	if sel != nil {
		return keys[sel[idx]]
	}

	return keys[idx]
}

// getGroupLengthForValue is a helper function that gets the length of the current group in a batch
// starting at idx, given the comparison value. Also returns a boolean indicating whether the group
// is known to be complete.
func getGroupLengthForValue(
	idx int, length int, keys []int64, sel []uint16, compVal int64,
) (int, bool) {
	if length == 0 {
		return 0, true
	}

	groupLength := 0
	for idx < length {
		if getValForIdx(keys, idx, sel) != compVal {
			return groupLength, true
		}
		groupLength++
		idx++
	}
	return groupLength, false
}

// calculateOutputCount is a helper function only exercised in the case that there are no output
// columns, ie the SQL statement was a SELECT COUNT. It uses the groups and the current output
// count to determine what the new output count is.
// SIDE EFFECT: if there is overflow in the output count, the overflow amount is added to
// countOverflow.
func (o *mergeJoinOp) calculateOutputCount(
	groups []group, groupsLen int, curOutputCount uint16,
) uint16 {
	count := uint64(0)
	for i := 0; i < groupsLen; i++ {
		count += uint64((groups[i].rowEndIdx - groups[i].rowStartIdx) * groups[i].numRepeats)
	}

	// Add count to overflow if it is larger than a uint16.
	if count+uint64(curOutputCount) > (1<<16 - 1) {
		o.countOverflow += count + uint64(curOutputCount) - (1<<16 - 1)
		count = 1<<16 - 1
	} else {
		count = count + uint64(curOutputCount)
	}

	return uint16(count)
}

// completeGroup extends the group in state given the source input.
// To do this, we first check that the next batch is still the same group by checking all equality
// columns except for the last equality column. Then we complete the group by Next'ing input
// until the group is over.
// SIDE EFFECT: extends the group in state corresponding to the source.
func (o *mergeJoinOp) completeGroup(
	input *mergeJoinInput, bat coldata.Batch, rowIdx int,
) (idx int, batch coldata.Batch, length int) {
	isGroupComplete := false
	length = int(bat.Length())
	sel := bat.Selection()
	// The equality column idx is the last equality column.
	eqColIdx := int(input.eqCols[len(input.eqCols)-1])
	keys := bat.ColVec(eqColIdx).Int64()
	savedGroup := o.proberState.lGroup
	savedGroupIdx := &o.proberState.lGroupEndIdx
	if input == &o.right {
		savedGroup = o.proberState.rGroup
		savedGroupIdx = &o.proberState.rGroupEndIdx
	}

	// Check all equality columns before the last equality column to make sure we're in the same group.
	for i := 0; i < len(input.eqCols)-1; i++ {
		colIdx := input.eqCols[i]
		prevVal := getValForIdx(savedGroup.ColVec(int(colIdx)).Int64(), *savedGroupIdx-1, nil)
		curVal := getValForIdx(bat.ColVec(int(colIdx)).Int64(), rowIdx, sel)
		if prevVal != curVal {
			return rowIdx, bat, length
		}
	}

	// Continue the group based on the last equality column.
	eqVal := getValForIdx(savedGroup.ColVec(eqColIdx).Int64(), *savedGroupIdx-1, nil)
	for !isGroupComplete {
		// Find the length of the group.
		groupLength, complete := getGroupLengthForValue(rowIdx, length, keys, sel, eqVal)
		isGroupComplete = complete

		// Save the group to state.
		o.saveGroupToState(rowIdx, groupLength, bat, sel, input, savedGroup, savedGroupIdx)
		rowIdx += groupLength

		// Get the next batch if we hit the end.
		if rowIdx == length {
			rowIdx, bat = 0, input.source.Next()
			sel = bat.Selection()
			length = int(bat.Length())
			if length == 0 {
				// The group is complete if there are no more batches left.
				break
			}
			keys = bat.ColVec(eqColIdx).Int64()
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
EqLoop:
	for eqColIdx := 0; eqColIdx < len(o.left.eqCols); eqColIdx++ {
		lKeys := o.proberState.lBatch.ColVec(int(o.left.eqCols[eqColIdx])).Int64()
		rKeys := o.proberState.rBatch.ColVec(int(o.right.eqCols[eqColIdx])).Int64()
		// Iterate over all the groups in the column.
		var lGroup, rGroup group
		for o.groups.nextGroupInCol(&lGroup, &rGroup) {
			curLIdx := lGroup.rowStartIdx
			curRIdx := rGroup.rowStartIdx
			curLLength := lGroup.rowEndIdx
			curRLength := rGroup.rowEndIdx
			// Expand or filter each group based on the current equality column.
			for curLIdx < curLLength && curRIdx < curRLength {
				lVal := getValForIdx(lKeys, curLIdx, lSel)
				rVal := getValForIdx(rKeys, curRIdx, rSel)

				if lVal == rVal { // match
					// Find the length of the groups on each side.
					lGroupLength, lComplete := getGroupLengthForValue(curLIdx, curLLength, lKeys, lSel, lVal)
					rGroupLength, rComplete := getGroupLengthForValue(curRIdx, curRLength, rKeys, rSel, rVal)

					// Last equality column and either group is incomplete. Save state and have it handled in the next iteration.
					if eqColIdx == len(o.left.eqCols)-1 && (!lComplete || !rComplete) {
						o.saveGroupToState(curLIdx, lGroupLength, o.proberState.lBatch, lSel, &o.left, o.proberState.lGroup, &o.proberState.lGroupEndIdx)
						o.proberState.lIdx = lGroupLength + curLIdx
						o.saveGroupToState(curRIdx, rGroupLength, o.proberState.rBatch, rSel, &o.right, o.proberState.rGroup, &o.proberState.rGroupEndIdx)
						o.proberState.rIdx = rGroupLength + curRIdx

						o.groups.finishedCol()
						break EqLoop
					}

					// Neither group ends with the batch so add the group to the circular buffer and increment the indices.
					o.groups.addGroupsToNextCol(curLIdx, lGroupLength, curRIdx, rGroupLength)
					curLIdx += lGroupLength
					curRIdx += rGroupLength
				} else { // mismatch
					if lVal < rVal {
						curLIdx++
					} else {
						curRIdx++
					}
				}
			}
			// Both o.proberState.lIdx and o.proberState.rIdx should point to the last elements processed in their respective batches.
			o.proberState.lIdx = curLIdx
			o.proberState.rIdx = curRIdx
		}
		// Look at the groups associated with the next equality column by moving the circular buffer pointer up.
		o.groups.finishedCol()
	}
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

// finishProbe completes the groups on both sides of the input.
func (o *mergeJoinOp) finishProbe() {
	o.proberState.lIdx, o.proberState.lBatch, o.proberState.lLength = o.completeGroup(&o.left, o.proberState.lBatch, o.proberState.lIdx)
	o.proberState.rIdx, o.proberState.rBatch, o.proberState.rLength = o.completeGroup(&o.right, o.proberState.rBatch, o.proberState.rIdx)
}

// setBuilderSourceToGroupBuffer sets the builder state to use the group that
// ended with a batch, with its source being the buffered rows in state.
func (o *mergeJoinOp) setBuilderSourceToGroupBuffer() {
	// The capacity of builder state lGroups and rGroups is always at least 1
	// given the init.
	o.builderState.lGroups = o.builderState.lGroups[:1]
	o.builderState.lGroups[0] = group{0, o.proberState.lGroupEndIdx, o.proberState.rGroupEndIdx}
	o.builderState.rGroups = o.builderState.rGroups[:1]
	o.builderState.rGroups[0] = group{0, o.proberState.rGroupEndIdx, o.proberState.lGroupEndIdx}
	o.builderState.groupsLen = 1

	o.builderState.lBatch = o.proberState.lGroup
	o.builderState.rBatch = o.proberState.rGroup

	o.proberState.lGroupEndIdx = 0
	o.proberState.rGroupEndIdx = 0
}

// build creates the cross product, and writes it to the output member.
func (o *mergeJoinOp) build() {
	outStartIdx := o.builderState.outCount
	o.builderState.outCount = o.buildLeftGroups(o.builderState.lGroups, o.builderState.groupsLen, 0 /* colOffset */, &o.left, o.builderState.lBatch, outStartIdx)
	o.buildRightGroups(o.builderState.rGroups, o.builderState.groupsLen, len(o.left.sourceTypes), &o.right, o.builderState.rBatch, outStartIdx)
}

// initProberState sets the batches, lengths, and current indices to
// the right locations given the last iteration of the operator.
func (o *mergeJoinOp) initProberState() {
	// If this isn't the first batch and we're done with the current batch, get the next batch.
	if o.proberState.lLength != 0 && o.proberState.lIdx == o.proberState.lLength {
		o.proberState.lIdx, o.proberState.lBatch = 0, o.left.source.Next()
	}
	if o.proberState.rLength != 0 && o.proberState.rIdx == o.proberState.rLength {
		o.proberState.rIdx, o.proberState.rBatch = 0, o.right.source.Next()
	}

	o.proberState.lBatch = o.getBatch(&o.left)
	o.proberState.rBatch = o.getBatch(&o.right)
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

func (o *mergeJoinOp) Next() coldata.Batch {
	if o.countOverflow > 0 {
		outCount := o.countOverflow
		if outCount > (1<<16 - 1) {
			outCount = 1<<16 - 1
		}
		o.countOverflow -= outCount
		o.output.SetLength(uint16(outCount))
		return o.output
	}

	for {
		switch o.state {
		case mjSetup:
			o.initProberState()

			if o.sourceFinished() {
				o.state = mjSourceFinished
				break
			}

			if o.groupNeedsToBeFinished() {
				o.state = mjFinishGroup
				break
			}

			o.state = mjProbe
		case mjSourceFinished:
			o.setBuilderSourceToGroupBuffer()
			o.proberState.inputDone = true
			o.state = mjBuild
		case mjFinishGroup:
			o.finishProbe()
			o.setBuilderSourceToGroupBuffer()
			o.state = mjBuild
		case mjProbe:
			o.probe()
			o.setBuilderSourceToBatch()
			o.state = mjBuild
		case mjBuild:
			o.build()

			if o.builderState.left.finished != o.builderState.right.finished {
				panic("unexpected builder state, both left and right should finish at the same time")
			}

			if o.builderState.left.finished && o.builderState.right.finished {
				o.state = mjSetup
			}
			if o.proberState.inputDone || o.builderState.outCount == o.outputBatchSize {
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
