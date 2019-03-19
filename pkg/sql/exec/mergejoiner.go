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
	// Fields hold the input sources batches from which to build the cross products.
	lBatch coldata.Batch
	rBatch coldata.Batch

	// Fields to identify the groups in the input sources.
	lGroups   []group
	rGroups   []group
	groupsLen int

	// outCount keeps record of the current number of rows in the output.
	outCount uint16
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

	// globallyFinished is a flag to indicate whether the merge joiner has reached the end
	// of input, and thus should wrap up execution.
	globallyFinished bool
}

// mjState represents the state of the merge joiner.
type mjState int

const (
	// mjSetup is the entry state of the merge joiner where all the batches and indices
	// are properly set, regardless if Next was called the first time or the 1000th time.
	// This state also routes into the correct state based on the prober state after setup.
	mjSetup mjState = iota

	// mjSourceFinished is the state in which one of the input sources has no more available
	// batches, thus signalling that the joiner should begin wrapping up execution by outputting
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

// The merge join operator uses a "two scan" approach to generate the join.
// What this means is that instead of going through and expanding the cross product
// row by row, the operator performs two passes.
// The first pass generates a list of groups of matching rows based on the equality
// column. A group is an ADT representing a contiguous set of rows that match on their
// equality columns. This group is represented as the starting row ordinal, the ending
// row ordinal, and the number of times this group is expanded/repeated.
// The second pass is where each of these groups are either expanded or repeated
// into the output or savedOutput buffer, saving big on the type introspection.

// TODO(georgeutsin): Add outer joins functionality and templating to support different equality types

// Two buffers are used, one for the group on the left table and one for the group on the
// right table. These buffers are only used if the group ends with a batch, to make sure
// that we don't miss any cross product entries while expanding the groups
// (leftGroups and rightGroups) when a group spans multiple batches.

// There is also a savedOutput buffer in the case that the cross product overflows
// the output buffer.
type mergeJoinOp struct {
	left  mergeJoinInput
	right mergeJoinInput

	// Output overflow buffer definition for the cross product entries
	// that don't fit in the current batch.
	savedOutput       coldata.Batch
	savedOutputEndIdx int

	// Member to keep track of count overflow, in the case that calculateOutputCount
	// returns a number that doesn't fit into a uint16.
	countOverflow uint64

	// Output buffer definition.
	output          coldata.Batch
	outputBatchSize uint16

	// Local buffer for the "working" repeated groups.
	groups circularGroupsBuffer

	// Merge joiner state
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

func (c *mergeJoinOp) Init() {
	c.initWithBatchSize(coldata.BatchSize)
}

func (c *mergeJoinOp) initWithBatchSize(outBatchSize uint16) {
	outColTypes := make([]types.T, len(c.left.sourceTypes)+len(c.right.sourceTypes))
	copy(outColTypes, c.left.sourceTypes)
	copy(outColTypes[len(c.left.sourceTypes):], c.right.sourceTypes)

	c.output = coldata.NewMemBatchWithSize(outColTypes, int(outBatchSize))
	c.savedOutput = coldata.NewMemBatchWithSize(outColTypes, coldata.BatchSize)
	c.left.source.Init()
	c.right.source.Init()
	c.outputBatchSize = outBatchSize

	c.proberState.lGroup = coldata.NewMemBatchWithSize(c.left.sourceTypes, coldata.BatchSize)
	c.proberState.rGroup = coldata.NewMemBatchWithSize(c.right.sourceTypes, coldata.BatchSize)

	c.groups = makeGroupsBuffer(coldata.BatchSize)
	c.proberState.globallyFinished = false
}

// getBatch takes a mergeJoinInput and returns either the next batch (from source),
// or the saved batch from state (if it exists).
func (c *mergeJoinOp) getBatch(input *mergeJoinInput) coldata.Batch {
	batch := c.proberState.lBatch

	if input == &c.right {
		batch = c.proberState.rBatch
	}

	if batch != nil {
		return batch
	}

	n := input.source.Next()
	return n
}

// nextBatch takes a mergeJoinInput and returns the next batch. Also returns other handy state
// such as the starting index (always 0) and selection vector.
func nextBatch(input *mergeJoinInput) (int, coldata.Batch) {
	bat := input.source.Next()

	return 0, bat
}

// getValForIdx returns the value for comparison given a slice and an index.
// TODO(georgeutsin): template this to work for all types.
func getValForIdx(keys []int64, idx int, sel []uint16) int64 {
	if sel != nil {
		return keys[sel[idx]]
	}

	return keys[idx]
}

// TODO (georgeutsin): remove saved output buffer and this associated function
// buildSavedOutput flushes the savedOutput to output.
func (c *mergeJoinOp) buildSavedOutput() uint16 {
	toAppend := c.savedOutputEndIdx
	offset := len(c.left.sourceTypes)
	if toAppend > int(c.outputBatchSize) {
		toAppend = int(c.outputBatchSize)
	}

	for _, idx := range c.left.outCols {
		c.output.ColVec(int(idx)).AppendSlice(c.savedOutput.ColVec(int(idx)), c.left.sourceTypes[idx], 0 /* destStartIdx */, 0 /* srcStartIdx */, uint16(toAppend))
	}
	for _, idx := range c.right.outCols {
		c.output.ColVec(offset+int(idx)).AppendSlice(c.savedOutput.ColVec(offset+int(idx)), c.right.sourceTypes[idx], 0 /* destStartIdx */, 0 /* srcStartIdx */, uint16(toAppend))
	}

	if c.savedOutputEndIdx > toAppend {
		for _, idx := range c.left.outCols {
			c.savedOutput.ColVec(int(idx)).Copy(c.savedOutput.ColVec(int(idx)), uint64(toAppend), uint64(c.savedOutputEndIdx), c.left.sourceTypes[idx])
		}
		for _, idx := range c.right.outCols {
			c.savedOutput.ColVec(offset+int(idx)).Copy(c.savedOutput.ColVec(offset+int(idx)), uint64(toAppend), uint64(c.savedOutputEndIdx), c.right.sourceTypes[idx])
		}
	}
	c.savedOutputEndIdx -= toAppend
	return uint16(toAppend)
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
func (c *mergeJoinOp) calculateOutputCount(
	groups []group, groupsLen int, curOutputCount uint16,
) uint16 {
	count := uint64(0)
	for i := 0; i < groupsLen; i++ {
		count += uint64((groups[i].rowEndIdx - groups[i].rowStartIdx) * groups[i].numRepeats)
	}

	// Add count to overflow if it is larger than a uint16.
	if count+uint64(curOutputCount) > (1<<16 - 1) {
		c.countOverflow += count + uint64(curOutputCount) - (1<<16 - 1)
		count = 1<<16 - 1
	} else {
		count = count + uint64(curOutputCount)
	}

	return uint16(count)
}

// completeGroup extends the group in state given the source input.
// To do this, we first check that the next batch is still the same group by checking all equality
// columns except for the last minor equality column. Then we complete the group by Next'ing input
// until the group is over.
// SIDE EFFECT: extends the group in state corresponding to the source.
func (c *mergeJoinOp) completeGroup(
	source *mergeJoinInput, bat coldata.Batch, rowIdx int,
) (idx int, batch coldata.Batch, length int) {
	isGroupComplete := false
	length = int(bat.Length())
	sel := bat.Selection()
	// The equality column idx is the last equality column.
	eqColIdx := int(source.eqCols[len(source.eqCols)-1])
	keys := bat.ColVec(eqColIdx).Int64()
	savedGroup := c.proberState.lGroup
	savedGroupIdx := &c.proberState.lGroupEndIdx
	if source == &c.right {
		savedGroup = c.proberState.rGroup
		savedGroupIdx = &c.proberState.rGroupEndIdx
	}

	// Check all equality columns before the last equality column to make sure we're in the same group.
	for i := 0; i < len(source.eqCols)-1; i++ {
		colIdx := source.eqCols[i]
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
		c.saveGroupToState(rowIdx, groupLength, bat, sel, source, savedGroup, savedGroupIdx)
		rowIdx += groupLength

		// Get the next batch if we hit the end.
		if rowIdx == length {
			rowIdx, bat = nextBatch(source)
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
func (c *mergeJoinOp) saveGroupToState(
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
func (c *mergeJoinOp) probe() {
	c.groups.reset(c.proberState.lIdx, c.proberState.lLength, c.proberState.rIdx, c.proberState.rLength)
	lSel := c.proberState.lBatch.Selection()
	rSel := c.proberState.rBatch.Selection()
EqLoop:
	for eqColIdx := 0; eqColIdx < len(c.left.eqCols); eqColIdx++ {
		lKeys := c.proberState.lBatch.ColVec(int(c.left.eqCols[eqColIdx])).Int64()
		rKeys := c.proberState.rBatch.ColVec(int(c.right.eqCols[eqColIdx])).Int64()
		// Iterate over all the groups in the column.
		var lGroup, rGroup group
		for c.groups.nextGroupInCol(&lGroup, &rGroup) {
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
					if eqColIdx == len(c.left.eqCols)-1 && (!lComplete || !rComplete) {
						c.saveGroupToState(curLIdx, lGroupLength, c.proberState.lBatch, lSel, &c.left, c.proberState.lGroup, &c.proberState.lGroupEndIdx)
						c.proberState.lIdx = lGroupLength + curLIdx
						c.saveGroupToState(curRIdx, rGroupLength, c.proberState.rBatch, rSel, &c.right, c.proberState.rGroup, &c.proberState.rGroupEndIdx)
						c.proberState.rIdx = rGroupLength + curRIdx

						c.groups.finishedCol()
						break EqLoop
					}

					// Neither group ends with the batch so add the group to the circular buffer and increment the indices.
					c.groups.addGroupsToNextCol(curLIdx, lGroupLength, curRIdx, rGroupLength)
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
			// Both c.proberState.lIdx and c.proberState.rIdx should point to the last elements processed in their respective batches.
			c.proberState.lIdx = curLIdx
			c.proberState.rIdx = curRIdx
		}
		// Look at the groups associated with the next equality column by moving the circular buffer pointer up.
		c.groups.finishedCol()
	}
}

// setBuilderSourceToBatch sets the builder state to use groups from the circular
// group buffer, and the batches from input.
func (c *mergeJoinOp) setBuilderSourceToBatch() {
	c.builderState.lGroups = c.groups.getLGroups()
	c.builderState.rGroups = c.groups.getRGroups()
	c.builderState.groupsLen = c.groups.getBufferLen()
	c.builderState.lBatch = c.proberState.lBatch
	c.builderState.rBatch = c.proberState.rBatch
}

// finishProbe completes the groups on both sides of the input.
func (c *mergeJoinOp) finishProbe() {
	c.proberState.lIdx, c.proberState.lBatch, c.proberState.lLength = c.completeGroup(&c.left, c.proberState.lBatch, c.proberState.lIdx)
	c.proberState.rIdx, c.proberState.rBatch, c.proberState.rLength = c.completeGroup(&c.right, c.proberState.rBatch, c.proberState.rIdx)
}

// setBuilderSourceToGroupBuffer sets the builder state to use the group that
// ended with a batch, with its source being the buffered rows in state.
func (c *mergeJoinOp) setBuilderSourceToGroupBuffer() {
	c.builderState.lGroups = []group{{0, c.proberState.lGroupEndIdx, c.proberState.rGroupEndIdx}}
	c.builderState.rGroups = []group{{0, c.proberState.rGroupEndIdx, c.proberState.lGroupEndIdx}}
	c.builderState.groupsLen = 1
	c.builderState.lBatch = c.proberState.lGroup
	c.builderState.rBatch = c.proberState.rGroup

	c.proberState.lGroupEndIdx = 0
	c.proberState.rGroupEndIdx = 0
}

// build creates the cross product, and writes it to the out member.
func (c *mergeJoinOp) build() {
	outStartIdx := c.builderState.outCount
	savedOutCount := 0
	c.builderState.outCount, savedOutCount = c.buildLeftGroups(c.builderState.lGroups, c.builderState.groupsLen, 0 /* colOffset */, &c.left, c.builderState.lBatch, outStartIdx)
	c.buildRightGroups(c.builderState.rGroups, c.builderState.groupsLen, len(c.left.sourceTypes), &c.right, c.builderState.rBatch, outStartIdx)
	c.savedOutputEndIdx += savedOutCount
}

// initProberState sets the batches, lengths, and current indices to
// the right locations given the last iteration of the operator.
func (c *mergeJoinOp) initProberState() {
	// If this isn't the first batch and we're done with the current batch, get the next batch.
	if c.proberState.lLength != 0 && c.proberState.lIdx == c.proberState.lLength {
		c.proberState.lIdx, c.proberState.lBatch = nextBatch(&c.left)
	}
	if c.proberState.rLength != 0 && c.proberState.rIdx == c.proberState.rLength {
		c.proberState.rIdx, c.proberState.rBatch = nextBatch(&c.right)
	}

	c.proberState.lBatch = c.getBatch(&c.left)
	c.proberState.rBatch = c.getBatch(&c.right)
	c.proberState.lLength = int(c.proberState.lBatch.Length())
	c.proberState.rLength = int(c.proberState.rBatch.Length())
}

// groupNeedsToBeFinished is syntactic sugar for the state machine, as it wraps the boolean
// logic to determine if there is a group in state that needs to be finished.
func (c *mergeJoinOp) groupNeedsToBeFinished() bool {
	return c.proberState.lGroupEndIdx > 0 || c.proberState.rGroupEndIdx > 0
}

// sourceFinished is syntactic sugar for the state machine, as it wraps the boolean logic
// to determine if either input source has no more rows.
func (c *mergeJoinOp) sourceFinished() bool {
	// TODO (georgeutsin): update this logic to be able to support joins other than INNER.
	return c.proberState.lLength == 0 || c.proberState.rLength == 0
}

func (c *mergeJoinOp) Next() coldata.Batch {
	// TODO (georgeutsin): remove the saved output buffer
	if c.savedOutputEndIdx > 0 {
		count := c.buildSavedOutput()
		c.output.SetLength(count)
		return c.output
	}

	if c.countOverflow > 0 {
		outCount := c.countOverflow
		if outCount > (1<<16 - 1) {
			outCount = 1<<16 - 1
		}
		c.countOverflow -= outCount
		c.output.SetLength(uint16(outCount))
		return c.output
	}

	for {
		switch c.state {
		case mjSetup:
			c.initProberState()

			if c.sourceFinished() {
				c.state = mjSourceFinished
				break
			}

			if c.groupNeedsToBeFinished() {
				c.state = mjFinishGroup
				break
			}

			c.state = mjProbe
			break
		case mjSourceFinished:
			c.setBuilderSourceToGroupBuffer()
			c.proberState.globallyFinished = true
			c.state = mjBuild
			break
		case mjFinishGroup:
			c.finishProbe()
			c.setBuilderSourceToGroupBuffer()
			c.state = mjBuild
			break
		case mjProbe:
			c.probe()
			c.setBuilderSourceToBatch()
			c.state = mjBuild
			break
		case mjBuild:
			c.build()

			// TODO (georgeutsin): flesh out this conditional (and builder state) to avoid buffering output.
			// if builder completed its current source {
			c.state = mjSetup
			// }
			if c.proberState.globallyFinished || c.builderState.outCount > 0 {
				c.output.SetLength(c.builderState.outCount)
				// Reset builder out count.
				c.builderState.outCount = uint16(0)
				return c.output
			}
			break
		default:
			panic(fmt.Sprintf("unexpected merge joiner state in Next: %v", c.state))
		}
	}
}
