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
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// group is an ADT representing a run, and numRepeats is used when
// expanding each run into a cross product in the build phase.
// Note that a run becomes a group when it is finished and the number
// of times it is repeated in the cross product is determined.
type group struct {
	rowStartIdx int
	rowEndIdx   int
	numRepeats  int
}

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
// column. A group is an ADT representing a run, or in other words, multiple
// repeated values. This run is represented as the starting row ordinal, the ending
// row ordinal, and the number of times this group is expanded/repeated.
// The second pass is where each of these groups are either expanded or repeated
// into the output or savedOutput buffer, saving big on the type introspection.

// TODO(georgeutsin): Add outer joins functionality and templating to support different equality types

// Two buffers are used, one for the run on the left table and one for the run on the
// right table. These buffers are only used if the run ends with a batch, to make sure
// that we don't miss any cross product entries while expanding the groups
// (leftGroups and rightGroups) when a run spans multiple batches.

// There is also a savedOutput buffer in the case that the cross product overflows
// the output buffer.
type mergeJoinOp struct {
	left  mergeJoinInput
	right mergeJoinInput

	// Fields to save the "working" batches to state in between outputs.
	savedLeftBatch  coldata.Batch
	savedLeftIdx    int
	savedRightBatch coldata.Batch
	savedRightIdx   int

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

	// Local buffer for the last left and right saved runs.
	// Used when the run ends with a batch and the run on each side needs to be saved to state
	// in order to be able to continue it in the next batch.
	lRun       coldata.Batch
	lRunEndIdx int
	rRun       coldata.Batch
	rRunEndIdx int

	// Local buffer for the "working" repeated groups.
	groups circularGroupsBuffer
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

	c.lRun = coldata.NewMemBatchWithSize(c.left.sourceTypes, coldata.BatchSize)
	c.rRun = coldata.NewMemBatchWithSize(c.right.sourceTypes, coldata.BatchSize)

	c.groups = makeGroupsBuffer(coldata.BatchSize)
}

// getBatch takes a mergeJoinInput and returns either the next batch (from source),
// or the saved batch from state (if it exists).
func (c *mergeJoinOp) getBatch(input *mergeJoinInput) (coldata.Batch, []uint16) {
	batch := c.savedLeftBatch

	if input == &c.right {
		batch = c.savedRightBatch
	}

	if batch != nil {
		return batch, batch.Selection()
	}

	n := input.source.Next()
	return n, n.Selection()
}

// nextBatch takes a mergeJoinInput and returns the next batch. Also returns other handy state
// such as the starting index (always 0) and selection vector.
func nextBatch(input *mergeJoinInput) (int, coldata.Batch, []uint16) {
	bat := input.source.Next()
	sel := bat.Selection()

	return 0, bat, sel
}

// getValForIdx returns the value for comparison given a slice and an index.
// TODO(georgeutsin): template this to work for all types.
func getValForIdx(keys []int64, idx int, sel []uint16) int64 {
	if sel != nil {
		return keys[sel[idx]]
	}

	return keys[idx]
}

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

// saveBatchesToState puts both "working" batches in state to have the ability to resume them
// in the next call to Next().
func (c *mergeJoinOp) saveBatchesToState(
	lIdx int, lBat coldata.Batch, rIdx int, rBat coldata.Batch,
) {
	c.savedLeftIdx = lIdx
	c.savedLeftBatch = lBat

	c.savedRightIdx = rIdx
	c.savedRightBatch = rBat
}

// getRunLengthForValue is a helper function that gets the length of the current run in a batch
// starting at idx, given the comparison value. Also returns a boolean indicating whether the run
// is known to be complete.
func getRunLengthForValue(
	idx int, length int, keys []int64, sel []uint16, compVal int64,
) (int, bool) {
	if length == 0 {
		return 0, true
	}

	runLength := 0
	for idx < length {
		if getValForIdx(keys, idx, sel) != compVal {
			return runLength, true
		}
		runLength++
		idx++
	}
	return runLength, false
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

// completeRun extends the run in state given the source input.
// To do this, we first check that the next batch is still the same run by checking all equality
// columns except for the last minor equality column. Then we complete the run by Next'ing input
// until the run is over.
// SIDE EFFECT: extends the run in state corresponding to the source.
func (c *mergeJoinOp) completeRun(
	source *mergeJoinInput, bat coldata.Batch, rowIdx int,
) (idx int, batch coldata.Batch, length int, sel []uint16) {
	isRunComplete := false
	length = int(bat.Length())
	sel = bat.Selection()
	// The equality column idx is the last equality column.
	eqColIdx := int(source.eqCols[len(source.eqCols)-1])
	keys := bat.ColVec(eqColIdx).Int64()
	savedRun := c.lRun
	savedRunIdx := &c.lRunEndIdx
	if source == &c.right {
		savedRun = c.rRun
		savedRunIdx = &c.rRunEndIdx
	}

	// Check all equality columns before the last equality column to make sure we're in the same run.
	for i := 0; i < len(source.eqCols)-1; i++ {
		colIdx := source.eqCols[i]
		prevVal := getValForIdx(savedRun.ColVec(int(colIdx)).Int64(), *savedRunIdx-1, nil)
		curVal := getValForIdx(bat.ColVec(int(colIdx)).Int64(), rowIdx, sel)
		if prevVal != curVal {
			return rowIdx, bat, length, sel
		}
	}

	// Continue the saved run based on the last equality column.
	eqVal := getValForIdx(savedRun.ColVec(eqColIdx).Int64(), *savedRunIdx-1, nil)
	for !isRunComplete {
		// Find the length of the run.
		runLength, complete := getRunLengthForValue(rowIdx, length, keys, sel, eqVal)
		isRunComplete = complete

		// Save the run to state.
		c.saveRunToState(rowIdx, runLength, bat, sel, source, savedRun, savedRunIdx)
		rowIdx += runLength

		// Get the next batch if we hit the end.
		if rowIdx == length {
			rowIdx, bat, sel = nextBatch(source)
			length = int(bat.Length())
			if length == 0 {
				// The run is complete if there are no more batches left.
				break
			}
			keys = bat.ColVec(eqColIdx).Int64()
		}
	}

	return rowIdx, bat, length, sel
}

// saveRunToState puts each column of the batch in a run into state, to be able to build
// output from this run later.
// SIDE EFFECT: increments destStartIdx by the runLength.
func (c *mergeJoinOp) saveRunToState(
	idx int,
	runLength int,
	bat coldata.Batch,
	sel []uint16,
	src *mergeJoinInput,
	destBatch coldata.Batch,
	destStartIdx *int,
) {
	endIdx := idx + runLength
	if sel != nil {
		for cIdx, cType := range src.sourceTypes {
			destBatch.ColVec(cIdx).AppendSliceWithSel(bat.ColVec(cIdx), cType, uint64(*destStartIdx), uint16(idx), uint16(endIdx), sel)
		}
	} else {
		for cIdx, cType := range src.sourceTypes {
			destBatch.ColVec(cIdx).AppendSlice(bat.ColVec(cIdx), cType, uint64(*destStartIdx), uint16(idx), uint16(endIdx))
		}
	}

	*destStartIdx += runLength
}

// buildSavedRuns expands the left and right runs in state into their cross product, dumping it
// into the output buffer (and savedOutput overflow buffer if necessary).
func (c *mergeJoinOp) buildSavedRuns(outCount uint16) uint16 {
	leftGroups := []group{{0, c.lRunEndIdx, c.rRunEndIdx}}
	rightGroups := []group{{0, c.rRunEndIdx, c.lRunEndIdx}}

	rowOutCount, savedOutCount := c.buildLeftGroups(leftGroups, 1, 0, &c.left, c.lRun, nil, outCount)
	c.buildRightGroups(rightGroups, 1, len(c.left.sourceTypes), &c.right, c.rRun, nil, outCount)

	c.savedOutputEndIdx += savedOutCount
	c.lRunEndIdx = 0
	c.rRunEndIdx = 0

	return rowOutCount
}

func (c *mergeJoinOp) Next() coldata.Batch {
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

	lBat, lSel := c.getBatch(&c.left)
	rBat, rSel := c.getBatch(&c.right)
	lIdx, rIdx := c.savedLeftIdx, c.savedRightIdx

	for {
		outCount := uint16(0)

		lLength := int(lBat.Length())
		rLength := int(rBat.Length())

		// If one of the sources is finished, then build the runs and return.
		// The (lLength == 0 || rLength == 0) clause is specifically for inner joins.
		// TODO (georgeutsin): update this logic to be able to support joins other than INNER.
		if lLength == 0 || rLength == 0 {
			outCount = c.buildSavedRuns(outCount)
			c.output.SetLength(outCount)
			return c.output
		}

		// Phase 0: finish previous run if it exists (mini probe and build).
		if c.lRunEndIdx > 0 || c.rRunEndIdx > 0 {
			lIdx, lBat, lLength, lSel = c.completeRun(&c.left, lBat, lIdx)
			rIdx, rBat, rLength, rSel = c.completeRun(&c.right, rBat, rIdx)

			outCount = c.buildSavedRuns(outCount)
		}

		// Phase 1: probe.
		// In this phase, we generate the groups slices that are used in the build phase.
		// We do this by first assuming that every row in both batches contributes to the
		// cross product. Then, with every equality column, we filter out the rows that
		// don't contribute to the cross product (ie they don't have a matching row on
		// the other side in the case of an inner join), and set the correct cardinality.
		// Note that in this phase, we do this for every group, except the last group in
		// the batch. The last group in the batch is still considered a "run", since we
		// don't know if it is complete yet or not, so we handle it in the next iteration.

		c.groups.reset(lIdx, lLength, rIdx, rLength)
	EqLoop:
		for eqColIdx := 0; eqColIdx < len(c.left.eqCols); eqColIdx++ {
			lKeys := lBat.ColVec(int(c.left.eqCols[eqColIdx])).Int64()
			rKeys := rBat.ColVec(int(c.right.eqCols[eqColIdx])).Int64()
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
						// Find the length of the runs on each side.
						lRunLength, lComplete := getRunLengthForValue(curLIdx, curLLength, lKeys, lSel, lVal)
						rRunLength, rComplete := getRunLengthForValue(curRIdx, curRLength, rKeys, rSel, rVal)

						// Last equality column and either run is incomplete. Save state and have it handled in the next iteration.
						if eqColIdx == len(c.left.eqCols)-1 && (!lComplete || !rComplete) {
							c.saveRunToState(curLIdx, lRunLength, lBat, lSel, &c.left, c.lRun, &c.lRunEndIdx)
							lIdx = lRunLength + curLIdx
							c.saveRunToState(curRIdx, rRunLength, rBat, rSel, &c.right, c.rRun, &c.rRunEndIdx)
							rIdx = rRunLength + curRIdx

							c.groups.finishedCol()
							break EqLoop
						}

						// Neither run ends with the batch so convert the runs to groups and increment the indices.
						c.groups.addGroupsToNextCol(curLIdx, lRunLength, curRIdx, rRunLength)
						curLIdx += lRunLength
						curRIdx += rRunLength
					} else { // mismatch
						if lVal < rVal {
							curLIdx++
						} else {
							curRIdx++
						}
					}
				}
				// Both lIdx and rIdx should point to the last elements processed in their respective batches.
				lIdx = curLIdx
				rIdx = curRIdx

			}
			// Look at the groups associated with the next equality column by moving the circular buffer pointer up.
			c.groups.finishedCol()
		}

		// Phase 2: build.
		bufferLen := c.groups.getBufferLen()
		rowOutCount, savedOutCount := c.buildLeftGroups(c.groups.getLGroups(), bufferLen, 0 /* colOffset */, &c.left, lBat, lSel, outCount)
		c.buildRightGroups(c.groups.getRGroups(), bufferLen, len(c.left.sourceTypes), &c.right, rBat, rSel, outCount)
		c.savedOutputEndIdx += savedOutCount
		outCount = rowOutCount

		// Get the next batch if we're done with the current batch.
		if lIdx == lLength {
			lIdx, lBat, lSel = nextBatch(&c.left)
		}
		if rIdx == rLength {
			rIdx, rBat, rSel = nextBatch(&c.right)
		}

		c.saveBatchesToState(lIdx, lBat, rIdx, rBat)

		c.output.SetLength(outCount)

		if outCount > 0 {
			return c.output
		}
	}
}
