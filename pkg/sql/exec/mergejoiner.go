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

import "github.com/cockroachdb/cockroach/pkg/sql/exec/types"

// side is an enum that allows for switching between the left and right
// input sources, useful for abstraction.
type side int

const (
	left  side = 0
	right side = 1
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
	savedLeftBatch  ColBatch
	savedLeftIdx    int
	savedRightBatch ColBatch
	savedRightIdx   int

	// Output overflow buffer definition for the cross product entries
	// that don't fit in the current batch.
	savedOutput       ColBatch
	savedOutputEndIdx int

	// Member to keep track of count overflow, in the case that getExpectedOutCount
	// returns a number that doesn't fit into a uint16.
	countOverflow uint64

	// Output buffer definition.
	output          ColBatch
	outputBatchSize int

	// Local buffer for the last left and right saved runs.
	// Used when the run ends with a batch and the run on each side needs to be saved to state
	// in order to be able to continue it in the next batch.
	lRun       ColBatch
	lRunEndIdx int
	rRun       ColBatch
	rRunEndIdx int
	matchVal   int64

	// Local buffer for the "working" repeated groups.
	leftGroups  []group
	rightGroups []group
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
	c.initWithBatchSize(ColBatchSize)
}

func (c *mergeJoinOp) initWithBatchSize(outBatchSize int) {
	outColTypes := make([]types.T, len(c.left.sourceTypes)+len(c.right.sourceTypes))
	copy(outColTypes, c.left.sourceTypes)
	copy(outColTypes[len(c.left.sourceTypes):], c.right.sourceTypes)

	c.output = NewMemBatchWithSize(outColTypes, outBatchSize)
	c.savedOutput = NewMemBatchWithSize(outColTypes, ColBatchSize)
	c.left.source.Init()
	c.right.source.Init()
	c.outputBatchSize = outBatchSize

	c.lRun = NewMemBatchWithSize(c.left.sourceTypes, ColBatchSize)
	c.rRun = NewMemBatchWithSize(c.right.sourceTypes, ColBatchSize)

	c.leftGroups = make([]group, ColBatchSize)
	c.rightGroups = make([]group, ColBatchSize)
}

// getBatch takes a side as input and returns either the next batch (from source),
// or the saved batch from state (if it exists).
func (c *mergeJoinOp) getBatch(s side) (ColBatch, []uint16) {
	batch := c.savedLeftBatch
	source := c.left.source

	if s == right {
		batch = c.savedRightBatch
		source = c.right.source
	}

	if batch != nil {
		return batch, batch.Selection()
	}

	n := source.Next()
	return n, n.Selection()
}

// nextBatch takes a mergeJoinInput and returns the next batch. Also returns other handy state
// such as the starting index (always 0) and selection vector.
func nextBatch(input *mergeJoinInput) (int, ColBatch, []uint16) {
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
	toAppend := uint16(c.savedOutputEndIdx)
	offset := len(c.left.sourceTypes)
	if toAppend > uint16(c.outputBatchSize) {
		toAppend = uint16(c.outputBatchSize)
	}

	for _, idx := range c.left.outCols {
		c.output.ColVec(int(idx)).AppendSlice(c.savedOutput.ColVec(int(idx)), c.left.sourceTypes[idx], 0 /* destStartIdx */, 0 /* srcStartIdx */, toAppend)
	}
	for _, idx := range c.right.outCols {
		c.output.ColVec(offset+int(idx)).AppendSlice(c.savedOutput.ColVec(offset+int(idx)), c.right.sourceTypes[idx], 0 /* destStartIdx */, 0 /* srcStartIdx */, toAppend)
	}

	if c.savedOutputEndIdx > int(toAppend) {
		for _, idx := range c.left.outCols {
			c.savedOutput.ColVec(int(idx)).Copy(c.savedOutput.ColVec(int(idx)), uint64(toAppend), uint64(c.savedOutputEndIdx), c.left.sourceTypes[idx])
		}
		for _, idx := range c.right.outCols {
			c.savedOutput.ColVec(offset+int(idx)).Copy(c.savedOutput.ColVec(offset+int(idx)), uint64(toAppend), uint64(c.savedOutputEndIdx), c.right.sourceTypes[idx])
		}
	}
	c.savedOutputEndIdx -= int(toAppend)
	return toAppend
}

// saveBatchesToState puts both "working" batches in state to have the ability to resume them
// in the next call to Next().
func (c *mergeJoinOp) saveBatchesToState(lIdx int, lBat ColBatch, rIdx int, rBat ColBatch) {
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

// getExpectedOutCount is a helper function to generate the right length of output,
// if there are no output columns. ex: in the case of a COUNT().
func (c *mergeJoinOp) getExpectedOutCount(groups []group, groupsLen int) uint16 {
	count := uint64(0)
	for i := 0; i < groupsLen; i++ {
		count += uint64((groups[i].rowEndIdx - groups[i].rowStartIdx) * groups[i].numRepeats)
	}

	// Add count to overflow if it is larger than a uint16.
	if count > (1<<16 - 1) {
		c.countOverflow = count - (1<<16 - 1)
		count = 1<<16 - 1
	}

	return uint16(count)
}

// saveRunToState puts each column of the batch in a run into state, to be able to build
// output from this run later.
// SIDE EFFECT: increments destStartIdx by the runLength.
func (c *mergeJoinOp) saveRunToState(
	idx int,
	runLength int,
	bat ColBatch,
	sel []uint16,
	src *mergeJoinInput,
	destBatch ColBatch,
	destStartIdx *int,
) {
	endIdx := idx + runLength
	if sel != nil {
		for _, cIdx := range src.outCols {
			destBatch.ColVec(int(cIdx)).AppendSliceWithSel(bat.ColVec(int(cIdx)), src.sourceTypes[cIdx], uint64(*destStartIdx), uint16(idx), uint16(endIdx), sel)
		}
	} else {
		for _, cIdx := range src.outCols {
			destBatch.ColVec(int(cIdx)).AppendSlice(bat.ColVec(int(cIdx)), src.sourceTypes[cIdx], uint64(*destStartIdx), uint16(idx), uint16(endIdx))
		}
	}

	*destStartIdx += runLength
}

// buildSavedRuns expands the left and right runs in state into their cross product, dumping it
// into the output buffer (and savedOutput overflow buffer if necessary).
func (c *mergeJoinOp) buildSavedRuns() uint16 {
	leftGroups := []group{{0, c.lRunEndIdx, c.rRunEndIdx}}
	rightGroups := []group{{0, c.rRunEndIdx, c.lRunEndIdx}}

	outCount, savedOutCount := c.buildLeftGroups(leftGroups, 1, 0, &c.left, c.lRun, nil, 0)
	c.buildRightGroups(rightGroups, 1, len(c.left.sourceTypes), &c.right, c.rRun, nil, 0)

	c.savedOutputEndIdx += savedOutCount
	c.lRunEndIdx = 0
	c.rRunEndIdx = 0

	return outCount
}

func (c *mergeJoinOp) Next() ColBatch {
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

	lBat, lSel := c.getBatch(left)
	rBat, rSel := c.getBatch(right)
	eqColIdx := 0
	lIdx, rIdx := c.savedLeftIdx, c.savedRightIdx

	for {
		outCount := uint16(0)

		lLength := int(lBat.Length())
		rLength := int(rBat.Length())

		// If one of the sources is finished, then build the runs and return.
		// The (lLength == 0 || rLength == 0) clause is specifically for inner joins.
		// TODO (georgeutsin): update this logic to be able to support joins other than INNER.
		if lLength == 0 || rLength == 0 {
			outCount = c.buildSavedRuns()
			c.output.SetLength(outCount)
			return c.output
		}

		lKeys := lBat.ColVec(int(c.left.eqCols[eqColIdx])).Int64()
		rKeys := rBat.ColVec(int(c.right.eqCols[eqColIdx])).Int64()

		// Phase 0: finish previous run if it exists (mini probe and build).
		if c.lRunEndIdx > 0 || c.rRunEndIdx > 0 {
			isLRunComplete := false
			for !isLRunComplete {
				// Find the length of the run.
				runLength, complete := getRunLengthForValue(lIdx, lLength, lKeys, lSel, c.matchVal)
				isLRunComplete = complete

				// Save the run to state.
				c.saveRunToState(lIdx, runLength, lBat, lSel, &c.left, c.lRun, &c.lRunEndIdx)
				lIdx += runLength

				// Get the next batch if we hit the end.
				if lIdx == lLength {
					lIdx, lBat, lSel = nextBatch(&c.left)
					lLength = int(lBat.Length())
					lKeys = lBat.ColVec(int(c.left.eqCols[eqColIdx])).Int64()
				}
			}

			isRRunComplete := false
			for !isRRunComplete {
				// Find the length of the run.
				runLength, complete := getRunLengthForValue(rIdx, rLength, rKeys, rSel, c.matchVal)
				isRRunComplete = complete

				// Save the run to state.
				c.saveRunToState(rIdx, runLength, rBat, rSel, &c.right, c.rRun, &c.rRunEndIdx)
				rIdx += runLength

				// Get the next batch if we hit the end.
				if rIdx == rLength {
					rIdx, rBat, rSel = nextBatch(&c.right)
					rLength = int(rBat.Length())
					rKeys = rBat.ColVec(int(c.right.eqCols[eqColIdx])).Int64()
				}
			}

			outCount = c.buildSavedRuns()
		}

		// Phase 1: probe.
		leftGroupsLen := 0
		rightGroupsLen := 0
		for lIdx < lLength && rIdx < rLength {
			lVal := getValForIdx(lKeys, lIdx, lSel)
			rVal := getValForIdx(rKeys, rIdx, rSel)

			if lVal == rVal { // match
				// Find the length of the runs on each side.
				lRunLength, _ := getRunLengthForValue(lIdx, lLength, lKeys, lSel, lVal)
				rRunLength, _ := getRunLengthForValue(rIdx, rLength, rKeys, rSel, rVal)

				// Either run ends with a batch. Save state and have it handled in the next iteration.
				if lRunLength+lIdx >= lLength || rRunLength+rIdx >= rLength {
					c.saveRunToState(lIdx, lRunLength, lBat, lSel, &c.left, c.lRun, &c.lRunEndIdx)
					lIdx += lRunLength
					c.saveRunToState(rIdx, rRunLength, rBat, rSel, &c.right, c.rRun, &c.rRunEndIdx)
					rIdx += rRunLength

					c.matchVal = lVal
					break
				}

				// Neither run ends with the batch so convert the runs to groups and increment the indices.
				c.leftGroups[leftGroupsLen] = group{lIdx, lIdx + lRunLength, rRunLength}
				leftGroupsLen++
				lIdx += lRunLength

				c.rightGroups[rightGroupsLen] = group{rIdx, rIdx + rRunLength, lRunLength}
				rightGroupsLen++
				rIdx += rRunLength
			} else { // mismatch
				if lVal < rVal {
					lIdx++
				} else {
					rIdx++
				}
			}
		}

		// Phase 2: build.
		rowOutCount, savedOutCount := c.buildLeftGroups(c.leftGroups, leftGroupsLen, 0 /* colOffset */, &c.left, lBat, lSel, outCount)
		c.buildRightGroups(c.rightGroups, rightGroupsLen, len(c.left.sourceTypes), &c.right, rBat, rSel, outCount)
		c.savedOutputEndIdx += savedOutCount
		outCount += rowOutCount

		// Avoid empty output batches.
		if outCount == 0 {
			// But only do this check when we've reached the end of one of the input batches (with no matches).
			if lIdx == lLength {
				lIdx, lBat, lSel = nextBatch(&c.left)
			}
			if rIdx == rLength {
				rIdx, rBat, rSel = nextBatch(&c.right)
			}
		}

		c.saveBatchesToState(lIdx, lBat, rIdx, rBat)

		c.output.SetLength(outCount)

		if outCount > 0 {
			return c.output
		}
	}
}
