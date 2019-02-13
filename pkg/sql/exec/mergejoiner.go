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

type side int

const (
	left  side = 0
	right side = 1
)

type repeatedChunk struct {
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
// row by row, the operator performs a first pass where it
// generates a list of chunks (denoted by the row ordinals) of matching rows based
// on the equality column, and then performs a second pass where each of these
// chunks are either expanded or repeated, saving big on the type introspection.

// The current implementation only works on an inner join
// with a single equality column of Int64s.

// Two buffers are used, one for the run on the left table and
// one for the run on the right table. These buffers are only used if the run ends
// with a batch, to make sure that we don't miss any cross product entries while
// expanding the chunks (repeatedRows and repeatedGroups) when a run
// spans multiple batches.

// There is also a savedOutput buffer in the case that the cross product overflows
// the out buffer.
type mergeJoinOp struct {
	left  mergeJoinInput
	right mergeJoinInput

	// Fields to save the "working" batches to state in between outputs.
	savedLeftBatch  ColBatch
	savedLeftIdx    int
	savedRightBatch ColBatch
	savedRightIdx   int

	// Output overflow buffer definition.
	savedOutput       ColBatch // for the cross product entries that don't fit in the current batch.
	savedOutputEndIdx int

	// Output buffer definition.
	out             ColBatch
	outputBatchSize int

	// Local buffer for the left and right saved runs.
	lGroup       ColBatch
	lGroupEndIdx int
	rGroup       ColBatch
	rGroupEndIdx int
	matchVal     int64
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
		left:              mergeJoinInput{source: left, outCols: leftOutCols, sourceTypes: leftTypes, eqCols: leftEqCols},
		right:             mergeJoinInput{source: right, outCols: rightOutCols, sourceTypes: rightTypes, eqCols: rightEqCols},
		savedLeftBatch:    nil,
		savedLeftIdx:      0,
		savedRightBatch:   nil,
		savedRightIdx:     0,
		savedOutputEndIdx: 0,
		lGroupEndIdx:      0,
		rGroupEndIdx:      0,
	}
	return c
}

func (c *mergeJoinOp) Init() {
	c.InitWithBatchSize(ColBatchSize)
}

func (c *mergeJoinOp) InitWithBatchSize(outBatchSize int) {
	outColTypes := make([]types.T, len(c.left.sourceTypes)+len(c.right.sourceTypes))
	copy(outColTypes, c.left.sourceTypes)
	copy(outColTypes[len(c.left.sourceTypes):], c.right.sourceTypes)

	c.out = NewMemBatchWithSize(outColTypes, outBatchSize)
	c.savedOutput = NewMemBatchWithSize(outColTypes, ColBatchSize)
	c.left.source.Init()
	c.right.source.Init()
	c.outputBatchSize = outBatchSize

	c.lGroup = NewMemBatchWithSize(c.left.sourceTypes, ColBatchSize)
	c.rGroup = NewMemBatchWithSize(c.right.sourceTypes, ColBatchSize)
}

// getBatch takes a side as input and returns either the next batch (from source),
// or the saved batch in state (if it exists).
func (c *mergeJoinOp) getBatch(s side) (ColBatch, []uint16) {
	if s == left {
		if c.savedLeftBatch != nil {
			return c.savedLeftBatch, c.savedLeftBatch.Selection()
		}

		n := c.left.source.Next()
		return n, n.Selection()
	}

	if c.savedRightBatch != nil {
		return c.savedRightBatch, c.savedRightBatch.Selection()
	}

	n := c.right.source.Next()
	return n, n.Selection()
}

// nextBatch takes a mergeJoinInput and returns the next batch. Also returns other handy state
// such as the starting index (always 0) and selection vector.
func nextBatch(input *mergeJoinInput) (int, ColBatch, []uint16) {
	bat := input.source.Next()
	sel := bat.Selection()

	return 0, bat, sel
}

// getValForIdx returns the value for comparison given a slice and an index
// TODO(georgeutsin): template this to work for all types
func getValForIdx(keys []int64, idx int, sel []uint16) int64 {
	if sel != nil {
		return keys[sel[idx]]
	}

	return keys[idx]
}

// buildSavedOutput flushes the savedOutput to out.
func (c *mergeJoinOp) buildSavedOutput() uint16 {
	toAppend := uint16(c.savedOutputEndIdx)
	if toAppend > uint16(c.outputBatchSize) {
		toAppend = uint16(c.outputBatchSize)
	}
	for gi, idx := range c.left.outCols {
		c.out.ColVec(int(idx)).AppendSlice(c.savedOutput.ColVec(gi), c.left.sourceTypes[idx], 0, 0, toAppend)
	}
	for gi, idx := range c.right.outCols {
		c.out.ColVec(len(c.left.sourceTypes)+int(idx)).AppendSlice(c.savedOutput.ColVec(len(c.left.outCols)+gi), c.right.sourceTypes[idx], 0, 0, toAppend)
	}

	if c.savedOutputEndIdx > int(toAppend) {
		for gi, idx := range c.left.outCols {
			c.savedOutput.ColVec(int(idx)).Copy(c.savedOutput.ColVec(gi), uint64(toAppend), uint64(c.savedOutputEndIdx), c.left.sourceTypes[idx])
		}
		for _, idx := range c.right.outCols {
			c.savedOutput.ColVec(len(c.left.sourceTypes)+int(idx)).Copy(c.savedOutput.ColVec(len(c.left.sourceTypes)+int(idx)), uint64(toAppend), uint64(c.savedOutputEndIdx), c.right.sourceTypes[idx])
		}
	}
	c.savedOutputEndIdx -= int(toAppend)
	return toAppend
}

func (c *mergeJoinOp) saveBatchesToState(lIdx int, lBat ColBatch, rIdx int, rBat ColBatch) {
	c.savedLeftIdx = lIdx
	c.savedLeftBatch = lBat

	c.savedRightIdx = rIdx
	c.savedRightBatch = rBat
}

// getRunCount is a helper function that gets the length of the current run in a batch.
func getRunCount(idx int, length int, keys []int64, sel []uint16, compVal int64) int {
	runLength := 0
	for idx < length && getValForIdx(keys, idx, sel) == compVal {
		runLength++
		idx++
	}
	return runLength
}

// getExpectedOutCount is a helper function to generate the right length of output,
// if there are no output columns. ex: in the case of a COUNT().
func (c *mergeJoinOp) getExpectedOutCount(repeatedRows []repeatedChunk) uint16 {
	count := c.out.Length()
	for _, row := range repeatedRows {
		count += uint16(row.rowEndIdx-row.rowStartIdx) * uint16(row.numRepeats)
	}
	if int(count) > c.outputBatchSize {
		count = uint16(c.outputBatchSize) - c.out.Length()
	}

	return count
}

func addSliceToColVec(
	dest ColVec,
	destStartIdx int,
	src ColVec,
	srcStartIdx int,
	srcEndIdx int,
	sel []uint16,
	colType types.T,
) {
	if sel != nil {
		dest.AppendSliceWithSel(src, colType, uint64(destStartIdx), uint16(srcStartIdx), uint16(srcEndIdx), sel)
	} else {
		dest.AppendSlice(src, colType, uint64(destStartIdx), uint16(srcStartIdx), uint16(srcEndIdx))
	}
}

func (c *mergeJoinOp) saveRunToState(
	idx int,
	runLength int,
	bat ColBatch,
	sel []uint16,
	src *mergeJoinInput,
	destBatch ColBatch,
	destStartIdx int,
) {
	for _, cIdx := range src.outCols {
		addSliceToColVec(destBatch.ColVec(int(cIdx)), destStartIdx, bat.ColVec(int(cIdx)), idx, idx+runLength, sel, src.sourceTypes[cIdx])
	}
}

func (c *mergeJoinOp) saveRunsToState(
	lIdx int,
	lRunLength int,
	lBat ColBatch,
	lSel []uint16,
	rIdx int,
	rRunLength int,
	rBat ColBatch,
	rSel []uint16,
) (int, int) {
	c.saveRunToState(lIdx, lRunLength, lBat, lSel, &c.left, c.lGroup, c.lGroupEndIdx)
	c.saveRunToState(rIdx, rRunLength, rBat, rSel, &c.right, c.rGroup, c.rGroupEndIdx)

	lIdx += lRunLength
	c.lGroupEndIdx += lRunLength
	rIdx += rRunLength
	c.rGroupEndIdx += rRunLength

	return lIdx, rIdx
}

func (c *mergeJoinOp) continueProbingSavedRuns(
	lIdx int,
	lLength int,
	lBat ColBatch,
	lSel []uint16,
	lKeys []int64,
	rIdx int,
	rLength int,
	rBat ColBatch,
	rSel []uint16,
	rKeys []int64,
) (int, int) {
	// Find the length of the runs on each side.
	lRunLength := getRunCount(lIdx, lLength, lKeys, lSel, c.matchVal)
	rRunLength := getRunCount(rIdx, rLength, rKeys, rSel, c.matchVal)
	// Save the runs to state.
	lIdx, rIdx = c.saveRunsToState(lIdx, lRunLength, lBat, lSel, rIdx, rRunLength, rBat, rSel)

	return lIdx, rIdx
}

func (c *mergeJoinOp) buildSavedRuns() uint16 {
	repeatedRows := []repeatedChunk{{0, c.lGroupEndIdx, c.rGroupEndIdx}}
	repeatedGroups := []repeatedChunk{{0, c.rGroupEndIdx, c.lGroupEndIdx}}

	outCount, savedOutCount := c.buildRepeatedRows(repeatedRows, 1, 0, &c.left, c.lGroup, nil, 0) // dump left cols
	c.buildRepeatedGroups(repeatedGroups, 1, len(c.left.sourceTypes), &c.right, c.rGroup, nil, 0)

	c.savedOutputEndIdx += savedOutCount
	c.lGroupEndIdx = 0
	c.rGroupEndIdx = 0

	return outCount
}

func (c *mergeJoinOp) Next() ColBatch {
	if c.savedOutputEndIdx > 0 {
		count := c.buildSavedOutput()
		c.out.SetLength(count)
		return c.out
	}

	lBat, lSel := c.getBatch(left)
	rBat, rSel := c.getBatch(right)
	eqColIdx := 0
	lIdx, rIdx := c.savedLeftIdx, c.savedRightIdx

	for {
		outCount := uint16(0)

		lLength := int(lBat.Length())
		rLength := int(rBat.Length())

		// The (lLength == 0 || rLength == 0) clause is specifically for inner joins.
		// TODO (georgeutsin): check for non inner joins
		if lLength == 0 || rLength == 0 {
			outCount = c.buildSavedRuns()
			c.out.SetLength(outCount)
			return c.out
		}

		lKeys := lBat.ColVec(int(c.left.eqCols[eqColIdx])).Int64()
		rKeys := rBat.ColVec(int(c.right.eqCols[eqColIdx])).Int64()

		// Phase 0: finish previous run if it exists (mini probe and build, only to continue the current run).
		if c.lGroupEndIdx > 0 || c.rGroupEndIdx > 0 {
			lIdx, rIdx = c.continueProbingSavedRuns(lIdx, lLength, lBat, lSel, lKeys, rIdx, rLength, rBat, rSel, rKeys)
			if lIdx != lLength && rIdx != rLength || (lLength == 0 || rLength == 0) {
				outCount = c.buildSavedRuns()
			}
		}

		// Phase 1: probe
		repeatedGroups := make([]repeatedChunk, ColBatchSize) // Its literally faster if I keep this here??
		repeatedGroupsIdx := 0
		repeatedRows := make([]repeatedChunk, ColBatchSize) // Its literally faster if I keep this here??
		repeatedRowsIdx := 0
		for lIdx < lLength && rIdx < rLength {
			lVal := getValForIdx(lKeys, lIdx, lSel)
			rVal := getValForIdx(rKeys, rIdx, rSel)

			if lVal == rVal { // match
				// Find the length of the runs on each side.
				lRunLength := getRunCount(lIdx, lLength, lKeys, lSel, lVal)
				rRunLength := getRunCount(rIdx, rLength, rKeys, rSel, rVal)

				// Either run ends with a batch. Save state and have it handled in the next iteration.
				if lRunLength+lIdx >= lLength || rRunLength+rIdx >= rLength {
					lIdx, rIdx = c.saveRunsToState(lIdx, lRunLength, lBat, lSel, rIdx, rRunLength, rBat, rSel)
					c.matchVal = lVal
					break
				}

				repeatedRows[repeatedRowsIdx] = repeatedChunk{lIdx, lIdx + lRunLength, rRunLength}
				repeatedRowsIdx++
				lIdx += lRunLength

				repeatedGroups[repeatedGroupsIdx] = repeatedChunk{rIdx, rIdx + rRunLength, lRunLength}
				repeatedGroupsIdx++
				rIdx += rRunLength
			} else { // mismatch
				if lVal <= rVal {
					lIdx++
				} else {
					rIdx++
				}
			}
		}

		// Phase 2: build
		rowOutCount, savedOutCount := c.buildRepeatedRows(repeatedRows, repeatedRowsIdx, 0, &c.left, lBat, lSel, outCount)
		c.buildRepeatedGroups(repeatedGroups, repeatedGroupsIdx, len(c.left.sourceTypes), &c.right, rBat, rSel, outCount)
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

		c.out.SetLength(outCount)

		if outCount > 0 {
			return c.out
		}
	}
}
