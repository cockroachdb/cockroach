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

// repeatedChunk is an ADT representing a run, and numRepeats is used when
// expanding each run into a cross product in the build phase.
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
// row by row, the operator performs two passes.
// The first pass generates a list of chunks of matching rows based on the equality
// column. A "chunk" is a ADT representing a run, or in other words, multiple
// repeated values. This run is represented as the starting row ordinal, the ending
// row ordinal, and the number of times this chunk is expanded/repeated.
// The second pass is where each of these chunks are either expanded or repeated
// into the output or savedOutput buffer, saving big on the type introspection.

// The current implementation only works on an inner join
// with a single equality column of Int64s.

// Two buffers are used, one for the run on the left table and one for the run on the
// right table. These buffers are only used if the run ends with a batch, to make sure
// that we don't miss any cross product entries while expanding the chunks
// (repeatedRows and repeatedGroups) when a run spans multiple batches.

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
	// Used when the run ends with a batch and the run on each side needs to be saved to state
	// in order to be able to continue it in the next batch.
	lRun       ColBatch
	lRunEndIdx int
	rRun       ColBatch
	rRunEndIdx int
	matchVal   int64

	// Local buffer for the "working" repeated chunks.
	repeatedGroups []repeatedChunk
	repeatedRows   []repeatedChunk
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
		lRunEndIdx:        0,
		rRunEndIdx:        0,
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

	c.lRun = NewMemBatchWithSize(c.left.sourceTypes, ColBatchSize)
	c.rRun = NewMemBatchWithSize(c.right.sourceTypes, ColBatchSize)

	c.repeatedGroups = make([]repeatedChunk, ColBatchSize)
	c.repeatedRows = make([]repeatedChunk, ColBatchSize)
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

// getValForIdx returns the value for comparison given a slice and an index.
// TODO(georgeutsin): template this to work for all types.
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
		c.out.ColVec(int(idx)).AppendSlice(c.savedOutput.ColVec(gi), c.left.sourceTypes[idx], 0 /* destStartIdx */, 0 /* srcStartIdx */, toAppend)
	}
	for gi, idx := range c.right.outCols {
		c.out.ColVec(len(c.left.sourceTypes)+int(idx)).AppendSlice(c.savedOutput.ColVec(len(c.left.outCols)+gi), c.right.sourceTypes[idx], 0 /* destStartIdx */, 0 /* srcStartIdx */, toAppend)
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

// saveBatchesToState puts both "working" batches in state to have the ability to resume them
// in the next call to Next().
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
// TODO (georgeutsin): exercise code path in unit tests (in addition to logic tests).
func (c *mergeJoinOp) getExpectedOutCount(repeatedRows []repeatedChunk) uint16 {
	count := uint16(0)
	for _, row := range repeatedRows {
		count += uint16(row.rowEndIdx-row.rowStartIdx) * uint16(row.numRepeats)
	}
	if int(count) > c.outputBatchSize {
		count = uint16(c.outputBatchSize) - c.out.Length()
	}

	return count
}

// addSliceToColVec is a helper function to abstract away the selection vector switch
// when appending a slice of one ColVec to another.
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

// saveRunToState puts each column of the batch in a run into state, to be able to build
// output from this run later.
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

// saveRunsToState saves each run to state and increments the current search indice to right after the run.
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
	c.saveRunToState(lIdx, lRunLength, lBat, lSel, &c.left, c.lRun, c.lRunEndIdx)
	c.saveRunToState(rIdx, rRunLength, rBat, rSel, &c.right, c.rRun, c.rRunEndIdx)

	lIdx += lRunLength
	c.lRunEndIdx += lRunLength
	rIdx += rRunLength
	c.rRunEndIdx += rRunLength

	return lIdx, rIdx
}

// continueProbingSavedRuns continues each run by finding the number of values that
// match the equality value of the current run in state.
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

// buildSavedRuns expands the left and right run in state into their crossproduct, dumping it
// into the out buffer (and savedOutput overflow buffer if necessary).
func (c *mergeJoinOp) buildSavedRuns() uint16 {
	repeatedRows := []repeatedChunk{{0, c.lRunEndIdx, c.rRunEndIdx}}
	repeatedGroups := []repeatedChunk{{0, c.rRunEndIdx, c.lRunEndIdx}}

	outCount, savedOutCount := c.buildRepeatedRows(repeatedRows, 1, 0, &c.left, c.lRun, nil, 0)
	c.buildRepeatedGroups(repeatedGroups, 1, len(c.left.sourceTypes), &c.right, c.rRun, nil, 0)

	c.savedOutputEndIdx += savedOutCount
	c.lRunEndIdx = 0
	c.rRunEndIdx = 0

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
		// TODO (georgeutsin): update this logic to be able to support joins other than INNER.
		if lLength == 0 || rLength == 0 {
			outCount = c.buildSavedRuns()
			c.out.SetLength(outCount)
			return c.out
		}

		lKeys := lBat.ColVec(int(c.left.eqCols[eqColIdx])).Int64()
		rKeys := rBat.ColVec(int(c.right.eqCols[eqColIdx])).Int64()

		// Phase 0: finish previous run if it exists (mini probe and build, only to continue the current run).
		if c.lRunEndIdx > 0 || c.rRunEndIdx > 0 {
			lIdx, rIdx = c.continueProbingSavedRuns(lIdx, lLength, lBat, lSel, lKeys, rIdx, rLength, rBat, rSel, rKeys)
			if lIdx != lLength && rIdx != rLength || (lLength == 0 || rLength == 0) {
				outCount = c.buildSavedRuns()
			}
		}

		// Phase 1: probe.
		repeatedGroupsIdx := 0
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

				c.repeatedRows[repeatedRowsIdx] = repeatedChunk{lIdx, lIdx + lRunLength, rRunLength}
				repeatedRowsIdx++
				lIdx += lRunLength

				c.repeatedGroups[repeatedGroupsIdx] = repeatedChunk{rIdx, rIdx + rRunLength, lRunLength}
				repeatedGroupsIdx++
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
		rowOutCount, savedOutCount := c.buildRepeatedRows(c.repeatedRows, repeatedRowsIdx, 0, &c.left, lBat, lSel, outCount)
		c.buildRepeatedGroups(c.repeatedGroups, repeatedGroupsIdx, len(c.left.sourceTypes), &c.right, rBat, rSel, outCount)
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
