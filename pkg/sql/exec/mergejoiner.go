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

type mergeJoinInput struct {
	// eqCols specify the indices of the source tables equality column during the
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

// The idea is to keep two main indices (one for the left column and one for the
// right), incrementing one or the other. If the value at the left index is <= the
// value at the right index, we increment the left index, otherwise we increment
// the right index. If there is a match between the left and right equality columns
// and there is a run of duplicate values, the left index is incremented until we
// hit the end of the run, saving the batch of columns into lGroup.
// When we finish a run, we increment the right index, creating a cross product with
// lGroup for each row until we hit a value that does not match anymore.

// If we overflow our outputBuffer (out) at any point, we use savedOutput to buffer
// the remaining rows.
// In this case, we also save the current state of our left and right input batches
// and the respective indices to the operator state to be able to resume in the
// next call to Next().
type mergeJoinOp struct {
	left  mergeJoinInput
	right mergeJoinInput

	// fields to save the "working" batches to state in between outputs
	savedLeftBatch  ColBatch
	savedLeftIdx    int
	savedRightBatch ColBatch
	savedRightIdx   int

	// output overflow buffer definition
	savedOutput       ColBatch // for the cross product entries that don't fit in the current batch
	savedOutputEndIdx int

	// output buffer definition
	out             ColBatch
	outputBatchSize int

	// local buffer for the cross product batch from the left column
	lGroup []ColVec
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

	c.lGroup = make([]ColVec, len(c.left.outCols))
	for i, idx := range c.left.outCols {
		c.lGroup[i] = newMemColumn(c.left.sourceTypes[idx], ColBatchSize)
	}
}

func (c *mergeJoinOp) getNextColBatch(s side) (ColBatch, []uint16) {
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

func getValForIdx(colvec []int64, idx int, sel []uint16) int64 {
	if sel != nil {
		return colvec[sel[idx]]
	}

	return colvec[idx]
}

func (c *mergeJoinOp) buildSavedOutput() uint16 {
	toAppend := uint16(c.savedOutputEndIdx)
	if toAppend > uint16(c.outputBatchSize) {
		toAppend = uint16(c.outputBatchSize)
	}
	for gi, idx := range c.left.outCols {
		c.out.ColVec(int(idx)).AppendSlice(c.savedOutput.ColVec(gi), c.left.sourceTypes[idx], 0, 0, toAppend)
	}
	for gi, idx := range c.left.outCols {
		c.out.ColVec(len(c.left.sourceTypes)+int(idx)).AppendSlice(c.savedOutput.ColVec(len(c.left.outCols)+gi), c.right.sourceTypes[idx], 0, 0, toAppend)
	}

	if c.savedOutputEndIdx > int(toAppend) {
		for gi, idx := range c.left.outCols {
			c.savedOutput.ColVec(int(idx)).Copy(c.savedOutput.ColVec(gi), uint64(toAppend), uint64(c.savedOutputEndIdx), c.left.sourceTypes[idx])
		}
		for gi, idx := range c.left.outCols {
			c.savedOutput.ColVec(len(c.left.sourceTypes)+int(idx)).Copy(c.savedOutput.ColVec(len(c.left.outCols)+gi), uint64(toAppend), uint64(c.savedOutputEndIdx), c.left.sourceTypes[idx])
		}
	}
	c.savedOutputEndIdx -= int(toAppend)
	return toAppend
}

func getValInNextBatch(input *mergeJoinInput) (int, int, ColBatch, []uint16) {
	bat := input.source.Next()
	sel := bat.Selection()

	return 0, int(bat.Length()), bat, sel
}

func (c *mergeJoinOp) finishedProcessingABatchWithOutput(
	count uint16, i int, llength int, j int, rlength int,
) bool {
	return count == uint16(c.outputBatchSize) || (count > 0 && (i == llength || j == rlength))
}

func (c *mergeJoinOp) saveRelevantBatchesToState(
	i int, llength int, lbat ColBatch, j int, rlength int, rbat ColBatch,
) {
	if i != llength {
		c.savedLeftIdx = i
		c.savedLeftBatch = lbat
	} else {
		c.savedLeftIdx = 0
		c.savedLeftBatch = nil
	}

	if j != rlength {
		c.savedRightIdx = j
		c.savedRightBatch = rbat
	} else {
		c.savedRightIdx = 0
		c.savedRightBatch = nil
	}
}

func addSliceToColVec(
	dest ColVec,
	bat ColBatch,
	sel []uint16,
	input *mergeJoinInput,
	inputIdx uint32,
	destinationStartIdx int,
	sourceStartIdx int,
	sourceEndIdx int,
) {
	if sel != nil {
		dest.AppendSliceWithSel(bat.ColVec(int(inputIdx)), input.sourceTypes[inputIdx], uint64(destinationStartIdx), uint16(sourceStartIdx), uint16(sourceEndIdx), sel)
	} else {
		dest.AppendSlice(bat.ColVec(int(inputIdx)), input.sourceTypes[inputIdx], uint64(destinationStartIdx), uint16(sourceStartIdx), uint16(sourceEndIdx))
	}
}

func (c *mergeJoinOp) Next() ColBatch {
	lbat, lsel := c.getNextColBatch(left)
	rbat, rsel := c.getNextColBatch(right)
	eqColIdx := 0
	for {

		// return some/all of the saved output if it exists
		if c.savedOutputEndIdx > 0 {
			count := c.buildSavedOutput()
			c.out.SetLength(count)
			return c.out
		}

		llength := int(lbat.Length())
		rlength := int(rbat.Length())
		if llength == 0 || rlength == 0 { // the OR here is because one of the tables could be empty: TODO (georgeutsin): check for non inner joins
			c.out.SetLength(0)
			return c.out
		}

		lkeys := lbat.ColVec(int(c.left.eqCols[eqColIdx])).Int64()
		rkeys := rbat.ColVec(int(c.right.eqCols[eqColIdx])).Int64()

		count := uint16(0)
		for i, j := c.savedLeftIdx, c.savedRightIdx; i < llength && j < rlength && count < uint16(c.outputBatchSize); {
			lval := getValForIdx(lkeys, i, lsel)
			rval := getValForIdx(rkeys, j, rsel)
			if lval == rval { // match
				testval := lval

				// initialize the left group buffer
				lGroupEndIdx := 0

				// build the left group
				sliceStartIdx := i
				for llength > 0 && getValForIdx(lkeys, i, lsel) == testval {
					if i == llength-1 {
						for gi, idx := range c.left.outCols {
							addSliceToColVec(c.lGroup[gi], lbat, lsel, &c.left, idx, lGroupEndIdx, sliceStartIdx, llength)
						}
						lGroupEndIdx += llength - sliceStartIdx
						i, llength, lbat, lsel = getValInNextBatch(&c.left)
						if llength > 0 { // TODO(georgeutsin): test that needs this is the benchmark, but unit tests pass without this
							lkeys = lbat.ColVec(int(c.left.eqCols[eqColIdx])).Int64() // TODO(georgeutsin): tests still passed without this line, add a test that exercises this line/fails without it
						}
						sliceStartIdx = 0
					} else {
						i++
					}
				}

				// handle the edge case of there being a run that doesn't end with the batch
				if llength > 0 { // check if left batch is valid, TODO(georgeutsin): this seems wrong, test that needs this is the benchmark
					for gi, idx := range c.left.outCols {
						addSliceToColVec(c.lGroup[gi], lbat, lsel, &c.left, idx, lGroupEndIdx, sliceStartIdx, i) // TODO(georgeutsin): unit tests passed when i was llength, add a test that exercises the edge case
					}
				}

				lGroupEndIdx += i - sliceStartIdx

				// as we scan through the right group, build out the cross product
				// currently, we evaluate the complete "run" of duplicates in the right table (and append to saved out) before breaking out
				for rlength > 0 && getValForIdx(rkeys, j, rsel) == testval {
					// match, so we cross multiply the current right element with the left group
					// toAppend is the min of the size of the left group or the remaining spots left in the output buffer
					toAppend := lGroupEndIdx
					if toAppend > c.outputBatchSize-int(count) {
						toAppend = c.outputBatchSize - int(count)
					}
					// add the entire left group to the output
					for gi, idx := range c.left.outCols { // append to out
						c.out.ColVec(int(idx)).CopyAt(c.lGroup[gi], uint64(count), 0, uint64(toAppend), c.left.sourceTypes[idx])
						if toAppend != lGroupEndIdx { // append overflow to saved out
							c.savedOutput.ColVec(int(idx)).AppendSlice(c.lGroup[gi], c.left.sourceTypes[idx], uint64(c.savedOutputEndIdx), uint16(toAppend), uint16(lGroupEndIdx))
						}
					}
					// add all the columns from the right table to the output
					for _, idx := range c.right.outCols {
						for x := 0; x < lGroupEndIdx; x++ { // duplicate the row lGroupEndIdx = (length of the left group) times
							if x < toAppend { //append to out
								addSliceToColVec(c.out.ColVec(int(idx)+len(c.left.sourceTypes)), rbat, rsel, &c.right, idx, int(count)+x, j, j+1)
							} else { // append overflow to saved out
								addSliceToColVec(c.savedOutput.ColVec(int(idx)+len(c.left.sourceTypes)), rbat, rsel, &c.right, idx, c.savedOutputEndIdx+(x-toAppend), j, j+1)
							}
						}
					}
					count += uint16(toAppend)
					c.savedOutputEndIdx += lGroupEndIdx - toAppend

					// increment right index
					if j == rlength-1 {
						j, rlength, rbat, rsel = getValInNextBatch(&c.right)
					} else {
						j++
					}
				}

			} else { // no match
				// default to incrementing left if we have mismatch
				if lval <= rval {
					i++
				} else {
					j++
				}
			}

			// avoid empty output batches
			if count == 0 {
				// but only do this check when we've reached the end of one of the colbatches (with no matches)
				if i == llength {
					i, llength, lbat, lsel = getValInNextBatch(&c.left)
				}
				if j == rlength {
					j, rlength, rbat, rsel = getValInNextBatch(&c.right)
				}
			}

			// saved the current state of the batches being "worked on" if they aren't completed yet
			if c.finishedProcessingABatchWithOutput(count, i, llength, j, rlength) {
				c.saveRelevantBatchesToState(i, llength, lbat, j, rlength, rbat)
			}
		}

		c.out.SetLength(count)

		if count > 0 {
			return c.out
		}

	}
}
