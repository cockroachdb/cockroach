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
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
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

	// outer specifies whether an outer join is required over the input.
	//outer bool
}

// offsetOp is an operator that implements offset, returning everything
// after the first n tuples in its input.
type mergeJoinOp struct {
	left  mergeJoinInput
	right mergeJoinInput

	savedLeftBatch  ColBatch
	savedLeftIdx    int
	savedRightBatch ColBatch
	savedRightIdx   int

	savedOutput       ColBatch // for the cross product entries that don't fit in the current batch
	savedOutputEndIdx int
	out               ColBatch
	outputBatchSize   int

	lGroup []ColVec
}

// NewOffsetOp returns a new offset operator with the given offset.
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
	c.lGroup = make([]ColVec, len(c.left.outCols))
	for i, idx := range c.left.outCols {
		c.lGroup[i] = newMemColumn(c.left.sourceTypes[idx], ColBatchSize)
	}
}

func (c *mergeJoinOp) InitWithBatchSize(outBatchSize int) {
	outColTypes := make([]types.T, len(c.left.outCols)+len(c.right.outCols))
	for i, idx := range c.left.outCols {
		outColTypes[i] = c.left.sourceTypes[idx]
	}
	for i, idx := range c.right.outCols {
		outColTypes[len(c.left.outCols)+i] = c.right.sourceTypes[idx]
	}

	c.out = NewMemBatchWithSize(outColTypes, outBatchSize)
	c.left.source.Init()
	c.right.source.Init()
	c.outputBatchSize = outBatchSize
}

func (c *mergeJoinOp) getNextColBatch(isLeft bool) ColBatch {
	if isLeft {
		if c.savedLeftBatch != nil {
			return c.savedLeftBatch
		} else {
			return c.left.source.Next()
		}
	} else {
		if c.savedRightBatch != nil {
			return c.savedRightBatch
		} else {
			return c.right.source.Next()
		}
	}
}

func getValForIdx(colvec []int64, idx int, b ColBatch) int64 { // this function is probably hella slow but YOLO
	sel := b.Selection()
	if sel != nil {
		return colvec[sel[idx]]
	} else {
		return colvec[idx]
	}
}

func (c *mergeJoinOp) buildSavedOutput() uint16 {
	toAppend := uint16(c.savedOutputEndIdx)
	if toAppend > ColBatchSize {
		toAppend = ColBatchSize
	}
	for gi, idx := range c.left.outCols {
		c.out.ColVec(gi).AppendSlice(c.savedOutput.ColVec(gi), c.left.sourceTypes[idx], 0, 0, toAppend)
	}
	for gi, idx := range c.left.outCols {
		c.out.ColVec(len(c.left.outCols)+gi).AppendSlice(c.savedOutput.ColVec(len(c.left.outCols)+gi), c.right.sourceTypes[idx], 0, 0, toAppend)
	}

	if c.savedOutputEndIdx > int(toAppend) {
		for gi, idx := range c.left.outCols {
			c.savedOutput.ColVec(gi).Copy(c.savedOutput.ColVec(gi), uint64(toAppend), uint64(c.savedOutputEndIdx), c.left.sourceTypes[idx])
		}
		for gi, idx := range c.left.outCols {
			c.savedOutput.ColVec(len(c.left.outCols)+gi).Copy(c.savedOutput.ColVec(len(c.left.outCols)+gi), uint64(toAppend), uint64(c.savedOutputEndIdx), c.left.sourceTypes[idx])
		}
	}
	c.savedOutputEndIdx -= int(toAppend)
	return toAppend
}

func getValInNextBatch(idx int, length int, bat ColBatch, input *mergeJoinInput) (int, int, ColBatch) {
	bat = input.source.Next()
	length = int(bat.Length())
	idx = 0

	return idx, length, bat
}

func finishedProcessingABatchWithOutput(count uint16, i int, llength int, j int, rlength int) bool {
	return count == ColBatchSize || (count > 0 && (i == llength || j == rlength))
}

func (c *mergeJoinOp) saveRelevantBatchesToState(i int, llength int, lbat ColBatch, j int, rlength int, rbat ColBatch) {
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

func addSliceToColVec(dest ColVec, bat ColBatch, input *mergeJoinInput, inputIdx uint32, destinationStartIdx int, sourceStartIdx int, sourceEndIdx int) {
	sel := bat.Selection()
	if sel != nil {
		dest.AppendSliceWithSel(bat.ColVec(int(inputIdx)), input.sourceTypes[inputIdx], uint64(destinationStartIdx), uint16(sourceStartIdx), uint16(sourceEndIdx), sel)
	} else {
		dest.AppendSlice(bat.ColVec(int(inputIdx)), input.sourceTypes[inputIdx], uint64(destinationStartIdx), uint16(sourceStartIdx), uint16(sourceEndIdx))
	}
}

func (c *mergeJoinOp) Next() ColBatch {
	lbat := c.getNextColBatch(true)
	rbat := c.getNextColBatch(false)
	for {

		// return some/all of the saved output if it exists
		if c.savedOutputEndIdx > 0 {
			count := c.buildSavedOutput()
			c.out.SetLength(count)
			return c.out
		}

		llength := int(lbat.Length())
		rlength := int(rbat.Length())
		if llength == 0 || rlength == 0 { // the OR here is because one of the tables could be empty: TODO check for non inner joins
			c.out.SetLength(0)
			return c.out
		}

		lkeys := lbat.ColVec(int(c.left.eqCols[0])).Int64()
		rkeys := rbat.ColVec(int(c.right.eqCols[0])).Int64()

		count := uint16(0)
		for i, j := c.savedLeftIdx, c.savedRightIdx; i < llength && j < rlength && count < ColBatchSize; {
			if getValForIdx(lkeys, i, lbat) == getValForIdx(rkeys, j, rbat) { // match
				testval := getValForIdx(lkeys, i, lbat)

				// initialize the left group buffer
				lGroupEndIdx := 0

				// build the left group
				sliceStartIdx := i
				for llength > 0 && getValForIdx(lkeys, i, lbat) == testval {
					if i == llength-1 {
						for gi, idx := range c.left.outCols {
							addSliceToColVec(c.lGroup[gi], lbat, &c.left, idx, lGroupEndIdx, sliceStartIdx, llength)
						}
						lGroupEndIdx += int(llength - sliceStartIdx)
						lbat = c.left.source.Next()
						llength = int(lbat.Length())
						i = 0
						sliceStartIdx = 0
					} else {
						i++
					}
				}

				if llength > 0 { // handle the edge case of there being a run that doesn't end with the batch
					for gi, idx := range c.left.outCols {
						addSliceToColVec(c.lGroup[gi], lbat, &c.left, idx, lGroupEndIdx, sliceStartIdx, llength)
					}
				}

				lGroupEndIdx += int(i - sliceStartIdx)

				// as we scan through the right group, build out the cross product
				// currently, we evaluate the complete "run" of duplicates in the right table (and append to saved out) before breaking out
				for rlength > 0 && getValForIdx(rkeys, j, rbat) == testval {
					// match, so we cross multiply the current right element with the left group
					// toAppend is the min of the size of the left group or the remaining spots left in the output buffer
					toAppend := lGroupEndIdx
					if toAppend > ColBatchSize-int(count) {
						toAppend = ColBatchSize - int(count)
					}
					// add all the columns from the left table to the output
					for gi, idx := range c.left.outCols { // append to out
						c.out.ColVec(gi).AppendSlice(c.lGroup[gi], c.left.sourceTypes[idx], uint64(count), 0, uint16(toAppend))
						if toAppend != lGroupEndIdx { // append to saved out
							c.savedOutput.ColVec(gi).AppendSlice(c.lGroup[gi], c.left.sourceTypes[idx], uint64(c.savedOutputEndIdx), uint16(toAppend), uint16(lGroupEndIdx))
						}
					}
					// add all the columns from the right table to the output
					for gi, idx := range c.right.outCols {
						for x := 0; x < lGroupEndIdx; x++ { // duplicate the row lGroupEndIdx = (length of the left group) times
							if x < toAppend { //append to out
								addSliceToColVec(c.out.ColVec(gi+len(c.left.outCols)), rbat, &c.right, idx, int(count)+x, j, j+1)
							} else { // append to saved out
								addSliceToColVec(c.out.ColVec(gi+len(c.left.outCols)), rbat, &c.right, idx, c.savedOutputEndIdx+x, j, j+1)
							}
						}
					}
					count += uint16(toAppend)
					c.savedOutputEndIdx += lGroupEndIdx - toAppend

					// increment right finger
					if j == rlength-1 {
						rbat = c.right.source.Next()
						rlength = int(rbat.Length())
						j = 0
					} else {
						j++
					}
				}

			} else { // no match
				// default to incrementing left if we have mismatch
				if getValForIdx(lkeys, i, lbat) <= getValForIdx(rkeys, j, rbat) {
					i++
				} else {
					j++
				}
			}

			// avoid empty output batches
			if count == 0 {
				// but only do this check when we've reached the end of one of the colbatches (with no matches)
				if i == llength {
					i, llength, lbat = getValInNextBatch(i, llength, lbat, &c.left)
				}
				if j == rlength {
					j, rlength, rbat = getValInNextBatch(j, rlength, rbat, &c.right)
				}
			}

			// saved the current state of the batches being "worked on" if they aren't completed yet
			if finishedProcessingABatchWithOutput(count, i, llength, j, rlength) {
				c.saveRelevantBatchesToState(i, llength, lbat, j, rlength, rbat)
			}
		}

		c.out.SetLength(count)

		if count > 0 {
			return c.out
		}

	}
}

// Reset resets the offsetOp for another run. Primarily used for
// benchmarks.
func (c *mergeJoinOp) Reset() {
}
