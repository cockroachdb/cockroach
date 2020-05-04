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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type caseOp struct {
	allocator *colmem.Allocator
	buffer    *bufferOp

	caseOps []colexecbase.Operator
	elseOp  colexecbase.Operator

	thenIdxs  []int
	outputIdx int
	typ       *types.T

	// origSel is a buffer used to keep track of the original selection vector of
	// the input batch. We need to do this because we're going to destructively
	// modify the selection vector in order to do the work of the case statement.
	origSel []int
	// prevSel is a buffer used to keep track of the selection vector before
	// running a case arm (i.e. "previous to the current case arm"). We need to
	// keep track of it because case arm will modify the selection vector of the
	// batch, and then we need to figure out which tuples have not been matched
	// by the current case arm (those present in the "previous" sel and not
	// present in the "current" sel).
	prevSel []int
}

var _ InternalMemoryOperator = &caseOp{}

func (c *caseOp) ChildCount(verbose bool) int {
	return 1 + len(c.caseOps) + 1
}

func (c *caseOp) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		return c.buffer
	} else if nth < len(c.caseOps)+1 {
		return c.caseOps[nth-1]
	} else if nth == 1+len(c.caseOps) {
		return c.elseOp
	}
	colexecerror.InternalError(fmt.Sprintf("invalid idx %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

func (c *caseOp) InternalMemoryUsage() int {
	// We internally use two selection vectors, origSel and prevSel.
	return 2 * colmem.SizeOfBatchSizeSelVector
}

// NewCaseOp returns an operator that runs a case statement.
// buffer is a bufferOp that will return the input batch repeatedly.
// caseOps is a list of operator chains, one per branch in the case statement.
//   Each caseOp is connected to the input buffer op, and filters the input based
//   on the case arm's WHEN condition, and then projects the remaining selected
//   tuples based on the case arm's THEN condition.
// elseOp is the ELSE condition.
// whenCol is the index into the input batch to read from.
// thenCol is the index into the output batch to write to.
// typ is the type of the CASE expression.
func NewCaseOp(
	allocator *colmem.Allocator,
	buffer colexecbase.Operator,
	caseOps []colexecbase.Operator,
	elseOp colexecbase.Operator,
	thenIdxs []int,
	outputIdx int,
	typ *types.T,
) colexecbase.Operator {
	return &caseOp{
		allocator: allocator,
		buffer:    buffer.(*bufferOp),
		caseOps:   caseOps,
		elseOp:    elseOp,
		thenIdxs:  thenIdxs,
		outputIdx: outputIdx,
		typ:       typ,
		origSel:   make([]int, coldata.BatchSize()),
		prevSel:   make([]int, coldata.BatchSize()),
	}
}

func (c *caseOp) Init() {
	for i := range c.caseOps {
		c.caseOps[i].Init()
	}
	c.elseOp.Init()
}

func (c *caseOp) Next(ctx context.Context) coldata.Batch {
	c.buffer.advance(ctx)
	origLen := c.buffer.batch.Length()
	if origLen == 0 {
		return coldata.ZeroBatch
	}
	var origHasSel bool
	if sel := c.buffer.batch.Selection(); sel != nil {
		origHasSel = true
		copy(c.origSel, sel)
	}

	prevLen := origLen
	prevHasSel := false
	if sel := c.buffer.batch.Selection(); sel != nil {
		prevHasSel = true
		c.prevSel = c.prevSel[:origLen]
		copy(c.prevSel[:origLen], sel[:origLen])
	}
	outputCol := c.buffer.batch.ColVec(c.outputIdx)
	if outputCol.MaybeHasNulls() {
		// We need to make sure that there are no left over null values in the
		// output vector.
		// Note: technically, this is not necessary because we're using
		// Vec.Copy method when populating the output vector which itself
		// handles the null values, but we want to be on the safe side, so we
		// have this (at the moment) redundant resetting behavior.
		outputCol.Nulls().UnsetNulls()
	}
	c.allocator.PerformOperation([]coldata.Vec{outputCol}, func() {
		for i := range c.caseOps {
			// Run the next case operator chain. It will project its THEN expression
			// for all tuples that matched its WHEN expression and that were not
			// already matched.
			batch := c.caseOps[i].Next(ctx)
			// The batch's projection column now additionally contains results for all
			// of the tuples that passed the ith WHEN clause. The batch's selection
			// vector is set to the same selection of tuples.
			// Now, we must subtract this selection vector from the previous
			// selection vector, so that the next operator gets to operate on the
			// remaining set of tuples in the input that haven't matched an arm of the
			// case statement.
			// As an example, imagine the first WHEN op matched tuple 3. The following
			// diagram shows the selection vector before running WHEN, after running
			// WHEN, and then the desired selection vector after subtraction:
			// - origSel
			// | - selection vector after running WHEN
			// | | - desired selection vector after subtraction
			// | | |
			// 1   1
			// 2   2
			// 3 3
			// 4   4
			toSubtract := batch.Selection()
			toSubtract = toSubtract[:batch.Length()]
			// toSubtract is now a selection vector containing all matched tuples of the
			// current case arm.
			var subtractIdx int
			var curIdx int
			if batch.Length() > 0 {
				inputCol := batch.ColVec(c.thenIdxs[i])
				// Copy the results into the output vector, using the toSubtract selection
				// vector to copy only the elements that we actually wrote according to the
				// current case arm.
				outputCol.Copy(
					coldata.CopySliceArgs{
						SliceArgs: coldata.SliceArgs{
							Src:         inputCol,
							Sel:         toSubtract,
							SrcStartIdx: 0,
							SrcEndIdx:   len(toSubtract),
						},
						SelOnDest: true,
					})
				if prevHasSel {
					// We have a previous selection vector, which represents the tuples
					// that haven't yet been matched. Remove the ones that just matched
					// from the previous selection vector.
					for i := range c.prevSel {
						if subtractIdx < len(toSubtract) && toSubtract[subtractIdx] == c.prevSel[i] {
							// The ith element of the previous selection vector matched the
							// current one in toSubtract. Skip writing this element, removing
							// it from the previous selection vector.
							subtractIdx++
							continue
						}
						c.prevSel[curIdx] = c.prevSel[i]
						curIdx++
					}
				} else {
					// No selection vector means there have been no matches yet, and we were
					// considering the entire batch of tuples for this case arm. Make a new
					// selection vector with all of the tuples but the ones that just matched.
					c.prevSel = c.prevSel[:cap(c.prevSel)]
					for i := 0; i < origLen; i++ {
						if subtractIdx < len(toSubtract) && toSubtract[subtractIdx] == i {
							subtractIdx++
							continue
						}
						c.prevSel[curIdx] = i
						curIdx++
					}
				}
				// Set the buffered batch into the desired state.
				c.buffer.batch.SetLength(curIdx)
				prevLen = curIdx
				c.buffer.batch.SetSelection(true)
				prevHasSel = true
				copy(c.buffer.batch.Selection()[:curIdx], c.prevSel)
				c.prevSel = c.prevSel[:curIdx]
			} else {
				// There were no matches with the current WHEN arm, so we simply need
				// to restore the buffered batch into the previous state.
				c.buffer.batch.SetLength(prevLen)
				c.buffer.batch.SetSelection(prevHasSel)
				if prevHasSel {
					copy(c.buffer.batch.Selection()[:prevLen], c.prevSel)
					c.prevSel = c.prevSel[:prevLen]
				}
			}
			// Now our selection vector is set to exclude all the things that have
			// matched so far. Reset the buffer and run the next case arm.
			c.buffer.rewind()
		}
		// Finally, run the else operator, which will project into all tuples that
		// are remaining in the selection vector (didn't match any case arms). Once
		// that's done, restore the original selection vector and return the batch.
		batch := c.elseOp.Next(ctx)
		if batch.Length() > 0 {
			inputCol := batch.ColVec(c.thenIdxs[len(c.thenIdxs)-1])
			outputCol.Copy(
				coldata.CopySliceArgs{
					SliceArgs: coldata.SliceArgs{
						Src:         inputCol,
						Sel:         batch.Selection(),
						SrcStartIdx: 0,
						SrcEndIdx:   batch.Length(),
					},
					SelOnDest: true,
				})
		}
	})
	// Restore the original state of the buffered batch.
	c.buffer.batch.SetLength(origLen)
	c.buffer.batch.SetSelection(origHasSel)
	if origHasSel {
		copy(c.buffer.batch.Selection()[:origLen], c.origSel[:origLen])
	}
	return c.buffer.batch
}
