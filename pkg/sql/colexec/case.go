// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type caseOp struct {
	colexecop.InitHelper

	allocator *colmem.Allocator
	buffer    *bufferOp

	caseOps []colexecop.Operator
	elseOp  colexecop.Operator

	thenIdxs  []int
	outputIdx int
	typ       *types.T

	// scratch is used to populate the output vector out of order, before
	// copying it into the batch. We need this because each WHEN arm can match
	// an arbitrary set of tuples and some vectors don't support SET operation
	// in arbitrary order.
	//
	// Consider the following example:
	//   input column c = {0, 1, 2, 1, 0, 2}
	// and CASE projection as
	//   CASE WHEN c = 1 THEN -1 WHEN c = 2 THEN -2 ELSE 42 END.
	// Then after running the first WHEN arm projection, we have
	//   scratch output = {-1, -1}
	//   scratch order = {x, 0, x, 1, x, x} (note that 'x' means 'not matched yet').
	// After running the second WHEN arm projection:
	//   scratch output = {-1, -1, -2, -2}
	//   scratch order = {x, 0, 2, 1, x, 3}.
	// After running the ELSE arm projection:
	//   scratch output = {-1, -1, -2, -2, 42, 42}
	//   scratch order = {4, 0, 2, 1, 5, 3}.
	//
	// It is guaranteed that after running all WHEN and ELSE arms, order will
	// contain all tuple indices, and once that is the case, we simply need to
	// copy the data out of the scratch into the output batch according to sel.
	//
	// Note that order doesn't satisfy the assumption of our normal selection
	// vectors which are always increasing sequences, but that is ok since order
	// is only used internally and Vec.Copy can handle it correctly.
	//
	// Things are a bit more complicated in case the batch comes with a
	// selection vector itself. Consider the following example (where 'x'
	// denotes some garbage value):
	//   input column c = {x, 0, 1, x, 2, 1, 0, x, 2, x}
	//   origSel = {1, 2, 4, 5, 6, 8}.
	// Then after running the first WHEN arm projection, we have
	//   scratch output = {-1, -1}
	//   scratch order = {x, x, 0, x, x, 1, x, x, x}.
	// After running the second WHEN arm projection:
	//   scratch output = {-1, -1, -2, -2}
	//   scratch order = {x, x, 0, x, 2, 1, x, x, 3}.
	// After running the ELSE arm projection:
	//   scratch output = {-1, -1, -2, -2, 42, 42}
	//   scratch order = {x, 4, 0, x, 2, 1, 5, x, 3}.
	// Note that order has the length sufficient to support all indices
	// mentioned in the original selection vector and not selected elements will
	// have garbage left in the corresponding positions in order.
	//
	// At this point we have to copy the output with "reordering" according to
	// the selection vector so that
	//   result[origSel[i]] = output[order[origSel[i]]]
	// for all i in range [0, len(batch)).
	scratch struct {
		// output contains the result of CASE projections where results that
		// came from the same WHEN arm are grouped together.
		output *coldata.Vec
		order  []int
	}

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

var _ colexecop.Operator = &caseOp{}

func (c *caseOp) ChildCount(verbose bool) int {
	return 1 + len(c.caseOps) + 1
}

func (c *caseOp) Child(nth int, verbose bool) execopnode.OpNode {
	if nth == 0 {
		return c.buffer
	} else if nth < len(c.caseOps)+1 {
		return c.caseOps[nth-1]
	} else if nth == 1+len(c.caseOps) {
		return c.elseOp
	}
	colexecerror.InternalError(errors.AssertionFailedf("invalid idx %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// NewCaseOp returns an operator that runs a case statement.
// buffer is a bufferOp that will return the input batch repeatedly.
// caseOps is a list of operator chains, one per branch in the case statement.
//
//	Each caseOp is connected to the input buffer op, and filters the input based
//	on the case arm's WHEN condition, and then projects the remaining selected
//	tuples based on the case arm's THEN condition.
//
// elseOp is the ELSE condition.
// whenCol is the index into the input batch to read from.
// thenCol is the index into the output batch to write to.
// typ is the output type of the CASE expression.
func NewCaseOp(
	allocator *colmem.Allocator,
	buffer colexecop.Operator,
	caseOps []colexecop.Operator,
	elseOp colexecop.Operator,
	thenIdxs []int,
	outputIdx int,
	typ *types.T,
) colexecop.Operator {
	// We internally use three selection vectors, scratch.order, origSel, and
	// prevSel.
	allocator.AdjustMemoryUsage(3 * colmem.SelVectorSize(coldata.BatchSize()))
	return &caseOp{
		allocator: allocator,
		buffer:    buffer.(*bufferOp),
		caseOps:   caseOps,
		elseOp:    elseOp,
		thenIdxs:  thenIdxs,
		outputIdx: outputIdx,
		typ:       typ,
	}
}

func (c *caseOp) Init(ctx context.Context) {
	if !c.InitHelper.Init(ctx) {
		return
	}
	for i := range c.caseOps {
		c.caseOps[i].Init(c.Ctx)
	}
	c.elseOp.Init(c.Ctx)
}

// copyIntoScratch copies all values specified by the selection vector of the
// batch from column at position inputColIdx into the scratch output vector. The
// copied values are put starting at numAlreadyMatched index in the output
// vector. It is assumed that the selection vector of batch is non-nil.
func (c *caseOp) copyIntoScratch(batch coldata.Batch, inputColIdx int, numAlreadyMatched int) {
	n := batch.Length()
	// Copy the results into the scratch output vector, using the selection
	// vector to copy only the elements that we actually wrote according to the
	// current projection arm.
	c.scratch.output.Copy(
		coldata.SliceArgs{
			Src:       batch.ColVecs()[inputColIdx],
			Sel:       batch.Selection()[:n],
			DestIdx:   numAlreadyMatched,
			SrcEndIdx: n,
		})
	for j, tupleIdx := range batch.Selection()[:n] {
		c.scratch.order[tupleIdx] = numAlreadyMatched + j
	}
}

func (c *caseOp) Next() coldata.Batch {
	c.buffer.advance()
	origLen := c.buffer.batch.Length()
	if origLen == 0 {
		return coldata.ZeroBatch
	}
	var origHasSel bool
	if sel := c.buffer.batch.Selection(); sel != nil {
		origHasSel = true
		c.origSel = colexecutils.EnsureSelectionVectorLength(c.origSel, origLen)
		copy(c.origSel, sel)
	}

	prevHasSel := false
	if sel := c.buffer.batch.Selection(); sel != nil {
		prevHasSel = true
		c.prevSel = colexecutils.EnsureSelectionVectorLength(c.prevSel, origLen)
		copy(c.prevSel, sel)
	}
	outputCol := c.buffer.batch.ColVec(c.outputIdx)

	if c.scratch.output == nil || c.scratch.output.Capacity() < origLen {
		c.scratch.output = c.allocator.NewVec(c.typ, origLen)
	} else {
		coldata.ResetIfBytesLike(c.scratch.output)
	}
	orderCapacity := origLen
	if origHasSel {
		orderCapacity = c.origSel[origLen-1] + 1
	}
	c.scratch.order = colexecutils.EnsureSelectionVectorLength(c.scratch.order, orderCapacity)

	// Run all WHEN arms.
	numAlreadyMatched := 0
	c.allocator.PerformOperation([]*coldata.Vec{c.scratch.output}, func() {
		for i := range c.caseOps {
			// Run the next case operator chain. It will project its THEN expression
			// for all tuples that matched its WHEN expression and that were not
			// already matched.
			batch := c.caseOps[i].Next()
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
			if batch.Length() > 0 {
				c.copyIntoScratch(batch, c.thenIdxs[i], numAlreadyMatched)

				numAlreadyMatched += len(toSubtract)
				if numAlreadyMatched == origLen {
					// All tuples have already matched, so we can short-circuit.
					return
				}

				var subtractIdx int
				var curIdx int
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
					c.prevSel = colexecutils.EnsureSelectionVectorLength(c.prevSel, origLen)
					for i := 0; i < origLen; i++ {
						// Note that here we rely on the assumption that
						// toSubtract is an increasing sequence (because our
						// selection vectors are such) to optimize the
						// subtraction.
						if subtractIdx < len(toSubtract) && toSubtract[subtractIdx] == i {
							subtractIdx++
							continue
						}
						c.prevSel[curIdx] = i
						curIdx++
					}
				}
				// Set the buffered batch into the desired state.
				colexecutils.UpdateBatchState(c.buffer.batch, curIdx, true /* usesSel */, c.prevSel)
				prevHasSel = true
				c.prevSel = c.prevSel[:curIdx]
			} else {
				// There were no matches with the current WHEN arm, so we simply need
				// to restore the buffered batch into the previous state.
				prevLen := origLen - numAlreadyMatched
				colexecutils.UpdateBatchState(c.buffer.batch, prevLen, prevHasSel, c.prevSel)
			}
			// Now our selection vector is set to exclude all the things that have
			// matched so far. Reset the buffer and run the next case arm.
			c.buffer.rewind()
		}
	})

	// Run the ELSE arm if necessary.
	outputReady := false
	if numAlreadyMatched == 0 && !origHasSel {
		// If no tuples matched with any of the WHEN arms and the original batch
		// didn't have a selection vector, we can copy the result of the ELSE
		// projection straight into the output batch because the result will be
		// in the correct order.
		batch := c.elseOp.Next()
		c.allocator.PerformOperation([]*coldata.Vec{outputCol}, func() {
			outputCol.Copy(
				coldata.SliceArgs{
					Src:       batch.ColVec(c.thenIdxs[len(c.thenIdxs)-1]),
					SrcEndIdx: origLen,
				})
		})
		outputReady = true
	} else if numAlreadyMatched < origLen {
		batch := c.elseOp.Next()
		c.allocator.PerformOperation([]*coldata.Vec{c.scratch.output}, func() {
			c.copyIntoScratch(batch, c.thenIdxs[len(c.thenIdxs)-1], numAlreadyMatched)
		})
	}

	// Copy the output vector from the scratch space into the batch if
	// necessary.
	if !outputReady {
		c.allocator.PerformOperation([]*coldata.Vec{outputCol}, func() {
			if origHasSel {
				// If the original batch had a selection vector, we cannot just
				// copy the output from the scratch because we want to preserve
				// that selection vector. See comment above c.scratch for an
				// example.
				outputCol.CopyWithReorderedSource(c.scratch.output, c.origSel, c.scratch.order)
			} else {
				outputCol.Copy(
					coldata.SliceArgs{
						Src:       c.scratch.output,
						Sel:       c.scratch.order,
						SrcEndIdx: origLen,
					})
			}
		})
	}

	// Restore the original state of the buffered batch.
	colexecutils.UpdateBatchState(c.buffer.batch, origLen, origHasSel, c.origSel)
	return c.buffer.batch
}
