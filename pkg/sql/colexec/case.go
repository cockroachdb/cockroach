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
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
)

type caseOp struct {
	allocator *Allocator
	buffer    *bufferOp

	caseOps []Operator
	elseOp  Operator

	thenIdxs  []int
	outputIdx int
	typ       coltypes.T

	// origSel is a buffer used to keep track of the original selection vector of
	// the input batch. We need to do this because we're going to destructively
	// modify the selection vector in order to do the work of the case statement.
	origSel []uint16
}

var _ InternalMemoryOperator = &caseOp{}

func (c *caseOp) ChildCount() int {
	return 1 + len(c.caseOps) + 1
}

func (c *caseOp) Child(nth int) execinfra.OpNode {
	if nth == 0 {
		return c.buffer
	} else if nth < len(c.caseOps)+1 {
		return c.caseOps[nth-1]
	} else if nth == 1+len(c.caseOps) {
		return c.elseOp
	}
	execerror.VectorizedInternalPanic(fmt.Sprintf("invalid idx %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

func (c *caseOp) InternalMemoryUsage() int {
	// We internally use a single selection vector, origSel.
	return sizeOfBatchSizeSelVector
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
	allocator *Allocator,
	buffer Operator,
	caseOps []Operator,
	elseOp Operator,
	thenIdxs []int,
	outputIdx int,
	typ coltypes.T,
) Operator {
	return &caseOp{
		allocator: allocator,
		buffer:    buffer.(*bufferOp),
		caseOps:   caseOps,
		elseOp:    elseOp,
		thenIdxs:  thenIdxs,
		outputIdx: outputIdx,
		typ:       typ,
		origSel:   make([]uint16, coldata.BatchSize()),
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
	origLen := c.buffer.batch.Batch.Length()
	if c.buffer.batch.Width() == c.outputIdx {
		c.allocator.AppendColumn(c.buffer.batch, c.typ)
	}
	// NB: we don't short-circuit if the batch is length 0 here, because we have
	// to make sure to run all of our case arms. This is unfortunate.
	// TODO(jordan): add this back in once batches are right-sized by planning.
	var origHasSel bool
	if sel := c.buffer.batch.Batch.Selection(); sel != nil {
		origHasSel = true
		copy(c.origSel, sel)
	}

	outputCol := c.buffer.batch.Batch.ColVec(c.outputIdx)
	for i := range c.caseOps {
		// Run the next case operator chain. It will project its THEN expression
		// for all tuples that matched its WHEN expression and that were not
		// already matched.
		batch := c.caseOps[i].Next(ctx)
		// The batch's projection column now additionally contains results for all
		// of the tuples that passed the ith WHEN clause. The batch's selection
		// vector is set to the same selection of tuples.
		// Now, we must subtract this selection vector from the last buffered
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
		var curIdx uint16
		inputCol := c.buffer.batch.Batch.ColVec(c.thenIdxs[i])
		// Copy the results into the output vector, using the toSubtract selection
		// vector to copy only the elements that we actually wrote according to the
		// current case arm.
		outputCol.Copy(coldata.CopySliceArgs{
			SliceArgs: coldata.SliceArgs{
				ColType:     c.typ,
				Src:         inputCol,
				Sel:         toSubtract,
				SrcStartIdx: 0,
				SrcEndIdx:   uint64(len(toSubtract)),
			},
			SelOnDest: true,
		})
		if oldSel := c.buffer.batch.Batch.Selection(); oldSel != nil {
			// We have an old selection vector, which represents the tuples that
			// haven't yet been matched. Remove the ones that just matched from the
			// old selection vector.
			for i := range oldSel[:c.buffer.batch.Batch.Length()] {
				if subtractIdx < len(toSubtract) && toSubtract[subtractIdx] == oldSel[i] {
					// The ith element of the old selection vector matched the current one
					// in toSubtract. Skip writing this element, removing it from the old
					// selection vector.
					subtractIdx++
					continue
				}
				oldSel[curIdx] = oldSel[i]
				curIdx++
			}
		} else {
			// No selection vector means there have been no matches yet, and we were
			// considering the entire batch of tuples for this case arm. Make a new
			// selection vector with all of the tuples but the ones that just matched.
			c.buffer.batch.Batch.SetSelection(true)
			oldSel = c.buffer.batch.Batch.Selection()
			for i := uint16(0); i < c.buffer.batch.Batch.Length(); i++ {
				if subtractIdx < len(toSubtract) && toSubtract[subtractIdx] == i {
					subtractIdx++
					continue
				}
				oldSel[curIdx] = i
				curIdx++
			}
		}
		c.buffer.batch.Batch.SetLength(curIdx)

		// Now our selection vector is set to exclude all the things that have
		// matched so far. Reset the buffer and run the next case arm.
		c.buffer.rewind()
	}
	// Finally, run the else operator, which will project into all tuples that
	// are remaining in the selection vector (didn't match any case arms). Once
	// that's done, restore the original selection vector and return the batch.
	batch := c.elseOp.Next(ctx)
	inputCol := c.buffer.batch.Batch.ColVec(c.thenIdxs[len(c.thenIdxs)-1])
	outputCol.Copy(coldata.CopySliceArgs{
		SliceArgs: coldata.SliceArgs{
			ColType:     c.typ,
			Src:         inputCol,
			Sel:         batch.Selection(),
			SrcStartIdx: 0,
			SrcEndIdx:   uint64(batch.Length()),
		},
		SelOnDest: true,
	})
	batch.SetLength(origLen)
	batch.SetSelection(origHasSel)
	if origHasSel {
		copy(batch.Selection()[:origLen], c.origSel[:origLen])
	}
	return batch
}
