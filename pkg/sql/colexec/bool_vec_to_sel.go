// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
)

// boolVecToSelOp transforms a boolean column into a selection vector by adding
// an index to the selection for each true value in the boolean column.
type boolVecToSelOp struct {
	OneInputNode
	NonExplainable

	// outputCol is the boolean output column. It should be shared by other
	// operators that write to it.
	outputCol []bool
}

var _ Operator = &boolVecToSelOp{}

func (p *boolVecToSelOp) Next(ctx context.Context) coldata.Batch {
	// Loop until we have non-zero amount of output to return, or our input's been
	// exhausted.
	for {
		batch := p.input.Next(ctx)
		n := batch.Length()
		if n == 0 {
			return batch
		}
		outputCol := p.outputCol

		// Convert outputCol to a selection vector by outputting the index of each
		// tuple whose outputCol value is true.
		// Note that, if the input already had a selection vector, the output
		// selection vector will be a subset of the input selection vector.
		idx := 0
		if sel := batch.Selection(); sel != nil {
			sel = sel[:n]
			for s := range sel {
				i := sel[s]
				var inc int
				// This form is transformed into a data dependency by the compiler,
				// avoiding an expensive conditional branch.
				if outputCol[i] {
					inc = 1
				}
				sel[idx] = i
				idx += inc
			}
		} else {
			batch.SetSelection(true)
			sel := batch.Selection()
			for i := range outputCol[:n] {
				var inc int
				// Ditto above: replace a conditional with a data dependency.
				if outputCol[i] {
					inc = 1
				}
				sel[idx] = i
				idx += inc
			}
		}

		if idx == 0 {
			continue
		}

		batch.SetLength(idx)
		return batch
	}
}

func (p *boolVecToSelOp) Init() {
	p.input.Init()
}

func boolVecToSel64(vec []bool, sel []int) []int {
	l := len(vec)
	for i := 0; i < l; i++ {
		if vec[i] {
			sel = append(sel, i)
		}
	}
	return sel
}

// NewBoolVecToSelOp is the operator form of boolVecToSelOp. It filters its
// input batch by the boolean column specified by colIdx.
//
// For internal use cases that just need a way to create a selection vector
// based on a boolean column that *isn't* in a batch, just create a
// boolVecToSelOp directly with the desired boolean slice.
func NewBoolVecToSelOp(input Operator, colIdx int) Operator {
	d := selBoolOp{OneInputNode: NewOneInputNode(input), colIdx: colIdx}
	ret := &boolVecToSelOp{OneInputNode: NewOneInputNode(&d)}
	d.boolVecToSelOp = ret
	return ret
}

// selBoolOp is a small helper operator that transforms a boolVecToSelOp into
// an operator that can see the inside of its input batch for NewBoolVecToSelOp.
type selBoolOp struct {
	OneInputNode
	NonExplainable
	boolVecToSelOp *boolVecToSelOp
	colIdx         int
}

func (d selBoolOp) Init() {
	d.input.Init()
}

func (d selBoolOp) Next(ctx context.Context) coldata.Batch {
	batch := d.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return batch
	}
	inputCol := batch.ColVec(d.colIdx)
	d.boolVecToSelOp.outputCol = inputCol.Bool()
	if inputCol.MaybeHasNulls() {
		// If the input column has null values, we need to explicitly set the
		// values of the output column that correspond to those null values to
		// false. For example, doing the comparison 'NULL < 0' will put true into
		// the boolean Vec (because NULLs are smaller than any integer) but will
		// also set the null. In the code above, we only copied the values' vector,
		// so we need to adjust it.
		// TODO(yuzefovich): think through this case more, possibly clean this up.
		outputCol := d.boolVecToSelOp.outputCol
		sel := batch.Selection()
		nulls := inputCol.Nulls()
		if sel != nil {
			sel = sel[:n]
			for _, i := range sel {
				if nulls.NullAt(i) {
					outputCol[i] = false
				}
			}
		} else {
			outputCol = outputCol[0:n]
			for i := range outputCol {
				if nulls.NullAt(i) {
					outputCol[i] = false
				}
			}
		}
	}
	return batch
}
