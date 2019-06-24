// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
)

// boolVecToSelOp transforms a boolean column into a selection vector by adding
// an index to the selection for each true value in the boolean column.
type boolVecToSelOp struct {
	input Operator

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
		idx := uint16(0)
		if sel := batch.Selection(); sel != nil {
			sel = sel[:n]
			for s := range sel {
				i := sel[s]
				var inc uint16
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
				var inc uint16
				// Ditto above: replace a conditional with a data dependency.
				if outputCol[i] {
					inc = 1
				}
				sel[idx] = uint16(i)
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

func boolVecToSel64(vec []bool, sel []uint64) []uint64 {
	l := uint64(len(vec))
	for i := uint64(0); i < l; i++ {
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
	d := selBoolOp{input: input, colIdx: colIdx}
	ret := &boolVecToSelOp{input: &d}
	d.boolVecToSelOp = ret
	return ret
}

// selBoolOp is a small helper operator that transforms a boolVecToSelOp into
// an operator that can see the inside of its input batch for NewBoolVecToSelOp.
type selBoolOp struct {
	input          Operator
	boolVecToSelOp *boolVecToSelOp
	colIdx         int
}

func (d selBoolOp) Init() {
	d.input.Init()
}

func (d selBoolOp) Next(ctx context.Context) coldata.Batch {
	batch := d.input.Next(ctx)
	d.boolVecToSelOp.outputCol = batch.ColVec(d.colIdx).Bool()
	return batch
}
