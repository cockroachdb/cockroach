// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecutils

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// BoolOrUnknownToSelOp plans an infrastructure necessary to convert a column
// of either Bool or Unknown type into a selection vector on the input batches.
func BoolOrUnknownToSelOp(
	input colexecop.Operator, typs []*types.T, vecIdx int,
) (colexecop.Operator, error) {
	switch typs[vecIdx].Family() {
	case types.BoolFamily:
		return NewBoolVecToSelOp(input, vecIdx), nil
	case types.UnknownFamily:
		// If the column is of an Unknown type, then all values in that column
		// must be NULLs, so the selection vector will always be empty, and we
		// can simply plan a zero operator.
		return NewZeroOp(input), nil
	default:
		return nil, errors.Errorf("unexpectedly %s is neither bool nor unknown", typs[vecIdx])
	}
}

// BoolVecToSelOp transforms a boolean column into a selection vector by adding
// an index to the selection for each true value in the boolean column.
type BoolVecToSelOp struct {
	colexecop.OneInputHelper
	colexecop.NonExplainable

	// OutputCol is the boolean output column. It should be shared by other
	// operators that write to it.
	OutputCol []bool
	// ProcessOnlyOneBatch indicates whether BoolVecToSelOp should always return
	// a batch after it has processed the input batch (even if all tuples are
	// deselected).
	ProcessOnlyOneBatch bool
}

var _ colexecop.ResettableOperator = &BoolVecToSelOp{}

// Next implements the colexecop.Operator interface.
func (p *BoolVecToSelOp) Next() coldata.Batch {
	// Loop until we have non-zero amount of output to return, or our input's been
	// exhausted.
	for {
		batch := p.Input.Next()
		n := batch.Length()
		if n == 0 {
			return batch
		}
		outputCol := p.OutputCol

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
			col := outputCol[:n]
			for i := range col {
				var inc int
				// Ditto above: replace a conditional with a data dependency.
				if col[i] {
					inc = 1
				}
				sel[idx] = i
				idx += inc
			}
		}

		if idx > 0 || p.ProcessOnlyOneBatch {
			batch.SetLength(idx)
			return batch
		}
	}
}

// Reset implements the colexecop.Resetter interface.
func (p *BoolVecToSelOp) Reset(ctx context.Context) {
	if r, ok := p.Input.(colexecop.Resetter); ok {
		r.Reset(ctx)
	}
}

// NewBoolVecToSelOp is the operator form of BoolVecToSelOp. It filters its
// input batch by the boolean column specified by colIdx.
//
// For internal use cases that just need a way to create a selection vector
// based on a boolean column that *isn't* in a batch, just create a
// BoolVecToSelOp directly with the desired boolean slice.
//
// NOTE: if the column can be of a type other than boolean,
// BoolOrUnknownToSelOp *must* be used instead.
func NewBoolVecToSelOp(input colexecop.Operator, colIdx int) colexecop.Operator {
	d := &selBoolOp{OneInputHelper: colexecop.MakeOneInputHelper(input), colIdx: colIdx}
	ret := &BoolVecToSelOp{OneInputHelper: colexecop.MakeOneInputHelper(d)}
	d.boolVecToSelOp = ret
	return ret
}

// selBoolOp is a small helper operator that transforms a BoolVecToSelOp into
// an operator that can see the inside of its input batch for NewBoolVecToSelOp.
type selBoolOp struct {
	colexecop.OneInputHelper
	colexecop.NonExplainable
	boolVecToSelOp *BoolVecToSelOp
	colIdx         int
}

func (d *selBoolOp) Next() coldata.Batch {
	batch := d.Input.Next()
	n := batch.Length()
	if n == 0 {
		return batch
	}
	inputCol := batch.ColVec(d.colIdx)
	d.boolVecToSelOp.OutputCol = inputCol.Bool()
	if inputCol.MaybeHasNulls() {
		// If the input column has null values, we need to explicitly set the
		// values of the output column that correspond to those null values to
		// false. For example, doing the comparison 'NULL < 0' will put true into
		// the boolean Vec (because NULLs are smaller than any integer) but will
		// also set the null. In the code above, we only copied the values' vector,
		// so we need to adjust it.
		// TODO(yuzefovich): think through this case more, possibly clean this up.
		outputCol := d.boolVecToSelOp.OutputCol
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
