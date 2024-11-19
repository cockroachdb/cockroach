// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// notExprProjOp is an Operator that projects into outputIdx Vec the
// corresponding negated value / expression in colIdx Vec (i.e. NOT of the value
// is TRUE). If the underlying value is NULL, then notExprProjOp projects a NULL
// value in the output.
type notExprProjOp struct {
	colexecop.OneInputHelper
	allocator *colmem.Allocator
	inputIdx  int
	outputIdx int
}

var _ colexecop.Operator = &notExprProjOp{}

// NewNotExprProjOp returns a new notExprProjOp if the underlying type family of
// the input expression is boolean. If the type family of the input expression
// is unknown, it returns a notNullProj operator. Else, it returns an error.
func NewNotExprProjOp(
	typFamily types.Family,
	allocator *colmem.Allocator,
	input colexecop.Operator,
	inputIdx, outputIdx int,
) (colexecop.Operator, error) {
	input = colexecutils.NewVectorTypeEnforcer(allocator, input, types.Bool, outputIdx)
	switch typFamily {
	case types.BoolFamily:
		return &notExprProjOp{
			OneInputHelper: colexecop.MakeOneInputHelper(input),
			allocator:      allocator,
			inputIdx:       inputIdx,
			outputIdx:      outputIdx,
		}, nil
	case types.UnknownFamily:
		return newNotNullProjOp(allocator, input, inputIdx, outputIdx), nil
	default:
		return nil, errors.AssertionFailedf("unexpected type for NOT expr")
	}
}

func (o *notExprProjOp) Next() coldata.Batch {
	batch := o.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	inputVec, outputVec := batch.ColVec(o.inputIdx), batch.ColVec(o.outputIdx)
	inputBools, inputNulls := inputVec.Bool(), inputVec.Nulls()
	outputBools, outputNulls := outputVec.Bool(), outputVec.Nulls()
	if inputNulls.MaybeHasNulls() {
		if sel := batch.Selection(); sel != nil {
			sel = sel[:n]
			for _, idx := range sel {
				if inputNulls.NullAt(idx) {
					outputNulls.SetNull(idx)
				} else {
					exprVal := inputBools.Get(idx)
					outputBools.Set(idx, !exprVal)
				}
			}
		} else {
			inputBools = inputBools[:n]
			outputBools = outputBools[:n]
			for idx := 0; idx < n; idx++ {
				if inputNulls.NullAt(idx) {
					outputNulls.SetNull(idx)
				} else {
					//gcassert:bce
					exprVal := inputBools.Get(idx)
					//gcassert:bce
					outputBools.Set(idx, !exprVal)
				}
			}
		}
	} else {
		if sel := batch.Selection(); sel != nil {
			sel = sel[:n]
			for _, idx := range sel {
				exprVal := inputBools.Get(idx)
				outputBools.Set(idx, !exprVal)
			}
		} else {
			inputBools = inputBools[:n]
			outputBools = outputBools[:n]
			for idx := 0; idx < n; idx++ {
				//gcassert:bce
				exprVal := inputBools.Get(idx)
				//gcassert:bce
				outputBools.Set(idx, !exprVal)
			}
		}

	}
	return batch
}

// notExprSelOp is an Operator that selects all the values in the input vector
// where the expression evaluates to FALSE (i.e. NOT of the expression evaluates
// to TRUE). If the input value is NULL, then that value is not selected.
type notExprSelOp struct {
	colexecop.OneInputHelper
	inputIdx int
}

var _ colexecop.Operator = &notExprSelOp{}

// NewNotExprSelOp returns a new notExprSelOp if the underlying type family of
// the input expression is boolean. If the type family of the input expression
// is unknown, it returns a zero operator. Else, it returns an error.
func NewNotExprSelOp(
	typeFamily types.Family, input colexecop.Operator, inputIdx int,
) (colexecop.Operator, error) {
	switch typeFamily {
	case types.BoolFamily:
		return &notExprSelOp{
			OneInputHelper: colexecop.MakeOneInputHelper(input),
			inputIdx:       inputIdx,
		}, nil
	case types.UnknownFamily:
		return colexecutils.NewZeroOp(input), nil
	default:
		return nil, errors.AssertionFailedf("unexpected type for NOT expr")
	}
}

func (o *notExprSelOp) Next() coldata.Batch {
	for {
		batch := o.Input.Next()
		n := batch.Length()
		if n == 0 {
			return batch
		}
		inputVec, selectedValuesIdx := batch.ColVec(o.inputIdx), 0
		inputNulls, inputBools := inputVec.Nulls(), inputVec.Bool()
		if inputNulls.MaybeHasNulls() {
			if sel := batch.Selection(); sel != nil {
				sel = sel[:n]
				for _, idx := range sel {
					if !inputNulls.NullAt(idx) && !inputBools.Get(idx) {
						sel[selectedValuesIdx] = idx
						selectedValuesIdx++
					}
				}
			} else {
				batch.SetSelection(true)
				sel = batch.Selection()[:n]
				inputBools = inputBools[:n]
				for idx := 0; idx < n; idx++ {
					if !inputNulls.NullAt(idx) {
						//gcassert:bce
						if !inputBools.Get(idx) {
							sel[selectedValuesIdx] = idx
							selectedValuesIdx++
						}
					}
				}
			}
		} else {
			if sel := batch.Selection(); sel != nil {
				sel = sel[:n]
				for _, idx := range sel {
					if !inputBools.Get(idx) {
						sel[selectedValuesIdx] = idx
						selectedValuesIdx++
					}
				}
			} else {
				batch.SetSelection(true)
				sel = batch.Selection()[:n]
				inputBools = inputBools[:n]
				for idx := 0; idx < n; idx++ {
					//gcassert:bce
					if !inputBools.Get(idx) {
						sel[selectedValuesIdx] = idx
						selectedValuesIdx++
					}
				}
			}
		}
		if selectedValuesIdx > 0 {
			batch.SetLength(selectedValuesIdx)
			return batch
		}
	}
}

// notNullProjOp is an Operator that projects NULL into outputIdx Vec.
type notNullProjOp struct {
	colexecop.OneInputHelper
	allocator *colmem.Allocator
	inputIdx  int
	outputIdx int
}

var _ colexecop.Operator = &notNullProjOp{}

// newNotNullProjOp returns a new notNullProjOp.
func newNotNullProjOp(
	allocator *colmem.Allocator, input colexecop.Operator, inputIdx, outputIdx int,
) colexecop.Operator {
	input = colexecutils.NewVectorTypeEnforcer(allocator, input, types.Bool, outputIdx)
	return &notNullProjOp{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		allocator:      allocator,
		inputIdx:       inputIdx,
		outputIdx:      outputIdx,
	}
}

func (o *notNullProjOp) Next() coldata.Batch {
	batch := o.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	outputNulls := batch.ColVec(o.outputIdx).Nulls()
	outputNulls.SetNulls()
	return batch
}
