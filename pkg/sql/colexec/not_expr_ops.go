// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type notExprProjBase struct {
	colexecop.OneInputHelper
	allocator *colmem.Allocator
	inputIdx  int
	outputIdx int
}

// notExprProjOp is an Operator that projects into outputIdx Vec
// the corresponding negated value / expression in colIdx Vec (i.e. NOT of the
// value is TRUE). If the underlying value is NULL, then notExprProjOp projects
// a NULL value in the output.
type notExprProjOp struct {
	notExprProjBase
}

var _ colexecop.Operator = &notExprProjOp{}

// NewNotExprProjOp returns a new notExprProjOp.
func NewNotExprProjOp(
	allocator *colmem.Allocator, input colexecop.Operator, inputIdx, outputIdx int,
) colexecop.Operator {
	input = colexecutils.NewVectorTypeEnforcer(allocator, input, types.Bool, outputIdx)
	base := notExprProjBase{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		allocator:      allocator,
		inputIdx:       inputIdx,
		outputIdx:      outputIdx,
	}
	return &notExprProjOp{notExprProjBase: base}
}

// projectOuput populates the output vector with the negated bool value of the
// input vector for non-null values. For null value, it projects null.
func projectOutput(
	idx int, inputNulls, outputNulls *coldata.Nulls, inputVec coldata.Vec, outputBools coldata.Bools,
) {
	if inputNulls.NullAt(idx) {
		outputNulls.SetNull(idx)
	} else {
		inputVal := inputVec.Bool().Get(idx)
		outputBools.Set(idx, !inputVal)
	}
}

func (o *notExprProjOp) Next() coldata.Batch {
	batch := o.Input.Next()
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	inputVec, outputVec := batch.ColVec(o.inputIdx), batch.ColVec(o.outputIdx)
	inputNulls := inputVec.Nulls()
	outputBools, outputNulls := outputVec.Bool(), outputVec.Nulls()
	if outputNulls.MaybeHasNulls() {
		// Unsetting any potential nulls in the output in case there are null
		// values present beforehand.
		outputNulls.UnsetNulls()
	}
	if sel := batch.Selection(); sel != nil {
		sel = sel[:n]
		for _, idx := range sel {
			projectOutput(idx, inputNulls, outputNulls, inputVec, outputBools)
		}
	} else {
		outputBools = outputBools[:n]
		for idx := range outputBools {
			projectOutput(idx, inputNulls, outputNulls, inputVec, outputBools)
		}
	}
	return batch
}

type notExprSelBase struct {
	colexecop.OneInputHelper
	inputIdx int
}

// notExprSelOp is an Operator that selects all the values in the input vector
// where the expression evaluates to FALSE (i.e. NOT of the expression evaluates
// to TRUE). If the input value is NULL, then that value is not selected.
type notExprSelOp struct {
	notExprSelBase
}

var _ colexecop.Operator = &notExprSelOp{}

// NewNotExprSelOp returns a new notExprSelOp.
func NewNotExprSelOp(input colexecop.Operator, inputIdx int) colexecop.Operator {
	base := notExprSelBase{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
		inputIdx:       inputIdx,
	}
	return &notExprSelOp{notExprSelBase: base}
}

func (o *notExprSelOp) Next() coldata.Batch {
	for {
		batch := o.Input.Next()
		n := batch.Length()
		if n == 0 {
			return batch
		}
		inputVec, selectedValuesIdx := batch.ColVec(o.inputIdx), 0
		inputNulls, sel := inputVec.Nulls(), batch.Selection()
		useValue := sel != nil
		if useValue {
			sel = sel[:n]
		} else {
			batch.SetSelection(true)
			sel = batch.Selection()[:n]
		}
		for i, val := range sel {
			idx := i
			if useValue {
				idx = val
			}
			if !inputNulls.NullAt(idx) && !inputVec.Bool().Get(idx) {
				sel[selectedValuesIdx] = idx
				selectedValuesIdx++
			}
		}
		if selectedValuesIdx > 0 {
			batch.SetLength(selectedValuesIdx)
			return batch
		}
	}
}
