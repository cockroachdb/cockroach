// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecbase

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// OrderedDistinctColsToOperators is a utility function that given an input and
// a slice of columns, creates a chain of distinct operators and returns the
// last distinct operator in that chain as well as its output column.
func OrderedDistinctColsToOperators(
	input colexecop.Operator, distinctCols []uint32, typs []*types.T,
) (colexecop.ResettableOperator, []bool, error) {
	distinctCol := make([]bool, coldata.BatchSize())
	// zero the boolean column on every iteration.
	input = fnOp{
		OneInputNode: colexecop.NewOneInputNode(input),
		fn:           func() { copy(distinctCol, colexecutils.ZeroBoolColumn) },
	}
	var (
		err error
		r   colexecop.ResettableOperator
		ok  bool
	)
	for i := range distinctCols {
		input, err = newSingleDistinct(input, int(distinctCols[i]), distinctCol, typs[distinctCols[i]])
		if err != nil {
			return nil, nil, err
		}
	}
	if r, ok = input.(colexecop.ResettableOperator); !ok {
		colexecerror.InternalError(errors.AssertionFailedf("unexpectedly an ordered distinct is not a Resetter"))
	}
	distinctChain := &distinctChainOps{
		ResettableOperator: r,
	}
	return distinctChain, distinctCol, nil
}

type distinctChainOps struct {
	colexecop.ResettableOperator
}

var _ colexecop.ResettableOperator = &distinctChainOps{}

// NewOrderedDistinct creates a new ordered distinct operator on the given
// input columns with the given types.
func NewOrderedDistinct(
	input colexecop.Operator, distinctCols []uint32, typs []*types.T,
) (colexecop.ResettableOperator, error) {
	op, outputCol, err := OrderedDistinctColsToOperators(input, distinctCols, typs)
	if err != nil {
		return nil, err
	}
	return &colexecutils.BoolVecToSelOp{
		OneInputNode: colexecop.NewOneInputNode(op),
		OutputCol:    outputCol,
	}, nil
}
