// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package vecbuiltins

import (
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

// TODO(yuzefovich): add randomized tests.
// TODO(yuzefovich): add benchmarks.

// NewRankOperator creates a new exec.Operator that computes window function
// RANK or DENSE_RANK. dense distinguishes between the two functions. input
// *must* already be ordered on orderingCols (which should not be empty).
// outputColIdx specifies in which exec.Vec the operator should put its output
// (if there is no such column, a new column is appended).
func NewRankOperator(
	input exec.Operator,
	inputTyps []types.T,
	dense bool,
	orderingCols []uint32,
	outputColIdx int,
	partitionColIdx int,
) (exec.Operator, error) {
	if len(orderingCols) == 0 {
		return exec.NewConstOp(input, types.Int64, int64(1), outputColIdx)
	}
	op, outputCol, err := exec.OrderedDistinctColsToOperators(input, orderingCols, inputTyps)
	if err != nil {
		return nil, err
	}
	initFields := rankInitFields{
		input:           op,
		distinctCol:     outputCol,
		outputColIdx:    outputColIdx,
		partitionColIdx: partitionColIdx,
	}
	if dense {
		if partitionColIdx != -1 {
			return &rankDense_true_HasPartition_true_Op{rankInitFields: initFields}, nil
		}
		return &rankDense_true_HasPartition_false_Op{rankInitFields: initFields}, nil
	}
	if partitionColIdx != -1 {
		return &rankDense_false_HasPartition_true_Op{rankInitFields: initFields}, nil
	}
	return &rankDense_false_HasPartition_false_Op{rankInitFields: initFields}, nil
}
