// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
