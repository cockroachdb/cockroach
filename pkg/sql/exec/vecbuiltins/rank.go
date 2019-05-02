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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

type rankOp struct {
	input exec.Operator
	batch coldata.Batch
	dense bool
	// distinctCol is the output column of the chain of ordered distinct
	// operators in which true will indicate that a new rank needs to be assigned
	// to the corresponding tuple.
	distinctCol     []bool
	outputColIdx    int
	partitionColIdx int

	// rank indicates which rank should be assigned to the next tuple.
	rank int64
	// rankIncrement indicates by how much rank should be incremented when a
	// tuple distinct from the previous one on the ordering columns is seen. It
	// is used only in case of a regular rank function (i.e. not dense).
	rankIncrement int64
}

var _ exec.Operator = &rankOp{}

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
	return &rankOp{input: op, dense: dense, distinctCol: outputCol, outputColIdx: outputColIdx, partitionColIdx: partitionColIdx}, nil
}

func (r *rankOp) Init() {
	r.input.Init()
	// RANK and DENSE_RANK start counting from 1. Before we assign the rank to a
	// tuple in the batch, we first increment r.rank, so setting this
	// rankIncrement to 1 will update r.rank to 1 on the very first tuple (as
	// desired).
	r.rankIncrement = 1
}

func (r *rankOp) Next(ctx context.Context) coldata.Batch {
	r.batch = r.input.Next(ctx)
	if r.batch.Length() == 0 {
		return r.batch
	}
	if r.partitionColIdx != -1 {
		r.nextBodyWithPartition()
	} else {
		r.nextBodyNoPartition()
	}
	return r.batch
}
