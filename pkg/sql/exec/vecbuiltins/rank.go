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

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

type rankOp struct {
	input exec.Operator
	dense bool
	// distinctCol is the output column of the chain of ordered distinct
	// operators in which true will indicate that a new rank needs to be assigned
	// to the corresponding tuple.
	distinctCol  []bool
	outputColIdx int

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
// outputColIdx specifies in which exec.ColVec the operator should put its
// output (if there is no such column, a new column is appended).
func NewRankOperator(
	input exec.Operator,
	inputTyps []types.T,
	dense bool,
	orderingCols []distsqlpb.Ordering_Column,
	outputColIdx int,
) (exec.Operator, error) {
	if len(orderingCols) == 0 {
		return exec.NewConstOp(input, types.Int64, int64(1), outputColIdx)
	}
	distinctCols := make([]uint32, len(orderingCols))
	for i := range orderingCols {
		distinctCols[i] = orderingCols[i].ColIdx
	}
	op, outputCol, err := exec.OrderedDistinctColsToOperators(input, distinctCols, inputTyps)
	if err != nil {
		return nil, err
	}
	return &rankOp{input: op, dense: dense, distinctCol: outputCol, outputColIdx: outputColIdx}, nil
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
	b := r.input.Next(ctx)
	if b.Length() == 0 {
		return b
	}
	if r.outputColIdx == b.Width() {
		b.AppendCol(types.Int64)
	} else if r.outputColIdx > b.Width() {
		panic("unexpected: column outputColIdx is neither present nor the next to be appended")
	}
	rankCol := b.ColVec(r.outputColIdx).Int64()
	if r.distinctCol == nil {
		panic("unexpected: distinctCol is nil in rankOp")
	}
	sel := b.Selection()
	if sel != nil {
		for i := uint16(0); i < b.Length(); i++ {
			if r.distinctCol[sel[i]] {
				// TODO(yuzefovich): template this part out to generate two different
				// rank operators.
				if r.dense {
					r.rank++
				} else {
					r.rank += r.rankIncrement
					r.rankIncrement = 1
				}
				rankCol[sel[i]] = r.rank
			} else {
				rankCol[sel[i]] = r.rank
				if !r.dense {
					r.rankIncrement++
				}
			}
		}
	} else {
		for i := uint16(0); i < b.Length(); i++ {
			if r.distinctCol[i] {
				// TODO(yuzefovich): template this part out to generate two different
				// rank operators.
				if r.dense {
					r.rank++
				} else {
					r.rank += r.rankIncrement
					r.rankIncrement = 1
				}
				rankCol[i] = r.rank
			} else {
				rankCol[i] = r.rank
				if !r.dense {
					r.rankIncrement++
				}
			}
		}
	}
	return b
}
