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

package exec

import (
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

type rowNumberOperator struct {
	input        Operator
	outputColIdx int

	rowNumber int64
}

var _ Operator = &rowNumberOperator{}

func NewRowNumberOperator(input Operator, outputColIdx int) Operator {
	return &rowNumberOperator{input: input, outputColIdx: outputColIdx}
}

func (r *rowNumberOperator) Init() {
	r.input.Init()
	// row_number starts counting from 1.
	r.rowNumber = 1
}

func (r *rowNumberOperator) Next() coldata.Batch {
	b := r.input.Next()
	if b.Length() == 0 {
		return b
	}
	if r.outputColIdx == b.Width() {
		b.AppendCol(types.Int64)
	}
	rowNumberCol := b.ColVec(r.outputColIdx).Int64()
	for i := uint16(0); i < b.Length(); i++ {
		rowNumberCol[i] = r.rowNumber
		r.rowNumber++
	}
	return b
}

type rankOperator struct {
	input Operator
	dense bool
	// distinctCol is the output column of the chain of ordered distinct
	// operators in which true will indicate that a new rank needs to be assigned
	// to the corresponding tuple. It is used only when there was an ORDER BY
	// clause in the query, and it is nil when there was no such clause in which
	// case all tuples will get 1 as their rank.
	distinctCol  []bool
	outputColIdx int

	// rank indicates which rank should be assigned to the next tuple.
	rank int64
	// rankIncrement indicates by how much rank should be incremented when a
	// tuple distinct from the previous one on the ordering columns is seen. It
	// is used only in case of a regular rank function (i.e. not dense).
	rankIncrement int64
}

var _ Operator = &rankOperator{}

func NewRankOperator(
	input Operator,
	dense bool,
	orderingCols []distsqlpb.Ordering_Column,
	typs []types.T,
	outputColIdx int,
) (Operator, error) {
	if len(orderingCols) > 0 {
		distinctCols := make([]uint32, len(orderingCols))
		for i := range orderingCols {
			distinctCols[i] = orderingCols[i].ColIdx
		}
		op, outputCol, err := orderedDistinctColsToOperators(input, distinctCols, typs)
		if err != nil {
			return nil, err
		}
		return &rankOperator{input: op, dense: dense, distinctCol: outputCol, outputColIdx: outputColIdx}, nil
	}
	return &rankOperator{input: input, outputColIdx: outputColIdx}, nil
}

func (r *rankOperator) Init() {
	r.input.Init()
	r.rankIncrement = 1

	// TODO(yuzefovich): is there a better place to do this initialization?
	if r.distinctCol == nil && oneInt64Vec == nil {
		oneInt64Vec = make([]int64, coldata.BatchSize)
		for i := range oneInt64Vec {
			oneInt64Vec[i] = 1
		}
	}
}

var oneInt64Vec []int64

func (r *rankOperator) Next() coldata.Batch {
	b := r.input.Next()
	if b.Length() == 0 {
		return b
	}
	if r.outputColIdx == b.Width() {
		b.AppendCol(types.Int64)
	}
	rankCol := b.ColVec(r.outputColIdx).Int64()
	if r.distinctCol != nil {
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
	} else {
		copy(rankCol, oneInt64Vec[:b.Length()])
	}
	return b
}
