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
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

type rowNumberOp struct {
	input        Operator
	outputColIdx int

	rowNumber int64
}

var _ Operator = &rowNumberOp{}

// NewRowNumberOperator creates a new Operator that computes window function
// ROW_NUMBER. outputColIdx specifies in which ColVec the operator should put
// its output (if there is no such column, a new column is appended).
// Note that we put a deselectorOp before rowNumberOp for simplicity.
// TODO(yuzefovich): I'm thinking of using a selection vector to determine
// boundaries of the partitions, so it is easier to put a deselector up front
// for now.
func NewRowNumberOperator(input Operator, inputTyps []types.T, outputColIdx int) Operator {
	input = NewDeselectorOp(input, inputTyps)
	return &rowNumberOp{input: input, outputColIdx: outputColIdx}
}

func (r *rowNumberOp) Init() {
	r.input.Init()
	// ROW_NUMBER starts counting from 1.
	r.rowNumber = 1
}

func (r *rowNumberOp) Next(ctx context.Context) coldata.Batch {
	b := r.input.Next(ctx)
	if b.Length() == 0 {
		return b
	}
	if r.outputColIdx == b.Width() {
		b.AppendCol(types.Int64)
	} else if r.outputColIdx > b.Width() {
		panic("unexpected: column outputColIdx is neither present nor the next to be appended")
	}
	rowNumberCol := b.ColVec(r.outputColIdx).Int64()
	for i := uint16(0); i < b.Length(); i++ {
		rowNumberCol[i] = r.rowNumber
		r.rowNumber++
	}
	return b
}

type rankOp struct {
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

var _ Operator = &rankOp{}

// NewRankOperator creates a new Operator that computes window function RANK or
// DENSE_RANK. dense distinguishes between the two functions. input *must*
// already be ordered on orderingCols. outputColIdx specifies in which ColVec
// the operator should put its output (if there is no such column, a new column
// is appended).
// Note that we put a deselectorOp before rankOp for simplicity.
// TODO(yuzefovich): I'm thinking of using a selection vector to determine
// boundaries of the partitions, so it is easier to put a deselector up front
// for now.
func NewRankOperator(
	input Operator,
	inputTyps []types.T,
	dense bool,
	orderingCols []distsqlpb.Ordering_Column,
	outputColIdx int,
) (Operator, error) {
	input = NewDeselectorOp(input, inputTyps)
	if len(orderingCols) > 0 {
		distinctCols := make([]uint32, len(orderingCols))
		for i := range orderingCols {
			distinctCols[i] = orderingCols[i].ColIdx
		}
		op, outputCol, err := orderedDistinctColsToOperators(input, distinctCols, inputTyps)
		if err != nil {
			return nil, err
		}
		return &rankOp{input: op, dense: dense, distinctCol: outputCol, outputColIdx: outputColIdx}, nil
	}
	// TODO(yuzefovich): once optimizer can substitute RANK and DENSE_RANK that
	// have no ordering columns with `1` projection, remove this code.
	return &rankOp{input: input, outputColIdx: outputColIdx}, nil
}

func (r *rankOp) Init() {
	r.input.Init()
	// RANK and DENSE_RANK start counting from 1. Before we assign the rank to a
	// tuple in the batch, we first increment r.rank, so setting this
	// rankIncrement to 1 will update r.rank to 1 on the very first tuple (as
	// desired).
	r.rankIncrement = 1
}

var (
	oneInt64Vec            []int64
	oneInt64VecInitialized int32
)

func init() {
	if atomic.CompareAndSwapInt32(&oneInt64VecInitialized, 0, 1) {
		oneInt64Vec = make([]int64, coldata.BatchSize)
		for i := range oneInt64Vec {
			oneInt64Vec[i] = 1
		}
	}
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
