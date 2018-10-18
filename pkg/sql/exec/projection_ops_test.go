// Copyright 2018 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestProjPlusInt64Int64ConstOp(t *testing.T) {
	runTests(t, []tuples{{{1}, {2}}}, []types.T{types.Int64}, func(t *testing.T, input []Operator) {
		op := projPlusInt64Int64ConstOp{
			input:     input[0],
			colIdx:    0,
			constArg:  1,
			outputIdx: 1,
		}
		op.Init()
		out := newOpTestOutput(&op, []int{0, 1}, tuples{{1, 2}, {2, 3}})
		if err := out.Verify(); err != nil {
			t.Error(err)
		}
	})
}

func TestProjPlusInt64Int64Op(t *testing.T) {
	runTests(t, []tuples{{{1, 2}, {3, 4}}}, []types.T{types.Int64}, func(t *testing.T, input []Operator) {
		op := projPlusInt64Int64Op{
			input:     input[0],
			col1Idx:   0,
			col2Idx:   1,
			outputIdx: 2,
		}
		op.Init()
		out := newOpTestOutput(&op, []int{0, 1, 2}, tuples{{1, 2, 3}, {3, 4, 7}})
		if err := out.Verify(); err != nil {
			t.Error(err)
		}
	})
}

func BenchmarkProjPlusInt64Int64ConstOp(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	batch := NewMemBatch([]types.T{types.Int64, types.Int64})
	col := batch.ColVec(0).Int64()
	for i := int64(0); i < ColBatchSize; i++ {
		col[i] = rng.Int63()
	}
	batch.SetLength(ColBatchSize)
	source := newRepeatableBatchSource(batch)
	source.Init()

	plusOp := &projPlusInt64Int64ConstOp{
		input:     source,
		colIdx:    0,
		constArg:  rng.Int63(),
		outputIdx: 1,
	}
	plusOp.Init()

	b.SetBytes(int64(8 * ColBatchSize))
	for i := 0; i < b.N; i++ {
		plusOp.Next()
	}
}

func BenchmarkProjPlusInt64Int64Op(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	batch := NewMemBatch([]types.T{types.Int64, types.Int64, types.Int64})
	col1 := batch.ColVec(0).Int64()
	col2 := batch.ColVec(1).Int64()
	for i := int64(0); i < ColBatchSize; i++ {
		col1[i] = rng.Int63()
		col2[i] = rng.Int63()
	}
	batch.SetLength(ColBatchSize)
	source := newRepeatableBatchSource(batch)
	source.Init()

	plusOp := &projPlusInt64Int64Op{
		input:     source,
		col1Idx:   0,
		col2Idx:   1,
		outputIdx: 2,
	}
	plusOp.Init()

	b.SetBytes(int64(8 * ColBatchSize * 2))
	for i := 0; i < b.N; i++ {
		plusOp.Next()
	}
}
