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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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

func TestGetProjectionConstOperator(t *testing.T) {
	ct := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_FLOAT}
	binOp := tree.Mult
	var input Operator
	colIdx := 3
	constVal := float64(31.37)
	constArg := tree.NewDFloat(tree.DFloat(constVal))
	outputIdx := 5
	op, err := GetProjectionConstOperator(ct, binOp, input, colIdx, constArg, outputIdx)
	if err != nil {
		t.Error(err)
	}
	expected := &projMultFloat64Float64ConstOp{
		input:     input,
		colIdx:    colIdx,
		constArg:  constVal,
		outputIdx: outputIdx,
	}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v, expected %+v", op, expected)
	}
}

func TestGetProjectionOperator(t *testing.T) {
	ct := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT, Width: 16}
	binOp := tree.Mult
	var input Operator
	col1Idx := 5
	col2Idx := 7
	outputIdx := 9
	op, err := GetProjectionOperator(ct, binOp, input, col1Idx, col2Idx, outputIdx)
	if err != nil {
		t.Error(err)
	}
	expected := &projMultInt16Int16Op{
		input:     input,
		col1Idx:   col1Idx,
		col2Idx:   col2Idx,
		outputIdx: outputIdx,
	}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v, expected %+v", op, expected)
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
