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

func TestSelLTInt64Int64ConstOp(t *testing.T) {
	tups := tuples{{0}, {1}, {2}}
	runTests(t, []tuples{tups}, nil /* extraTypes */, func(t *testing.T, input []Operator) {
		op := selLTInt64Int64ConstOp{
			input:    input[0],
			colIdx:   0,
			constArg: 2,
		}
		op.Init()
		out := newOpTestOutput(&op, []int{0}, tuples{{0}, {1}})
		if err := out.Verify(); err != nil {
			t.Error(err)
		}
	})
}

func TestSelLTInt64Int64(t *testing.T) {
	tups := tuples{
		{0, 0},
		{0, 1},
		{1, 0},
		{1, 1},
	}
	runTests(t, []tuples{tups}, nil /* extraTypes */, func(t *testing.T, input []Operator) {
		op := selLTInt64Int64Op{
			input:   input[0],
			col1Idx: 0,
			col2Idx: 1,
		}
		op.Init()
		out := newOpTestOutput(&op, []int{0, 1}, tuples{{0, 1}})
		if err := out.Verify(); err != nil {
			t.Error(err)
		}
	})
}

func TestGetSelectionConstOperator(t *testing.T) {
	ct := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_DATE}
	cmpOp := tree.LT
	var input Operator
	colIdx := 3
	constVal := int64(31)
	constArg := tree.NewDDate(tree.DDate(constVal))
	op, err := GetSelectionConstOperator(ct, cmpOp, input, colIdx, constArg)
	if err != nil {
		t.Error(err)
	}
	expected := &selLTInt64Int64ConstOp{input: input, colIdx: colIdx, constArg: constVal}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v, expected %+v", op, expected)
	}
}

func TestGetSelectionOperator(t *testing.T) {
	ct := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT, Width: 16}
	cmpOp := tree.GE
	var input Operator
	col1Idx := 5
	col2Idx := 7
	op, err := GetSelectionOperator(ct, cmpOp, input, col1Idx, col2Idx)
	if err != nil {
		t.Error(err)
	}
	expected := &selGEInt16Int16Op{input: input, col1Idx: col1Idx, col2Idx: col2Idx}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v, expected %+v", op, expected)
	}
}

func BenchmarkSelLTInt64Int64ConstOp(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	batch := NewMemBatch([]types.T{types.Int64})
	col := batch.ColVec(0).Int64()
	for i := int64(0); i < ColBatchSize; i++ {
		col[i] = rng.Int63()
	}
	batch.SetLength(ColBatchSize)
	source := newRepeatableBatchSource(batch)
	source.Init()

	plusOp := &selLTInt64Int64ConstOp{
		input:    source,
		colIdx:   0,
		constArg: rng.Int63(),
	}
	plusOp.Init()

	b.SetBytes(int64(8 * ColBatchSize))
	for i := 0; i < b.N; i++ {
		plusOp.Next()
	}
}

func BenchmarkSelLTInt64Int64Op(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	batch := NewMemBatch([]types.T{types.Int64, types.Int64})
	col1 := batch.ColVec(0).Int64()
	col2 := batch.ColVec(1).Int64()
	for i := int64(0); i < ColBatchSize; i++ {
		col1[i] = rng.Int63()
		col2[i] = rng.Int63()
	}
	batch.SetLength(ColBatchSize)
	source := newRepeatableBatchSource(batch)
	source.Init()

	plusOp := &selLTInt64Int64Op{
		input:   source,
		col1Idx: 0,
		col2Idx: 1,
	}
	plusOp.Init()

	b.SetBytes(int64(8 * ColBatchSize * 2))
	for i := 0; i < b.N; i++ {
		plusOp.Next()
	}
}
