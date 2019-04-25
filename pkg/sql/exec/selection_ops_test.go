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
	"context"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestSelLTInt64Int64ConstOp(t *testing.T) {
	tups := tuples{{0}, {1}, {2}}
	runTests(t, []tuples{tups}, func(t *testing.T, input []Operator) {
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
	runTests(t, []tuples{tups}, func(t *testing.T, input []Operator) {
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
	cmpOp := tree.LT
	var input Operator
	colIdx := 3
	constVal := int64(31)
	constArg := tree.NewDDate(tree.DDate(constVal))
	op, err := GetSelectionConstOperator(semtypes.Date, cmpOp, input, colIdx, constArg)
	if err != nil {
		t.Error(err)
	}
	expected := &selLTInt64Int64ConstOp{input: input, colIdx: colIdx, constArg: constVal}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v, expected %+v", op, expected)
	}
}

func TestGetSelectionOperator(t *testing.T) {
	ct := semtypes.Int2
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

func benchmarkSelLTInt64Int64ConstOp(b *testing.B, useSelectionVector bool) {
	rng, _ := randutil.NewPseudoRand()
	ctx := context.Background()

	// We need to generate such a batch that selection operator will output at
	// least one tuple - otherwise, the benchmark will be stuck in an infinite
	// loop, so we put MinInt64 as the first element and make sure that constArg
	// is not MinInt64.
	batch := coldata.NewMemBatch([]types.T{types.Int64})
	col := batch.ColVec(0).Int64()
	col[0] = math.MinInt64
	for i := int64(1); i < coldata.BatchSize; i++ {
		col[i] = rng.Int63()
	}
	batch.SetLength(coldata.BatchSize)
	if useSelectionVector {
		batch.SetSelection(true)
		sel := batch.Selection()
		for i := int64(0); i < coldata.BatchSize; i++ {
			sel[i] = uint16(i)
		}
	}
	constArg := rng.Int63()
	for constArg == math.MinInt64 {
		constArg = rng.Int63()
	}

	source := newRepeatableBatchSource(batch)
	source.Init()

	plusOp := &selLTInt64Int64ConstOp{
		input:    source,
		colIdx:   0,
		constArg: constArg,
	}
	plusOp.Init()

	b.SetBytes(int64(8 * coldata.BatchSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plusOp.Next(ctx)
	}
}

func BenchmarkSelLTInt64Int64ConstOp(b *testing.B) {
	for _, useSel := range []bool{true, false} {
		b.Run(fmt.Sprintf("useSel=%t", useSel), func(b *testing.B) {
			benchmarkSelLTInt64Int64ConstOp(b, useSel)
		})
	}
}

func benchmarkSelLTInt64Int64Op(b *testing.B, useSelectionVector bool) {
	rng, _ := randutil.NewPseudoRand()
	ctx := context.Background()

	batch := coldata.NewMemBatch([]types.T{types.Int64, types.Int64})
	col1 := batch.ColVec(0).Int64()
	col2 := batch.ColVec(1).Int64()
	// We need to generate such a batch that selection operator will output at
	// least one tuple - otherwise, the benchmark will be stuck in an infinite
	// loop, so we put 0 and 1 as the first tuple of the batch.
	col1[0], col2[0] = 0, 1
	for i := int64(1); i < coldata.BatchSize; i++ {
		col1[i] = rng.Int63()
		col2[i] = rng.Int63()
	}
	batch.SetLength(coldata.BatchSize)
	if useSelectionVector {
		batch.SetSelection(true)
		sel := batch.Selection()
		for i := int64(0); i < coldata.BatchSize; i++ {
			sel[i] = uint16(i)
		}
	}
	source := newRepeatableBatchSource(batch)
	source.Init()

	plusOp := &selLTInt64Int64Op{
		input:   source,
		col1Idx: 0,
		col2Idx: 1,
	}
	plusOp.Init()

	b.SetBytes(int64(8 * coldata.BatchSize * 2))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plusOp.Next(ctx)
	}
}

func BenchmarkSelLTInt64Int64Op(b *testing.B) {
	for _, useSel := range []bool{true, false} {
		b.Run(fmt.Sprintf("useSel=%t", useSel), func(b *testing.B) {
			benchmarkSelLTInt64Int64Op(b, useSel)
		})
	}
}
