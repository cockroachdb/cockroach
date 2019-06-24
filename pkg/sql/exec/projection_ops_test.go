// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/types"
)

func TestProjPlusInt64Int64ConstOp(t *testing.T) {
	runTests(t, []tuples{{{1}, {2}, {nil}}}, func(t *testing.T, input []Operator) {
		op := projPlusInt64Int64ConstOp{
			input:     input[0],
			colIdx:    0,
			constArg:  1,
			outputIdx: 1,
		}
		op.Init()
		out := newOpTestOutput(&op, []int{0, 1}, tuples{{1, 2}, {2, 3}, {nil, nil}})
		if err := out.Verify(); err != nil {
			t.Error(err)
		}
	})
}

func TestProjPlusInt64Int64Op(t *testing.T) {
	runTests(t, []tuples{{{1, 2}, {3, 4}, {5, nil}}}, func(t *testing.T, input []Operator) {
		op := projPlusInt64Int64Op{
			input:     input[0],
			col1Idx:   0,
			col2Idx:   1,
			outputIdx: 2,
		}
		op.Init()
		out := newOpTestOutput(&op, []int{0, 1, 2}, tuples{{1, 2, 3}, {3, 4, 7}, {5, nil, nil}})
		if err := out.Verify(); err != nil {
			t.Error(err)
		}
	})
}

func benchmarkProjPlusInt64Int64ConstOp(b *testing.B, useSelectionVector bool, hasNulls bool) {
	ctx := context.Background()

	batch := coldata.NewMemBatch([]types.T{types.Int64, types.Int64})
	col := batch.ColVec(0).Int64()
	for i := int64(0); i < coldata.BatchSize; i++ {
		col[i] = 1
	}
	if hasNulls {
		for i := 0; i < coldata.BatchSize; i++ {
			if rand.Float64() < nullProbability {
				batch.ColVec(0).Nulls().SetNull(uint16(i))
			}
		}
	}
	batch.SetLength(coldata.BatchSize)
	if useSelectionVector {
		batch.SetSelection(true)
		sel := batch.Selection()
		for i := int64(0); i < coldata.BatchSize; i++ {
			sel[i] = uint16(i)
		}
	}
	source := NewRepeatableBatchSource(batch)
	source.Init()

	plusOp := &projPlusInt64Int64ConstOp{
		input:     source,
		colIdx:    0,
		constArg:  1,
		outputIdx: 1,
	}
	plusOp.Init()

	b.SetBytes(int64(8 * coldata.BatchSize))
	for i := 0; i < b.N; i++ {
		plusOp.Next(ctx)
	}
}

func BenchmarkProjPlusInt64Int64ConstOp(b *testing.B) {
	for _, useSel := range []bool{true, false} {
		for _, hasNulls := range []bool{true, false} {
			b.Run(fmt.Sprintf("useSel=%t,hasNulls=%t", useSel, hasNulls), func(b *testing.B) {
				benchmarkProjPlusInt64Int64ConstOp(b, useSel, hasNulls)
			})
		}
	}
}

func TestGetProjectionConstOperator(t *testing.T) {
	binOp := tree.Mult
	var input Operator
	colIdx := 3
	constVal := float64(31.37)
	constArg := tree.NewDFloat(tree.DFloat(constVal))
	outputIdx := 5
	op, err := GetProjectionRConstOperator(semtypes.Float, binOp, input, colIdx, constArg, outputIdx)
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
	ct := semtypes.Int2
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

func benchmarkProjPlusInt64Int64Op(b *testing.B, useSelectionVector bool, hasNulls bool) {
	ctx := context.Background()

	batch := coldata.NewMemBatch([]types.T{types.Int64, types.Int64, types.Int64})
	col1 := batch.ColVec(0).Int64()
	col2 := batch.ColVec(1).Int64()
	for i := int64(0); i < coldata.BatchSize; i++ {
		col1[i] = 1
		col2[i] = 1
	}
	if hasNulls {
		for i := 0; i < coldata.BatchSize; i++ {
			if rand.Float64() < nullProbability {
				batch.ColVec(0).Nulls().SetNull(uint16(i))
			}
			if rand.Float64() < nullProbability {
				batch.ColVec(1).Nulls().SetNull(uint16(i))
			}
		}
	}
	batch.SetLength(coldata.BatchSize)
	if useSelectionVector {
		batch.SetSelection(true)
		sel := batch.Selection()
		for i := int64(0); i < coldata.BatchSize; i++ {
			sel[i] = uint16(i)
		}
	}
	source := NewRepeatableBatchSource(batch)
	source.Init()

	plusOp := &projPlusInt64Int64Op{
		input:     source,
		col1Idx:   0,
		col2Idx:   1,
		outputIdx: 2,
	}
	plusOp.Init()

	b.SetBytes(int64(8 * coldata.BatchSize * 2))
	for i := 0; i < b.N; i++ {
		plusOp.Next(ctx)
	}
}

func BenchmarkProjPlusInt64Int64Op(b *testing.B) {
	for _, useSel := range []bool{true, false} {
		for _, hasNulls := range []bool{true, false} {
			b.Run(fmt.Sprintf("useSel=%t,hasNulls=%t", useSel, hasNulls), func(b *testing.B) {
				benchmarkProjPlusInt64Int64Op(b, useSel, hasNulls)
			})
		}
	}
}
