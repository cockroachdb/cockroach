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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func TestProjPlusInt64Int64ConstOp(t *testing.T) {
	runTests(t, []tuples{{{1}, {2}, {nil}}}, tuples{{1, 2}, {2, 3}, {nil, nil}}, orderedVerifier,
		[]int{0, 1}, func(input []Operator) (Operator, error) {
			return &projPlusInt64Int64ConstOp{
				OneInputNode: NewOneInputNode(input[0]),
				colIdx:       0,
				constArg:     1,
				outputIdx:    1,
			}, nil
		})
}

func TestProjPlusInt64Int64Op(t *testing.T) {
	runTests(t, []tuples{{{1, 2}, {3, 4}, {5, nil}}}, tuples{{1, 2, 3}, {3, 4, 7}, {5, nil, nil}},
		orderedVerifier, []int{0, 1, 2},
		func(input []Operator) (Operator, error) {
			return &projPlusInt64Int64Op{
				OneInputNode: NewOneInputNode(input[0]),
				col1Idx:      0,
				col2Idx:      1,
				outputIdx:    2,
			}, nil
		})
}

func benchmarkProjPlusInt64Int64ConstOp(b *testing.B, useSelectionVector bool, hasNulls bool) {
	ctx := context.Background()

	batch := coldata.NewMemBatch([]coltypes.T{coltypes.Int64, coltypes.Int64})
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
		OneInputNode: NewOneInputNode(source),
		colIdx:       0,
		constArg:     1,
		outputIdx:    1,
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
	op, err := GetProjectionRConstOperator(types.Float, binOp, input, colIdx, constArg, outputIdx)
	if err != nil {
		t.Error(err)
	}
	expected := &projMultFloat64Float64ConstOp{
		OneInputNode: NewOneInputNode(input),
		colIdx:       colIdx,
		constArg:     constVal,
		outputIdx:    outputIdx,
	}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v, expected %+v", op, expected)
	}
}

func TestGetProjectionOperator(t *testing.T) {
	ct := types.Int2
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
		OneInputNode: NewOneInputNode(input),
		col1Idx:      col1Idx,
		col2Idx:      col2Idx,
		outputIdx:    outputIdx,
	}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v, expected %+v", op, expected)
	}
}

func benchmarkProjOp(
	b *testing.B,
	makeProjOp func(source *RepeatableBatchSource) Operator,
	useSelectionVector bool,
	hasNulls bool,
) {
	ctx := context.Background()

	batch := coldata.NewMemBatch([]coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64})
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

	op := makeProjOp(source)
	op.Init()

	b.SetBytes(int64(8 * coldata.BatchSize * 2))
	for i := 0; i < b.N; i++ {
		op.Next(ctx)
	}
}

func BenchmarkProjOp(b *testing.B) {
	projOpMap := map[string]func(*RepeatableBatchSource) Operator{
		"projPlusInt64Int64Op": func(source *RepeatableBatchSource) Operator {
			return &projPlusInt64Int64Op{
				OneInputNode: NewOneInputNode(source),
				col1Idx:      0,
				col2Idx:      1,
				outputIdx:    2,
			}
		},
		"projMinusInt64Int64Op": func(source *RepeatableBatchSource) Operator {
			return &projMinusInt64Int64Op{
				OneInputNode: NewOneInputNode(source),
				col1Idx:      0,
				col2Idx:      1,
				outputIdx:    2,
			}
		},
		"projMultInt64Int64Op": func(source *RepeatableBatchSource) Operator {
			return &projMultInt64Int64Op{
				OneInputNode: NewOneInputNode(source),
				col1Idx:      0,
				col2Idx:      1,
				outputIdx:    2,
			}
		},
		"projDivInt64Int64Op": func(source *RepeatableBatchSource) Operator {
			return &projDivInt64Int64Op{
				OneInputNode: NewOneInputNode(source),
				col1Idx:      0,
				col2Idx:      1,
				outputIdx:    2,
			}
		},
	}

	for projOp, makeProjOp := range projOpMap {
		for _, useSel := range []bool{true, false} {
			for _, hasNulls := range []bool{true, false} {
				b.Run(fmt.Sprintf("op=%s/useSel=%t/hasNulls=%t", projOp, useSel, hasNulls), func(b *testing.B) {
					benchmarkProjOp(b, makeProjOp, useSel, hasNulls)
				})
			}
		}
	}
}
