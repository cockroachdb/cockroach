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
	op, err := GetProjectionRConstOperator(types.Float, types.Float, binOp, input, colIdx, constArg, outputIdx)
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

func TestGetProjectionConstMixedTypeOperator(t *testing.T) {
	binOp := tree.GE
	var input Operator
	colIdx := 3
	constVal := int16(31)
	constArg := tree.NewDInt(tree.DInt(constVal))
	outputIdx := 5
	op, err := GetProjectionRConstOperator(types.Int, types.Int2, binOp, input, colIdx, constArg, outputIdx)
	if err != nil {
		t.Error(err)
	}
	expected := &projGEInt64Int16ConstOp{
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
	op, err := GetProjectionOperator(ct, ct, binOp, input, col1Idx, col2Idx, outputIdx)
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
	makeProjOp func(source *RepeatableBatchSource, intType coltypes.T) Operator,
	useSelectionVector bool,
	hasNulls bool,
	intType coltypes.T,
) {
	ctx := context.Background()

	var batch coldata.Batch
	switch intType {
	case coltypes.Int64:
		batch = coldata.NewMemBatch([]coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64})
		col1 := batch.ColVec(0).Int64()
		col2 := batch.ColVec(1).Int64()
		for i := int64(0); i < coldata.BatchSize; i++ {
			col1[i] = 1
			col2[i] = 1
		}
	case coltypes.Int32:
		batch = coldata.NewMemBatch([]coltypes.T{coltypes.Int32, coltypes.Int32, coltypes.Int32})
		col1 := batch.ColVec(0).Int32()
		col2 := batch.ColVec(1).Int32()
		for i := int32(0); i < coldata.BatchSize; i++ {
			col1[i] = 1
			col2[i] = 1
		}
	default:
		b.Fatalf("unsupported type: %s", intType)
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

	op := makeProjOp(source, intType)
	op.Init()

	b.SetBytes(int64(8 * coldata.BatchSize * 2))
	for i := 0; i < b.N; i++ {
		op.Next(ctx)
	}
}

func BenchmarkProjOp(b *testing.B) {
	projOpMap := map[string]func(*RepeatableBatchSource, coltypes.T) Operator{
		"projPlusIntIntOp": func(source *RepeatableBatchSource, intType coltypes.T) Operator {
			switch intType {
			case coltypes.Int64:
				return &projPlusInt64Int64Op{
					OneInputNode: NewOneInputNode(source),
					col1Idx:      0,
					col2Idx:      1,
					outputIdx:    2,
				}
			case coltypes.Int32:
				return &projPlusInt32Int32Op{
					OneInputNode: NewOneInputNode(source),
					col1Idx:      0,
					col2Idx:      1,
					outputIdx:    2,
				}
			default:
				b.Fatalf("unsupported type: %s", intType)
				return nil
			}
		},
		"projMinusIntIntOp": func(source *RepeatableBatchSource, intType coltypes.T) Operator {
			switch intType {
			case coltypes.Int64:
				return &projMinusInt64Int64Op{
					OneInputNode: NewOneInputNode(source),
					col1Idx:      0,
					col2Idx:      1,
					outputIdx:    2,
				}
			case coltypes.Int32:
				return &projMinusInt32Int32Op{
					OneInputNode: NewOneInputNode(source),
					col1Idx:      0,
					col2Idx:      1,
					outputIdx:    2,
				}
			default:
				b.Fatalf("unsupported type: %s", intType)
				return nil
			}
		},
		"projMultIntIntOp": func(source *RepeatableBatchSource, intType coltypes.T) Operator {
			switch intType {
			case coltypes.Int64:
				return &projMultInt64Int64Op{
					OneInputNode: NewOneInputNode(source),
					col1Idx:      0,
					col2Idx:      1,
					outputIdx:    2,
				}
			case coltypes.Int32:
				return &projMultInt32Int32Op{
					OneInputNode: NewOneInputNode(source),
					col1Idx:      0,
					col2Idx:      1,
					outputIdx:    2,
				}
			default:
				b.Fatalf("unsupported type: %s", intType)
				return nil
			}
		},
		"projDivIntIntOp": func(source *RepeatableBatchSource, intType coltypes.T) Operator {
			switch intType {
			case coltypes.Int64:
				return &projDivInt64Int64Op{
					OneInputNode: NewOneInputNode(source),
					col1Idx:      0,
					col2Idx:      1,
					outputIdx:    2,
				}
			case coltypes.Int32:
				return &projDivInt32Int32Op{
					OneInputNode: NewOneInputNode(source),
					col1Idx:      0,
					col2Idx:      1,
					outputIdx:    2,
				}
			default:
				b.Fatalf("unsupported type: %s", intType)
				return nil
			}
		},
	}

	for projOp, makeProjOp := range projOpMap {
		for _, intType := range []coltypes.T{coltypes.Int64, coltypes.Int32} {
			for _, useSel := range []bool{true, false} {
				for _, hasNulls := range []bool{true, false} {
					b.Run(fmt.Sprintf("op=%s/type=%s/useSel=%t/hasNulls=%t",
						projOp, intType, useSel, hasNulls), func(b *testing.B) {
						benchmarkProjOp(b, makeProjOp, useSel, hasNulls, intType)
					})
				}
			}
		}
	}
}
