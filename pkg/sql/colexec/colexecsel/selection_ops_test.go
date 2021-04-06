// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecsel

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
)

const (
	selectivity     = .5
	nullProbability = .1
)

func TestSelLTInt64Int64ConstOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tups := colexectestutils.Tuples{{0}, {1}, {2}, {nil}}
	colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{tups}, colexectestutils.Tuples{{0}, {1}}, colexectestutils.OrderedVerifier, func(input []colexecop.Operator) (colexecop.Operator, error) {
		return &selLTInt64Int64ConstOp{
			selConstOpBase: selConstOpBase{
				OneInputNode: colexecop.NewOneInputNode(input[0]),
				colIdx:       0,
			},
			constArg: 2,
		}, nil
	})
}

func TestSelLTInt64Int64(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tups := colexectestutils.Tuples{
		{0, 0},
		{0, 1},
		{1, 0},
		{1, 1},
		{nil, 1},
		{-1, nil},
		{nil, nil},
	}
	colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{tups}, colexectestutils.Tuples{{0, 1}}, colexectestutils.OrderedVerifier, func(input []colexecop.Operator) (colexecop.Operator, error) {
		return &selLTInt64Int64Op{
			selOpBase: selOpBase{
				OneInputNode: colexecop.NewOneInputNode(input[0]),
				col1Idx:      0,
				col2Idx:      1,
			},
		}, nil
	})
}

func TestGetSelectionConstOperator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	cmpOp := tree.LT
	var input colexecop.Operator
	colIdx := 3
	inputTypes := make([]*types.T, colIdx+1)
	inputTypes[colIdx] = types.Date
	constVal := int64(31)
	constArg := tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(constVal))
	op, err := GetSelectionConstOperator(
		cmpOp, input, inputTypes, colIdx, constArg, nil /* EvalCtx */, nil, /* cmpExpr */
	)
	if err != nil {
		t.Error(err)
	}
	expected := &selLTInt64Int64ConstOp{
		selConstOpBase: selConstOpBase{
			OneInputNode: colexecop.NewOneInputNode(input),
			colIdx:       colIdx,
		},
		constArg: constVal,
	}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v, expected %+v", op, expected)
	}
}

func TestGetSelectionConstMixedTypeOperator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	cmpOp := tree.LT
	var input colexecop.Operator
	colIdx := 3
	inputTypes := make([]*types.T, colIdx+1)
	inputTypes[colIdx] = types.Int2
	constVal := int64(31)
	constArg := tree.NewDInt(tree.DInt(constVal))
	op, err := GetSelectionConstOperator(
		cmpOp, input, inputTypes, colIdx, constArg, nil /* EvalCtx */, nil, /* cmpExpr */
	)
	if err != nil {
		t.Error(err)
	}
	expected := &selLTInt16Int64ConstOp{
		selConstOpBase: selConstOpBase{
			OneInputNode: colexecop.NewOneInputNode(input),
			colIdx:       colIdx,
		},
		constArg: constVal,
	}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v, expected %+v", op, expected)
	}
}

func TestGetSelectionOperator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ct := types.Int2
	cmpOp := tree.GE
	var input colexecop.Operator
	col1Idx := 5
	col2Idx := 7
	inputTypes := make([]*types.T, col2Idx+1)
	inputTypes[col1Idx] = ct
	inputTypes[col2Idx] = ct
	op, err := GetSelectionOperator(
		cmpOp, input, inputTypes, col1Idx, col2Idx, nil /* EvalCtx */, nil, /* cmpExpr */
	)
	if err != nil {
		t.Error(err)
	}
	expected := &selGEInt16Int16Op{
		selOpBase: selOpBase{
			OneInputNode: colexecop.NewOneInputNode(input),
			col1Idx:      col1Idx,
			col2Idx:      col2Idx,
		},
	}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v, expected %+v", op, expected)
	}
}

func benchmarkSelLTInt64Int64ConstOp(b *testing.B, useSelectionVector bool, hasNulls bool) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	typs := []*types.T{types.Int}
	batch := testAllocator.NewMemBatchWithMaxCapacity(typs)
	col := batch.ColVec(0).Int64()
	for i := 0; i < coldata.BatchSize(); i++ {
		if float64(i) < float64(coldata.BatchSize())*selectivity {
			col[i] = -1
		} else {
			col[i] = 1
		}
	}
	if hasNulls {
		for i := 0; i < coldata.BatchSize(); i++ {
			if rand.Float64() < nullProbability {
				batch.ColVec(0).Nulls().SetNull(i)
			}
		}
	}
	batch.SetLength(coldata.BatchSize())
	if useSelectionVector {
		batch.SetSelection(true)
		sel := batch.Selection()
		for i := 0; i < coldata.BatchSize(); i++ {
			sel[i] = i
		}
	}
	source := colexecop.NewRepeatableBatchSource(testAllocator, batch, typs)
	source.Init()

	plusOp := &selLTInt64Int64ConstOp{
		selConstOpBase: selConstOpBase{
			OneInputNode: colexecop.NewOneInputNode(source),
			colIdx:       0,
		},
		constArg: 0,
	}
	plusOp.Init()

	b.SetBytes(int64(8 * coldata.BatchSize()))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plusOp.Next(ctx)
	}
}

func BenchmarkSelLTInt64Int64ConstOp(b *testing.B) {
	for _, useSel := range []bool{true, false} {
		for _, hasNulls := range []bool{true, false} {
			b.Run(fmt.Sprintf("useSel=%t,hasNulls=%t", useSel, hasNulls), func(b *testing.B) {
				benchmarkSelLTInt64Int64ConstOp(b, useSel, hasNulls)
			})
		}
	}
}

func benchmarkSelLTInt64Int64Op(b *testing.B, useSelectionVector bool, hasNulls bool) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	typs := []*types.T{types.Int, types.Int}
	batch := testAllocator.NewMemBatchWithMaxCapacity(typs)
	col1 := batch.ColVec(0).Int64()
	col2 := batch.ColVec(1).Int64()
	for i := 0; i < coldata.BatchSize(); i++ {
		if float64(i) < float64(coldata.BatchSize())*selectivity {
			col1[i], col2[i] = -1, 1
		} else {
			col1[i], col2[i] = 1, -1
		}
	}
	if hasNulls {
		for i := 0; i < coldata.BatchSize(); i++ {
			if rand.Float64() < nullProbability {
				batch.ColVec(0).Nulls().SetNull(i)
			}
			if rand.Float64() < nullProbability {
				batch.ColVec(1).Nulls().SetNull(i)
			}
		}
	}
	batch.SetLength(coldata.BatchSize())
	if useSelectionVector {
		batch.SetSelection(true)
		sel := batch.Selection()
		for i := 0; i < coldata.BatchSize(); i++ {
			sel[i] = i
		}
	}
	source := colexecop.NewRepeatableBatchSource(testAllocator, batch, typs)
	source.Init()

	plusOp := &selLTInt64Int64Op{
		selOpBase: selOpBase{
			OneInputNode: colexecop.NewOneInputNode(source),
			col1Idx:      0,
			col2Idx:      1,
		},
	}
	plusOp.Init()

	b.SetBytes(int64(8 * coldata.BatchSize() * 2))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plusOp.Next(ctx)
	}
}

func BenchmarkSelLTInt64Int64Op(b *testing.B) {
	for _, useSel := range []bool{true, false} {
		for _, hasNulls := range []bool{true, false} {
			b.Run(fmt.Sprintf("useSel=%t,hasNulls=%t", useSel, hasNulls), func(b *testing.B) {
				benchmarkSelLTInt64Int64Op(b, useSel, hasNulls)
			})
		}
	}
}
