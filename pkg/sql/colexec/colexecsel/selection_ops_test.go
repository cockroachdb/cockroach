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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/stretchr/testify/require"
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
				OneInputHelper: colexecop.MakeOneInputHelper(input[0]),
				colIdx:         0,
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
				OneInputHelper: colexecop.MakeOneInputHelper(input[0]),
				col1Idx:        0,
				col2Idx:        1,
			},
		}, nil
	})
}

func TestGetSelectionConstOperator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	cmpOp := treecmp.MakeComparisonOperator(treecmp.LT)
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
			OneInputHelper: colexecop.MakeOneInputHelper(input),
			colIdx:         colIdx,
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
	cmpOp := treecmp.MakeComparisonOperator(treecmp.LT)
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
			OneInputHelper: colexecop.MakeOneInputHelper(input),
			colIdx:         colIdx,
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
	cmpOp := treecmp.MakeComparisonOperator(treecmp.GE)
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
			OneInputHelper: colexecop.MakeOneInputHelper(input),
			col1Idx:        col1Idx,
			col2Idx:        col2Idx,
		},
	}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v, expected %+v", op, expected)
	}
}

func runSelOpBenchmarks(
	b *testing.B,
	makeSelOp func(source *colexecop.RepeatableBatchSource) (colexecop.Operator, error),
	inputTypes []*types.T,
) {
	rng, _ := randutil.NewTestRand()
	for _, useSel := range []bool{true, false} {
		for _, hasNulls := range []bool{true, false} {
			batch := testAllocator.NewMemBatchWithMaxCapacity(inputTypes)
			nullProb := 0.0
			if hasNulls {
				nullProb = nullProbability
			}
			for _, colVec := range batch.ColVecs() {
				coldatatestutils.RandomVec(coldatatestutils.RandomVecArgs{
					Rand:             rng,
					Vec:              colVec,
					N:                coldata.BatchSize(),
					NullProbability:  nullProb,
					BytesFixedLength: 8,
				})
			}
			batch.SetLength(coldata.BatchSize())
			if useSel {
				batch.SetSelection(true)
				sel := batch.Selection()
				for i := 0; i < coldata.BatchSize(); i++ {
					sel[i] = i
				}
			}
			source := colexecop.NewRepeatableBatchSource(testAllocator, batch, inputTypes)
			op, err := makeSelOp(source)
			require.NoError(b, err)
			op.Init(context.Background())

			b.Run(fmt.Sprintf("useSel=%t,hasNulls=%t", useSel, hasNulls), func(b *testing.B) {
				b.SetBytes(int64(len(inputTypes) * 8 * coldata.BatchSize()))
				for i := 0; i < b.N; i++ {
					op.Next()
				}
			})
		}
	}
}

func BenchmarkSelLTInt64Int64ConstOp(b *testing.B) {
	inputTypes := []*types.T{types.Int}
	runSelOpBenchmarks(
		b,
		func(source *colexecop.RepeatableBatchSource) (colexecop.Operator, error) {
			constArg := tree.DInt(0)
			return GetSelectionConstOperator(
				treecmp.MakeComparisonOperator(treecmp.LT), source, inputTypes, 0, /* colIdx */
				&constArg, nil /* evalCtx */, nil, /* cmpExpr */
			)
		},
		inputTypes,
	)
}

func BenchmarkSelLTInt64Int64Op(b *testing.B) {
	inputTypes := []*types.T{types.Int, types.Int}
	runSelOpBenchmarks(
		b,
		func(source *colexecop.RepeatableBatchSource) (colexecop.Operator, error) {
			return GetSelectionOperator(
				treecmp.MakeComparisonOperator(treecmp.LT), source, inputTypes, 0 /* col1Idx */, 1, /* col2Idx */
				nil /* evalCtx */, nil, /* cmpExpr */
			)
		},
		inputTypes,
	)
}

func BenchmarkSelLTBytesBytesOp(b *testing.B) {
	inputTypes := []*types.T{types.Bytes, types.Bytes}
	runSelOpBenchmarks(
		b,
		func(source *colexecop.RepeatableBatchSource) (colexecop.Operator, error) {
			return GetSelectionOperator(
				treecmp.MakeComparisonOperator(treecmp.LT), source, inputTypes, 0 /* col1Idx */, 1, /* col2Idx */
				nil /* evalCtx */, nil, /* cmpExpr */
			)
		},
		inputTypes,
	)
}
