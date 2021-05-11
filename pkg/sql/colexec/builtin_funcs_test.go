// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestBasicBuiltinFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Trick to get the init() for the builtins package to run.
	_ = builtins.AllBuiltinNames
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}

	testCases := []struct {
		desc         string
		expr         string
		inputCols    []int
		inputTuples  colexectestutils.Tuples
		inputTypes   []*types.T
		outputTuples colexectestutils.Tuples
	}{
		{
			desc:         "AbsVal",
			expr:         "abs(@1)",
			inputCols:    []int{0},
			inputTuples:  colexectestutils.Tuples{{1}, {-2}},
			inputTypes:   []*types.T{types.Int},
			outputTuples: colexectestutils.Tuples{{1, 1}, {-2, 2}},
		},
		{
			desc:         "StringLen",
			expr:         "length(@1)",
			inputCols:    []int{0},
			inputTuples:  colexectestutils.Tuples{{"Hello"}, {"The"}},
			inputTypes:   []*types.T{types.String},
			outputTuples: colexectestutils.Tuples{{"Hello", 5}, {"The", 3}},
		},
	}

	for _, tc := range testCases {
		log.Infof(ctx, "%s", tc.desc)
		colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{tc.inputTuples}, tc.outputTuples, colexectestutils.OrderedVerifier,
			func(input []colexecop.Operator) (colexecop.Operator, error) {
				return colexectestutils.CreateTestProjectingOperator(
					ctx, flowCtx, input[0], tc.inputTypes,
					tc.expr, false /* canFallbackToRowexec */, testMemAcc,
				)
			})
	}
}

func benchmarkBuiltinFunctions(b *testing.B, useSelectionVector bool, hasNulls bool) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}

	batch := testAllocator.NewMemBatchWithMaxCapacity([]*types.T{types.Int})
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

	typs := []*types.T{types.Int}
	source := colexecop.NewRepeatableBatchSource(testAllocator, batch, typs)
	op, err := colexectestutils.CreateTestProjectingOperator(
		ctx, flowCtx, source, typs,
		"abs(@1)" /* projectingExpr */, false /* canFallbackToRowexec */, testMemAcc,
	)
	require.NoError(b, err)
	op.Init(ctx)

	b.SetBytes(int64(8 * coldata.BatchSize()))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op.Next()
	}
}

func BenchmarkBuiltinFunctions(b *testing.B) {
	_ = builtins.AllBuiltinNames
	for _, useSel := range []bool{true, false} {
		for _, hasNulls := range []bool{true, false} {
			b.Run(fmt.Sprintf("useSel=%t,hasNulls=%t", useSel, hasNulls), func(b *testing.B) {
				benchmarkBuiltinFunctions(b, useSel, hasNulls)
			})
		}
	}
}

// Perform a comparison between the default substring operator
// and the specialized operator.
func BenchmarkCompareSpecializedOperators(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(ctx)

	typs := []*types.T{types.String, types.Int, types.Int}
	batch := testAllocator.NewMemBatchWithMaxCapacity(typs)
	outputIdx := 3
	bCol := batch.ColVec(0).Bytes()
	sCol := batch.ColVec(1).Int64()
	eCol := batch.ColVec(2).Int64()
	for i := 0; i < coldata.BatchSize(); i++ {
		bCol.Set(i, []byte("hello there"))
		sCol[i] = 1
		eCol[i] = 4
	}
	batch.SetLength(coldata.BatchSize())
	var source colexecop.Operator
	source = colexecop.NewRepeatableBatchSource(testAllocator, batch, typs)
	source = colexecutils.NewVectorTypeEnforcer(testAllocator, source, types.Bytes, outputIdx)

	// Set up the default operator.
	expr, err := parser.ParseExpr("substring(@1, @2, @3)")
	if err != nil {
		b.Fatal(err)
	}
	inputCols := []int{0, 1, 2}
	p := &colexectestutils.MockTypeContext{Typs: typs}
	semaCtx := tree.MakeSemaContext()
	semaCtx.IVarContainer = p
	typedExpr, err := tree.TypeCheck(ctx, expr, &semaCtx, types.Any)
	if err != nil {
		b.Fatal(err)
	}
	defaultOp := &defaultBuiltinFuncOperator{
		OneInputHelper:      colexecop.MakeOneInputHelper(source),
		allocator:           testAllocator,
		evalCtx:             evalCtx,
		funcExpr:            typedExpr.(*tree.FuncExpr),
		outputIdx:           outputIdx,
		columnTypes:         typs,
		outputType:          types.String,
		toDatumConverter:    colconv.NewVecToDatumConverter(len(typs), inputCols, false /* willRelease */),
		datumToVecConverter: colconv.GetDatumToPhysicalFn(types.String),
		row:                 make(tree.Datums, outputIdx),
		argumentCols:        inputCols,
	}
	defaultOp.Init(ctx)

	// Set up the specialized substring operator.
	specOp := newSubstringOperator(
		testAllocator, typs, inputCols, outputIdx, source,
	)
	specOp.Init(ctx)

	b.Run("DefaultBuiltinOperator", func(b *testing.B) {
		b.SetBytes(int64(len("hello there") * coldata.BatchSize()))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b := defaultOp.Next()
			// Due to the flat byte updates, we have to reset the output
			// bytes col after each next call.
			b.ColVec(outputIdx).Bytes().Reset()
		}
	})

	b.Run("SpecializedSubstringOperator", func(b *testing.B) {
		b.SetBytes(int64(len("hello there") * coldata.BatchSize()))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b := specOp.Next()
			// Due to the flat byte updates, we have to reset the output
			// bytes col after each next call.
			b.ColVec(outputIdx).Bytes().Reset()
		}
	})
}
