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
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestBasicBuiltinFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
		inputTuples  tuples
		inputTypes   []*types.T
		outputTuples tuples
	}{
		{
			desc:         "AbsVal",
			expr:         "abs(@1)",
			inputCols:    []int{0},
			inputTuples:  tuples{{1}, {-2}},
			inputTypes:   []*types.T{types.Int},
			outputTuples: tuples{{1, 1}, {-2, 2}},
		},
		{
			desc:         "StringLen",
			expr:         "length(@1)",
			inputCols:    []int{0},
			inputTuples:  tuples{{"Hello"}, {"The"}},
			inputTypes:   []*types.T{types.String},
			outputTuples: tuples{{"Hello", 5}, {"The", 3}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			runTests(t, []tuples{tc.inputTuples}, tc.outputTuples, orderedVerifier,
				func(input []colexecbase.Operator) (colexecbase.Operator, error) {
					return createTestProjectingOperator(
						ctx, flowCtx, input[0], tc.inputTypes,
						tc.expr, false, /* canFallbackToRowexec */
					)
				})
		})
	}
}

func benchmarkBuiltinFunctions(b *testing.B, useSelectionVector bool, hasNulls bool) {
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

	batch := testAllocator.NewMemBatch([]*types.T{types.Int})
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
	source := colexecbase.NewRepeatableBatchSource(testAllocator, batch, typs)
	op, err := createTestProjectingOperator(
		ctx, flowCtx, source, typs,
		"abs(@1)" /* projectingExpr */, false, /* canFallbackToRowexec */
	)
	require.NoError(b, err)
	op.Init()

	b.SetBytes(int64(8 * coldata.BatchSize()))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op.Next(ctx)
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
	ctx := context.Background()
	tctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	typs := []*types.T{types.String, types.Int, types.Int}
	batch := testAllocator.NewMemBatch(typs)
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
	var source colexecbase.Operator
	source = colexecbase.NewRepeatableBatchSource(testAllocator, batch, typs)
	source = newVectorTypeEnforcer(testAllocator, source, types.Bytes, outputIdx)

	// Set up the default operator.
	expr, err := parser.ParseExpr("substring(@1, @2, @3)")
	if err != nil {
		b.Fatal(err)
	}
	inputCols := []int{0, 1, 2}
	p := &mockTypeContext{typs: typs}
	semaCtx := tree.MakeSemaContext()
	semaCtx.IVarContainer = p
	typedExpr, err := tree.TypeCheck(ctx, expr, &semaCtx, types.Any)
	if err != nil {
		b.Fatal(err)
	}
	defaultOp := &defaultBuiltinFuncOperator{
		OneInputNode:        NewOneInputNode(source),
		allocator:           testAllocator,
		evalCtx:             tctx,
		funcExpr:            typedExpr.(*tree.FuncExpr),
		outputIdx:           outputIdx,
		columnTypes:         typs,
		outputType:          types.String,
		toDatumConverter:    newVecToDatumConverter(len(typs), inputCols),
		datumToVecConverter: GetDatumToPhysicalFn(types.String),
		row:                 make(tree.Datums, outputIdx),
		argumentCols:        inputCols,
	}
	defaultOp.Init()

	// Set up the specialized substring operator.
	specOp := newSubstringOperator(
		testAllocator, typs, inputCols, outputIdx, source,
	)
	specOp.Init()

	b.Run("DefaultBuiltinOperator", func(b *testing.B) {
		b.SetBytes(int64(len("hello there") * coldata.BatchSize()))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b := defaultOp.Next(ctx)
			// Due to the flat byte updates, we have to reset the output
			// bytes col after each next call.
			b.ColVec(outputIdx).Bytes().Reset()
		}
	})

	b.Run("SpecializedSubstringOperator", func(b *testing.B) {
		b.SetBytes(int64(len("hello there") * coldata.BatchSize()))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b := specOp.Next(ctx)
			// Due to the flat byte updates, we have to reset the output
			// bytes col after each next call.
			b.ColVec(outputIdx).Bytes().Reset()
		}
	})
}
