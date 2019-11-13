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
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// Mock typing context for the typechecker.
type mockTypeContext struct {
	typs []types.T
}

func (p *mockTypeContext) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return tree.DNull.Eval(ctx)
}

func (p *mockTypeContext) IndexedVarResolvedType(idx int) *types.T {
	return &p.typs[idx]
}

func (p *mockTypeContext) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	n := tree.Name(fmt.Sprintf("$%d", idx))
	return &n
}

func TestBasicBuiltinFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Trick to get the init() for the builtins package to run.
	_ = builtins.AllBuiltinNames

	testCases := []struct {
		desc         string
		expr         string
		inputCols    []int
		inputTuples  tuples
		inputTypes   []types.T
		outputTypes  []types.T
		outputTuples tuples
	}{
		{
			desc:         "AbsVal",
			expr:         "abs(@1)",
			inputCols:    []int{0},
			inputTuples:  tuples{{1}, {-1}},
			inputTypes:   []types.T{*types.Int},
			outputTuples: tuples{{1, 1}, {-1, 1}},
			outputTypes:  []types.T{*types.Int, *types.Int},
		},
		{
			desc:         "StringLen",
			expr:         "length(@1)",
			inputCols:    []int{0},
			inputTuples:  tuples{{"Hello"}, {"The"}},
			inputTypes:   []types.T{*types.String},
			outputTuples: tuples{{"Hello", 5}, {"The", 3}},
			outputTypes:  []types.T{*types.String, *types.Int},
		},
	}

	tctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			runTests(t, []tuples{tc.inputTuples}, tc.outputTuples, orderedVerifier,
				func(input []Operator) (Operator, error) {
					expr, err := parser.ParseExpr(tc.expr)
					if err != nil {
						t.Fatal(err)
					}

					p := &mockTypeContext{typs: tc.inputTypes}
					typedExpr, err := tree.TypeCheck(expr, &tree.SemaContext{IVarContainer: p}, types.Any)
					if err != nil {
						t.Fatal(err)
					}

					return NewBuiltinFunctionOperator(testAllocator, tctx, typedExpr.(*tree.FuncExpr), tc.outputTypes, tc.inputCols, 1, input[0]), nil
				})
		})
	}
}

func benchmarkBuiltinFunctions(b *testing.B, useSelectionVector bool, hasNulls bool) {
	ctx := context.Background()
	tctx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	batch := testAllocator.NewMemBatch([]coltypes.T{coltypes.Int64})
	col := batch.ColVec(0).Int64()

	for i := 0; i < int(coldata.BatchSize()); i++ {
		if float64(i) < float64(coldata.BatchSize())*selectivity {
			col[i] = -1
		} else {
			col[i] = 1
		}
	}

	if hasNulls {
		for i := 0; i < int(coldata.BatchSize()); i++ {
			if rand.Float64() < nullProbability {
				batch.ColVec(0).Nulls().SetNull(uint16(i))
			}
		}
	}

	batch.SetLength(coldata.BatchSize())

	if useSelectionVector {
		batch.SetSelection(true)
		sel := batch.Selection()
		for i := 0; i < int(coldata.BatchSize()); i++ {
			sel[i] = uint16(i)
		}
	}

	source := NewRepeatableBatchSource(batch)
	source.Init()

	expr, err := parser.ParseExpr("abs(@1)")
	if err != nil {
		b.Fatal(err)
	}
	p := &mockTypeContext{typs: []types.T{*types.Int}}
	typedExpr, err := tree.TypeCheck(expr, &tree.SemaContext{IVarContainer: p}, types.Any)
	if err != nil {
		b.Fatal(err)
	}
	op := NewBuiltinFunctionOperator(testAllocator, tctx, typedExpr.(*tree.FuncExpr), []types.T{*types.Int}, []int{0}, 1, source)

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

	batch := testAllocator.NewMemBatch([]coltypes.T{coltypes.Bytes, coltypes.Int64, coltypes.Int64, coltypes.Bytes})
	bCol := batch.ColVec(0).Bytes()
	sCol := batch.ColVec(1).Int64()
	eCol := batch.ColVec(2).Int64()
	outCol := batch.ColVec(3).Bytes()
	for i := 0; i < int(coldata.BatchSize()); i++ {
		bCol.Set(i, []byte("hello there"))
		sCol[i] = 1
		eCol[i] = 4
	}
	batch.SetLength(coldata.BatchSize())
	source := NewRepeatableBatchSource(batch)
	source.Init()

	// Set up the default operator.
	expr, err := parser.ParseExpr("substring(@1, @2, @3)")
	if err != nil {
		b.Fatal(err)
	}
	typs := []types.T{*types.String, *types.Int, *types.Int}
	inputCols := []int{0, 1, 2}
	p := &mockTypeContext{typs: typs}
	typedExpr, err := tree.TypeCheck(expr, &tree.SemaContext{IVarContainer: p}, types.Any)
	if err != nil {
		b.Fatal(err)
	}
	defaultOp := &defaultBuiltinFuncOperator{
		OneInputNode:   NewOneInputNode(source),
		allocator:      testAllocator,
		evalCtx:        tctx,
		funcExpr:       typedExpr.(*tree.FuncExpr),
		outputIdx:      3,
		columnTypes:    typs,
		outputType:     types.String,
		outputPhysType: coltypes.Bytes,
		converter:      typeconv.GetDatumToPhysicalFn(types.String),
		row:            make(tree.Datums, 3),
		argumentCols:   inputCols,
	}
	defaultOp.Init()

	// Set up the specialized substring operator.
	specOp := &substringFunctionOperator{
		OneInputNode: NewOneInputNode(source),
		allocator:    testAllocator,
		argumentCols: inputCols,
		outputIdx:    3,
	}
	specOp.Init()

	b.Run("DefaultBuiltinOperator", func(b *testing.B) {
		b.SetBytes(int64(len("hello there") * int(coldata.BatchSize())))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			defaultOp.Next(ctx)
			// Due to the flat byte updates, we have to reset the output
			// bytes col after each next call.
			outCol.Reset()
		}
	})

	b.Run("SpecializedSubstringOperator", func(b *testing.B) {
		b.SetBytes(int64(len("hello there") * int(coldata.BatchSize())))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			specOp.Next(ctx)
			// Due to the flat byte updates, we have to reset the output
			// bytes col after each next call.
			outCol.Reset()
		}
	})
}
