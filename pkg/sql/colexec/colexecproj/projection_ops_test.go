// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecproj

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProjPlusInt64Int64ConstOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Mon:     evalCtx.TestingMon,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}
	colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{{{1}, {2}, {nil}}}, colexectestutils.Tuples{{1, 2}, {2, 3}, {nil, nil}}, colexectestutils.OrderedVerifier,
		func(input []colexecop.Operator) (colexecop.Operator, error) {
			return colexectestutils.CreateTestProjectingOperator(
				ctx, flowCtx, input[0], []*types.T{types.Int},
				"@1 + 1" /* projectingExpr */, testMemAcc,
			)
		})
}

func TestProjPlusInt64Int64Op(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Mon:     evalCtx.TestingMon,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}
	colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{{{1, 2}, {3, 4}, {5, nil}}}, colexectestutils.Tuples{{1, 2, 3}, {3, 4, 7}, {5, nil, nil}}, colexectestutils.OrderedVerifier,
		func(input []colexecop.Operator) (colexecop.Operator, error) {
			return colexectestutils.CreateTestProjectingOperator(
				ctx, flowCtx, input[0], []*types.T{types.Int, types.Int},
				"@1 + @2" /* projectingExpr */, testMemAcc,
			)
		})
}

func TestProjDivFloat64Float64Op(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Mon:     evalCtx.TestingMon,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}
	colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{{{1.0, 2.0}, {3.0, 4.0}, {5.0, nil}}}, colexectestutils.Tuples{{1.0, 2.0, 0.5}, {3.0, 4.0, 0.75}, {5.0, nil, nil}}, colexectestutils.OrderedVerifier,
		func(input []colexecop.Operator) (colexecop.Operator, error) {
			return colexectestutils.CreateTestProjectingOperator(
				ctx, flowCtx, input[0], []*types.T{types.Float, types.Float},
				"@1 / @2" /* projectingExpr */, testMemAcc,
			)
		})
}

// TestRandomComparisons runs comparisons against all scalar types with random
// non-null data verifying that the result of Datum.Compare matches the result
// of the exec projection.
func TestRandomComparisons(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Mon:     evalCtx.TestingMon,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}
	const numTuples = 2048
	rng, _ := randutil.NewTestRand()

	expected := make([]bool, numTuples)
	var da tree.DatumAlloc
	lDatums := make([]tree.Datum, numTuples)
	rDatums := make([]tree.Datum, numTuples)
	for _, typ := range types.Scalar {
		if typ.Family() == types.DateFamily {
			// TODO(jordan): #40354 tracks failure to compare infinite dates.
			continue
		}
		typs := []*types.T{typ, typ, types.Bool}
		bytesFixedLength := 0
		if typ.Family() == types.UuidFamily {
			bytesFixedLength = 16
		}
		b := testAllocator.NewMemBatchWithFixedCapacity(typs, numTuples)
		lVec := b.ColVec(0)
		rVec := b.ColVec(1)
		ret := b.ColVec(2)
		for _, vec := range []coldata.Vec{lVec, rVec} {
			coldatatestutils.RandomVec(
				coldatatestutils.RandomVecArgs{
					Rand:             rng,
					Vec:              vec,
					N:                numTuples,
					NullProbability:  0,
					BytesFixedLength: bytesFixedLength,
				},
			)
		}
		colconv.ColVecToDatumAndDeselect(lDatums, lVec, numTuples, nil /* sel */, &da)
		colconv.ColVecToDatumAndDeselect(rDatums, rVec, numTuples, nil /* sel */, &da)
		supportedCmpOps := []treecmp.ComparisonOperatorSymbol{treecmp.EQ, treecmp.NE, treecmp.LT, treecmp.LE, treecmp.GT, treecmp.GE}
		if typ.Family() == types.JsonFamily {
			supportedCmpOps = []treecmp.ComparisonOperatorSymbol{treecmp.EQ, treecmp.NE}
		}
		for _, cmpOpSymbol := range supportedCmpOps {
			for i := range lDatums {
				cmp := lDatums[i].Compare(&evalCtx, rDatums[i])
				var b bool
				switch cmpOpSymbol {
				case treecmp.EQ:
					b = cmp == 0
				case treecmp.NE:
					b = cmp != 0
				case treecmp.LT:
					b = cmp < 0
				case treecmp.LE:
					b = cmp <= 0
				case treecmp.GT:
					b = cmp > 0
				case treecmp.GE:
					b = cmp >= 0
				}
				expected[i] = b
			}
			cmpOp := treecmp.MakeComparisonOperator(cmpOpSymbol)
			input := colexectestutils.NewChunkingBatchSource(testAllocator, typs, []coldata.Vec{lVec, rVec, ret}, numTuples)
			op, err := colexectestutils.CreateTestProjectingOperator(
				ctx, flowCtx, input, []*types.T{typ, typ},
				fmt.Sprintf("@1 %s @2", cmpOp), testMemAcc,
			)
			require.NoError(t, err)
			op.Init(ctx)
			var idx int
			for batch := op.Next(); batch.Length() > 0; batch = op.Next() {
				for i := 0; i < batch.Length(); i++ {
					absIdx := idx + i
					assert.Equal(t, expected[absIdx], batch.ColVec(2).Bool()[i],
						"expected %s %s %s (%s[%d]) to be %t found %t", lDatums[absIdx], cmpOp, rDatums[absIdx], typ, absIdx,
						expected[absIdx], ret.Bool()[i])
				}
				idx += batch.Length()
			}
		}
	}
}

func TestGetProjectionOperator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	typ := types.Int2
	binOp := treebin.MakeBinaryOperator(treebin.Mult)
	var input colexecop.Operator
	col1Idx := 5
	col2Idx := 7
	inputTypes := make([]*types.T, col2Idx+1)
	inputTypes[col1Idx] = typ
	inputTypes[col2Idx] = typ
	outputIdx := 9
	calledOnNullInput := false
	op, err := GetProjectionOperator(
		testAllocator, inputTypes, types.Int2, binOp, input, col1Idx, col2Idx,
		outputIdx, nil /* EvalCtx */, nil /* BinFn */, nil /* cmpExpr */, calledOnNullInput,
	)
	if err != nil {
		t.Error(err)
	}
	expected := &projMultInt16Int16Op{
		projOpBase: projOpBase{
			OneInputHelper:    colexecop.MakeOneInputHelper(op.(*projMultInt16Int16Op).Input),
			allocator:         testAllocator,
			col1Idx:           col1Idx,
			col2Idx:           col2Idx,
			outputIdx:         outputIdx,
			calledOnNullInput: calledOnNullInput,
		},
	}
	if !reflect.DeepEqual(op, expected) {
		t.Errorf("got %+v,\nexpected %+v", op, expected)
	}
}

func benchmarkProjOp(
	b *testing.B,
	name string,
	makeProjOp func(source *colexecop.RepeatableBatchSource) (colexecop.Operator, error),
	inputTypes []*types.T,
	useSelectionVector bool,
	hasNulls bool,
) {
	ctx := context.Background()
	rng, _ := randutil.NewTestRand()
	batch := testAllocator.NewMemBatchWithMaxCapacity(inputTypes)
	nullProb := 0.0
	if hasNulls {
		nullProb = 0.1
	}
	for _, colVec := range batch.ColVecs() {
		coldatatestutils.RandomVec(coldatatestutils.RandomVecArgs{
			Rand:            rng,
			Vec:             colVec,
			N:               coldata.BatchSize(),
			NullProbability: nullProb,
			// We will limit the range of integers so that we won't get "out of
			// range" errors.
			IntRange: 64,
			// We prohibit zeroes because we might be performing a division.
			ZeroProhibited: true,
		})
	}
	batch.SetLength(coldata.BatchSize())
	if useSelectionVector {
		batch.SetSelection(true)
		sel := batch.Selection()
		for i := 0; i < coldata.BatchSize(); i++ {
			sel[i] = i
		}
	}
	source := colexecop.NewRepeatableBatchSource(testAllocator, batch, inputTypes)
	op, err := makeProjOp(source)
	require.NoError(b, err)
	op.Init(ctx)

	b.Run(name, func(b *testing.B) {
		b.SetBytes(int64(len(inputTypes) * 8 * coldata.BatchSize()))
		for i := 0; i < b.N; i++ {
			op.Next()
		}
	})
}

func BenchmarkProjOp(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Mon:     evalCtx.TestingMon,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}

	var (
		opNames []string
		opInfix []string
	)
	for _, binOp := range []treebin.BinaryOperatorSymbol{treebin.Plus, treebin.Minus, treebin.Mult, treebin.Div} {
		opNames = append(opNames, execgen.BinaryOpName[binOp])
		opInfix = append(opInfix, binOp.String())
	}
	for _, cmpOp := range []treecmp.ComparisonOperatorSymbol{treecmp.EQ, treecmp.NE, treecmp.LT, treecmp.LE, treecmp.GT, treecmp.GE} {
		opNames = append(opNames, execgen.ComparisonOpName[cmpOp])
		opInfix = append(opInfix, cmpOp.String())
	}
	for opIdx, opName := range opNames {
		opInfixForm := opInfix[opIdx]
		for _, useSel := range []bool{false, true} {
			for _, hasNulls := range []bool{false, true} {
				for _, rightConst := range []bool{false, true} {
					kind := ""
					inputTypes := []*types.T{types.Int, types.Int}
					if rightConst {
						kind = "Const"
						inputTypes = inputTypes[:1]
					}
					name := fmt.Sprintf("proj%sInt64Int64%s/useSel=%t/hasNulls=%t", opName, kind, useSel, hasNulls)
					benchmarkProjOp(b, name, func(source *colexecop.RepeatableBatchSource) (colexecop.Operator, error) {
						expr := fmt.Sprintf("@1 %s @2", opInfixForm)
						if rightConst {
							expr = fmt.Sprintf("@1 %s 2", opInfixForm)
						}
						return colexectestutils.CreateTestProjectingOperator(
							ctx, flowCtx, source, inputTypes, expr, testMemAcc,
						)
					}, inputTypes, useSel, hasNulls)
				}
			}
		}
	}
}
