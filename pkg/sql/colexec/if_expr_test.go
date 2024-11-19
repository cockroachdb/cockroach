// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestIfExprBasic(t *testing.T) {
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

	for _, tc := range []struct {
		tuples     colexectestutils.Tuples
		renderExpr string
		expected   colexectestutils.Tuples
		inputTypes []*types.T
	}{
		{
			// Basic test.
			tuples:     colexectestutils.Tuples{{true, 1, -1}, {false, 2, -2}, {nil, 3, -3}, {nil, 4, -4}, {true, 5, -5}, {false, 6, -6}},
			renderExpr: "IF(@1, @2, @3)",
			expected:   colexectestutils.Tuples{{1}, {-2}, {-3}, {-4}, {5}, {-6}},
			inputTypes: []*types.T{types.Bool, types.Int, types.Int},
		},
		{
			// All 'true'.
			tuples:     colexectestutils.Tuples{{true, 1}, {true, 2}, {true, 3}, {true, 4}},
			renderExpr: "IF(@1, @2, 1 // 0)",
			expected:   colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			inputTypes: []*types.T{types.Bool, types.Int},
		},
		{
			// All 'else'.
			tuples:     colexectestutils.Tuples{{false, 1}, {nil, 2}, {nil, 3}, {false, 4}},
			renderExpr: "IF(@1, 1 // 0, @2)",
			expected:   colexectestutils.Tuples{{1}, {2}, {3}, {4}},
			inputTypes: []*types.T{types.Bool, types.Int},
		},
	} {
		colexectestutils.RunTestsWithoutAllNullsInjectionWithErrorHandler(
			t, testAllocator, []colexectestutils.Tuples{tc.tuples}, [][]*types.T{tc.inputTypes},
			tc.expected, colexectestutils.OrderedVerifier,
			func(inputs []colexecop.Operator) (colexecop.Operator, error) {
				ifExprOp, err := colexectestutils.CreateTestProjectingOperator(
					ctx, flowCtx, inputs[0], tc.inputTypes, tc.renderExpr, testMemAcc,
				)
				if err != nil {
					return nil, err
				}
				// We will project out the input columns in order to have test
				// cases be less verbose.
				return colexecbase.NewSimpleProjectOp(ifExprOp, len(tc.inputTypes)+1, []uint32{uint32(len(tc.inputTypes))}), nil
			},
			// Random nulls injection can lead to a division by zero error in
			// some test cases, so we want to skip it.
			colexectestutils.SkipRandomNullsInjection,
			nil, /* orderedCols */
		)
	}
}

func TestIfExprRandomized(t *testing.T) {
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

	rng, _ := randutil.NewTestRand()
	outputType := getRandomTypeFavorNative(rng)
	ifExpr := "IF(@1, @2, @3)"
	const numInputCols = 3
	inputTypes := []*types.T{types.Bool, outputType, outputType}
	numInputRows := 1 + rng.Intn(coldata.BatchSize()) + coldata.BatchSize()*rng.Intn(5)
	inputRows := make(rowenc.EncDatumRows, numInputRows)
	// We will populate the expected output at the same time as we're generating
	// the input data set. Note that all input columns will be projected out, so
	// we memorize only the output column of the IF expression.
	expectedOutput := make([]rowenc.EncDatum, numInputRows)
	for i := range inputRows {
		inputRow := make(rowenc.EncDatumRow, numInputCols)
		for i, t := range inputTypes {
			inputRow[i].Datum = randgen.RandDatum(rng, t, true /* nullOk */)
		}
		inputRows[i] = inputRow
		if cond := inputRow[0].Datum; cond != tree.DNull && tree.MustBeDBool(cond) {
			expectedOutput[i] = inputRow[1]
		} else {
			expectedOutput[i] = inputRow[2]
		}
	}

	assertProjOpAgainstRowByRow(
		t, flowCtx, &evalCtx, ifExpr, inputTypes, inputRows, expectedOutput, outputType,
	)
}
