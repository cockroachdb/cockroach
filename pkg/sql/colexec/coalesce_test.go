// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexec

import (
	"context"
	"fmt"
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

func TestCoalesceBasic(t *testing.T) {
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
		tuples                colexectestutils.Tuples
		renderExpr            string
		expected              colexectestutils.Tuples
		inputTypes            []*types.T
		skipAllNullsInjection bool
	}{
		{
			// Basic test.
			tuples:     colexectestutils.Tuples{{1, -1}, {2, nil}, {nil, nil}, {nil, -4}, {nil, nil}, {nil, -6}, {7, nil}},
			renderExpr: "COALESCE(@1, @2, 0)",
			expected:   colexectestutils.Tuples{{1}, {2}, {0}, {-4}, {0}, {-6}, {7}},
			inputTypes: types.TwoIntCols,
		},
		{
			// Test the short-circuiting behavior.
			tuples:     colexectestutils.Tuples{{1, 1, 0}},
			renderExpr: "COALESCE(@1, @2 // @3)",
			expected:   colexectestutils.Tuples{{1}},
			inputTypes: types.ThreeIntCols,
		},
		{
			// Test the scenario when all expressions only project NULLs.
			tuples:     colexectestutils.Tuples{{nil, 1}, {nil, 2}},
			renderExpr: "COALESCE(@1, @2 + NULL, @1 + @2)",
			expected:   colexectestutils.Tuples{{nil}, {nil}},
			inputTypes: types.TwoIntCols,
			// The output here contains only nulls, so we inject all nulls into
			// the input, it won't change.
			skipAllNullsInjection: true,
		},
	} {
		runTests := colexectestutils.RunTestsWithTyps
		if tc.skipAllNullsInjection {
			runTests = colexectestutils.RunTestsWithoutAllNullsInjection
		}
		runTests(
			t, testAllocator, []colexectestutils.Tuples{tc.tuples}, [][]*types.T{tc.inputTypes},
			tc.expected, colexectestutils.OrderedVerifier,
			func(inputs []colexecop.Operator) (colexecop.Operator, error) {
				coalesceOp, err := colexectestutils.CreateTestProjectingOperator(
					ctx, flowCtx, inputs[0], tc.inputTypes, tc.renderExpr, testMemAcc,
				)
				if err != nil {
					return nil, err
				}
				// We will project out the input columns in order to have test
				// cases be less verbose.
				return colexecbase.NewSimpleProjectOp(coalesceOp, len(tc.inputTypes)+1, []uint32{uint32(len(tc.inputTypes))}), nil
			})
	}
}

func TestCoalesceRandomized(t *testing.T) {
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
	numExprs := 1 + rng.Intn(5)
	outputType := getRandomTypeFavorNative(rng)

	// Construct the COALESCE expression of the following form:
	//   COALESCE(IF(@1 = 0, @2, NULL), IF(@1 = 1, @3, NULL), ...).
	// We will use the first column from the input as the "partitioning" column,
	// and the following numExprs columns will be the projection columns.
	coalesceExpr := "COALESCE("
	for i := 0; i < numExprs; i++ {
		if i > 0 {
			coalesceExpr += ", "
		}
		coalesceExpr += fmt.Sprintf("IF(@1 = %d, @%d, NULL)", i, i+2)
	}
	coalesceExpr += ")"

	numInputCols := 1 + numExprs
	numInputRows := 1 + rng.Intn(coldata.BatchSize()) + coldata.BatchSize()*rng.Intn(5)
	inputRows := make(rowenc.EncDatumRows, numInputRows)
	// We always have an extra partition, regardless of whether we use an ELSE
	// projection or not (if we don't, the ELSE arm will project all NULLs).
	numPartitions := numExprs + 1
	// We will populate the expected output at the same time as we're generating
	// the input data set. Note that all input columns will be projected out, so
	// we memorize only the output column of the COALESCE expression.
	expectedOutput := make([]rowenc.EncDatum, numInputRows)
	for i := range inputRows {
		inputRow := make(rowenc.EncDatumRow, numInputCols)
		partitionIdx := rng.Intn(numPartitions)
		inputRow[0].Datum = tree.NewDInt(tree.DInt(partitionIdx))
		for j := 1; j < numInputCols; j++ {
			inputRow[j] = rowenc.DatumToEncDatum(outputType, randgen.RandDatum(rng, outputType, true /* nullOk */))
		}
		inputRows[i] = inputRow
		if partitionIdx == numExprs {
			expectedOutput[i] = rowenc.DatumToEncDatum(outputType, tree.DNull)
		} else {
			expectedOutput[i] = inputRow[partitionIdx+1]
		}
	}

	inputTypes := make([]*types.T, numInputCols)
	inputTypes[0] = types.Int
	for i := 1; i < numInputCols; i++ {
		inputTypes[i] = outputType
	}
	assertProjOpAgainstRowByRow(
		t, flowCtx, &evalCtx, coalesceExpr, inputTypes, inputRows, expectedOutput, outputType,
	)
}
