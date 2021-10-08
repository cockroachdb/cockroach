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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestCaseOp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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

	for _, tc := range []struct {
		tuples     colexectestutils.Tuples
		renderExpr string
		expected   colexectestutils.Tuples
		inputTypes []*types.T
	}{
		{
			// Basic test.
			tuples:     colexectestutils.Tuples{{1}, {2}, {nil}, {3}},
			renderExpr: "CASE WHEN @1 = 2 THEN 1 ELSE 42 END",
			expected:   colexectestutils.Tuples{{42}, {1}, {42}, {42}},
			inputTypes: []*types.T{types.Int},
		},
		{
			// Test "reordered when's."
			tuples:     colexectestutils.Tuples{{1, 1}, {2, 0}, {nil, nil}, {3, 3}},
			renderExpr: "CASE WHEN @1 + @2 > 3 THEN 0 WHEN @1 = 2 THEN 1 ELSE 2 END",
			expected:   colexectestutils.Tuples{{2}, {1}, {2}, {0}},
			inputTypes: []*types.T{types.Int, types.Int},
		},
		{
			// Test the short-circuiting behavior.
			tuples:     colexectestutils.Tuples{{1, 2}, {2, 0}, {nil, nil}, {3, 3}},
			renderExpr: "CASE WHEN @1 = 2 THEN 0::FLOAT WHEN @1 / @2 = 1 THEN 1::FLOAT END",
			expected:   colexectestutils.Tuples{{nil}, {0.0}, {nil}, {1.0}},
			inputTypes: []*types.T{types.Int, types.Int},
		},
		{
			// Test when only the ELSE arm matches.
			//
			// Note that all input values are NULLs so that the "all nulls
			// injection" subtest is skipped.
			tuples:     colexectestutils.Tuples{{nil}, {nil}, {nil}, {nil}},
			renderExpr: "CASE WHEN @1 = 42 THEN 1 WHEN @1 IS NOT NULL THEN 2 ELSE 42 END",
			expected:   colexectestutils.Tuples{{42}, {42}, {42}, {42}},
			inputTypes: []*types.T{types.Int},
		},
	} {
		colexectestutils.RunTests(t, testAllocator, []colexectestutils.Tuples{tc.tuples}, tc.expected, colexectestutils.OrderedVerifier, func(inputs []colexecop.Operator) (colexecop.Operator, error) {
			caseOp, err := colexectestutils.CreateTestProjectingOperator(
				ctx, flowCtx, inputs[0], tc.inputTypes, tc.renderExpr,
				false /* canFallbackToRowexec */, testMemAcc,
			)
			if err != nil {
				return nil, err
			}
			// We will project out the input columns in order to have test
			// cases be less verbose.
			return colexecbase.NewSimpleProjectOp(caseOp, len(tc.inputTypes)+1, []uint32{uint32(len(tc.inputTypes))}), nil
		})
	}
}

func TestCaseOpRandomized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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

	var da rowenc.DatumAlloc
	rng, _ := randutil.NewPseudoRand()

	numWhenArms := 1 + rng.Intn(5)
	hasElseArm := rng.Float64() < 0.5

	// Pick a random type to be used as the output type for the CASE expression.
	// We're giving more weight to the types natively supported by the
	// vectorized engine because all datum-backed types have the same backing
	// datumVec which would occur disproportionally often without adjusting the
	// weights.
	caseOutputType := randgen.RandType(rng)
	for retry := 0; retry < 3; retry++ {
		if typeconv.TypeFamilyToCanonicalTypeFamily(caseOutputType.Family()) != typeconv.DatumVecCanonicalTypeFamily {
			break
		}
		caseOutputType = randgen.RandType(rng)
	}

	// Construct such a CASE expression that the first column from the input is
	// used as the "partitioning" column (used by WHEN arms for matching), the
	// following numWhenArms columns are the projections for the corresponding
	// WHEN "partitions", and then optionally we have an ELSE projection.
	caseExpr := "CASE @1"
	for i := 0; i < numWhenArms; i++ {
		caseExpr += fmt.Sprintf(" WHEN %d THEN @%d", i, i+2)
	}
	if hasElseArm {
		caseExpr += fmt.Sprintf(" ELSE @%d", numWhenArms+2)
	}
	caseExpr += " END"

	numInputCols := 1 + numWhenArms
	if hasElseArm {
		numInputCols++
	}
	numInputRows := 1 + rng.Intn(coldata.BatchSize()) + coldata.BatchSize()*rng.Intn(5)
	inputRows := make(rowenc.EncDatumRows, numInputRows)
	// We always have an extra partition, regardless of whether we use an ELSE
	// projection or not (if we don't, the ELSE arm will project all NULLs).
	numPartitions := numWhenArms + 1
	// We will populate the expected output at the same time as we're generating
	// the input data set. Note that all input columns will be projected out, so
	// we memorize only the output column of the CASE expression.
	expectedOutput := make([]rowenc.EncDatum, numInputRows)
	for i := range inputRows {
		inputRow := make(rowenc.EncDatumRow, numInputCols)
		partitionIdx := rng.Intn(numPartitions)
		inputRow[0].Datum = tree.NewDInt(tree.DInt(partitionIdx))
		for j := 1; j < numInputCols; j++ {
			inputRow[j] = rowenc.DatumToEncDatum(caseOutputType, randgen.RandDatum(rng, caseOutputType, true /* nullOk */))
		}
		inputRows[i] = inputRow
		if !hasElseArm && partitionIdx == numWhenArms {
			expectedOutput[i] = rowenc.DatumToEncDatum(caseOutputType, tree.DNull)
		} else {
			expectedOutput[i] = inputRow[partitionIdx+1]
		}
	}

	inputTypes := make([]*types.T, numInputCols)
	inputTypes[0] = types.Int
	for i := 1; i < numInputCols; i++ {
		inputTypes[i] = caseOutputType
	}
	input := execinfra.NewRepeatableRowSource(inputTypes, inputRows)
	columnarizer := NewBufferingColumnarizer(testAllocator, flowCtx, 1 /* processorID */, input)
	caseOp, err := colexectestutils.CreateTestProjectingOperator(
		ctx, flowCtx, columnarizer, inputTypes, caseExpr,
		false /* canFallbackToRowexec */, testMemAcc,
	)
	require.NoError(t, err)
	// We will project out all input columns while keeping only the output
	// column of the case operator, for simplicity.
	op := colexecbase.NewSimpleProjectOp(caseOp, numInputCols+1, []uint32{uint32(numInputCols)})
	materializer := NewMaterializer(
		flowCtx,
		1, /* processorID */
		colexecargs.OpWithMetaInfo{Root: op},
		[]*types.T{caseOutputType},
	)

	materializer.Start(ctx)
	for _, expectedDatum := range expectedOutput {
		actualRow, meta := materializer.Next()
		require.Nil(t, meta)
		require.Equal(t, 1, len(actualRow))
		cmp, err := expectedDatum.Compare(caseOutputType, &da, &evalCtx, &actualRow[0])
		require.NoError(t, err)
		require.Equal(t, 0, cmp)
	}
	// The materializer must have been fully exhausted now.
	row, meta := materializer.Next()
	require.Nil(t, row)
	require.Nil(t, meta)
}
