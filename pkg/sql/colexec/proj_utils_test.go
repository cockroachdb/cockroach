// Copyright 2022 The Cockroach Authors.
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
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/stretchr/testify/require"
)

// getRandomTypeFavorNative returns a random type giving more weight to the
// natively supported typed.
//
// We're giving more weight to the types natively supported by the vectorized
// engine because all datum-backed types have the same backing datumVec which
// would occur disproportionally often without adjusting the weights.
func getRandomTypeFavorNative(rng *rand.Rand) *types.T {
	typ := randgen.RandType(rng)
	for retry := 0; retry < 3; retry++ {
		if typeconv.TypeFamilyToCanonicalTypeFamily(typ.Family()) != typeconv.DatumVecCanonicalTypeFamily {
			break
		}
		typ = randgen.RandType(rng)
		if typ.Family() == types.VoidFamily {
			// Skip the void family because it doesn't have some basic
			// comparison operators defined.
			retry--
		}
	}
	return typ
}

// assertProjOpAgainstRowByRow plans the vectorized operator chain for projExpr
// and verifies that the chain produces the expected results for the given input
// rows.
// - expectedOutput must contain a single datum for each input row that is the
// result of the projection for that row.
func assertProjOpAgainstRowByRow(
	t *testing.T,
	flowCtx *execinfra.FlowCtx,
	evalCtx *eval.Context,
	projExpr string,
	inputTypes []*types.T,
	inputRows rowenc.EncDatumRows,
	expectedOutput []rowenc.EncDatum,
	outputType *types.T,
) {
	ctx := context.Background()
	input := execinfra.NewRepeatableRowSource(inputTypes, inputRows)
	columnarizer := NewBufferingColumnarizerForTests(testAllocator, flowCtx, 1 /* processorID */, input)
	projOp, err := colexectestutils.CreateTestProjectingOperator(
		ctx, flowCtx, columnarizer, inputTypes, projExpr, testMemAcc,
	)
	require.NoError(t, err)
	// We will project out all input columns while keeping only the output
	// column of the projection operator.
	op := colexecbase.NewSimpleProjectOp(projOp, len(inputTypes)+1, []uint32{uint32(len(inputTypes))})
	materializer := NewMaterializer(
		nil, /* allocator */
		flowCtx,
		1, /* processorID */
		colexecargs.OpWithMetaInfo{Root: op},
		[]*types.T{outputType},
	)

	var da tree.DatumAlloc
	materializer.Start(ctx)
	for _, expectedDatum := range expectedOutput {
		actualRow, meta := materializer.Next()
		require.Nil(t, meta)
		require.Equal(t, 1, len(actualRow))
		cmp, err := expectedDatum.Compare(outputType, &da, evalCtx, &actualRow[0])
		require.NoError(t, err)
		require.Equal(t, 0, cmp)
	}
	// The materializer must have been fully exhausted now.
	row, meta := materializer.Next()
	require.Nil(t, row)
	require.Nil(t, meta)
}
