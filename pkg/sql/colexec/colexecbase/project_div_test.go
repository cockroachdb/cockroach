// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecbase_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecprojconst"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func BenchmarkProjDivInt64ConstInt64Op(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	rng, _ := randutil.NewTestRand()
	for _, typePair := range [][]*types.T{
		{types.Int, types.Int},
	} {
		colIdx := 1
		inputTypes := make([]*types.T, colIdx+1)
		inputTypes[colIdx] = types.Int
		binOp := treebin.MakeBinaryOperator(treebin.Div)
		constVal := 1
		constArg := tree.NewDInt(tree.DInt(constVal))
		outputIdx := 5

		for _, useSel := range []bool{true, false} {
			for _, hasNulls := range []bool{true, false} {
				b.Run(
					fmt.Sprintf("useSel=%t/hasNulls=%t/type=%s",
						useSel, hasNulls, typePair[0].Name(),
					), func(b *testing.B) {
						nullProbability := 0.1
						if !hasNulls {
							nullProbability = 0
						}
						selectivity := 0.5
						if !useSel {
							selectivity = 0
						}
						typs := []*types.T{typePair[0], typePair[1], types.Decimal, types.Decimal, types.Decimal}
						batch := coldatatestutils.RandomBatchWithSel(
							testAllocator, rng, typs,
							coldata.BatchSize(), nullProbability, selectivity,
						)
						source := colexecop.NewRepeatableBatchSource(testAllocator, batch, typs)

						op, err := colexecprojconst.GetProjectionRConstOperator(
							testAllocator /* allocator */, inputTypes, types.Int, types.Decimal, binOp, source, colIdx,
							constArg, outputIdx, nil /* EvalCtx */, nil /* BinFn */, nil /* cmpExpr */, false, /* calledOnNullInput */
						)
						if err != nil {
							b.Error(err)
						}

						//require.NoError(b, err)
						b.SetBytes(int64(8 * coldata.BatchSize()))
						b.ResetTimer()
						op.Init(ctx)
						for i := 0; i < b.N; i++ {
							op.Next()
						}
					})
			}
		}
	}
}
