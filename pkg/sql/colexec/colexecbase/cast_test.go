// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecbase_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/faketreeeval"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestRandomizedCast(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	evalCtx.Planner = &faketreeeval.DummyEvalPlanner{}
	evalCtx.StreamManagerFactory = &faketreeeval.DummyStreamManagerFactory{}
	rng, _ := randutil.NewTestRand()

	getValidSupportedCast := func() (from, to *types.T) {
		for {
			from, to = randgen.RandType(rng), randgen.RandType(rng)
			if _, ok := cast.LookupCastVolatility(from, to); ok {
				if colexecbase.IsCastSupported(from, to) {
					return from, to
				}
			}
		}
	}
	var numTypePairs = rng.Intn(10) + 1
	numRows := 1 + rng.Intn(coldata.BatchSize()) + rng.Intn(3)*coldata.BatchSize()
	log.Infof(ctx, "num rows = %d", numRows)
	for run := 0; run < numTypePairs; run++ {
		from, to := getValidSupportedCast()
		log.Infof(ctx, "%s to %s", from.String(), to.String())
		input := colexectestutils.Tuples{}
		output := colexectestutils.Tuples{}
		fromConverter := colconv.GetDatumToPhysicalFn(from)
		toConverter := colconv.GetDatumToPhysicalFn(to)
		errorExpected := false
		for i := 0; i < numRows; i++ {
			// We don't allow any NULL datums to be generated, so disable this
			// ability in the RandDatum function.
			fromDatum := randgen.RandDatum(rng, from, false)
			toDatum, err := eval.PerformCast(ctx, &evalCtx, fromDatum, to)
			var toPhys interface{}
			if err != nil {
				errorExpected = true
			} else {
				toPhys = toConverter(toDatum)
			}
			input = append(input, colexectestutils.Tuple{fromConverter(fromDatum)})
			output = append(output, colexectestutils.Tuple{fromConverter(fromDatum), toPhys})
		}
		colexectestutils.RunTestsWithoutAllNullsInjectionWithErrorHandler(t, testAllocator,
			[]colexectestutils.Tuples{input}, [][]*types.T{{from}}, output, colexectestutils.OrderedVerifier,
			func(input []colexecop.Operator) (colexecop.Operator, error) {
				return colexecbase.GetCastOperator(ctx, testAllocator, input[0], 0, 1, from, to, &evalCtx)
			}, func(err error) {
				if !errorExpected {
					t.Fatal(err)
				}
			}, nil /* orderedCols */)
	}
}

func BenchmarkCastOp(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	rng, _ := randutil.NewTestRand()
	for _, typePair := range [][]*types.T{
		{types.Int, types.Float},
		{types.Int, types.Decimal},
		{types.Float, types.Decimal},
	} {
		for _, useSel := range []bool{true, false} {
			for _, hasNulls := range []bool{true, false} {
				b.Run(
					fmt.Sprintf("useSel=%t/hasNulls=%t/%s_to_%s",
						useSel, hasNulls, typePair[0].Name(), typePair[1].Name(),
					), func(b *testing.B) {
						nullProbability := 0.1
						if !hasNulls {
							nullProbability = 0
						}
						selectivity := 0.5
						if !useSel {
							selectivity = 0
						}
						typs := []*types.T{typePair[0]}
						batch := coldatatestutils.RandomBatchWithSel(
							testAllocator, rng, typs,
							coldata.BatchSize(), nullProbability, selectivity,
						)
						source := colexecop.NewRepeatableBatchSource(testAllocator, batch, typs)
						op, err := colexecbase.GetCastOperator(ctx, testAllocator, source, 0, 1, typePair[0], typePair[1], &evalCtx)
						require.NoError(b, err)
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
