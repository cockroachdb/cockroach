// Copyright 2019 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/faketreeeval"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	evalCtx.Planner = &faketreeeval.DummyEvalPlanner{}
	rng, _ := randutil.NewPseudoRand()

	getValidSupportedCast := func() (from, to *types.T) {
		for {
			from, to = randgen.RandType(rng), randgen.RandType(rng)
			if _, ok := tree.LookupCastVolatility(from, to, nil /* sessiondata */); ok {
				if colexecbase.IsCastSupported(from, to) {
					return from, to
				}
			}
		}
	}
	const numTypePairs = 5
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
			var (
				fromDatum, toDatum tree.Datum
				err                error
			)
			for {
				// We don't allow any NULL datums to be generated, so disable
				// this ability in the RandDatum function.
				fromDatum = randgen.RandDatum(rng, from, false)
				toDatum, err = tree.PerformCast(&evalCtx, fromDatum, to)
				if to.String() == "char" && string(*toDatum.(*tree.DString)) == "" {
					// There is currently a problem when converting an empty
					// string datum to a physical representation, so we skip
					// such a datum and retry generation.
					// TODO(yuzefovich): figure it out.
					continue
				}
				break
			}
			var toPhys interface{}
			if err != nil {
				errorExpected = true
			} else {
				toPhys = toConverter(toDatum)
			}
			input = append(input, colexectestutils.Tuple{fromConverter(fromDatum)})
			output = append(output, colexectestutils.Tuple{fromConverter(fromDatum), toPhys})
		}
		colexectestutils.RunTestsWithoutAllNullsInjectionWithErrorHandler(
			t, testAllocator, []colexectestutils.Tuples{input}, [][]*types.T{{from}}, output, colexectestutils.OrderedVerifier,
			func(input []colexecop.Operator) (colexecop.Operator, error) {
				return colexecbase.GetCastOperator(testAllocator, input[0], 0, 1, from, to, &evalCtx)
			},
			func(err error) {
				if !errorExpected {
					t.Fatal(err)
				}
			},
		)
	}
}

func BenchmarkCastOp(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	rng, _ := randutil.NewPseudoRand()
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
						op, err := colexecbase.GetCastOperator(testAllocator, source, 0, 1, typePair[0], typePair[1], &evalCtx)
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
