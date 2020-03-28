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
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestRandomizedCast(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

	datumAsBool := func(d tree.Datum) interface{} {
		return bool(tree.MustBeDBool(d))
	}
	datumAsInt := func(d tree.Datum) interface{} {
		return int(tree.MustBeDInt(d))
	}
	datumAsFloat := func(d tree.Datum) interface{} {
		return float64(tree.MustBeDFloat(d))
	}
	datumAsDecimal := func(d tree.Datum) interface{} {
		return tree.MustBeDDecimal(d).Decimal
	}

	tc := []struct {
		fromTyp      *types.T
		fromPhysType func(tree.Datum) interface{}
		toTyp        *types.T
		toPhysType   func(tree.Datum) interface{}
		// Some types casting can fail, so retry if we
		// generate a datum that is unable to be casted.
		retryGeneration bool
	}{
		//bool -> t tests
		{types.Bool, datumAsBool, types.Bool, datumAsBool, false},
		{types.Bool, datumAsBool, types.Int, datumAsInt, false},
		{types.Bool, datumAsBool, types.Float, datumAsFloat, false},
		// decimal -> t tests
		{types.Decimal, datumAsDecimal, types.Bool, datumAsBool, false},
		// int -> t tests
		{types.Int, datumAsInt, types.Bool, datumAsBool, false},
		{types.Int, datumAsInt, types.Float, datumAsFloat, false},
		{types.Int, datumAsInt, types.Decimal, datumAsDecimal, false},
		// float -> t tests
		{types.Float, datumAsFloat, types.Bool, datumAsBool, false},
		// We can sometimes generate a float outside of the range of the integers,
		// so we want to retry with generation if that occurs.
		{types.Float, datumAsFloat, types.Int, datumAsInt, true},
		{types.Float, datumAsFloat, types.Decimal, datumAsDecimal, false},
	}

	rng, _ := randutil.NewPseudoRand()

	for _, c := range tc {
		t.Run(fmt.Sprintf("%sTo%s", c.fromTyp.String(), c.toTyp.String()), func(t *testing.T) {
			n := 100
			// Make an input vector of length n.
			input := tuples{}
			output := tuples{}
			for i := 0; i < n; i++ {
				// We don't allow any NULL datums to be generated, so disable
				// this ability in the RandDatum function.
				fromDatum := sqlbase.RandDatum(rng, c.fromTyp, false)
				var (
					toDatum tree.Datum
					err     error
				)
				toDatum, err = tree.PerformCast(&evalCtx, fromDatum, c.toTyp)
				if c.retryGeneration {
					for err != nil {
						// If we are allowed to retry, make a new datum and cast it on error.
						fromDatum = sqlbase.RandDatum(rng, c.fromTyp, false)
						toDatum, err = tree.PerformCast(&evalCtx, fromDatum, c.toTyp)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
				}
				input = append(input, tuple{c.fromPhysType(fromDatum)})
				output = append(output, tuple{c.fromPhysType(fromDatum), c.toPhysType(toDatum)})
			}
			runTests(t, []tuples{input}, output, orderedVerifier,
				func(input []Operator) (Operator, error) {
					return createTestCastOperator(ctx, flowCtx, input[0], c.fromTyp, c.toTyp)
				})
		})
	}
}

func BenchmarkCastOp(b *testing.B) {
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
	rng, _ := randutil.NewPseudoRand()
	for _, typePair := range [][]types.T{
		{*types.Int, *types.Float},
		{*types.Int, *types.Decimal},
		{*types.Float, *types.Decimal},
	} {
		for _, useSel := range []bool{true, false} {
			for _, hasNulls := range []bool{true, false} {
				b.Run(
					fmt.Sprintf("useSel=%t/hasNulls=%t/%s_to_%s",
						useSel, hasNulls, typePair[0].Name(), typePair[1].Name(),
					), func(b *testing.B) {
						fromType := typeconv.FromColumnType(&typePair[0])
						nullProbability := nullProbability
						if !hasNulls {
							nullProbability = 0
						}
						selectivity := selectivity
						if !useSel {
							selectivity = 1.0
						}
						batch := randomBatchWithSel(
							testAllocator, rng, []coltypes.T{fromType},
							coldata.BatchSize(), nullProbability, selectivity,
						)
						source := NewRepeatableBatchSource(testAllocator, batch)
						op, err := createTestCastOperator(ctx, flowCtx, source, &typePair[0], &typePair[1])
						require.NoError(b, err)
						b.SetBytes(int64(8 * coldata.BatchSize()))
						b.ResetTimer()
						op.Init()
						for i := 0; i < b.N; i++ {
							op.Next(ctx)
						}
					})
			}
		}
	}
}

func createTestCastOperator(
	ctx context.Context, flowCtx *execinfra.FlowCtx, input Operator, fromTyp *types.T, toTyp *types.T,
) (Operator, error) {
	// We currently don't support casting to decimal type (other than when
	// casting from decimal with the same precision), so we will allow falling
	// back to row-by-row engine.
	return createTestProjectingOperator(
		ctx, flowCtx, input, []types.T{*fromTyp},
		fmt.Sprintf("@1::%s", toTyp.Name()), true, /* canFallbackToRowexec */
	)
}
