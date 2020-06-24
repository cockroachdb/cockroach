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
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
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
	rng, _ := randutil.NewPseudoRand()

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
	datumAsColdataextDatum := func(datumVec coldata.DatumVec, d tree.Datum) interface{} {
		datumVec.Set(0, d)
		return datumVec.Get(0)
	}
	makeDatumVecAdapter := func(datumVec coldata.DatumVec) func(tree.Datum) interface{} {
		return func(d tree.Datum) interface{} {
			return datumAsColdataextDatum(datumVec, d)
		}
	}

	collatedStringType := types.MakeCollatedString(types.String, *sqlbase.RandCollationLocale(rng))
	collatedStringVec := testColumnFactory.MakeColumn(collatedStringType, 1 /* n */).(coldata.DatumVec)
	getCollatedStringsThatCanBeCastAsBools := func() []tree.Datum {
		var res []tree.Datum
		for _, validString := range []string{"true", "false", "yes", "no"} {
			d, err := tree.NewDCollatedString(validString, collatedStringType.Locale(), &tree.CollationEnvironment{})
			if err != nil {
				t.Fatal(err)
			}
			res = append(res, d)
		}
		return res
	}

	tc := []struct {
		fromTyp      *types.T
		fromPhysType func(tree.Datum) interface{}
		toTyp        *types.T
		toPhysType   func(tree.Datum) interface{}
		// getValidSet (when non-nil) is a function that returns a set of valid
		// datums of fromTyp type that can be cast to toTyp type. The test
		// harness will be randomly choosing a datum from this set. This
		// function should be specified when sqlbase.RandDatum will take ages
		// (if ever) to generate the datum that is valid for a cast.
		getValidSet func() []tree.Datum
		// Some types casting can fail, so retry if we generate a datum that is
		// unable to be cast.
		retryGeneration bool
	}{
		//bool -> t tests
		{fromTyp: types.Bool, fromPhysType: datumAsBool, toTyp: types.Bool, toPhysType: datumAsBool},
		{fromTyp: types.Bool, fromPhysType: datumAsBool, toTyp: types.Int, toPhysType: datumAsInt},
		{fromTyp: types.Bool, fromPhysType: datumAsBool, toTyp: types.Float, toPhysType: datumAsFloat},
		// decimal -> t tests
		{fromTyp: types.Decimal, fromPhysType: datumAsDecimal, toTyp: types.Bool, toPhysType: datumAsBool},
		// int -> t tests
		{fromTyp: types.Int, fromPhysType: datumAsInt, toTyp: types.Bool, toPhysType: datumAsBool},
		{fromTyp: types.Int, fromPhysType: datumAsInt, toTyp: types.Float, toPhysType: datumAsFloat},
		{fromTyp: types.Int, fromPhysType: datumAsInt, toTyp: types.Decimal, toPhysType: datumAsDecimal},
		// float -> t tests
		{fromTyp: types.Float, fromPhysType: datumAsFloat, toTyp: types.Bool, toPhysType: datumAsBool},
		// We can sometimes generate a float outside of the range of the integers,
		// so we want to retry with generation if that occurs.
		{fromTyp: types.Float, fromPhysType: datumAsFloat, toTyp: types.Int, toPhysType: datumAsInt, retryGeneration: true},
		{fromTyp: types.Float, fromPhysType: datumAsFloat, toTyp: types.Decimal, toPhysType: datumAsDecimal},
		// datum-backed type -> t tests
		{fromTyp: collatedStringType, fromPhysType: makeDatumVecAdapter(collatedStringVec), toTyp: types.Bool, toPhysType: datumAsBool, getValidSet: getCollatedStringsThatCanBeCastAsBools},
	}

	for _, c := range tc {
		t.Run(fmt.Sprintf("%sTo%s", c.fromTyp.String(), c.toTyp.String()), func(t *testing.T) {
			n := 100
			// Make an input vector of length n.
			input := tuples{}
			output := tuples{}
			for i := 0; i < n; i++ {
				var (
					fromDatum, toDatum tree.Datum
					err                error
				)
				if c.getValidSet != nil {
					validFromDatums := c.getValidSet()
					fromDatum = validFromDatums[rng.Intn(len(validFromDatums))]
					toDatum, err = tree.PerformCast(&evalCtx, fromDatum, c.toTyp)
				} else {
					// We don't allow any NULL datums to be generated, so disable
					// this ability in the RandDatum function.
					fromDatum = sqlbase.RandDatum(rng, c.fromTyp, false)
					toDatum, err = tree.PerformCast(&evalCtx, fromDatum, c.toTyp)
					if c.retryGeneration {
						for err != nil {
							// If we are allowed to retry, make a new datum and cast it on error.
							fromDatum = sqlbase.RandDatum(rng, c.fromTyp, false)
							toDatum, err = tree.PerformCast(&evalCtx, fromDatum, c.toTyp)
						}
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				input = append(input, tuple{c.fromPhysType(fromDatum)})
				output = append(output, tuple{c.fromPhysType(fromDatum), c.toPhysType(toDatum)})
			}
			runTestsWithTyps(t, []tuples{input}, [][]*types.T{{c.fromTyp}}, output, orderedVerifier,
				func(input []colexecbase.Operator) (colexecbase.Operator, error) {
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
						nullProbability := nullProbability
						if !hasNulls {
							nullProbability = 0
						}
						selectivity := selectivity
						if !useSel {
							selectivity = 0
						}
						typs := []*types.T{typePair[0]}
						batch := coldatatestutils.RandomBatchWithSel(
							testAllocator, rng, typs,
							coldata.BatchSize(), nullProbability, selectivity,
						)
						source := colexecbase.NewRepeatableBatchSource(testAllocator, batch, typs)
						op, err := createTestCastOperator(ctx, flowCtx, source, typePair[0], typePair[1])
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
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	input colexecbase.Operator,
	fromTyp *types.T,
	toTyp *types.T,
) (colexecbase.Operator, error) {
	// We currently don't support casting to decimal type (other than when
	// casting from decimal with the same precision), so we will allow falling
	// back to row-by-row engine.
	return createTestProjectingOperator(
		ctx, flowCtx, input, []*types.T{fromTyp},
		fmt.Sprintf("@1::%s", toTyp.Name()), true, /* canFallbackToRowexec */
	)
}
