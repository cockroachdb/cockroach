// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// testAggregateResultDeepCopy verifies that tree.Datum returned from tree.AggregateFunc's
// Result() method are not mutated during future accumulation. It verifies this by
// printing all values to strings immediately after calling Result(), and later
// printing all values to strings once the accumulation has finished. If the string
// slices are not equal, it means that the result tree.Datums were modified during later
// accumulation, which violates the "deep copy of any internal state" condition.
func testAggregateResultDeepCopy(
	t *testing.T,
	aggFunc func([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc,
	vals []tree.Datum,
) {
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	aggImpl := aggFunc([]*types.T{vals[0].ResolvedType()}, evalCtx, nil)
	defer aggImpl.Close(context.Background())
	runningDatums := make([]tree.Datum, len(vals))
	runningStrings := make([]string, len(vals))
	for i := range vals {
		if err := aggImpl.Add(context.Background(), vals[i]); err != nil {
			t.Fatal(err)
		}
		res, err := aggImpl.Result()
		if err != nil {
			t.Fatal(err)
		}
		runningDatums[i] = res
		runningStrings[i] = res.String()
	}
	finalStrings := make([]string, len(vals))
	for i, d := range runningDatums {
		finalStrings[i] = d.String()
	}
	if !reflect.DeepEqual(runningStrings, finalStrings) {
		t.Errorf("Aggregate result mutated during future accumulation: initial results were %v,"+
			" later results were %v", runningStrings, finalStrings)
	}
}

func TestAvgIntResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newIntAvgAggregate, makeIntTestDatum(10))
}

func TestAvgFloatResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newFloatAvgAggregate, makeFloatTestDatum(10))
}

func TestAvgDecimalResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newDecimalAvgAggregate, makeDecimalTestDatum(10))
}

func TestAvgIntervalResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newIntervalAvgAggregate, makeIntervalTestDatum(10))
}

func TestBitAndIntResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("all null", func(t *testing.T) {
		testAggregateResultDeepCopy(t, newIntBitAndAggregate, makeNullTestDatum(10))
	})
	t.Run("with null", func(t *testing.T) {
		testAggregateResultDeepCopy(t, newIntBitAndAggregate, makeTestWithNullDatum(10, makeIntTestDatum))
	})
	t.Run("without null", func(t *testing.T) {
		testAggregateResultDeepCopy(t, newIntBitAndAggregate, makeIntTestDatum(10))
	})
}

func TestBitAndBitResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("all null", func(t *testing.T) {
		testAggregateResultDeepCopy(t, newBitBitAndAggregate, makeNullTestDatum(10))
	})
	t.Run("with null", func(t *testing.T) {
		testAggregateResultDeepCopy(t, newBitBitAndAggregate, makeTestWithNullDatum(10, makeBitTestDatum))
	})
	t.Run("without null", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			testAggregateResultDeepCopy(t, newBitBitAndAggregate, makeBitTestDatum(10))
		}
	})
}

func TestBitOrIntResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("all null", func(t *testing.T) {
		testAggregateResultDeepCopy(t, newIntBitOrAggregate, makeNullTestDatum(10))
	})
	t.Run("with null", func(t *testing.T) {
		testAggregateResultDeepCopy(t, newIntBitOrAggregate, makeTestWithNullDatum(10, makeIntTestDatum))
	})
	t.Run("without null", func(t *testing.T) {
		testAggregateResultDeepCopy(t, newIntBitOrAggregate, makeIntTestDatum(10))
	})
}

func TestBitOrBitResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Run("all null", func(t *testing.T) {
		testAggregateResultDeepCopy(t, newBitBitOrAggregate, makeNullTestDatum(10))
	})
	t.Run("with null", func(t *testing.T) {
		testAggregateResultDeepCopy(t, newBitBitOrAggregate, makeTestWithNullDatum(10, makeBitTestDatum))
	})
	t.Run("without null", func(t *testing.T) {
		testAggregateResultDeepCopy(t, newBitBitOrAggregate, makeBitTestDatum(10))
	})
}

func TestBoolAndResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newBoolAndAggregate, makeBoolTestDatum(10))
}

func TestBoolOrResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newBoolOrAggregate, makeBoolTestDatum(10))
}

func TestCountResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newCountAggregate, makeIntTestDatum(10))
}

func TestMaxIntResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newMaxAggregate, makeIntTestDatum(10))
}

func TestMaxFloatResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newMaxAggregate, makeFloatTestDatum(10))
}

func TestMaxDecimalResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newMaxAggregate, makeDecimalTestDatum(10))
}

func TestMaxBoolResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newMaxAggregate, makeBoolTestDatum(10))
}

func TestMinIntResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newMinAggregate, makeIntTestDatum(10))
}

func TestMinFloatResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newMinAggregate, makeFloatTestDatum(10))
}

func TestMinDecimalResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newMinAggregate, makeDecimalTestDatum(10))
}

func TestMinBoolResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newMinAggregate, makeBoolTestDatum(10))
}

func TestSumSmallIntResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newSmallIntSumAggregate, makeSmallIntTestDatum(10))
}

func TestSumIntResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newIntSumAggregate, makeIntTestDatum(10))
}

func TestSumFloatResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newFloatSumAggregate, makeFloatTestDatum(10))
}

func TestSumDecimalResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newDecimalSumAggregate, makeDecimalTestDatum(10))
}

func TestSumIntervalResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newIntervalSumAggregate, makeIntervalTestDatum(10))
}

func TestVarianceIntResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newIntVarianceAggregate, makeIntTestDatum(10))
}

func TestVarianceFloatResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newFloatVarianceAggregate, makeFloatTestDatum(10))
}

func TestVarianceDecimalResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newDecimalVarianceAggregate, makeDecimalTestDatum(10))
}

func TestStdDevIntResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newIntStdDevAggregate, makeIntTestDatum(10))
}

func TestStdDevFloatResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newFloatStdDevAggregate, makeFloatTestDatum(10))
}

func TestStdDevDecimalResultDeepCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testAggregateResultDeepCopy(t, newDecimalStdDevAggregate, makeDecimalTestDatum(10))
}

// makeNullTestDatum will create an array of only DNull
// values to make sure the aggregation handles only nulls.
func makeNullTestDatum(count int) []tree.Datum {
	values := make([]tree.Datum, count)
	for i := range values {
		values[i] = tree.DNull
	}
	return values
}

func makeIntTestDatum(count int) []tree.Datum {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]tree.Datum, count)
	for i := range vals {
		vals[i] = tree.NewDInt(tree.DInt(rng.Int63()))
	}
	return vals
}

func makeBitTestDatum(count int) []tree.Datum {
	rng, _ := randutil.NewPseudoRand()

	// Compute randWidth outside the loop so that all bit arrays are the same
	// length. Generate widths in the range [0, 64].
	vals := make([]tree.Datum, count)
	randWidth := uint(rng.Intn(65))
	for i := range vals {
		vals[i], _ = tree.NewDBitArrayFromInt(rng.Int63(), randWidth)
	}
	return vals
}

// makeTestWithNullDatum will call the maker function
// to generate an array of datums, and then a null datum
// will be placed randomly in the array of datums and
// returned. Use this to ensure proper partial null
// handling of aggregations.
func makeTestWithNullDatum(count int, maker func(count int) []tree.Datum) []tree.Datum {
	rng, _ := randutil.NewPseudoRand()
	values := maker(count)
	values[rng.Int()%count] = tree.DNull
	return values
}

// makeSmallIntTestDatum creates integers that are sufficiently
// smaller than 2^64-1 that they can be added to each other for a
// significant part of the test without overflow. This is meant to
// test the implementation of aggregates that can use an int64 to
// optimize computations small decimal values.
func makeSmallIntTestDatum(count int) []tree.Datum {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]tree.Datum, count)
	for i := range vals {
		sign := int32(1)
		if rng.Int31()&1 == 0 {
			sign = -1
		}
		vals[i] = tree.NewDInt(tree.DInt(rng.Int31() * sign))
	}
	return vals
}

func makeFloatTestDatum(count int) []tree.Datum {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]tree.Datum, count)
	for i := range vals {
		vals[i] = tree.NewDFloat(tree.DFloat(rng.Float64()))
	}
	return vals
}

func makeDecimalTestDatum(count int) []tree.Datum {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]tree.Datum, count)
	for i := range vals {
		dd := &tree.DDecimal{}
		if _, err := dd.SetFloat64(rng.Float64()); err != nil {
			panic(err)
		}
		vals[i] = dd
	}
	return vals
}

func makeBoolTestDatum(count int) []tree.Datum {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]tree.Datum, count)
	for i := range vals {
		vals[i] = tree.MakeDBool(tree.DBool(rng.Int31n(2) == 0))
	}
	return vals
}

func makeIntervalTestDatum(count int) []tree.Datum {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]tree.Datum, count)
	for i := range vals {
		vals[i] = &tree.DInterval{Duration: duration.MakeDuration(rng.Int63n(1000000), rng.Int63n(1000), rng.Int63n(1000))}
	}
	return vals
}

func TestArrayAggNameOverload(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testArrayAggAliasedTypeOverload(context.Background(), t, types.Name)
}

func TestArrayAggOidOverload(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testArrayAggAliasedTypeOverload(context.Background(), t, types.Oid)
}

// testAliasedTypeOverload is a helper function for testing ARRAY_AGG's
// overloads that can take aliased scalar types like NAME and OID.
// These tests are necessary because some ORMs (e.g., sequelize) require
// ARRAY_AGG to work on these aliased types and produce a result with the
// correct type.
func testArrayAggAliasedTypeOverload(ctx context.Context, t *testing.T, expected *types.T) {
	defer tree.MockNameTypes(map[string]*types.T{
		"a": expected,
	})()
	exprStr := "array_agg(a)"
	expr, err := parser.ParseExpr(exprStr)
	if err != nil {
		t.Fatalf("%s: %v", exprStr, err)
	}
	typ := types.MakeArray(expected)
	typedExpr, err := tree.TypeCheck(ctx, expr, nil, typ)
	if err != nil {
		t.Fatalf("%s: %v", expr, err)
	}
	if !typedExpr.ResolvedType().ArrayContents().Identical(expected) {
		t.Fatalf(
			"Expression has incorrect type: expected %v but got %v",
			expected,
			typedExpr.ResolvedType(),
		)
	}
}

func runBenchmarkAggregate(
	b *testing.B,
	aggFunc func([]*types.T, *tree.EvalContext, tree.Datums) tree.AggregateFunc,
	vals []tree.Datum,
) {
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	params := []*types.T{vals[0].ResolvedType()}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		func() {
			aggImpl := aggFunc(params, evalCtx, nil)
			defer aggImpl.Close(context.Background())
			for i := range vals {
				if err := aggImpl.Add(context.Background(), vals[i]); err != nil {
					b.Fatal(err)
				}
			}
			res, err := aggImpl.Result()
			if err != nil || res == nil {
				b.Errorf("taking result of aggregate implementation %T failed", aggImpl)
			}
		}()
	}
}

func BenchmarkAvgAggregateInt(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newIntAvgAggregate, makeIntTestDatum(count))
		})
	}
}

func BenchmarkAvgAggregateSmallInt(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newIntAvgAggregate, makeSmallIntTestDatum(count))
		})
	}
}

func BenchmarkAvgAggregateFloat(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newFloatAvgAggregate, makeFloatTestDatum(count))
		})
	}
}

func BenchmarkAvgAggregateDecimal(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newDecimalAvgAggregate, makeDecimalTestDatum(count))
		})
	}
}

func BenchmarkAvgAggregateInterval(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newIntervalAvgAggregate, makeIntervalTestDatum(count))
		})
	}
}

func BenchmarkCountAggregate(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newCountAggregate, makeIntTestDatum(count))
		})
	}
}

func BenchmarkSumIntAggregateInt(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newSmallIntSumAggregate, makeSmallIntTestDatum(count))
		})
	}
}

func BenchmarkSumAggregateInt(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newIntSumAggregate, makeIntTestDatum(count))
		})
	}
}

func BenchmarkSumAggregateSmallInt(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newIntSumAggregate, makeSmallIntTestDatum(count))
		})
	}
}

func BenchmarkSumAggregateFloat(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newFloatSumAggregate, makeFloatTestDatum(count))
		})
	}
}

func BenchmarkSumAggregateDecimal(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newDecimalSumAggregate, makeDecimalTestDatum(count))
		})
	}
}

func BenchmarkMaxAggregateInt(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newMaxAggregate, makeIntTestDatum(count))
		})
	}
}

func BenchmarkMaxAggregateFloat(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newMaxAggregate, makeFloatTestDatum(count))
		})
	}
}

func BenchmarkMaxAggregateDecimal(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newMaxAggregate, makeDecimalTestDatum(count))
		})
	}
}

func BenchmarkMinAggregateInt(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newMinAggregate, makeIntTestDatum(count))
		})
	}
}

func BenchmarkMinAggregateFloat(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newMinAggregate, makeFloatTestDatum(count))
		})
	}
}

func BenchmarkMinAggregateDecimal(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newMinAggregate, makeDecimalTestDatum(count))
		})
	}
}

func BenchmarkVarianceAggregateInt(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newIntVarianceAggregate, makeIntTestDatum(count))
		})
	}
}

func BenchmarkVarianceAggregateFloat(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newFloatVarianceAggregate, makeFloatTestDatum(count))
		})
	}
}

func BenchmarkVarianceAggregateDecimal(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newDecimalVarianceAggregate, makeDecimalTestDatum(count))
		})
	}
}

func BenchmarkStdDevAggregateInt(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newIntStdDevAggregate, makeIntTestDatum(count))
		})
	}
}

func BenchmarkStdDevAggregateFloat(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newFloatStdDevAggregate, makeFloatTestDatum(count))
		})
	}
}

func BenchmarkStdDevAggregateDecimal(b *testing.B) {
	for _, count := range []int{1000} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			runBenchmarkAggregate(b, newDecimalStdDevAggregate, makeDecimalTestDatum(count))
		})
	}
}
