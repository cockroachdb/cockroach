// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package parser

import (
	"fmt"
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// testAggregateResultDeepCopy verifies that Datum returned from AggregateFunc's
// Result() method are not mutated during future accumulation. It verifies this by
// printing all values to strings immediately after calling Result(), and later
// printing all values to strings once the accumulation has finished. If the string
// slices are not equal, it means that the result Datums were modified during later
// accumulation, which violates the "deep copy of any internal state" condition.
func testAggregateResultDeepCopy(
	t *testing.T, aggFunc func([]Type, *EvalContext) AggregateFunc, vals []Datum,
) {
	evalCtx := NewTestingEvalContext()
	defer evalCtx.Stop(context.Background())
	aggImpl := aggFunc([]Type{vals[0].ResolvedType()}, evalCtx)
	runningDatums := make([]Datum, len(vals))
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
	testAggregateResultDeepCopy(t, newIntAvgAggregate, makeIntTestDatum(10))
}

func TestAvgFloatResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newFloatAvgAggregate, makeFloatTestDatum(10))
}

func TestAvgDecimalResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newDecimalAvgAggregate, makeDecimalTestDatum(10))
}

func TestBoolAndResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newBoolAndAggregate, makeBoolTestDatum(10))
}

func TestBoolOrResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newBoolOrAggregate, makeBoolTestDatum(10))
}

func TestCountResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newCountAggregate, makeIntTestDatum(10))
}

func TestMaxIntResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newMaxAggregate, makeIntTestDatum(10))
}

func TestMaxFloatResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newMaxAggregate, makeFloatTestDatum(10))
}

func TestMaxDecimalResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newMaxAggregate, makeDecimalTestDatum(10))
}

func TestMaxBoolResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newMaxAggregate, makeBoolTestDatum(10))
}

func TestMinIntResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newMinAggregate, makeIntTestDatum(10))
}

func TestMinFloatResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newMinAggregate, makeFloatTestDatum(10))
}

func TestMinDecimalResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newMinAggregate, makeDecimalTestDatum(10))
}

func TestMinBoolResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newMinAggregate, makeBoolTestDatum(10))
}

func TestSumSmallIntResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newSmallIntSumAggregate, makeIntTestDatum(10))
}

func TestSumIntResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newIntSumAggregate, makeIntTestDatum(10))
}

func TestSumFloatResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newFloatSumAggregate, makeFloatTestDatum(10))
}

func TestSumDecimalResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newDecimalSumAggregate, makeDecimalTestDatum(10))
}

func TestSumIntervalResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newIntervalSumAggregate, makeIntervalTestDatum(10))
}

func TestVarianceIntResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newIntVarianceAggregate, makeIntTestDatum(10))
}

func TestVarianceFloatResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newFloatVarianceAggregate, makeFloatTestDatum(10))
}

func TestVarianceDecimalResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newDecimalVarianceAggregate, makeDecimalTestDatum(10))
}

func TestStdDevIntResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newIntStdDevAggregate, makeIntTestDatum(10))
}

func TestStdDevFloatResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newFloatStdDevAggregate, makeFloatTestDatum(10))
}

func TestStdDevDecimalResultDeepCopy(t *testing.T) {
	testAggregateResultDeepCopy(t, newDecimalStdDevAggregate, makeDecimalTestDatum(10))
}

func makeIntTestDatum(count int) []Datum {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]Datum, count)
	for i := range vals {
		vals[i] = NewDInt(DInt(rng.Int63()))
	}
	return vals
}

// makeSmallIntTestDatum creates integers that are sufficiently
// smaller than 2^64-1 that they can be added to each other for a
// significant part of the test without overflow. This is meant to
// test the implementation of aggregates that can use an int64 to
// optimize computations small decimal values.
func makeSmallIntTestDatum(count int) []Datum {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]Datum, count)
	for i := range vals {
		sign := int32(1)
		if 0 == rng.Int31()&1 {
			sign = -1
		}
		vals[i] = NewDInt(DInt(rng.Int31() * sign))
	}
	return vals
}

func makeFloatTestDatum(count int) []Datum {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]Datum, count)
	for i := range vals {
		vals[i] = NewDFloat(DFloat(rng.Float64()))
	}
	return vals
}

func makeDecimalTestDatum(count int) []Datum {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]Datum, count)
	for i := range vals {
		dd := &DDecimal{}
		if _, err := dd.SetFloat64(rng.Float64()); err != nil {
			panic(err)
		}
		vals[i] = dd
	}
	return vals
}

func makeBoolTestDatum(count int) []Datum {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]Datum, count)
	for i := range vals {
		vals[i] = MakeDBool(DBool(rng.Int31n(2) == 0))
	}
	return vals
}

func makeIntervalTestDatum(count int) []Datum {
	rng, _ := randutil.NewPseudoRand()

	vals := make([]Datum, count)
	for i := range vals {
		vals[i] = &DInterval{Duration: duration.Duration{Months: rng.Int63n(1000),
			Days:  rng.Int63n(1000),
			Nanos: rng.Int63n(1000000),
		}}
	}
	return vals
}

func runBenchmarkAggregate(
	b *testing.B, aggFunc func([]Type, *EvalContext) AggregateFunc, vals []Datum,
) {
	evalCtx := NewTestingEvalContext()
	defer evalCtx.Stop(context.Background())
	params := []Type{vals[0].ResolvedType()}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aggImpl := aggFunc(params, evalCtx)
		for i := range vals {
			if err := aggImpl.Add(context.Background(), vals[i]); err != nil {
				b.Fatal(err)
			}
		}
		res, err := aggImpl.Result()
		if err != nil || res == nil {
			b.Errorf("taking result of aggregate implementation %T failed", aggImpl)
		}
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
