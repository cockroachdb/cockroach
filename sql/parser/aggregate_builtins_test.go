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
	"testing"

	"github.com/cockroachdb/cockroach/util/decimal"
	"github.com/cockroachdb/cockroach/util/randutil"
)

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
		decimal.SetFromFloat(&dd.Dec, rng.Float64())
		vals[i] = dd
	}
	return vals
}

func runBenchmarkAggregate(b *testing.B, aggFunc func() AggregateFunc, vals []Datum) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aggImpl := aggFunc()
		for i := range vals {
			aggImpl.Add(vals[i])
		}
		if aggImpl.Result() == nil {
			b.Errorf("taking result of aggregate implementation %T failed", aggImpl)
		}
	}
}

func BenchmarkAvgAggregateInt1K(b *testing.B) {
	runBenchmarkAggregate(b, newIntAvgAggregate, makeIntTestDatum(1000))
}

func BenchmarkAvgAggregateSmallInt1K(b *testing.B) {
	runBenchmarkAggregate(b, newIntAvgAggregate, makeSmallIntTestDatum(1000))
}

func BenchmarkAvgAggregateFloat1K(b *testing.B) {
	runBenchmarkAggregate(b, newFloatAvgAggregate, makeFloatTestDatum(1000))
}

func BenchmarkAvgAggregateDecimal1K(b *testing.B) {
	runBenchmarkAggregate(b, newDecimalAvgAggregate, makeDecimalTestDatum(1000))
}

func BenchmarkCountAggregate1K(b *testing.B) {
	runBenchmarkAggregate(b, newCountAggregate, makeIntTestDatum(1000))
}

func BenchmarkSumAggregateInt1K(b *testing.B) {
	runBenchmarkAggregate(b, newIntSumAggregate, makeIntTestDatum(1000))
}

func BenchmarkSumAggregateSmallInt1K(b *testing.B) {
	runBenchmarkAggregate(b, newIntSumAggregate, makeSmallIntTestDatum(1000))
}

func BenchmarkSumAggregateFloat1K(b *testing.B) {
	runBenchmarkAggregate(b, newFloatSumAggregate, makeFloatTestDatum(1000))
}

func BenchmarkSumAggregateDecimal1K(b *testing.B) {
	runBenchmarkAggregate(b, newDecimalSumAggregate, makeDecimalTestDatum(1000))
}

func BenchmarkMaxAggregateInt1K(b *testing.B) {
	runBenchmarkAggregate(b, newMaxAggregate, makeIntTestDatum(1000))
}

func BenchmarkMaxAggregateFloat1K(b *testing.B) {
	runBenchmarkAggregate(b, newMaxAggregate, makeFloatTestDatum(1000))
}

func BenchmarkMaxAggregateDecimal1K(b *testing.B) {
	runBenchmarkAggregate(b, newMaxAggregate, makeDecimalTestDatum(1000))
}

func BenchmarkMinAggregateInt1K(b *testing.B) {
	runBenchmarkAggregate(b, newMinAggregate, makeIntTestDatum(1000))
}

func BenchmarkMinAggregateFloat1K(b *testing.B) {
	runBenchmarkAggregate(b, newMinAggregate, makeFloatTestDatum(1000))
}

func BenchmarkMinAggregateDecimal1K(b *testing.B) {
	runBenchmarkAggregate(b, newMinAggregate, makeDecimalTestDatum(1000))
}

func BenchmarkVarianceAggregateInt1K(b *testing.B) {
	runBenchmarkAggregate(b, newIntVarianceAggregate, makeIntTestDatum(1000))
}

func BenchmarkVarianceAggregateFloat1K(b *testing.B) {
	runBenchmarkAggregate(b, newFloatVarianceAggregate, makeFloatTestDatum(1000))
}

func BenchmarkVarianceAggregateDecimal1K(b *testing.B) {
	runBenchmarkAggregate(b, newDecimalVarianceAggregate, makeDecimalTestDatum(1000))
}

func BenchmarkStddevAggregateInt1K(b *testing.B) {
	runBenchmarkAggregate(b, newIntStddevAggregate, makeIntTestDatum(1000))
}

func BenchmarkStddevAggregateFloat1K(b *testing.B) {
	runBenchmarkAggregate(b, newFloatStddevAggregate, makeFloatTestDatum(1000))
}

func BenchmarkStddevAggregateDecimal1K(b *testing.B) {
	runBenchmarkAggregate(b, newDecimalStddevAggregate, makeDecimalTestDatum(1000))
}
