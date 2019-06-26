// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package props

import (
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func TestHistogram(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	histData := []cat.HistogramBucket{
		{NumRange: 0, NumEq: 1, UpperBound: tree.NewDInt(1)},
		{NumRange: 3, NumEq: 3, UpperBound: tree.NewDInt(10)},
		{NumRange: 4, NumEq: 5, UpperBound: tree.NewDInt(25)},
		{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(30)},
		{NumRange: 40, NumEq: 35, UpperBound: tree.NewDInt(42)},
	}
	h := &Histogram{}
	h.Init(&evalCtx, opt.ColumnID(1), histData)

	testData := []struct {
		constraint string
		buckets    []HistogramBucket
		count      float64
	}{
		{
			constraint: "/1: [/0 - /0]",
			buckets: []HistogramBucket{
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(0)},
			},
			count: 0,
		},
		{
			constraint: "/1: [/50 - /100]",
			buckets: []HistogramBucket{
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(42)},
			},
			count: 0,
		},
		{
			constraint: "/1: [ - /1] [/11 - /24] [/30 - /45]",
			buckets: []HistogramBucket{
				{NumRange: 0, NumEq: 1, UpperBound: tree.NewDInt(1)},
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(10)},
				{NumRange: 4 * 13.0 / 14.0, NumEq: 4 * 1.0 / 14.0, UpperBound: tree.NewDInt(24)},
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(30)},
				{NumRange: 40, NumEq: 35, UpperBound: tree.NewDInt(42)},
			},
			count: 80,
		},
		{
			constraint: "/1: [/5 - /10] [/15 - /32] [/34 - /36] [/38 - ]",
			buckets: []HistogramBucket{
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(4)},
				{NumRange: 3 * 5.0 / 8.0, NumEq: 3, UpperBound: tree.NewDInt(10)},
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(14)},
				{NumRange: 4 * 10.0 / 14.0, NumEq: 5, UpperBound: tree.NewDInt(25)},
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(30)},
				{NumRange: 40.0 / 11.0, NumEq: 40.0 / 11.0, UpperBound: tree.NewDInt(32)},
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(33)},
				{NumRange: 2 * 40.0 / 11.0, NumEq: 40.0 / 11.0, UpperBound: tree.NewDInt(36)},
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(37)},
				{NumRange: 4 * 40.0 / 11.0, NumEq: 35, UpperBound: tree.NewDInt(42)},
			},
			count: 3*5.0/8.0 + 4*10.0/14.0 + 9*40.0/11 + 43,
		},
		{
			constraint: "/1: [ - /41]",
			buckets: []HistogramBucket{
				{NumRange: 0, NumEq: 1, UpperBound: tree.NewDInt(1)},
				{NumRange: 3, NumEq: 3, UpperBound: tree.NewDInt(10)},
				{NumRange: 4, NumEq: 5, UpperBound: tree.NewDInt(25)},
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(30)},
				{NumRange: 10 * 40.0 / 11.0, NumEq: 40.0 / 11.0, UpperBound: tree.NewDInt(41)},
			},
			count: 56,
		},
		{
			constraint: "/1: [/1 - ]",
			buckets: []HistogramBucket{
				{NumRange: 0, NumEq: 1, UpperBound: tree.NewDInt(1)},
				{NumRange: 3, NumEq: 3, UpperBound: tree.NewDInt(10)},
				{NumRange: 4, NumEq: 5, UpperBound: tree.NewDInt(25)},
				{NumRange: 0, NumEq: 0, UpperBound: tree.NewDInt(30)},
				{NumRange: 40, NumEq: 35, UpperBound: tree.NewDInt(42)},
			},
			count: 91,
		},
	}

	for i := range testData {
		c := constraint.ParseConstraint(&evalCtx, testData[i].constraint)
		if !h.CanFilter(&c) {
			t.Fatalf("constraint %s cannot filter histogram %v", c.String(), *h)
		}
		filtered := h.Filter(&c)
		if !reflect.DeepEqual(testData[i].buckets, filtered.buckets) {
			t.Fatalf("expected %v but found %v", testData[i].buckets, filtered.buckets)
		}
		count := filtered.ValuesCount()
		if testData[i].count != count {
			t.Fatalf("expected %f but found %f", testData[i].count, count)
		}
	}
}

func TestFilterBucket(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	keyCtx := constraint.KeyContext{EvalCtx: &evalCtx}
	keyCtx.Columns.InitSingle(opt.MakeOrderingColumn(opt.ColumnID(1), false /* descending */))

	type testCase struct {
		span     string
		expected *HistogramBucket
		isError  bool
	}
	runTestCase := func(
		bucket *HistogramBucket, lowerBound tree.Datum, span *constraint.Span,
	) (actual *HistogramBucket, err error) {
		defer func() {
			// Any errors will be propagated as panics.
			if r := recover(); r != nil {
				if e, ok := r.(error); ok {
					err = e
					return
				}
				panic(r)
			}
		}()

		return bucket.getFilteredBucket(&keyCtx, span, lowerBound), nil
	}

	runTest := func(
		bucket *HistogramBucket, lowerBound tree.Datum, testData []testCase, typ types.Family,
	) {
		for _, testCase := range testData {
			span := constraint.ParseSpan(&evalCtx, testCase.span, typ)
			actual, err := runTestCase(bucket, lowerBound, &span)
			if err != nil && !testCase.isError {
				t.Fatal(err)
			} else if err == nil && testCase.isError {
				t.Fatal("expected an error")
			} else if !reflect.DeepEqual(testCase.expected, actual) {
				t.Fatalf("exected %v but found %v", testCase.expected, actual)
			}
		}
	}

	t.Run("int", func(t *testing.T) {
		bucket := &HistogramBucket{NumEq: 5, NumRange: 10, UpperBound: tree.NewDInt(10)}
		lowerBound := tree.NewDInt(0)
		testData := []testCase{
			{
				span:     "[/0 - /0]",
				expected: &HistogramBucket{NumEq: 1, NumRange: 0, UpperBound: tree.NewDInt(0)},
			},
			{
				span:     "[/0 - /5]",
				expected: &HistogramBucket{NumEq: 1, NumRange: 5, UpperBound: tree.NewDInt(5)},
			},
			{
				span:     "[/2 - /9]",
				expected: &HistogramBucket{NumEq: 1, NumRange: 7, UpperBound: tree.NewDInt(9)},
			},
			{
				span:     "[/2 - /10]",
				expected: &HistogramBucket{NumEq: 5, NumRange: 8, UpperBound: tree.NewDInt(10)},
			},
			{
				span:     "[/10 - /10]",
				expected: &HistogramBucket{NumEq: 5, NumRange: 0, UpperBound: tree.NewDInt(10)},
			},
			{
				span:    "[/20 - /30]",
				isError: true,
			},
		}

		runTest(bucket, lowerBound, testData, types.IntFamily)
	})

	t.Run("float", func(t *testing.T) {
		bucket := &HistogramBucket{NumEq: 5, NumRange: 10, UpperBound: tree.NewDFloat(10)}
		lowerBound := tree.NewDFloat(0)

		testData := []testCase{
			{
				span:     "[/0 - /0]",
				expected: &HistogramBucket{NumEq: 0, NumRange: 0, UpperBound: tree.NewDFloat(0)},
			},
			{
				span:     "(/0 - /5]",
				expected: &HistogramBucket{NumEq: 0, NumRange: 5, UpperBound: tree.NewDFloat(5)},
			},
			{
				span:     "[/2.5 - /9)",
				expected: &HistogramBucket{NumEq: 0, NumRange: 6.5, UpperBound: tree.NewDFloat(9)},
			},
			{
				span:     "[/2 - /10]",
				expected: &HistogramBucket{NumEq: 5, NumRange: 8, UpperBound: tree.NewDFloat(10)},
			},
			{
				span:     "[/10 - /10]",
				expected: &HistogramBucket{NumEq: 5, NumRange: 0, UpperBound: tree.NewDFloat(10)},
			},
			{
				span:    "[/10 - /20]",
				isError: true,
			},
		}

		runTest(bucket, lowerBound, testData, types.FloatFamily)
	})

	t.Run("decimal", func(t *testing.T) {
		upperBound, err := tree.ParseDDecimal("10")
		if err != nil {
			t.Fatal(err)
		}
		bucket := &HistogramBucket{NumEq: 5, NumRange: 10, UpperBound: upperBound}
		lowerBound, err := tree.ParseDDecimal("0")
		if err != nil {
			t.Fatal(err)
		}

		ub1, err := tree.ParseDDecimal("9")
		if err != nil {
			t.Fatal(err)
		}
		ub2, err := tree.ParseDDecimal("10.00")
		if err != nil {
			t.Fatal(err)
		}

		testData := []testCase{
			{
				span:     "[/2.50 - /9)",
				expected: &HistogramBucket{NumEq: 0, NumRange: 6.5, UpperBound: ub1},
			},
			{
				span:     "[/2 - /10.00]",
				expected: &HistogramBucket{NumEq: 5, NumRange: 8, UpperBound: ub2},
			},
		}

		runTest(bucket, lowerBound, testData, types.DecimalFamily)
	})

	t.Run("date", func(t *testing.T) {
		upperBound, err := tree.ParseDDate(&evalCtx, "2019-08-01")
		if err != nil {
			t.Fatal(err)
		}
		bucket := &HistogramBucket{NumEq: 1, NumRange: 62, UpperBound: upperBound}
		lowerBound, err := tree.ParseDDate(&evalCtx, "2019-07-01")
		if err != nil {
			t.Fatal(err)
		}

		ub1, err := tree.ParseDDate(&evalCtx, "2019-07-02")
		if err != nil {
			t.Fatal(err)
		}

		testData := []testCase{
			{
				span:     "[/2019-07-01 - /2019-07-02]",
				expected: &HistogramBucket{NumEq: 2, NumRange: 2, UpperBound: ub1},
			},
			{
				span:     "[/2019-07-05 - /2019-08-01]",
				expected: &HistogramBucket{NumEq: 1, NumRange: 54, UpperBound: upperBound},
			},
		}

		runTest(bucket, lowerBound, testData, types.DateFamily)
	})

	t.Run("timestamp", func(t *testing.T) {
		upperBound, err := tree.ParseDTimestamp(&evalCtx, "2019-08-01 12:00:00.000000", time.Microsecond)
		if err != nil {
			t.Fatal(err)
		}
		bucket := &HistogramBucket{NumEq: 1, NumRange: 62, UpperBound: upperBound}
		lowerBound, err := tree.ParseDTimestamp(&evalCtx, "2019-07-01 12:00:00.000000", time.Microsecond)
		if err != nil {
			t.Fatal(err)
		}

		ub1, err := tree.ParseDTimestamp(&evalCtx, "2019-07-02 00:00:00.000000", time.Microsecond)
		if err != nil {
			t.Fatal(err)
		}

		testData := []testCase{
			{
				span:     "[/2019-07-01 12:00:00.000000 - /2019-07-02 00:00:00.000000)",
				expected: &HistogramBucket{NumEq: 0, NumRange: 1, UpperBound: ub1},
			},
			{
				span:     "[/2019-07-05 12:00:00.000000 - /2019-08-01 12:00:00.000000)",
				expected: &HistogramBucket{NumEq: 0, NumRange: 54, UpperBound: upperBound},
			},
		}

		runTest(bucket, lowerBound, testData, types.TimestampFamily)
	})

	t.Run("string", func(t *testing.T) {
		bucket := &HistogramBucket{NumEq: 5, NumRange: 10, UpperBound: tree.NewDString("foo")}
		lowerBound := tree.NewDString("bar")
		testData := []testCase{
			{
				span:     "[/bar - /baz]",
				expected: &HistogramBucket{NumEq: 0, NumRange: 5, UpperBound: tree.NewDString("baz")},
			},
			{
				span:     "[/baz - /foo]",
				expected: &HistogramBucket{NumEq: 5, NumRange: 5, UpperBound: tree.NewDString("foo")},
			},
		}

		runTest(bucket, lowerBound, testData, types.StringFamily)
	})

}
