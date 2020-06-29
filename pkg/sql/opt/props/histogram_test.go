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
	"math"
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

func TestCanFilter(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	// The histogram column ID is 1 for all test cases. CanFilter should only
	// return true for constraints in which column ID 1 is part of the exact
	// prefix or the first column after.
	testData := []struct {
		constraint string
		canFilter  bool
		colIdx     int
	}{
		{
			constraint: "/1: [/0 - /0]",
			canFilter:  true,
			colIdx:     0,
		},
		{
			constraint: "/2: [/0 - /0]",
			canFilter:  false,
		},
		{
			constraint: "/1: [/0 - /0] [/2 - /2]",
			canFilter:  true,
			colIdx:     0,
		},
		{
			constraint: "/1: [/3 - /20] [/22 - ]",
			canFilter:  true,
			colIdx:     0,
		},
		{
			constraint: "/1/2: [/0/3 - /0/3] [/2/3 - /2/3]",
			canFilter:  true,
			colIdx:     0,
		},
		{
			constraint: "/2/-1: [/0/3 - /0/3] [/2/3 - /2/3]",
			canFilter:  false,
		},
		{
			constraint: "/2/1: [/0/3 - /0/3] [/0/5 - /0/5]",
			canFilter:  true,
			colIdx:     1,
		},
		{
			constraint: "/2/-1: [/0/5 - /0/3] [/0/1 - /0/1]",
			canFilter:  true,
			colIdx:     1,
		},
		{
			constraint: "/2/3/1: [/0/3/NULL - /0/3/100] [/0/5/NULL - /0/5/100]",
			canFilter:  false,
		},
		{
			constraint: "/2/-3/1: [/0/5/NULL - /0/5/100] [/0/3/NULL - /0/3/100]",
			canFilter:  false,
		},
		{
			constraint: "/2/1/3: [/0/3/NULL - /0/3/100] [/0/3/200 - /0/3/300]",
			canFilter:  true,
			colIdx:     1,
		},
	}

	h := Histogram{}
	h.Init(&evalCtx, opt.ColumnID(1), []cat.HistogramBucket{})
	for _, tc := range testData {
		c := constraint.ParseConstraint(&evalCtx, tc.constraint)
		colIdx, _, ok := h.CanFilter(&c)
		if ok != tc.canFilter {
			t.Fatalf(
				"for constraint %s, expected canFilter=%v but found %v", tc.constraint, tc.canFilter, ok,
			)
		}
		if ok && colIdx != tc.colIdx {
			t.Fatalf(
				"for constraint %s, expected colIdx=%d but found %d", tc.constraint, tc.colIdx, colIdx,
			)
		}
	}
}

func TestHistogram(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	//   0  1  3  3   4  5   0  0   40  35
	// <--- 1 --- 10 --- 25 --- 30 ---- 42
	histData := []cat.HistogramBucket{
		{NumRange: 0, DistinctRange: 0, NumEq: 1, UpperBound: tree.NewDInt(1)},
		{NumRange: 3, DistinctRange: 2, NumEq: 3, UpperBound: tree.NewDInt(10)},
		{NumRange: 4, DistinctRange: 2, NumEq: 5, UpperBound: tree.NewDInt(25)},
		{NumRange: 0, DistinctRange: 0, NumEq: 0, UpperBound: tree.NewDInt(30)},
		{NumRange: 40, DistinctRange: 7, NumEq: 35, UpperBound: tree.NewDInt(42)},
	}
	h := &Histogram{}
	h.Init(&evalCtx, opt.ColumnID(1), histData)
	count, expected := h.ValuesCount(), float64(91)
	if count != expected {
		t.Fatalf("expected %f but found %f", expected, count)
	}
	maxDistinct, expected := h.maxDistinctValuesCount(), float64(22)
	if maxDistinct != expected {
		t.Fatalf("expected %f but found %f", expected, maxDistinct)
	}
	distinct, expected := h.DistinctValuesCount(), float64(15)
	if distinct != expected {
		t.Fatalf("expected %f but found %f", expected, distinct)
	}

	testData := []struct {
		constraint  string
		buckets     []cat.HistogramBucket
		count       float64
		maxDistinct float64
		distinct    float64
	}{
		{
			constraint:  "/1: [/0 - /0]",
			buckets:     []cat.HistogramBucket{},
			count:       0,
			maxDistinct: 0,
			distinct:    0,
		},
		{
			constraint:  "/1: [/50 - /100]",
			buckets:     []cat.HistogramBucket{},
			count:       0,
			maxDistinct: 0,
			distinct:    0,
		},
		{
			constraint: "/1: [ - /1] [/11 - /24] [/30 - /45]",
			//   0  1  0  0   3.7143 0.28571 0  0   40  35
			// <--- 1 --- 10 --------- 24 ----- 30 ---- 42
			buckets: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1, DistinctRange: 0, UpperBound: tree.NewDInt(1)},
				{NumRange: 0, NumEq: 0, DistinctRange: 0, UpperBound: tree.NewDInt(10)},
				{NumRange: 3.71, NumEq: 0.29, DistinctRange: 1.86, UpperBound: tree.NewDInt(24)},
				{NumRange: 0, NumEq: 0, DistinctRange: 0, UpperBound: tree.NewDInt(30)},
				{NumRange: 40, NumEq: 35, DistinctRange: 7, UpperBound: tree.NewDInt(42)},
			},
			count:       80,
			maxDistinct: 17,
			distinct:    11.14,
		},
		{
			constraint: "/1: [/5 - /10] [/15 - /32] [/34 - /36] [/38 - ]",
			//   0  0  1.875  3   0  0   2.8571  5   0  0   3.6364 3.6364 0  0   7.2727 3.6364 0  0   14.545  35
			// <--- 4 ------- 10 --- 14 -------- 25 --- 30 --------- 32 ---- 33 --------- 36 ---- 37 -------- 42
			buckets: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 0, DistinctRange: 0, UpperBound: tree.NewDInt(4)},
				{NumRange: 1.88, NumEq: 3, DistinctRange: 1.25, UpperBound: tree.NewDInt(10)},
				{NumRange: 0, NumEq: 0, DistinctRange: 0, UpperBound: tree.NewDInt(14)},
				{NumRange: 2.86, NumEq: 5, DistinctRange: 1.43, UpperBound: tree.NewDInt(25)},
				{NumRange: 0, NumEq: 0, DistinctRange: 0, UpperBound: tree.NewDInt(30)},
				{NumRange: 3.64, NumEq: 3.64, DistinctRange: 0.64, UpperBound: tree.NewDInt(32)},
				{NumRange: 0, NumEq: 0, DistinctRange: 0, UpperBound: tree.NewDInt(33)},
				{NumRange: 7.27, NumEq: 3.64, DistinctRange: 1.27, UpperBound: tree.NewDInt(36)},
				{NumRange: 0, NumEq: 0, DistinctRange: 0, UpperBound: tree.NewDInt(37)},
				{NumRange: 14.55, NumEq: 35, DistinctRange: 2.55, UpperBound: tree.NewDInt(42)},
			},
			count:       80.46,
			maxDistinct: 16.73,
			distinct:    12.13,
		},
		{
			constraint: "/1: [ - /41]",
			//   0  1  3  3   4  5   0  0   36.364 3.6364
			// <--- 1 --- 10 --- 25 --- 30 --------- 41 -
			buckets: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1, DistinctRange: 0, UpperBound: tree.NewDInt(1)},
				{NumRange: 3, NumEq: 3, DistinctRange: 2, UpperBound: tree.NewDInt(10)},
				{NumRange: 4, NumEq: 5, DistinctRange: 2, UpperBound: tree.NewDInt(25)},
				{NumRange: 0, NumEq: 0, DistinctRange: 0, UpperBound: tree.NewDInt(30)},
				{NumRange: 36.36, NumEq: 3.64, DistinctRange: 6.36, UpperBound: tree.NewDInt(41)},
			},
			count:       56,
			maxDistinct: 21,
			distinct:    14.36,
		},
		{
			constraint: "/1: [/1 - ]",
			//   0  1  3  3   4  5   0  0   40  35
			// <--- 1 --- 10 --- 25 --- 30 ---- 42
			buckets: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1, DistinctRange: 0, UpperBound: tree.NewDInt(1)},
				{NumRange: 3, NumEq: 3, DistinctRange: 2, UpperBound: tree.NewDInt(10)},
				{NumRange: 4, NumEq: 5, DistinctRange: 2, UpperBound: tree.NewDInt(25)},
				{NumRange: 0, NumEq: 0, DistinctRange: 0, UpperBound: tree.NewDInt(30)},
				{NumRange: 40, NumEq: 35, DistinctRange: 7, UpperBound: tree.NewDInt(42)},
			},
			count:       91,
			maxDistinct: 22,
			distinct:    15,
		},
		{
			constraint: "/1: [/40 - /40]",
			//   0 5.7143
			// <---- 40 -
			buckets: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 5.71, DistinctRange: 0, UpperBound: tree.NewDInt(40)},
			},
			count:       5.71,
			maxDistinct: 1,
			distinct:    1,
		},
		{
			constraint: "/1: [/0 - /100]",
			//   0  1  3  3   4  5   0  0   40  35
			// <--- 1 --- 10 --- 25 --- 30 ---- 42
			buckets: []cat.HistogramBucket{
				{NumRange: 0, DistinctRange: 0, NumEq: 1, UpperBound: tree.NewDInt(1)},
				{NumRange: 3, DistinctRange: 2, NumEq: 3, UpperBound: tree.NewDInt(10)},
				{NumRange: 4, DistinctRange: 2, NumEq: 5, UpperBound: tree.NewDInt(25)},
				{NumRange: 0, DistinctRange: 0, NumEq: 0, UpperBound: tree.NewDInt(30)},
				{NumRange: 40, DistinctRange: 7, NumEq: 35, UpperBound: tree.NewDInt(42)},
			},
			count:       91,
			maxDistinct: 22,
			distinct:    15,
		},

		// Tests with multiple columns.
		{
			constraint: "/1/2: [ - /1/3] [/11 - /24/3] [/30 - /45/3]",
			//   0  1  0  0   3.7143 0.28571 0  0   40  35
			// <--- 1 --- 10 --------- 24 ----- 30 ---- 42
			buckets: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1, DistinctRange: 0, UpperBound: tree.NewDInt(1)},
				{NumRange: 0, NumEq: 0, DistinctRange: 0, UpperBound: tree.NewDInt(10)},
				{NumRange: 3.71, NumEq: 0.29, DistinctRange: 1.86, UpperBound: tree.NewDInt(24)},
				{NumRange: 0, NumEq: 0, DistinctRange: 0, UpperBound: tree.NewDInt(30)},
				{NumRange: 40, NumEq: 35, DistinctRange: 7, UpperBound: tree.NewDInt(42)},
			},
			count:       80,
			maxDistinct: 17,
			distinct:    11.14,
		},
		{
			constraint: "/2/1: [/3 - /3/1] [/3/11 - /3/24] [/3/30 - /3/45]",
			//   0  1  0  0   3.7143 0.28571 0  0   40  35
			// <--- 1 --- 10 --------- 24 ----- 30 ---- 42
			buckets: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1, DistinctRange: 0, UpperBound: tree.NewDInt(1)},
				{NumRange: 0, NumEq: 0, DistinctRange: 0, UpperBound: tree.NewDInt(10)},
				{NumRange: 3.71, NumEq: 0.29, DistinctRange: 1.86, UpperBound: tree.NewDInt(24)},
				{NumRange: 0, NumEq: 0, DistinctRange: 0, UpperBound: tree.NewDInt(30)},
				{NumRange: 40, NumEq: 35, DistinctRange: 7, UpperBound: tree.NewDInt(42)},
			},
			count:       80,
			maxDistinct: 17,
			distinct:    11.14,
		},
		{
			constraint: "/2/1/3: [/1/40/2 - /1/40/3]",
			//   0 5.7143
			// <---- 40 -
			buckets: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 5.71, DistinctRange: 0, UpperBound: tree.NewDInt(40)},
			},
			count:       5.71,
			maxDistinct: 1,
			distinct:    1,
		},
		{
			constraint: "/2/1/3: [/1/40/2 - /1/40/2] [/1/40/4 - /1/40/4] [/1/40/6 - /1/40/6]",
			//   0 5.7143
			// <---- 40 -
			buckets: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 5.71, DistinctRange: 0, UpperBound: tree.NewDInt(40)},
			},
			count:       5.71,
			maxDistinct: 1,
			distinct:    1,
		},
	}

	for i := range testData {
		c := constraint.ParseConstraint(&evalCtx, testData[i].constraint)
		ascAndDesc := []constraint.Constraint{c, makeDescConstraint(&c)}

		// Make sure all test cases work with both ascending and descending columns.
		for _, c := range ascAndDesc {
			if _, _, ok := h.CanFilter(&c); !ok {
				t.Fatalf("constraint %s cannot filter histogram %v", c.String(), *h)
			}
			filtered := h.Filter(&c)
			count := roundVal(filtered.ValuesCount())
			if testData[i].count != count {
				t.Fatalf("expected %f but found %f", testData[i].count, count)
			}
			maxDistinct := roundVal(filtered.maxDistinctValuesCount())
			if testData[i].maxDistinct != maxDistinct {
				t.Fatalf("expected %f but found %f", testData[i].maxDistinct, maxDistinct)
			}
			distinct := roundVal(filtered.DistinctValuesCount())
			if testData[i].distinct != distinct {
				t.Fatalf("expected %f but found %f", testData[i].distinct, distinct)
			}
			roundHistogram(filtered)
			if !reflect.DeepEqual(testData[i].buckets, filtered.buckets) {
				t.Fatalf("expected %v but found %v", testData[i].buckets, filtered.buckets)
			}
		}
	}
}

func TestFilterBucket(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	keyCtx := constraint.KeyContext{EvalCtx: &evalCtx}
	col := opt.ColumnID(1)

	type testCase struct {
		span     string
		expected *cat.HistogramBucket
		isError  bool
	}

	runTestCase := func(
		h *Histogram, span *constraint.Span, desc bool,
	) (actual *cat.HistogramBucket, err error) {
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

		keyCtx.Columns.InitSingle(opt.MakeOrderingColumn(col, desc))
		var iter histogramIter
		iter.init(h, desc)

		// All test cases have two buckets. The first bucket is empty and used to
		// mark the lower bound of the second bucket. Set the iterator to point to
		// the second bucket.
		iter.setIdx(1)
		b := getFilteredBucket(&iter, &keyCtx, span, 0 /* colIdx */)
		roundBucket(b)
		return b, nil
	}

	runTest := func(h *Histogram, testData []testCase, typ types.Family) {
		for _, testCase := range testData {
			span := constraint.ParseSpan(&evalCtx, testCase.span, typ)
			ascAndDesc := []constraint.Span{span, makeDescSpan(&span)}

			// Make sure all test cases work with both ascending and descending columns.
			for i, span := range ascAndDesc {
				actual, err := runTestCase(h, &span, i == 1 /* desc */)
				if err != nil && !testCase.isError {
					t.Fatalf("for span %s got error %v", testCase.span, err)
				} else if err == nil {
					if testCase.isError {
						t.Fatalf("for span %s expected an error", testCase.span)
					}
					if !reflect.DeepEqual(testCase.expected, actual) {
						t.Fatalf("for span %s exected %v but found %v", testCase.span, testCase.expected, actual)
					}
				}
			}
		}
	}

	// Each of the tests below have a histogram with two buckets. The first
	// bucket is empty and simply used to mark the lower bound of the second
	// bucket.
	//
	// getPrevUpperBound is used to find the upper bound of the first bucket so
	// that the lower bound of the second bucket will equal upperBound.Next().
	getPrevUpperBound := func(lowerBound tree.Datum) tree.Datum {
		res, ok := lowerBound.Prev(&evalCtx)
		if !ok {
			res = lowerBound
		}
		return res
	}

	t.Run("int", func(t *testing.T) {
		h := &Histogram{evalCtx: &evalCtx, col: col, buckets: []cat.HistogramBucket{
			{NumEq: 0, NumRange: 0, DistinctRange: 0, UpperBound: getPrevUpperBound(tree.NewDInt(0))},
			{NumEq: 5, NumRange: 10, DistinctRange: 10, UpperBound: tree.NewDInt(10)},
		}}
		testData := []testCase{
			{
				span:     "[/0 - /0]",
				expected: &cat.HistogramBucket{NumEq: 1, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDInt(0)},
			},
			{
				span:     "[/0 - /5]",
				expected: &cat.HistogramBucket{NumEq: 1, NumRange: 5, DistinctRange: 5, UpperBound: tree.NewDInt(5)},
			},
			{
				span:     "[/2 - /9]",
				expected: &cat.HistogramBucket{NumEq: 1, NumRange: 7, DistinctRange: 7, UpperBound: tree.NewDInt(9)},
			},
			{
				span:     "[/2 - /10]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 8, DistinctRange: 8, UpperBound: tree.NewDInt(10)},
			},
			{
				span:     "[/10 - /10]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDInt(10)},
			},
			{
				span:    "[/20 - /30]",
				isError: true,
			},
		}

		runTest(h, testData, types.IntFamily)
	})

	t.Run("float", func(t *testing.T) {
		h := &Histogram{evalCtx: &evalCtx, col: col, buckets: []cat.HistogramBucket{
			{NumEq: 0, NumRange: 0, DistinctRange: 0, UpperBound: getPrevUpperBound(tree.NewDFloat(0))},
			{NumEq: 5, NumRange: 10, DistinctRange: 10, UpperBound: tree.NewDFloat(10)},
		}}
		testData := []testCase{
			{
				span:     "[/0 - /0]",
				expected: &cat.HistogramBucket{NumEq: 1, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDFloat(0)},
			},
			{
				span:     "(/0 - /5]",
				expected: &cat.HistogramBucket{NumEq: 0, NumRange: 5, DistinctRange: 5, UpperBound: tree.NewDFloat(5)},
			},
			{
				span:     "[/2.5 - /9)",
				expected: &cat.HistogramBucket{NumEq: 0, NumRange: 6.5, DistinctRange: 6.5, UpperBound: tree.NewDFloat(9)},
			},
			{
				span:     "[/2 - /10]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 8, DistinctRange: 8, UpperBound: tree.NewDFloat(10)},
			},
			{
				span:     "[/10 - /10]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDFloat(10)},
			},
			{
				span:    "[/10 - /20]",
				isError: true,
			},
		}

		runTest(h, testData, types.FloatFamily)
	})

	t.Run("decimal", func(t *testing.T) {
		upperBound, err := tree.ParseDDecimal("10")
		if err != nil {
			t.Fatal(err)
		}
		lowerBound, err := tree.ParseDDecimal("0")
		if err != nil {
			t.Fatal(err)
		}
		h := &Histogram{evalCtx: &evalCtx, col: col, buckets: []cat.HistogramBucket{
			{NumEq: 0, NumRange: 0, DistinctRange: 0, UpperBound: getPrevUpperBound(lowerBound)},
			{NumEq: 5, NumRange: 10, DistinctRange: 10, UpperBound: upperBound},
		}}

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
				expected: &cat.HistogramBucket{NumEq: 0, NumRange: 6.5, DistinctRange: 6.5, UpperBound: ub1},
			},
			{
				span:     "[/2 - /10.00]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 8, DistinctRange: 8, UpperBound: ub2},
			},
		}

		runTest(h, testData, types.DecimalFamily)
	})

	t.Run("date", func(t *testing.T) {
		upperBound, _, err := tree.ParseDDate(&evalCtx, "2019-08-01")
		if err != nil {
			t.Fatal(err)
		}
		lowerBound, _, err := tree.ParseDDate(&evalCtx, "2019-07-01")
		if err != nil {
			t.Fatal(err)
		}
		h := &Histogram{evalCtx: &evalCtx, col: col, buckets: []cat.HistogramBucket{
			{NumEq: 0, NumRange: 0, DistinctRange: 0, UpperBound: getPrevUpperBound(lowerBound)},
			{NumEq: 1, NumRange: 62, DistinctRange: 31, UpperBound: upperBound},
		}}

		ub1, _, err := tree.ParseDDate(&evalCtx, "2019-07-02")
		if err != nil {
			t.Fatal(err)
		}

		testData := []testCase{
			{
				span:     "[/2019-07-01 - /2019-07-02]",
				expected: &cat.HistogramBucket{NumEq: 2, NumRange: 2, DistinctRange: 1, UpperBound: ub1},
			},
			{
				span:     "[/2019-07-05 - /2019-08-01]",
				expected: &cat.HistogramBucket{NumEq: 1, NumRange: 54, DistinctRange: 27, UpperBound: upperBound},
			},
		}

		runTest(h, testData, types.DateFamily)
	})

	t.Run("timestamp", func(t *testing.T) {
		upperBound, _, err := tree.ParseDTimestamp(&evalCtx, "2019-08-01 12:00:00.000000", time.Microsecond)
		if err != nil {
			t.Fatal(err)
		}
		lowerBound, _, err := tree.ParseDTimestamp(&evalCtx, "2019-07-01 12:00:00.000000", time.Microsecond)
		if err != nil {
			t.Fatal(err)
		}
		h := &Histogram{evalCtx: &evalCtx, col: col, buckets: []cat.HistogramBucket{
			{NumEq: 0, NumRange: 0, DistinctRange: 0, UpperBound: getPrevUpperBound(lowerBound)},
			{NumEq: 1, NumRange: 62, DistinctRange: 31, UpperBound: upperBound},
		}}

		ub1, _, err := tree.ParseDTimestamp(&evalCtx, "2019-07-02 00:00:00.000000", time.Microsecond)
		if err != nil {
			t.Fatal(err)
		}

		testData := []testCase{
			{
				span:     "[/2019-07-01 12:00:00.000000 - /2019-07-02 00:00:00.000000)",
				expected: &cat.HistogramBucket{NumEq: 0, NumRange: 1, DistinctRange: 0.5, UpperBound: ub1},
			},
			{
				span:     "[/2019-07-05 12:00:00.000000 - /2019-08-01 12:00:00.000000)",
				expected: &cat.HistogramBucket{NumEq: 0, NumRange: 54, DistinctRange: 27, UpperBound: upperBound},
			},
		}

		runTest(h, testData, types.TimestampFamily)
	})

	t.Run("string", func(t *testing.T) {
		h := &Histogram{evalCtx: &evalCtx, col: col, buckets: []cat.HistogramBucket{
			{NumEq: 0, NumRange: 0, DistinctRange: 0, UpperBound: getPrevUpperBound(tree.NewDString("baq"))},
			{NumEq: 5, NumRange: 10, DistinctRange: 10, UpperBound: tree.NewDString("foo")},
		}}
		testData := []testCase{
			{
				span:     "[/bar - /baz]",
				expected: &cat.HistogramBucket{NumEq: 0, NumRange: 5, DistinctRange: 5, UpperBound: tree.NewDString("baz")},
			},
			{
				span:     "[/baz - /foo]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 5, DistinctRange: 5, UpperBound: tree.NewDString("foo")},
			},
		}

		runTest(h, testData, types.StringFamily)
	})

}

// makeDescSpan makes an equivalent version of s in which the start and end
// keys are swapped.
func makeDescSpan(s *constraint.Span) constraint.Span {
	var desc constraint.Span
	desc.Init(s.EndKey(), s.EndBoundary(), s.StartKey(), s.StartBoundary())
	return desc
}

// makeDescConstraint makes an equivalent version of c in which all columns
// are descending.
func makeDescConstraint(c *constraint.Constraint) constraint.Constraint {
	var desc constraint.Constraint

	// Negate all the columns.
	cols := make([]opt.OrderingColumn, c.Columns.Count())
	for i := range cols {
		cols[i] = -c.Columns.Get(i)
	}
	desc.Columns.Init(cols)

	// Add all the spans in reverse order, with their start and end keys
	// swapped.
	desc.Spans.Alloc(c.Spans.Count())
	for i := c.Spans.Count() - 1; i >= 0; i-- {
		s := makeDescSpan(c.Spans.Get(i))
		desc.Spans.Append(&s)
	}

	return desc
}

// Round all values to two decimal places.
func roundVal(val float64) float64 {
	return math.Round(val*100.0) / 100.0
}

func roundBucket(b *cat.HistogramBucket) {
	b.NumRange = roundVal(b.NumRange)
	b.NumEq = roundVal(b.NumEq)
	b.DistinctRange = roundVal(b.DistinctRange)
}

func roundHistogram(h *Histogram) {
	for i := range h.buckets {
		roundBucket(&h.buckets[i])
	}
}
