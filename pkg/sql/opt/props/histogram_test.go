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
		h *Histogram, span *constraint.Span, desc bool, colOffset int,
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
		b := getFilteredBucket(&iter, &keyCtx, span, colOffset)
		roundBucket(b)
		return b, nil
	}

	runTest := func(h *Histogram, testData []testCase, colOffset int, typs ...types.Family) {
		for _, testCase := range testData {
			span := constraint.ParseSpan(&evalCtx, testCase.span, typs...)
			ascAndDesc := []constraint.Span{span, makeDescSpan(&span)}

			// Make sure all test cases work with both ascending and descending columns.
			for i, span := range ascAndDesc {
				actual, err := runTestCase(h, &span, i == 1 /* desc */, colOffset)
				if err != nil && !testCase.isError {
					t.Fatalf("for span %s got error %v", testCase.span, err)
				} else if err == nil {
					if testCase.isError {
						t.Fatalf("for span %s expected an error", testCase.span)
					}
					if !reflect.DeepEqual(testCase.expected, actual) {
						t.Errorf("for span %s exected %v but found %v", testCase.span, testCase.expected, actual)
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

		runTest(h, testData, 0 /* colOffset */, types.IntFamily)
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

		runTest(h, testData, 0 /* colOffset */, types.FloatFamily)
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

		runTest(h, testData, 0 /* colOffset */, types.DecimalFamily)
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

		runTest(h, testData, 0 /* colOffset */, types.DateFamily)
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

		runTest(h, testData, 0 /* colOffset */, types.TimestampFamily)
	})

	t.Run("time", func(t *testing.T) {
		upperBound, _, err := tree.ParseDTime(&evalCtx, "05:00:00", time.Microsecond)
		if err != nil {
			t.Fatal(err)
		}
		lowerBound, _, err := tree.ParseDTime(&evalCtx, "04:00:00", time.Microsecond)
		if err != nil {
			t.Fatal(err)
		}
		h := &Histogram{evalCtx: &evalCtx, col: col, buckets: []cat.HistogramBucket{
			{NumEq: 0, NumRange: 0, DistinctRange: 0, UpperBound: getPrevUpperBound(lowerBound)},
			{NumEq: 1, NumRange: 62, DistinctRange: 31, UpperBound: upperBound},
		}}

		ub1, _, err := tree.ParseDTime(&evalCtx, "04:15:00", time.Microsecond)
		if err != nil {
			t.Fatal(err)
		}

		testData := []testCase{
			{
				span:     "[/04:00:00 - /04:15:00]",
				expected: &cat.HistogramBucket{NumEq: 0, NumRange: 15.5, DistinctRange: 7.75, UpperBound: ub1},
			},
			{
				span:     "[/04:30:00 - /05:00:00]",
				expected: &cat.HistogramBucket{NumEq: 1, NumRange: 31, DistinctRange: 15.5, UpperBound: upperBound},
			},
		}

		runTest(h, testData, 0 /* colOffset */, types.TimeFamily)
	})

	t.Run("timetz", func(t *testing.T) {
		upperBound1, _, err := tree.ParseDTimeTZ(&evalCtx, "05:00:00", time.Microsecond)
		if err != nil {
			t.Fatal(err)
		}
		lowerBound1, _, err := tree.ParseDTimeTZ(&evalCtx, "04:00:00", time.Microsecond)
		if err != nil {
			t.Fatal(err)
		}
		h1 := &Histogram{evalCtx: &evalCtx, col: col, buckets: []cat.HistogramBucket{
			{NumEq: 0, NumRange: 0, DistinctRange: 0, UpperBound: getPrevUpperBound(lowerBound1)},
			{NumEq: 1, NumRange: 62, DistinctRange: 31, UpperBound: upperBound1},
		}}

		ub1, _, err := tree.ParseDTimeTZ(&evalCtx, "04:15:00", time.Microsecond)
		if err != nil {
			t.Fatal(err)
		}

		testData1 := []testCase{
			{
				span:     "[/04:00:00 - /04:15:00]",
				expected: &cat.HistogramBucket{NumEq: 0, NumRange: 15.5, DistinctRange: 7.75, UpperBound: ub1},
			},
			{
				span:     "[/04:30:00 - /05:00:00]",
				expected: &cat.HistogramBucket{NumEq: 1, NumRange: 31, DistinctRange: 15.5, UpperBound: upperBound1},
			},
			{
				span:     "[/06:30:00+02:00:00 - /05:00:00]",
				expected: &cat.HistogramBucket{NumEq: 1, NumRange: 31, DistinctRange: 15.5, UpperBound: upperBound1},
			},
			{
				span:     "[/08:30:00+04:00:00 - /05:00:00]",
				expected: &cat.HistogramBucket{NumEq: 1, NumRange: 31, DistinctRange: 15.5, UpperBound: upperBound1},
			},
		}

		// This test distinguishes between values that have the same time but
		// different offsets.
		upperBound2, _, err := tree.ParseDTimeTZ(&evalCtx, "05:00:00.000001", time.Microsecond)
		if err != nil {
			t.Fatal(err)
		}
		h2 := &Histogram{evalCtx: &evalCtx, col: col, buckets: []cat.HistogramBucket{
			{NumEq: 0, NumRange: 0, DistinctRange: 0, UpperBound: upperBound1},
			{NumEq: 1, NumRange: 10000, DistinctRange: 1000, UpperBound: upperBound2},
		}}

		ub2, _, err := tree.ParseDTimeTZ(&evalCtx, "20:59:00.000001+15:59:00", time.Microsecond)
		if err != nil {
			t.Fatal(err)
		}

		testData2 := []testCase{
			{
				span:     "[/05:00:00.000001 - /05:00:00.000001]",
				expected: &cat.HistogramBucket{NumEq: 1, NumRange: 0, DistinctRange: 0, UpperBound: upperBound2},
			},
			{
				span:     "[/07:00:00.000001+02:00:00 - /05:00:00.000001]",
				expected: &cat.HistogramBucket{NumEq: 1, NumRange: 625.65, DistinctRange: 62.57, UpperBound: upperBound2},
			},
			{
				span:     "[/09:00:00.000001+04:00:00 - /05:00:00.000001]",
				expected: &cat.HistogramBucket{NumEq: 1, NumRange: 1251.3, DistinctRange: 125.13, UpperBound: upperBound2},
			},
			{
				span:     "[/04:59:59-00:00:01 - /20:59:00.000001+15:59:00]",
				expected: &cat.HistogramBucket{NumEq: 0, NumRange: 5000, DistinctRange: 500, UpperBound: ub2},
			},
			{
				span:     "[/20:59:00.000001+15:59:00 - /05:00:00.000001]",
				expected: &cat.HistogramBucket{NumEq: 1, NumRange: 5000, DistinctRange: 500, UpperBound: upperBound2},
			},
			{
				span:     "[/04:59:58-00:00:02 - /05:00:00.000001]",
				expected: &cat.HistogramBucket{NumEq: 1, NumRange: 9999.91, DistinctRange: 999.99, UpperBound: upperBound2},
			},
		}

		runTest(h1, testData1, 0 /* colOffset */, types.TimeTZFamily)
		runTest(h2, testData2, 0 /* colOffset */, types.TimeTZFamily)
	})

	t.Run("string-bytes", func(t *testing.T) {
		typesToTest := []struct {
			family        types.Family
			createDatumFn func(string) tree.Datum
		}{
			{
				family:        types.StringFamily,
				createDatumFn: func(s string) tree.Datum { return tree.NewDString(s) },
			},
			{
				family:        types.BytesFamily,
				createDatumFn: func(s string) tree.Datum { return tree.NewDBytes(tree.DBytes(s)) },
			},
		}
		for _, typ := range typesToTest {
			h1 := &Histogram{evalCtx: &evalCtx, col: col, buckets: []cat.HistogramBucket{
				{NumEq: 0, NumRange: 0, DistinctRange: 0, UpperBound: getPrevUpperBound(typ.createDatumFn("bear"))},
				{NumEq: 5, NumRange: 10, DistinctRange: 10, UpperBound: typ.createDatumFn("bobcat")},
			}}
			h2 := &Histogram{evalCtx: &evalCtx, col: col, buckets: []cat.HistogramBucket{
				{NumEq: 0, NumRange: 0, DistinctRange: 0, UpperBound: getPrevUpperBound(typ.createDatumFn("a"))},
				{NumEq: 5, NumRange: 10, DistinctRange: 10, UpperBound: typ.createDatumFn("c")},
			}}
			h3 := &Histogram{evalCtx: &evalCtx, col: col, buckets: []cat.HistogramBucket{
				{NumEq: 0, NumRange: 0, DistinctRange: 0, UpperBound: getPrevUpperBound(typ.createDatumFn("aaaaaaaaaaaa"))},
				{NumEq: 5, NumRange: 10, DistinctRange: 10, UpperBound: typ.createDatumFn("cccccccccccc")},
			}}

			t1 := []testCase{
				{
					span:     "[/bluejay - /boar]",
					expected: &cat.HistogramBucket{NumEq: 0, NumRange: 2.92, DistinctRange: 2.92, UpperBound: typ.createDatumFn("boar")},
				},
				{
					span:     "[/beer - /bobcat]",
					expected: &cat.HistogramBucket{NumEq: 5, NumRange: 9.98, DistinctRange: 9.98, UpperBound: typ.createDatumFn("bobcat")},
				},
			}

			t2 := []testCase{
				// Within the CRDB encoding, all null bytes are followed by an escape byte,
				// (255) which are left in for the rangeAfter calculations. For this
				// reason, the resulting NumRange is slightly lower than expected at 4.99
				// instead of 5.
				{
					span:     "[/a\x00 - /b]",
					expected: &cat.HistogramBucket{NumEq: 0, NumRange: 4.99, DistinctRange: 4.99, UpperBound: typ.createDatumFn("b")},
				},
				{
					span:     "[/as - /b]",
					expected: &cat.HistogramBucket{NumEq: 0, NumRange: 2.76, DistinctRange: 2.76, UpperBound: typ.createDatumFn("b")},
				},
				{
					span:     "[/as - /c]",
					expected: &cat.HistogramBucket{NumEq: 5, NumRange: 7.77, DistinctRange: 7.77, UpperBound: typ.createDatumFn("c")},
				},
				{
					span:     "[/bs - /c]",
					expected: &cat.HistogramBucket{NumEq: 5, NumRange: 2.76, DistinctRange: 2.76, UpperBound: typ.createDatumFn("c")},
				},
			}

			// The initial 8 bytes for lowerBound and upperBound of the span is the same.
			// Hence, the resulting NumRange/DistinctRange should be 0, as rangeAfter
			// only considers the first 8 bytes of the bounds.
			t3 := []testCase{
				{
					span:     "[/aaaaaaaabbbb - /aaaaaaaacccc]",
					expected: &cat.HistogramBucket{NumEq: 0, NumRange: 0, DistinctRange: 0, UpperBound: typ.createDatumFn("aaaaaaaacccc")},
				},
			}

			runTest(h1, t1, 0 /* colOffset */, typ.family)
			runTest(h2, t2, 0 /* colOffset */, typ.family)
			runTest(h3, t3, 0 /* colOffset */, typ.family)
		}
	})

	t.Run("uuid", func(t *testing.T) {
		l1, err := tree.ParseDUuidFromString("2189ad07-52f2-4d60-83e8-4a8347fef718")
		if err != nil {
			t.Fatal(err)
		}
		u1, err := tree.ParseDUuidFromString("4589ad07-52f2-4d60-83e8-4a8347fef718")
		if err != nil {
			t.Fatal(err)
		}
		l2, err := tree.ParseDUuidFromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
		if err != nil {
			t.Fatal(err)
		}
		u2, err := tree.ParseDUuidFromString("cccccccc-cccc-cccc-cccc-cccccccccccc")
		if err != nil {
			t.Fatal(err)
		}
		u3, err := tree.ParseDUuidFromString("4289ad07-52f2-4d60-83e8-4a8347fef718")
		if err != nil {
			t.Fatal(err)
		}

		h1 := &Histogram{evalCtx: &evalCtx, col: col, buckets: []cat.HistogramBucket{
			{NumEq: 0, NumRange: 0, DistinctRange: 0, UpperBound: getPrevUpperBound(l1)},
			{NumEq: 5, NumRange: 10, DistinctRange: 10, UpperBound: u1},
		}}
		h2 := &Histogram{evalCtx: &evalCtx, col: col, buckets: []cat.HistogramBucket{
			{NumEq: 0, NumRange: 0, DistinctRange: 0, UpperBound: getPrevUpperBound(l2)},
			{NumEq: 5, NumRange: 10, DistinctRange: 10, UpperBound: u2},
		}}

		t1 := []testCase{
			{
				span:     "[/3189ad07-52f2-4d60-83e8-4a8347fef718 - /4289ad07-52f2-4d60-83e8-4a8347fef718]",
				expected: &cat.HistogramBucket{NumEq: 0, NumRange: 4.72, DistinctRange: 4.72, UpperBound: u3},
			},
			{
				span:     "[/3189ad07-52f2-4d60-83e8-4a8347fef718 - /4589ad07-52f2-4d60-83e8-4a8347fef718]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 5.56, DistinctRange: 5.56, UpperBound: u1},
			},
		}

		t2 := []testCase{
			{
				span:     "[/bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb - /cccccccc-cccc-cccc-cccc-cccccccccccc]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 5, DistinctRange: 5, UpperBound: u2},
			},
			{
				span:     "[/b3333333-aaaa-aaaa-aaaa-aaaaaaaaaaaa - /cccccccc-cccc-cccc-cccc-cccccccccccc]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 7.5, DistinctRange: 7.5, UpperBound: u2},
			},
		}

		runTest(h1, t1, 0 /* colOffset */, types.UuidFamily)
		runTest(h2, t2, 0 /* colOffset */, types.UuidFamily)
	})

	t.Run("inet", func(t *testing.T) {
		l1, err := tree.ParseDIPAddrFromINetString("0.0.0.0")
		if err != nil {
			t.Fatal(err)
		}
		u1, err := tree.ParseDIPAddrFromINetString("255.255.255.255")
		if err != nil {
			t.Fatal(err)
		}
		l2, err := tree.ParseDIPAddrFromINetString("0:0:0:0:0:0:0:0")
		if err != nil {
			t.Fatal(err)
		}
		u2, err := tree.ParseDIPAddrFromINetString("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff")
		if err != nil {
			t.Fatal(err)
		}
		u3, err := tree.ParseDIPAddrFromINetString("0:0:0:0:ffff:ffff:ffff:ffff")
		if err != nil {
			t.Fatal(err)
		}
		h1 := &Histogram{evalCtx: &evalCtx, col: col, buckets: []cat.HistogramBucket{
			{NumEq: 0, NumRange: 0, DistinctRange: 0, UpperBound: getPrevUpperBound(l1)},
			{NumEq: 5, NumRange: 10, DistinctRange: 10, UpperBound: u1},
		}}
		h2 := &Histogram{evalCtx: &evalCtx, col: col, buckets: []cat.HistogramBucket{
			{NumEq: 0, NumRange: 0, DistinctRange: 0, UpperBound: getPrevUpperBound(l2)},
			{NumEq: 5, NumRange: 10, DistinctRange: 10, UpperBound: u2},
		}}
		h3 := &Histogram{evalCtx: &evalCtx, col: col, buckets: []cat.HistogramBucket{
			{NumEq: 0, NumRange: 0, DistinctRange: 0, UpperBound: getPrevUpperBound(l1)},
			{NumEq: 5, NumRange: 10, DistinctRange: 10, UpperBound: u2},
		}}

		t1 := []testCase{
			{
				span:     "[/128.128.128.128 - /255.255.255.255]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 5, DistinctRange: 5, UpperBound: u1},
			},
			{
				span:     "[/63.63.63.63 - /255.255.255.255]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 7.56, DistinctRange: 7.56, UpperBound: u1},
			},
		}
		t2 := []testCase{
			{
				span:     "[/7777:7777:7777:7777:7777:7777:7777:7777 - /ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 5.35, DistinctRange: 5.35, UpperBound: u2},
			},
			{
				span:     "[/3333:3333:3333:3333:3333:3333:3333:3333 - /ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 8.03, DistinctRange: 8.03, UpperBound: u2},
			},
		}
		t3 := []testCase{
			// Due to the large address space of IPV6, when the lower bound (IPV4) is
			// updated, but the upper bound (IPV6) stays the same, there is no
			// difference in the range estimate.
			{
				span:     "[/255.255.255.255 - /ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 10, DistinctRange: 10, UpperBound: u2},
			},
			// The dominant factor in the range difference between IPV4 & IPV6 is the
			// family-tag/mask which make up the first 2 bytes of the CRDB encoding.
			// Hence, there is still a very minor difference in NumRange even when the
			// upperbound is updated.
			{
				span:     "[/0.0.0.0 - /0000:0000:0000:0000:ffff:ffff:ffff:ffff]",
				expected: &cat.HistogramBucket{NumEq: 0, NumRange: 9.92, DistinctRange: 9.92, UpperBound: u3},
			},
		}

		runTest(h1, t1, 0 /* colOffset */, types.INetFamily)
		runTest(h2, t2, 0 /* colOffset */, types.INetFamily)
		runTest(h3, t3, 0 /* colOffset */, types.INetFamily)
	})

	t.Run("multi-col", func(t *testing.T) {
		h1 := &Histogram{evalCtx: &evalCtx, col: col, buckets: []cat.HistogramBucket{
			{NumEq: 0, NumRange: 0, DistinctRange: 0, UpperBound: getPrevUpperBound(tree.NewDInt(0))},
			{NumEq: 5, NumRange: 10, DistinctRange: 10, UpperBound: tree.NewDInt(10)},
		}}
		t1 := []testCase{
			{
				span:     "[/0 - /5/foo)",
				expected: &cat.HistogramBucket{NumEq: 1, NumRange: 5, DistinctRange: 5, UpperBound: tree.NewDInt(5)},
			},
			{
				span:     "(/2/foo - /9]",
				expected: &cat.HistogramBucket{NumEq: 1, NumRange: 7, DistinctRange: 7, UpperBound: tree.NewDInt(9)},
			},
			{
				span:     "[/2 - /10/foo)",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 8, DistinctRange: 8, UpperBound: tree.NewDInt(10)},
			},
			{
				span:     "[/10/ - /10/foo)",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDInt(10)},
			},
			{
				span:     "(/10/foo - /10]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDInt(10)},
			},
			{
				span:     "[/10/ - /10/foo]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDInt(10)},
			},
			{
				span:     "[/10/foo - /10]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDInt(10)},
			},
			{
				span:     "[/10/bar - /10/foo)",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDInt(10)},
			},
			{
				span:     "(/10/bar - /10/foo]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDInt(10)},
			},
			{
				span:     "[/10/bar - /10/foo]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDInt(10)},
			},
			{
				span:     "(/10/bar - /10/foo)",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDInt(10)},
			},
		}

		h2 := &Histogram{evalCtx: &evalCtx, col: col, buckets: []cat.HistogramBucket{
			{NumEq: 0, NumRange: 0, DistinctRange: 0, UpperBound: getPrevUpperBound(tree.NewDString("a"))},
			{NumEq: 5, NumRange: 10, DistinctRange: 10, UpperBound: tree.NewDString("c")},
		}}
		t2 := []testCase{
			{
				span:     "[/0/a\x00 - /0/b/foo)",
				expected: &cat.HistogramBucket{NumEq: 0, NumRange: 4.99, DistinctRange: 4.99, UpperBound: tree.NewDString("b")},
			},
			{
				span:     "(/0/a\x00/foo - /0/b]",
				expected: &cat.HistogramBucket{NumEq: 0, NumRange: 4.99, DistinctRange: 4.99, UpperBound: tree.NewDString("b")},
			},
			{
				span:     "(/0/a\x00/foo - /0/c)",
				expected: &cat.HistogramBucket{NumEq: 0, NumRange: 10, DistinctRange: 10, UpperBound: tree.NewDString("c")},
			},
			{
				span:     "(/0/a\x00/foo - /0/c]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 10, DistinctRange: 10, UpperBound: tree.NewDString("c")},
			},
			{
				span:     "[/0/a\x00 - /0/c/foo)",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 10, DistinctRange: 10, UpperBound: tree.NewDString("c")},
			},
			{
				span:     "[/0/c/ - /0/c/foo)",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDString("c")},
			},
			{
				span:     "(/0/c/foo - /0/c]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDString("c")},
			},
			{
				span:     "[/0/c/ - /0/c/foo]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDString("c")},
			},
			{
				span:     "[/0/c/foo - /0/c]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDString("c")},
			},
			{
				span:     "[/0/c/bar - /0/c/foo)",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDString("c")},
			},
			{
				span:     "(/0/c/bar - /0/c/foo]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDString("c")},
			},
			{
				span:     "[/0/c/bar - /0/c/foo]",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDString("c")},
			},
			{
				span:     "(/0/c/bar - /0/c/foo)",
				expected: &cat.HistogramBucket{NumEq: 5, NumRange: 0, DistinctRange: 0, UpperBound: tree.NewDString("c")},
			},
		}

		runTest(h1, t1, 0 /* colOffset */)
		runTest(h2, t2, 1 /* colOffset */)
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
