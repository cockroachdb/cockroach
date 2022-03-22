// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stats

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

func TestEquiDepthHistogram(t *testing.T) {
	type expBucket struct {
		upper        int
		numEq        int64
		numLess      int64
		distinctLess float64
	}
	testCases := []struct {
		samples       []int64
		numRows       int64
		distinctCount int64
		maxBuckets    int
		buckets       []expBucket
	}{
		{
			samples:       []int64{1, 2, 4, 5, 5, 9},
			numRows:       6,
			distinctCount: 5,
			maxBuckets:    3,
			buckets: []expBucket{
				{
					// Bucket contains 1.
					upper: 1, numEq: 1, numLess: 0, distinctLess: 0,
				},
				{
					// Bucket contains 2, 4.
					upper: 4, numEq: 1, numLess: 1, distinctLess: 0.73,
				},
				{
					// Bucket contains 5, 5, 9.
					upper: 9, numEq: 1, numLess: 2, distinctLess: 1.27,
				},
			},
		},
		{
			samples:       []int64{1, 1, 1, 1, 2, 2},
			numRows:       6,
			distinctCount: 2,
			maxBuckets:    2,
			buckets: []expBucket{
				{
					// Bucket contains 1, 1, 1, 1.
					upper: 1, numEq: 4, numLess: 0, distinctLess: 0,
				},
				{
					// Bucket contains 2, 2.
					upper: 2, numEq: 2, numLess: 0, distinctLess: 0,
				},
			},
		},
		{
			samples:       []int64{1, 1, 1, 1, 2, 2},
			numRows:       6,
			distinctCount: 2,
			maxBuckets:    3,
			buckets: []expBucket{
				{
					// Bucket contains 1, 1, 1, 1.
					upper: 1, numEq: 4, numLess: 0, distinctLess: 0,
				},
				{
					// Bucket contains 2, 2.
					upper: 2, numEq: 2, numLess: 0, distinctLess: 0,
				},
			},
		},
		{
			samples:       []int64{1, 1, 2, 2, 2, 2},
			numRows:       6,
			distinctCount: 2,
			maxBuckets:    2,
			buckets: []expBucket{
				{
					// Bucket contains 1, 1.
					upper: 1, numEq: 2, numLess: 0, distinctLess: 0,
				},
				{
					// Bucket contains 2, 2, 2, 2.
					upper: 2, numEq: 4, numLess: 0, distinctLess: 0,
				},
			},
		},
		{
			samples:       []int64{1, 1, 2, 2, 2, 2},
			numRows:       6,
			distinctCount: 2,
			maxBuckets:    3,
			buckets: []expBucket{
				{
					// Bucket contains 1, 1.
					upper: 1, numEq: 2, numLess: 0, distinctLess: 0,
				},
				{
					// Bucket contains 2, 2, 2, 2.
					upper: 2, numEq: 4, numLess: 0, distinctLess: 0,
				},
			},
		},
		{
			samples:       []int64{1, 1, 1, 1, 1, 1},
			numRows:       600,
			distinctCount: 1,
			maxBuckets:    10,
			buckets: []expBucket{
				{
					// Bucket contains everything.
					upper: 1, numEq: 600, numLess: 0, distinctLess: 0,
				},
			},
		},
		{
			samples:       []int64{1, 2, 3, 4},
			numRows:       4000,
			distinctCount: 4,
			maxBuckets:    3,
			buckets: []expBucket{
				{
					// Bucket contains 1.
					upper: 1, numEq: 1000, numLess: 0, distinctLess: 0,
				},
				{
					// Bucket contains 2.
					upper: 2, numEq: 1000, numLess: 0, distinctLess: 0,
				},
				{
					// Bucket contains 3, 4.
					upper: 4, numEq: 1000, numLess: 1000, distinctLess: 1,
				},
			},
		},
		{
			samples:       []int64{-9222292034315889200, -9130100296576294525, -9042492057500701159},
			numRows:       3000,
			distinctCount: 300,
			maxBuckets:    2,
			buckets: []expBucket{
				{
					// Bucket contains -9222292034315889200.
					upper: -9222292034315889200, numEq: 1000, numLess: 0, distinctLess: 0,
				},
				{
					// Bucket contains -9130100296576294525, -9042492057500701159.
					upper: -9042492057500701159, numEq: 1000, numLess: 1000, distinctLess: 298,
				},
			},
		},
		{
			samples:       []int64{1, 10},
			numRows:       3000,
			distinctCount: 300,
			maxBuckets:    3,
			buckets: []expBucket{
				{
					upper: math.MinInt64, numEq: 0, numLess: 0, distinctLess: 0,
				},
				{
					// Bucket contains 1.
					upper: 1, numEq: 1351, numLess: 145, distinctLess: 145,
				},
				{
					// Bucket contains 10.
					upper: 10, numEq: 1351, numLess: 8, distinctLess: 8,
				},
				{
					upper: math.MaxInt64, numEq: 0, numLess: 145, distinctLess: 145,
				},
			},
		},
		{
			samples:       []int64{1, 10},
			numRows:       3000,
			distinctCount: 3000,
			maxBuckets:    3,
			buckets: []expBucket{
				{
					upper: math.MinInt64, numEq: 0, numLess: 0, distinctLess: 0,
				},
				{
					// Bucket contains 1.
					upper: 1, numEq: 1, numLess: 1495, distinctLess: 1495,
				},
				{
					// Bucket contains 10.
					upper: 10, numEq: 1, numLess: 8, distinctLess: 8,
				},
				{
					upper: math.MaxInt64, numEq: 0, numLess: 1495, distinctLess: 1495,
				},
			},
		},
		{
			// Test where all values in the table are null.
			samples:       []int64{},
			numRows:       3000,
			distinctCount: 1,
			maxBuckets:    2,
			buckets:       []expBucket{},
		},
	}

	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			samples := make(tree.Datums, len(tc.samples))
			perm := rand.Perm(len(samples))
			for i := range samples {
				// Randomly permute the samples.
				val := tc.samples[perm[i]]

				samples[i] = tree.NewDInt(tree.DInt(val))
			}

			h, _, err := EquiDepthHistogram(
				evalCtx, types.Int, samples, tc.numRows, tc.distinctCount, tc.maxBuckets,
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(h.Buckets) != len(tc.buckets) {
				t.Fatalf("Invalid number of buckets %d, expected %d", len(h.Buckets), len(tc.buckets))
			}
			for i, b := range h.Buckets {
				_, val, err := encoding.DecodeVarintAscending(b.UpperBound)
				if err != nil {
					t.Fatal(err)
				}
				exp := tc.buckets[i]
				if val != int64(exp.upper) {
					t.Errorf("bucket %d: incorrect boundary %d, expected %d", i, val, exp.upper)
				}
				if b.NumEq != exp.numEq {
					t.Errorf("bucket %d: incorrect EqRows %d, expected %d", i, b.NumEq, exp.numEq)
				}
				if b.NumRange != exp.numLess {
					t.Errorf("bucket %d: incorrect RangeRows %d, expected %d", i, b.NumRange, exp.numLess)
				}
				// Round to two decimal places.
				distinctRange := math.Round(b.DistinctRange*100.0) / 100.0
				if distinctRange != exp.distinctLess {
					t.Errorf("bucket %d: incorrect DistinctRows %f, expected %f", i, distinctRange, exp.distinctLess)
				}
			}
		})
	}

	t.Run("invalid-numRows", func(t *testing.T) {
		samples := tree.Datums{tree.NewDInt(1), tree.NewDInt(2), tree.NewDInt(3)}
		_, _, err := EquiDepthHistogram(
			evalCtx, types.Int, samples, 2 /* numRows */, 2 /* distinctCount */, 10, /* maxBuckets */
		)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("nulls", func(t *testing.T) {
		samples := tree.Datums{tree.NewDInt(1), tree.NewDInt(2), tree.DNull}
		_, _, err := EquiDepthHistogram(
			evalCtx, types.Int, samples, 100 /* numRows */, 3 /* distinctCount */, 10, /* maxBuckets */
		)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestAdjustCounts(t *testing.T) {
	d := func(val tree.DInt) tree.Datum {
		return tree.NewDInt(val)
	}
	f := func(val float64) tree.Datum {
		return tree.NewDFloat(tree.DFloat(val))
	}
	enums := makeEnums(t)
	uuids := makeUuids(t)

	testData := []struct {
		h             []cat.HistogramBucket
		rowCount      float64
		distinctCount float64
		expected      []cat.HistogramBucket
	}{
		{ // One bucket already matching counts.
			h: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1, DistinctRange: 0, UpperBound: d(1)},
			},
			rowCount:      1,
			distinctCount: 1,
			expected: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1, DistinctRange: 0, UpperBound: d(1)},
			},
		},
		{ // One bucket matching distinctCount but not rowCount.
			h: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1, DistinctRange: 0, UpperBound: d(1)},
			},
			rowCount:      10,
			distinctCount: 1,
			expected: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 10, DistinctRange: 0, UpperBound: d(1)},
			},
		},
		{ // One bucket but two distinct values.
			h: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 4, DistinctRange: 0, UpperBound: d(1)},
			},
			rowCount:      10,
			distinctCount: 2,
			expected: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 0, DistinctRange: 0, UpperBound: d(math.MinInt64)},
				{NumRange: 1, NumEq: 8, DistinctRange: 0.5, UpperBound: d(1)},
				{NumRange: 1, NumEq: 0, DistinctRange: 0.5, UpperBound: d(math.MaxInt64)},
			},
		},
		{ // One bucket with UUIDs.
			h: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1000, DistinctRange: 0, UpperBound: uuids[1]},
			},
			rowCount:      100000,
			distinctCount: 1001,
			expected: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 0, DistinctRange: 0, UpperBound: uuids[0]},
				{NumRange: 25000, NumEq: 50000, DistinctRange: 500, UpperBound: uuids[1]},
				{NumRange: 25000, NumEq: 0, DistinctRange: 500, UpperBound: uuids[len(uuids)-1]},
			},
		},
		{ // One bucket with enums.
			h: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1000, DistinctRange: 0, UpperBound: enums[1]},
			},
			rowCount:      10000,
			distinctCount: 5,
			expected: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 9.96, DistinctRange: 0, UpperBound: enums[0]},
				{NumRange: 0, NumEq: 9960.16, DistinctRange: 0, UpperBound: enums[1]},
				{NumRange: 19.92, NumEq: 9.96, DistinctRange: 2, UpperBound: enums[len(enums)-1]},
			},
		},
		{ // One bucket with bools.
			h: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1000, DistinctRange: 0, UpperBound: tree.DBoolFalse},
			},
			rowCount:      1000000,
			distinctCount: 2,
			expected: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 999001, DistinctRange: 0, UpperBound: tree.DBoolFalse},
				{NumRange: 0, NumEq: 999, DistinctRange: 0, UpperBound: tree.DBoolTrue},
			},
		},
		{ // A different bucket with bools.
			h: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1, DistinctRange: 0, UpperBound: tree.DBoolTrue},
			},
			rowCount:      2,
			distinctCount: 2,
			expected: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1, DistinctRange: 0, UpperBound: tree.DBoolFalse},
				{NumRange: 0, NumEq: 1, DistinctRange: 0, UpperBound: tree.DBoolTrue},
			},
		},
		{ // Two buckets already matching counts.
			h: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1, DistinctRange: 0, UpperBound: d(1)},
				{NumRange: 4, NumEq: 2, DistinctRange: 3, UpperBound: d(10)},
			},
			rowCount:      7,
			distinctCount: 5,
			expected: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1, DistinctRange: 0, UpperBound: d(1)},
				{NumRange: 4, NumEq: 2, DistinctRange: 3, UpperBound: d(10)},
			},
		},
		{ // Two buckets matching distinctCount but not rowCount.
			h: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 4, DistinctRange: 0, UpperBound: d(1)},
				{NumRange: 4, NumEq: 2, DistinctRange: 3, UpperBound: d(10)},
			},
			rowCount:      14,
			distinctCount: 5,
			expected: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 5.6, DistinctRange: 0, UpperBound: d(1)},
				{NumRange: 5.6, NumEq: 2.8, DistinctRange: 3, UpperBound: d(10)},
			},
		},
		{ // Two buckets, matching rowCount but not distinctCount.
			h: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1, DistinctRange: 0, UpperBound: d(1)},
				{NumRange: 4, NumEq: 2, DistinctRange: 3, UpperBound: d(10)},
			},
			rowCount:      7,
			distinctCount: 6,
			expected: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 0.88, DistinctRange: 0, UpperBound: d(1)},
				{NumRange: 4.38, NumEq: 1.75, DistinctRange: 4, UpperBound: d(10)},
			},
		},
		{ // Two buckets, matching neither count.
			h: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1000, DistinctRange: 0, UpperBound: d(1)},
				{NumRange: 4000, NumEq: 2000, DistinctRange: 3, UpperBound: d(10)},
			},
			rowCount:      6000,
			distinctCount: 2,
			expected: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 2000, DistinctRange: 0, UpperBound: d(1)},
				{NumRange: 0, NumEq: 4000, DistinctRange: 0, UpperBound: d(10)},
			},
		},
		{ // Two buckets, more distinct values than the range can hold.
			h: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1, DistinctRange: 0, UpperBound: d(1)},
				{NumRange: 4, NumEq: 2, DistinctRange: 3, UpperBound: d(10)},
			},
			rowCount:      20,
			distinctCount: 19,
			expected: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 0, DistinctRange: 0, UpperBound: d(math.MinInt64)},
				{NumRange: 4.29, NumEq: 0.95, DistinctRange: 4.5, UpperBound: d(1)},
				{NumRange: 8.57, NumEq: 1.9, DistinctRange: 8, UpperBound: d(10)},
				{NumRange: 4.29, NumEq: 0, DistinctRange: 4.5, UpperBound: d(math.MaxInt64)},
			},
		},
		{ // Two buckets with floats.
			h: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1, DistinctRange: 0, UpperBound: f(1)},
				{NumRange: 4, NumEq: 2, DistinctRange: 3, UpperBound: f(10)},
			},
			rowCount:      20,
			distinctCount: 19,
			expected: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 0.95, DistinctRange: 0, UpperBound: f(1)},
				{NumRange: 17.14, NumEq: 1.9, DistinctRange: 17, UpperBound: f(10)},
			},
		},
		{ // Two buckets with enums.
			h: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 2000, DistinctRange: 0, UpperBound: enums[1]},
				{NumRange: 0, NumEq: 3000, DistinctRange: 0, UpperBound: enums[3]},
			},
			rowCount:      5000,
			distinctCount: 5,
			expected: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1, DistinctRange: 0, UpperBound: enums[0]},
				{NumRange: 0, NumEq: 1998.8, DistinctRange: 0, UpperBound: enums[1]},
				{NumRange: 1, NumEq: 2998.2, DistinctRange: 1, UpperBound: enums[3]},
				{NumRange: 0, NumEq: 1, DistinctRange: 0, UpperBound: enums[len(enums)-1]},
			},
		},
		{ // Three buckets with enums.
			h: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 3300, DistinctRange: 0, UpperBound: enums[0]},
				{NumRange: 0, NumEq: 3400, DistinctRange: 0, UpperBound: enums[1]},
				{NumRange: 0, NumEq: 3300, DistinctRange: 0, UpperBound: enums[2]},
			},
			rowCount:      10000,
			distinctCount: 5,
			expected: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 3299.34, DistinctRange: 0, UpperBound: enums[0]},
				{NumRange: 0, NumEq: 3399.32, DistinctRange: 0, UpperBound: enums[1]},
				{NumRange: 0, NumEq: 3299.34, DistinctRange: 0, UpperBound: enums[2]},
				{NumRange: 1, NumEq: 1, DistinctRange: 1, UpperBound: enums[len(enums)-1]},
			},
		},
		{ // Large number of rows and distinct values.
			h: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1, DistinctRange: 0, UpperBound: d(1)},
				{NumRange: 5, NumEq: 100, DistinctRange: 2, UpperBound: d(10)},
				{NumRange: 5, NumEq: 100, DistinctRange: 5, UpperBound: d(1000)},
			},
			rowCount:      10000,
			distinctCount: 900,
			expected: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 9.08, DistinctRange: 0, UpperBound: d(1)},
				{NumRange: 94.4, NumEq: 908.27, DistinctRange: 7.39, UpperBound: d(10)},
				{NumRange: 8079.98, NumEq: 908.27, DistinctRange: 889.61, UpperBound: d(1000)},
			},
		},
		{ // Large number of rows and distinct values with floats.
			h: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 1, DistinctRange: 0, UpperBound: f(1)},
				{NumRange: 5, NumEq: 1000, DistinctRange: 2, UpperBound: f(10)},
				{NumRange: 5, NumEq: 1000, DistinctRange: 5, UpperBound: f(1000)},
			},
			rowCount:      10000,
			distinctCount: 900,
			expected: []cat.HistogramBucket{
				{NumRange: 0, NumEq: 3.45, DistinctRange: 0, UpperBound: f(1)},
				{NumRange: 1551.19, NumEq: 3447.09, DistinctRange: 447, UpperBound: f(10)},
				{NumRange: 1551.19, NumEq: 3447.09, DistinctRange: 450, UpperBound: f(1000)},
			},
		},
	}

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	for i, tc := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			actual := histogram{buckets: make([]cat.HistogramBucket, len(tc.h))}
			copy(actual.buckets, tc.h)
			actual.adjustCounts(&evalCtx, tc.rowCount, tc.distinctCount)
			roundHistogram(&actual)
			if !reflect.DeepEqual(actual.buckets, tc.expected) {
				t.Fatalf("expected %v but found %v", tc.expected, actual.buckets)
			}
		})
	}

	t.Run("random", func(t *testing.T) {
		// randHist returns a random histogram with anywhere from 1-200 buckets.
		randHist := func() histogram {
			numBuckets := rand.Intn(200) + 1
			buckets := make([]cat.HistogramBucket, numBuckets)
			ub := rand.Intn(100000000)
			// Half the time, make it negative.
			if rand.Intn(2) == 0 {
				ub = -ub
			}
			buckets[0].UpperBound = tree.NewDInt(tree.DInt(ub))
			buckets[0].NumEq = float64(rand.Intn(1000)) + 1
			for i := 1; i < len(buckets); i++ {
				inc := rand.Intn(1000) + 1
				ub += inc
				buckets[i].DistinctRange = float64(rand.Intn(inc))
				buckets[i].NumRange = buckets[i].DistinctRange * (1 + rand.Float64())
				buckets[i].NumEq = float64(rand.Intn(1000)) + 1
				buckets[i].UpperBound = tree.NewDInt(tree.DInt(ub))
			}
			// Half the time, use floats instead of ints.
			if rand.Intn(2) == 0 {
				for i := range buckets {
					buckets[i].UpperBound = tree.NewDFloat(tree.DFloat(*buckets[i].UpperBound.(*tree.DInt)))
				}
			}
			return histogram{buckets: buckets}
		}

		// Create 100 random histograms, and check that we can correctly adjust the
		// counts to match a random row count and distinct count.
		for trial := 0; trial < 100; trial++ {
			h := randHist()
			rowCount := rand.Intn(1000000)
			distinctCount := rand.Intn(rowCount + 1)

			// We should have at least as many rows and distinct values as there are
			// buckets in the histogram.
			rowCount = max(rowCount, len(h.buckets))
			distinctCount = max(distinctCount, len(h.buckets))

			// Adjust the counts in the histogram to match the provided counts.
			h.adjustCounts(&evalCtx, float64(rowCount), float64(distinctCount))

			// Check that the resulting histogram is valid.
			if h.buckets[0].NumRange > 0 || h.buckets[0].DistinctRange > 0 {
				t.Errorf("the first histogram bucket should be empty. found %v", h.buckets[0])
			}
			// Make sure the distinct counts in each range are <= the max.
			for i := 1; i < len(h.buckets); i++ {
				lowerBound := h.buckets[i-1].UpperBound
				upperBound := h.buckets[i].UpperBound
				maxDistRange, countable := maxDistinctRange(&evalCtx, lowerBound, upperBound)
				if countable && math.Round(h.buckets[i].DistinctRange) > math.Round(maxDistRange) {
					t.Errorf(
						"distinct range in bucket exceeds maximum (%f). found %f",
						maxDistRange, h.buckets[i].DistinctRange,
					)
				}
			}
			// Make sure the total counts add up to the correct numbers.
			var actualRowCount, actualDistinctCount float64
			for i := range h.buckets {
				actualRowCount += h.buckets[i].NumEq
				actualRowCount += h.buckets[i].NumRange
				actualDistinctCount += h.buckets[i].DistinctRange
				if h.buckets[i].NumEq > 0 {
					actualDistinctCount++
				}
			}
			if int(math.Round(actualRowCount)) != rowCount {
				t.Errorf("actual row count (%f) != expected row count (%d)", actualRowCount, rowCount)
			}
			if int(math.Round(actualDistinctCount)) != distinctCount {
				t.Errorf(
					"actual distinct count (%f) != expected distinct count (%d)",
					actualDistinctCount, distinctCount,
				)
			}
		}
	})
}

func makeEnums(t *testing.T) tree.Datums {
	t.Helper()
	enumMembers := []string{"a", "b", "c", "d", "e"}
	enumType := types.MakeEnum(typedesc.TypeIDToOID(500), typedesc.TypeIDToOID(100500))
	enumType.TypeMeta = types.UserDefinedTypeMetadata{
		Name: &types.UserDefinedTypeName{
			Schema: "test",
			Name:   "letters",
		},
		EnumData: &types.EnumMetadata{
			LogicalRepresentations: enumMembers,
			PhysicalRepresentations: [][]byte{
				encoding.EncodeUntaggedIntValue(nil, 0),
				encoding.EncodeUntaggedIntValue(nil, 1),
				encoding.EncodeUntaggedIntValue(nil, 2),
				encoding.EncodeUntaggedIntValue(nil, 3),
				encoding.EncodeUntaggedIntValue(nil, 4),
			},
			IsMemberReadOnly: make([]bool, len(enumMembers)),
		},
	}
	res := make(tree.Datums, len(enumMembers))
	var err error
	for i := range enumMembers {
		res[i], err = tree.MakeDEnumFromLogicalRepresentation(enumType, enumMembers[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	return res
}

func makeUuids(t *testing.T) tree.Datums {
	res := make(tree.Datums, 5)
	var err error
	res[0] = tree.DMinUUID
	res[1], err = tree.ParseDUuidFromString("4589ad07-52f2-4d60-83e8-4a8347fef718")
	if err != nil {
		t.Fatal(err)
	}
	res[2], err = tree.ParseDUuidFromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	if err != nil {
		t.Fatal(err)
	}
	res[3], err = tree.ParseDUuidFromString("cccccccc-cccc-cccc-cccc-cccccccccccc")
	if err != nil {
		t.Fatal(err)
	}
	res[4] = tree.DMaxUUID
	return res
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

func roundHistogram(h *histogram) {
	for i := range h.buckets {
		roundBucket(&h.buckets[i])
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
