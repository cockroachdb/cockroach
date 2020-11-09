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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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

			h, err := EquiDepthHistogram(
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
		_, err := EquiDepthHistogram(
			evalCtx, types.Int, samples, 2 /* numRows */, 2 /* distinctCount */, 10, /* maxBuckets */
		)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("nulls", func(t *testing.T) {
		samples := tree.Datums{tree.NewDInt(1), tree.NewDInt(2), tree.DNull}
		_, err := EquiDepthHistogram(
			evalCtx, types.Int, samples, 100 /* numRows */, 3 /* distinctCount */, 10, /* maxBuckets */
		)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}
