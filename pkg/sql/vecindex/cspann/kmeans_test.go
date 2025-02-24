// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/floats/scalar"
	"gonum.org/v1/gonum/stat"
)

func TestBalancedKMeans(t *testing.T) {
	calcCentroid := func(vectors *vector.Set, offsets []uint64) vector.T {
		centroid := make(vector.T, vectors.Dims)
		for _, offset := range offsets {
			num32.Add(centroid, vectors.At(int(offset)))
		}
		num32.Scale(1/float32(len(offsets)), centroid)
		return centroid
	}

	calcMeanDistance := func(vectors *vector.Set, centroid vector.T, offsets []uint64) float32 {
		var distanceSum float32
		for _, offset := range offsets {
			distanceSum += num32.L2Distance(vectors.At(int(offset)), centroid)
		}
		return distanceSum / float32(len(offsets))
	}

	workspace := &workspace.T{}
	rng := rand.New(rand.NewSource(42))
	kmeans := BalancedKmeans{Workspace: workspace, Rand: rng}

	testCases := []struct {
		desc         string
		vectors      vector.Set
		leftOffsets  []uint64
		rightOffsets []uint64
	}{
		{
			desc:         "partition vector set with only 2 elements",
			vectors:      vector.MakeSetFromRawData([]float32{1, 2}, 1),
			leftOffsets:  []uint64{1},
			rightOffsets: []uint64{0},
		},
		{
			desc: "partition vector set with duplicates values",
			vectors: vector.MakeSetFromRawData([]float32{
				1, 1,
				1, 1,
				1, 1,
				1, 1,
				1, 1,
			}, 2),
			leftOffsets:  []uint64{0, 1},
			rightOffsets: []uint64{2, 3, 4},
		},
		{
			desc: "partition 5x3 set of vectors",
			vectors: vector.MakeSetFromRawData([]float32{
				1, 2, 3,
				2, 5, 10,
				4, 6, 1,
				10, 15, 20,
				3, 8, 1,
			}, 3),
			leftOffsets:  []uint64{1, 3},
			rightOffsets: []uint64{0, 2, 4},
		},
		{
			// Unbalanced vector set, with 4 vectors close together and 1 far.
			// One of the close vectors will be grouped with the far vector due
			// to the balancing constraint.
			desc: "unbalanced vector set",
			vectors: vector.MakeSetFromRawData([]float32{
				2, 2,
				2, 1,
				1, 2,
				1, 1,
				20, 30,
			}, 2),
			leftOffsets:  []uint64{1, 2, 3},
			rightOffsets: []uint64{0, 4},
		},
		{
			desc: "very small values close to one another",
			vectors: vector.MakeSetFromRawData([]float32{
				1.23e-10, 2.58e-10,
				1.25e-10, 2.60e-10,
				1.26e-10, 2.61e-10,
				1.24e-10, 2.59e-10,
			}, 2),
			leftOffsets:  []uint64{0, 3},
			rightOffsets: []uint64{1, 2},
		},
		{
			desc:    "high-dimensional unit vectors",
			vectors: testutils.LoadFeatures(t, 100),
		},
	}

	for _, tc := range testCases {
		offsets := make([]uint64, tc.vectors.Count)
		leftOffsets, rightOffsets := kmeans.Compute(&tc.vectors, offsets)
		ratio := float64(len(leftOffsets)) / float64(len(rightOffsets))
		require.False(t, ratio < 0.5)
		require.False(t, ratio > 2)
		if tc.leftOffsets != nil {
			require.Equal(t, tc.leftOffsets, leftOffsets)
		}
		if tc.rightOffsets != nil {
			require.Equal(t, tc.rightOffsets, rightOffsets)
		}

		// Ensure that distance to left centroid is less for vectors in the left
		// partition than those in the right partition.
		leftCentroid := calcCentroid(&tc.vectors, leftOffsets)
		leftMean := calcMeanDistance(&tc.vectors, leftCentroid, leftOffsets)
		rightMean := calcMeanDistance(&tc.vectors, leftCentroid, rightOffsets)
		require.LessOrEqual(t, leftMean, rightMean)
	}

	t.Run("use global random number generator", func(t *testing.T) {
		kmeans = BalancedKmeans{Workspace: workspace}
		vectors := vector.MakeSetFromRawData([]float32{1, 2, 3, 4}, 2)
		offsets := make([]uint64, vectors.Count)
		kmeans.Compute(&vectors, offsets)
	})
}

func TestMeanOfVariances(t *testing.T) {
	testCases := []struct {
		name     string
		vectors  vector.Set
		expected float64
		noRound  bool
	}{
		{
			name: "zero variance",
			vectors: vector.MakeSetFromRawData([]float32{
				1, 1, 1,
				1, 1, 1,
				1, 1, 1,
			}, 3),
			expected: 0,
		},
		{
			name: "simple values",
			vectors: vector.MakeSetFromRawData([]float32{
				1, 2, 3,
				4, 5, 6,
				7, 8, 9,
			}, 3),
			expected: 9,
		},
		{
			name: "larger set of floating-point values",
			vectors: vector.MakeSetFromRawData([]float32{
				4.2, 5.4, -6.3,
				10.3, -11.0, 12.9,
				1.5, 2.5, 3.5,
				-13.7, 14.8, 15.9,
				-7.9, -8.1, -9.4,
			}, 3),
			expected: 109.3903,
		},
		{
			name: "one-dimensional vectors",
			vectors: vector.MakeSetFromRawData([]float32{
				1, 2, 3, 4, 5, 6,
				2, 3, 4, 5, 6, 7,
				3, 4, 5, 6, 7, 8,
			}, 1),
			expected: 3.7941,
		},
		{
			name: "large numbers with small variance",
			vectors: vector.MakeSetFromRawData([]float32{
				1e7 + 1, 1e7 + 2, 1e7 + 3, 1e7 + 4,
			}, 1),
			expected: 1.6667,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kmeans := BalancedKmeans{Workspace: &workspace.T{}, vectors: &tc.vectors}
			result := float64(kmeans.calculateMeanOfVariances())
			if !tc.noRound {
				result = scalar.Round(result, 4)
			}
			require.Equal(t, tc.expected, result)

			// Compare result against calculation performed using gonum stat
			// library.
			variances := make([]float64, tc.vectors.Dims)
			for dimIdx := 0; dimIdx < tc.vectors.Dims; dimIdx++ {
				values := make([]float64, tc.vectors.Count)
				for vecIdx := range tc.vectors.Count {
					values[vecIdx] = float64(tc.vectors.At(vecIdx)[dimIdx])
				}
				_, variances[dimIdx] = stat.MeanVariance(values, nil)
			}

			mean := stat.Mean(variances, nil)
			mean = scalar.Round(mean, 4)
			require.Equal(t, mean, result)
		})
	}
}
