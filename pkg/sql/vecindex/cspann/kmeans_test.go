// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"math/rand"
	"slices"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/vecdist"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
	"gonum.org/v1/gonum/floats/scalar"
	"gonum.org/v1/gonum/stat"
)

func TestBalancedKMeans(t *testing.T) {
	calcMeanDistance := func(
		distanceMetric vecdist.Metric,
		vectors vector.Set,
		centroid vector.T,
		offsets []uint64,
	) float32 {
		var distanceSum float32
		for _, offset := range offsets {
			distanceSum += vecdist.Measure(distanceMetric, vectors.At(int(offset)), centroid)
		}
		return distanceSum / float32(len(offsets))
	}

	workspace := &workspace.T{}
	rng := rand.New(rand.NewSource(42))
	dataset := testutils.LoadDataset(t, testutils.ImagesDataset)

	testCases := []struct {
		desc           string
		distanceMetric vecdist.Metric
		vectors        vector.Set
		leftOffsets    []uint64
		rightOffsets   []uint64
		leftCentroid   vector.T
		rightCentroid  vector.T
		skipPinTest    bool
	}{
		{
			desc:           "partition vector set with only 2 elements",
			distanceMetric: vecdist.L2Squared,
			vectors:        vector.MakeSetFromRawData([]float32{1, 2}, 1),
			leftOffsets:    []uint64{1},
			rightOffsets:   []uint64{0},
			leftCentroid:   []float32{2},
			rightCentroid:  []float32{1},
		},
		{
			desc:           "partition vector set with duplicates values",
			distanceMetric: vecdist.L2Squared,
			vectors: vector.MakeSetFromRawData([]float32{
				1, 1,
				1, 1,
				1, 1,
				1, 1,
				1, 1,
			}, 2),
			leftOffsets:   []uint64{0, 1},
			rightOffsets:  []uint64{2, 3, 4},
			leftCentroid:  []float32{1, 1},
			rightCentroid: []float32{1, 1},
		},
		{
			desc:           "partition 5x3 set of vectors",
			distanceMetric: vecdist.L2Squared,
			vectors: vector.MakeSetFromRawData([]float32{
				1, 2, 3,
				2, 5, 10,
				4, 6, 1,
				10, 15, 20,
				4, 7, 2,
			}, 3),
			leftOffsets:   []uint64{0, 2, 4},
			rightOffsets:  []uint64{1, 3},
			leftCentroid:  []float32{3, 5, 2},
			rightCentroid: []float32{6, 10, 15},
		},
		{
			// Unbalanced vector set, with 4 vectors close together and 1 far.
			// One of the close vectors will be grouped with the far vector due
			// to the balancing constraint.
			desc:           "unbalanced vector set",
			distanceMetric: vecdist.L2Squared,
			vectors: vector.MakeSetFromRawData([]float32{
				3, 0,
				2, 1,
				1, 2,
				4, 2,
				20, 30,
			}, 2),
			leftOffsets:   []uint64{0, 1, 2},
			rightOffsets:  []uint64{3, 4},
			leftCentroid:  []float32{2, 1},
			rightCentroid: []float32{12, 16},
		},
		{
			desc:           "very small values close to one another",
			distanceMetric: vecdist.L2Squared,
			vectors: vector.MakeSetFromRawData([]float32{
				1.23e-10, 2.58e-10,
				1.25e-10, 2.60e-10,
				1.26e-10, 2.61e-10,
				1.24e-10, 2.59e-10,
			}, 2),
			leftOffsets:   []uint64{1, 2},
			rightOffsets:  []uint64{0, 3},
			leftCentroid:  vector.T{1.255e-10, 2.605e-10},
			rightCentroid: vector.T{1.235e-10, 2.585e-10},
		},
		{
			desc:           "inner product distance",
			distanceMetric: vecdist.InnerProduct,
			vectors: vector.MakeSetFromRawData([]float32{
				1, 2, 3,
				2, 5, -10,
				-4, 6, 1,
				9, -14, 20,
				5, 9, 4,
			}, 3),
			leftOffsets:  []uint64{0, 3},
			rightOffsets: []uint64{1, 2, 4},
		},
		{
			desc:           "cosine distance",
			distanceMetric: vecdist.Cosine,
			vectors: vector.MakeSetFromRawData([]float32{
				1, 0, 0,
				0.57735, 0.57735, 0.57735,
				0, 0, 1,
				0, 1, 0,
				0.95672, -0.06355, -0.28399,
			}, 3),
			leftOffsets:  []uint64{0, 4},
			rightOffsets: []uint64{1, 2, 3},
		},
		{
			desc:           "high-dimensional unit vectors, Euclidean distance",
			distanceMetric: vecdist.L2Squared,
			vectors:        dataset.Slice(0, 100),
			// It's challenging to test pinLeftCentroid for this case, due to the
			// inherent randomness of the K-means++ algorithm. The other test cases
			// should be sufficient to test that, however.
			skipPinTest: true,
		},
		{
			desc:           "high-dimensional unit vectors, InnerProduct distance",
			distanceMetric: vecdist.InnerProduct,
			vectors:        dataset.Slice(0, 100),
			skipPinTest:    true,
		},
		{
			desc:           "high-dimensional unit vectors, Cosine distance",
			distanceMetric: vecdist.Cosine,
			vectors:        dataset.Slice(0, 100),
			skipPinTest:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			kmeans := BalancedKmeans{
				Workspace:      workspace,
				Rand:           rng,
				DistanceMetric: tc.distanceMetric,
			}

			// Compute centroids for the vectors.
			leftCentroid := make(vector.T, tc.vectors.Dims)
			rightCentroid := make(vector.T, tc.vectors.Dims)
			kmeans.ComputeCentroids(
				tc.vectors, leftCentroid, rightCentroid, false /* pinLeftCentroid */)

			// Assign vectors to closest centroid.
			offsets := make([]uint64, tc.vectors.Count)
			leftOffsets, rightOffsets := kmeans.AssignPartitions(
				tc.vectors, leftCentroid, rightCentroid, offsets)
			slices.Sort(leftOffsets)
			slices.Sort(rightOffsets)
			if tc.leftOffsets != nil {
				require.Equal(t, tc.leftOffsets, leftOffsets)
			}
			if tc.rightOffsets != nil {
				require.Equal(t, tc.rightOffsets, rightOffsets)
			}
			if tc.leftCentroid != nil {
				require.Equal(t, tc.leftCentroid, leftCentroid)
			} else {
				// Fallback on calculation.
				expected := make(vector.T, tc.vectors.Dims)
				calcPartitionCentroid(tc.distanceMetric, tc.vectors, leftOffsets, expected)
				require.InDeltaSlice(t, expected, leftCentroid, 1e-6)
			}
			if tc.rightCentroid != nil {
				require.Equal(t, tc.rightCentroid, rightCentroid)
			} else {
				// Fallback on calculation.
				expected := make(vector.T, tc.vectors.Dims)
				calcPartitionCentroid(tc.distanceMetric, tc.vectors, rightOffsets, expected)
				require.InDeltaSlice(t, expected, rightCentroid, 1e-6)
			}
			ratio := float64(len(leftOffsets)) / float64(len(rightOffsets))
			require.False(t, ratio < 0.45)
			require.False(t, ratio > 2.05)

			// Ensure that distance to left centroid is less for vectors in the left
			// partition than those in the right partition.
			leftMean := calcMeanDistance(tc.distanceMetric, tc.vectors, leftCentroid, leftOffsets)
			rightMean := calcMeanDistance(tc.distanceMetric, tc.vectors, leftCentroid, rightOffsets)
			require.LessOrEqual(t, leftMean, rightMean)

			if !tc.skipPinTest {
				// Check that pinning the left centroid returns the same right centroid.
				newLeftCentroid := slices.Clone(leftCentroid)
				newRightCentroid := make(vector.T, len(rightCentroid))
				kmeans.ComputeCentroids(
					tc.vectors, newLeftCentroid, newRightCentroid, true /* pinLeftCentroid */)
				require.Equal(t, leftCentroid, newLeftCentroid)
				require.Equal(t, rightCentroid, newRightCentroid)
			}
		})
	}

	t.Run("use global random number generator", func(t *testing.T) {
		kmeans := BalancedKmeans{Workspace: workspace}
		vectors := vector.MakeSetFromRawData([]float32{1, 2, 3, 4}, 2)
		leftCentroid := make(vector.T, 2)
		rightCentroid := make(vector.T, 2)
		kmeans.ComputeCentroids(
			vectors, leftCentroid, rightCentroid, false /* pinLeftCentroid */)
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
			kmeans := BalancedKmeans{Workspace: &workspace.T{}}
			result := float64(kmeans.calculateMeanOfVariances(tc.vectors))
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

func TestCalcPartitionCentroid(t *testing.T) {
	testCases := []struct {
		name           string
		distanceMetric vecdist.Metric
		vectors        vector.Set
		offsets        []uint64
		expected       vector.T
	}{
		{
			name:           "L2Squared simple mean",
			distanceMetric: vecdist.L2Squared,
			// Only use the [1,2] and [3,4] vectors.
			vectors:  vector.MakeSetFromRawData([]float32{1, 2, 10, 11, 3, 4, 12, 13}, 2),
			offsets:  []uint64{0, 2},
			expected: vector.T{2, 3},
		},
		{
			name:           "Cosine normalization",
			distanceMetric: vecdist.Cosine,
			// Only use the [1,0] and [0,1] vectors.
			vectors:  vector.MakeSetFromRawData([]float32{10, 11, 1, 0, 12, 13, 0, 1}, 2),
			offsets:  []uint64{1, 3},
			expected: vector.T{0.7071, 0.7071},
		},
		{
			// The degenerate case for cosine occurs when the sum of the input
			// vectors is the zero vector. When this happens, the norm is zero
			// and the direction of the vector can't be determined. In that case,
			// return the zero vector.
			name:           "Cosine degenerate zero vector",
			distanceMetric: vecdist.Cosine,
			vectors:        vector.MakeSetFromRawData([]float32{0.7071, 0.7071, -0.7071, -0.7071}, 2),
			offsets:        []uint64{0, 1},
			expected:       vector.T{0, 0},
		},
		{
			name:           "InnerProduct with 3 dimensions",
			distanceMetric: vecdist.InnerProduct,
			vectors:        vector.MakeSetFromRawData([]float32{-5, 2, -3, 4, 8, 6, 10, 2, -3}, 3),
			offsets:        []uint64{0, 1, 2},
			expected:       vector.T{0.6, 0.8, 0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			centroid := make(vector.T, tc.vectors.Dims)
			calcPartitionCentroid(tc.distanceMetric, tc.vectors, tc.offsets, centroid)
			require.InDeltaSlice(
				t, tc.expected, centroid, 1e-4, "centroid not as expected: %0.4f", centroid)
		})
	}
}
