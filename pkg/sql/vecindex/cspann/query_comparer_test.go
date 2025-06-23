// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/num32"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/stretchr/testify/require"
)

func TestQueryComparer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name        string
		metric      vecpb.DistanceMetric
		level       Level
		queryVector vector.T
		candidates  []SearchResult
		expected    []float32
	}{
		// L2Squared tests.
		{
			name:        "L2Squared leaf level - query vector at origin",
			metric:      vecpb.L2SquaredDistance,
			level:       LeafLevel,
			queryVector: vector.T{0, 0},
			candidates: []SearchResult{
				{Vector: vector.T{3, 4}},  // (0-3)² + (0-4)² = 25
				{Vector: vector.T{0, 0}},  // (0-0)² + (0-0)² = 0
				{Vector: vector.T{-3, 4}}, // (0+3)² + (0-4)² = 25
			},
			expected: []float32{25, 0, 25},
		},
		{
			name:        "L2Squared leaf level - query vector not at origin",
			metric:      vecpb.L2SquaredDistance,
			level:       LeafLevel,
			queryVector: vector.T{3, 4},
			candidates: []SearchResult{
				{Vector: vector.T{3, 4}}, // (3-3)² + (4-4)² = 0
				{Vector: vector.T{2, 1}}, // (3-2)² + (4-1)² = 10
				{Vector: vector.T{0, 0}}, // (3-0)² + (4-0)² = 25
			},
			expected: []float32{0, 10, 25},
		},
		{
			name:        "L2Squared interior level - same as leaf",
			metric:      vecpb.L2SquaredDistance,
			level:       SecondLevel,
			queryVector: vector.T{3, 4},
			candidates: []SearchResult{
				{Vector: vector.T{3, 4}}, // (3-3)² + (4-4)² = 0
				{Vector: vector.T{2, 1}}, // (3-2)² + (4-1)² = 10
				{Vector: vector.T{0, 0}}, // (3-0)² + (4-0)² = 25
			},
			expected: []float32{0, 10, 25},
		},

		// InnerProduct tests.
		{
			name:        "InnerProduct leaf level",
			metric:      vecpb.InnerProductDistance,
			level:       LeafLevel,
			queryVector: vector.T{3, 4},
			candidates: []SearchResult{
				{Vector: vector.T{6, 5}}, // -(3*6 + 4*5) = -38
				{Vector: vector.T{0, 0}}, // -(3*0 + 4*0) = 0
			},
			expected: []float32{-38, 0},
		},
		{
			name:        "InnerProduct interior level - normalized centroids",
			metric:      vecpb.InnerProductDistance,
			level:       SecondLevel,
			queryVector: vector.T{3, 4},
			candidates: []SearchResult{
				{Vector: vector.T{0, 0}},  // normalized = {0, 0}
				{Vector: vector.T{6, 8}},  // normalized = {0.6, 0.8}
				{Vector: vector.T{4, -3}}, // normalized = {0.8, -0.6}
				{Vector: vector.T{-6, 8}}, // normalized = {-0.6, 0.8}
			},
			expected: []float32{0, -5, 0, -1.4},
		},
		{
			name:        "InnerProduct interior level - zero query vector",
			metric:      vecpb.InnerProductDistance,
			level:       SecondLevel,
			queryVector: vector.T{0, 0},
			candidates: []SearchResult{
				{Vector: vector.T{0, 0}}, // normalized = {0, 0}
				{Vector: vector.T{6, 8}}, // normalized = {0.6, 0.8}
			},
			expected: []float32{0, 0}, // Should not be NaN
		},

		// Cosine tests.
		{
			name:        "Cosine leaf level",
			metric:      vecpb.CosineDistance,
			level:       LeafLevel,
			queryVector: vector.T{4, 3}, // normalized = {0.8, 0.6}
			candidates: []SearchResult{
				{Vector: vector.T{0, 0}},   // normalized = {0, 0}
				{Vector: vector.T{-3, 4}},  // normalized = {-0.6, 0.8}
				{Vector: vector.T{10, 0}},  // normalized = {1, 0}
				{Vector: vector.T{-3, -4}}, // normalized = {-0.6, -0.8}
			},
			expected: []float32{1, 1, 0.2, 1.96},
		},
		{
			name:        "Cosine leaf level - zero query vector",
			metric:      vecpb.CosineDistance,
			level:       LeafLevel,
			queryVector: vector.T{0, 0}, // normalized = {0, 0}
			candidates: []SearchResult{
				{Vector: vector.T{0, 0}},  // normalized = {0, 0}
				{Vector: vector.T{-3, 4}}, // normalized = {-0.6, 0.8}
				{Vector: vector.T{10, 0}}, // normalized = {1, 0}
			},
			expected: []float32{1, 1, 1},
		},
		{
			name:        "Cosine interior level",
			metric:      vecpb.CosineDistance,
			level:       SecondLevel,
			queryVector: vector.T{4, 3}, // normalized = {0.8, 0.6}
			candidates: []SearchResult{
				{Vector: vector.T{0, 0}},
				{Vector: vector.T{1, 0}},
				{Vector: vector.T{0.8, 0.6}},
			},
			expected: []float32{1, 0.2, 0},
		},
		{
			name:        "Cosine interior level - zero query vector",
			metric:      vecpb.CosineDistance,
			level:       SecondLevel,
			queryVector: vector.T{0, 0},
			candidates: []SearchResult{
				{Vector: vector.T{0, 0}},
				{Vector: vector.T{1, 0}},
				{Vector: vector.T{0.8, 0.6}},
			},
			expected: []float32{1, 1, 1}, // Should not be NaN
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup queryComparer.
			var rot RandomOrthoTransformer
			rot.Init(vecpb.RotGivens, len(tc.queryVector), 42)

			var comparer queryComparer
			comparer.InitOriginal(tc.metric, tc.queryVector, &rot)

			// Make a copy of the candidates.
			candidates := make([]SearchResult, len(tc.candidates))
			copy(candidates, tc.candidates)

			// Randomize candidates from an interior level.
			if tc.level != LeafLevel {
				for i := range tc.candidates {
					rot.RandomizeVector(tc.candidates[i].Vector, candidates[i].Vector)
				}
			}

			// Test ComputeExactDistances.
			comparer.ComputeExactDistances(tc.level, candidates)
			require.Len(t, candidates, len(tc.expected), "number of candidates should be preserved")

			for i, expected := range tc.expected {
				require.InDelta(t, expected, candidates[i].QueryDistance, 1e-5,
					"distance mismatch for candidate %d", i)

				// Error bound should always be 0 for exact distances.
				require.Equal(t, float32(0), candidates[i].ErrorBound,
					"error bound should be 0 for exact distances")
			}

			// Test InitRandomized for interior levels.
			if tc.level == LeafLevel {
				return
			}

			// Transform the query vector.
			queryVector := make([]float32, len(tc.queryVector))
			rot.RandomizeVector(tc.queryVector, queryVector)
			if tc.metric == vecpb.CosineDistance {
				num32.Normalize(queryVector)
			}
			comparer.InitTransformed(tc.metric, queryVector, &rot)

			// Test ComputeExactDistances.
			comparer.ComputeExactDistances(tc.level, candidates)
			require.Len(t, candidates, len(tc.expected), "number of candidates should be preserved")

			for i, expected := range tc.expected {
				require.InDelta(t, expected, candidates[i].QueryDistance, 1e-5,
					"distance mismatch for candidate %d", i)

				// Error bound should always be 0 for exact distances.
				require.Equal(t, float32(0), candidates[i].ErrorBound,
					"error bound should be 0 for exact distances")
			}
		})
	}
}
