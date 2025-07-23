// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package geomfn

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

func TestGeometricMedian(t *testing.T) {
	testCases := []struct {
		name                 string
		wkt                  string
		tolerance            *float64
		maxIterations        int
		failIfNotConverged   bool
		expectedWKT          string
		expectedError        string
		toleranceForComparison float64
	}{
		{
			name:                   "single point",
			wkt:                    "MULTIPOINT((0 0))",
			tolerance:              nil,
			maxIterations:          10000,
			failIfNotConverged:     false,
			expectedWKT:            "POINT(0 0)",
			toleranceForComparison: 1e-10,
		},
		{
			name:                   "two points",
			wkt:                    "MULTIPOINT((0 0), (2 0))",
			tolerance:              nil,
			maxIterations:          10000,
			failIfNotConverged:     false,
			expectedWKT:            "POINT(1 0)",
			toleranceForComparison: 1e-6,
		},
		{
			name:                   "three points in line",
			wkt:                    "MULTIPOINT((0 0), (1 0), (2 0))",
			tolerance:              nil,
			maxIterations:          10000,
			failIfNotConverged:     false,
			expectedWKT:            "POINT(1 0)",
			toleranceForComparison: 1e-6,
		},
		{
			name:                   "three points forming triangle",
			wkt:                    "MULTIPOINT((0 0), (1 0), (0.5 0.866))",
			tolerance:              nil,
			maxIterations:          10000,
			failIfNotConverged:     false,
			expectedWKT:            "POINT(0.5 0.288675134594813)",
			toleranceForComparison: 1e-6,
		},
		{
			name:                   "four points forming square",
			wkt:                    "MULTIPOINT((0 0), (1 0), (1 1), (0 1))",
			tolerance:              nil,
			maxIterations:          10000,
			failIfNotConverged:     false,
			expectedWKT:            "POINT(0.5 0.5)",
			toleranceForComparison: 1e-6,
		},
		{
			name:                   "with explicit tolerance",
			wkt:                    "MULTIPOINT((0 0), (1 0), (2 0))",
			tolerance:              floatPtr(1e-8),
			maxIterations:          10000,
			failIfNotConverged:     false,
			expectedWKT:            "POINT(1 0)",
			toleranceForComparison: 1e-6,
		},
		{
			name:                   "with low max iterations and fail on not converged",
			wkt:                    "MULTIPOINT((0 0), (100 0), (200 0), (300 0), (500 0))",
			tolerance:              floatPtr(1e-15),
			maxIterations:          2,
			failIfNotConverged:     true,
			expectedError:          "ST_GeometricMedian failed to converge within maximum iterations",
		},
		{
			name:                   "with low max iterations but not fail on not converged",
			wkt:                    "MULTIPOINT((0 0), (1 0), (2 0))",
			tolerance:              floatPtr(1e-12),
			maxIterations:          1,
			failIfNotConverged:     false,
			expectedWKT:            "POINT(1 0)",
			toleranceForComparison: 0.1, // Less precise due to early termination
		},
		{
			name:                   "points with M values as weights",
			wkt:                    "MULTIPOINT M((0 0 1), (2 0 3))",
			tolerance:              nil,
			maxIterations:          10000,
			failIfNotConverged:     false,
			expectedWKT:            "POINT(2 0)",
			toleranceForComparison: 1e-4, // Algorithm converges very close to higher-weighted point
		},
		{
			name:                   "points with negative M values (should default to weight 1)",
			wkt:                    "MULTIPOINT M((0 0 -1), (2 0 1))",
			tolerance:              nil,
			maxIterations:          10000,
			failIfNotConverged:     false,
			expectedWKT:            "POINT(1 0)",
			toleranceForComparison: 1e-6,
		},
		{
			name:          "empty multipoint",
			wkt:           "MULTIPOINT EMPTY",
			expectedError: "ST_GeometricMedian requires at least one point",
		},
		{
			name:          "not a multipoint",
			wkt:           "POINT(0 0)",
			expectedError: "ST_GeometricMedian requires MultiPoint geometry",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)

			result, err := GeometricMedian(g, tc.tolerance, tc.maxIterations, tc.failIfNotConverged)

			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
				return
			}

			require.NoError(t, err)

			expected, err := geo.ParseGeometry(tc.expectedWKT)
			require.NoError(t, err)

			// Compare coordinates with tolerance
			resultGeomT, err := result.AsGeomT()
			require.NoError(t, err)
			expectedGeomT, err := expected.AsGeomT()
			require.NoError(t, err)

			resultPoint, ok := resultGeomT.(*geom.Point)
			require.True(t, ok)
			expectedPoint, ok := expectedGeomT.(*geom.Point)
			require.True(t, ok)

			resultCoords := resultPoint.FlatCoords()
			expectedCoords := expectedPoint.FlatCoords()

			require.True(t, len(resultCoords) >= 2)
			require.True(t, len(expectedCoords) >= 2)

			require.InDelta(t, expectedCoords[0], resultCoords[0], tc.toleranceForComparison, "X coordinate")
			require.InDelta(t, expectedCoords[1], resultCoords[1], tc.toleranceForComparison, "Y coordinate")
		})
	}
}

func TestGeometricMedian_LargeDataSet(t *testing.T) {
	// Test with a larger set of points arranged in a circle
	// The geometric median should be close to the center
	numPoints := 100
	radius := 10.0
	wktParts := make([]string, numPoints)
	
	for i := 0; i < numPoints; i++ {
		angle := 2.0 * math.Pi * float64(i) / float64(numPoints)
		x := radius * math.Cos(angle)
		y := radius * math.Sin(angle)
		wktParts[i] = fmt.Sprintf("(%f %f)", x, y)
	}
	
	wkt := "MULTIPOINT(" + strings.Join(wktParts, ", ") + ")"
	
	g, err := geo.ParseGeometry(wkt)
	require.NoError(t, err)

	result, err := GeometricMedian(g, nil, 10000, true)
	require.NoError(t, err)

	resultGeomT, err := result.AsGeomT()
	require.NoError(t, err)
	resultPoint, ok := resultGeomT.(*geom.Point)
	require.True(t, ok)

	coords := resultPoint.FlatCoords()
	require.True(t, len(coords) >= 2)

	// The result should be very close to (0, 0) for a symmetric circle
	require.InDelta(t, 0.0, coords[0], 1e-6, "X coordinate should be near 0")
	require.InDelta(t, 0.0, coords[1], 1e-6, "Y coordinate should be near 0")
}

func TestGeometricMedian_WeightedPoints(t *testing.T) {
	// Test with weighted points where one point has much higher weight
	// The median should be pulled towards the high-weight point
	wkt := "MULTIPOINT M((0 0 1), (10 0 100))"
	
	g, err := geo.ParseGeometry(wkt)
	require.NoError(t, err)

	result, err := GeometricMedian(g, nil, 10000, true)
	require.NoError(t, err)

	resultGeomT, err := result.AsGeomT()
	require.NoError(t, err)
	resultPoint, ok := resultGeomT.(*geom.Point)
	require.True(t, ok)

	coords := resultPoint.FlatCoords()
	require.True(t, len(coords) >= 2)

	// The result should be much closer to (10, 0) due to the higher weight
	require.True(t, coords[0] > 5.0, "X coordinate should be closer to the high-weight point")
	require.InDelta(t, 0.0, coords[1], 1e-6, "Y coordinate should remain 0")
}

func TestExtractPointsAndWeights(t *testing.T) {
	testCases := []struct {
		name            string
		wkt             string
		expectedPoints  []Point2D
		expectedWeights []float64
	}{
		{
			name:            "XY points",
			wkt:             "MULTIPOINT((0 0), (1 1))",
			expectedPoints:  []Point2D{{0, 0}, {1, 1}},
			expectedWeights: []float64{1.0, 1.0},
		},
		{
			name:            "XYM points",
			wkt:             "MULTIPOINT M((0 0 2), (1 1 3))",
			expectedPoints:  []Point2D{{0, 0}, {1, 1}},
			expectedWeights: []float64{2.0, 3.0},
		},
		{
			name:            "XYM points with negative weight",
			wkt:             "MULTIPOINT M((0 0 -1), (1 1 3))",
			expectedPoints:  []Point2D{{0, 0}, {1, 1}},
			expectedWeights: []float64{1.0, 3.0}, // Negative weight defaults to 1.0
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g, err := geo.ParseGeometry(tc.wkt)
			require.NoError(t, err)

			geomRepr, err := g.AsGeomT()
			require.NoError(t, err)

			multiPoint, ok := geomRepr.(*geom.MultiPoint)
			require.True(t, ok)

			points, weights, err := extractPointsAndWeights(multiPoint)
			require.NoError(t, err)

			require.Equal(t, tc.expectedPoints, points)
			require.Equal(t, tc.expectedWeights, weights)
		})
	}
}

func TestCalculateToleranceFromExtent(t *testing.T) {
	testCases := []struct {
		name              string
		points            []Point2D
		expectedTolerance float64
	}{
		{
			name:              "empty points",
			points:            []Point2D{},
			expectedTolerance: 1e-9,
		},
		{
			name:              "single point",
			points:            []Point2D{{0, 0}},
			expectedTolerance: 1e-9,
		},
		{
			name:              "points with extent",
			points:            []Point2D{{0, 0}, {10, 5}},
			expectedTolerance: 10 * 1e-9,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tolerance := calculateToleranceFromExtent(tc.points)
			require.Equal(t, tc.expectedTolerance, tolerance)
		})
	}
}

func TestWeiszfeldAlgorithm(t *testing.T) {
	points := []Point2D{{0, 0}, {2, 0}}
	weights := []float64{1.0, 1.0}
	tolerance := 1e-8
	maxIterations := 1000

	median, converged, err := weiszfeldAlgorithm(points, weights, tolerance, maxIterations)
	require.NoError(t, err)
	require.True(t, converged)
	require.InDelta(t, 1.0, median.X, 1e-6)
	require.InDelta(t, 0.0, median.Y, 1e-6)
}

func TestEuclideanDistance(t *testing.T) {
	p1 := Point2D{0, 0}
	p2 := Point2D{3, 4}
	distance := euclideanDistance(p1, p2)
	require.InDelta(t, 5.0, distance, 1e-10)
}

func floatPtr(f float64) *float64 {
	return &f
}