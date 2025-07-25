// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package geomfn

import (
	"math"
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/errors"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
)

// GeometricMedian computes the approximate geometric median of a MultiPoint geometry
// using the Weiszfeld algorithm. The geometric median is the point minimizing the sum
// of distances to the input points.
//
// Parameters:
// - g: The input MultiPoint geometry
// - tolerance: The algorithm iterates until the distance change between successive
//   iterations is less than the supplied tolerance parameter. If tolerance is nil,
//   the tolerance value is calculated based on the extent of the input geometry.
// - maxIterations: If convergence has not been met after maxIterations iterations,
//   the function produces an error and exits, unless failIfNotConverged is false.
// - failIfNotConverged: Controls whether the function produces an error when
//   convergence is not achieved within maxIterations.
//
// Returns the geometric median as a Point geometry.
func GeometricMedian(g geo.Geometry, tolerance *float64, maxIterations int, failIfNotConverged bool) (geo.Geometry, error) {
	geomRepr, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}

	// Ensure we have a MultiPoint geometry
	multiPoint, ok := geomRepr.(*geom.MultiPoint)
	if !ok {
		return geo.Geometry{}, errors.Newf("ST_GeometricMedian requires MultiPoint geometry, got %T", geomRepr)
	}

	if multiPoint.NumPoints() == 0 {
		return geo.Geometry{}, errors.New("ST_GeometricMedian requires at least one point")
	}

	// If we have only one point, return it
	if multiPoint.NumPoints() == 1 {
		point := multiPoint.Point(0)
		ewkb, err := ewkb.Marshal(point, geo.DefaultEWKBEncodingFormat)
		if err != nil {
			return geo.Geometry{}, err
		}
		return geo.ParseGeometryFromEWKB(ewkb)
	}

	// Extract points and weights
	points, weights, err := extractPointsAndWeights(multiPoint)
	if err != nil {
		return geo.Geometry{}, err
	}

	// Calculate tolerance if not provided
	actualTolerance := tolerance
	if actualTolerance == nil {
		calculatedTolerance := calculateToleranceFromExtent(points)
		actualTolerance = &calculatedTolerance
	}

	// Run Weiszfeld algorithm
	median, converged, err := weiszfeldAlgorithm(points, weights, *actualTolerance, maxIterations)
	if err != nil {
		return geo.Geometry{}, err
	}

	if !converged && failIfNotConverged {
		return geo.Geometry{}, errors.New("ST_GeometricMedian failed to converge within maximum iterations")
	}

	// Create result Point geometry
	resultPoint := geom.NewPointFlat(geom.XY, []float64{median.X, median.Y}).SetSRID(int(g.SRID()))
	ewkb, err := ewkb.Marshal(resultPoint, geo.DefaultEWKBEncodingFormat)
	if err != nil {
		return geo.Geometry{}, err
	}

	return geo.ParseGeometryFromEWKB(ewkb)
}

// Point2D represents a 2D point
type Point2D struct {
	X, Y float64
}

// extractPointsAndWeights extracts points and weights from a MultiPoint geometry
func extractPointsAndWeights(multiPoint *geom.MultiPoint) ([]Point2D, []float64, error) {
	numPoints := multiPoint.NumPoints()
	points := make([]Point2D, numPoints)
	weights := make([]float64, numPoints)

	layout := multiPoint.Layout()
	hasM := layout == geom.XYM || layout == geom.XYZM

	for i := 0; i < numPoints; i++ {
		point := multiPoint.Point(i)
		coords := point.FlatCoords()
		
		if len(coords) < 2 {
			return nil, nil, errors.New("invalid point coordinates")
		}

		points[i] = Point2D{X: coords[0], Y: coords[1]}
		
		// Use M coordinate as weight if present, otherwise default weight is 1
		if hasM && len(coords) >= layout.Stride() {
			mIndex := layout.MIndex()
			if mIndex >= 0 && mIndex < len(coords) {
				weights[i] = coords[mIndex]
			} else {
				weights[i] = 1.0
			}
		} else {
			weights[i] = 1.0
		}

		// Ensure weight is positive
		if weights[i] <= 0 {
			weights[i] = 1.0
		}
	}

	return points, weights, nil
}

// calculateToleranceFromExtent calculates tolerance based on the extent of input points
func calculateToleranceFromExtent(points []Point2D) float64 {
	if len(points) == 0 {
		return 1e-9
	}

	minX, maxX := points[0].X, points[0].X
	minY, maxY := points[0].Y, points[0].Y

	for _, p := range points[1:] {
		if p.X < minX {
			minX = p.X
		}
		if p.X > maxX {
			maxX = p.X
		}
		if p.Y < minY {
			minY = p.Y
		}
		if p.Y > maxY {
			maxY = p.Y
		}
	}

	extent := math.Max(maxX-minX, maxY-minY)
	if extent == 0 {
		return 1e-9
	}
	return extent * 1e-9
}

// weiszfeldAlgorithm implements the Weiszfeld algorithm for computing geometric median
func weiszfeldAlgorithm(points []Point2D, weights []float64, tolerance float64, maxIterations int) (Point2D, bool, error) {
	if len(points) != len(weights) {
		return Point2D{}, false, errors.New("points and weights must have same length")
	}

	// Initialize with weighted centroid
	current := weightedCentroid(points, weights)
	
	for iteration := 0; iteration < maxIterations; iteration++ {
		next, err := weiszfeldIteration(points, weights, current)
		if err != nil {
			return Point2D{}, false, err
		}

		// Check convergence
		distance := euclideanDistance(current, next)
		if distance < tolerance {
			return next, true, nil
		}

		current = next
	}

	// Return last result even if not converged
	return current, false, nil
}

// weightedCentroid calculates the weighted centroid of points
func weightedCentroid(points []Point2D, weights []float64) Point2D {
	var sumX, sumY, sumWeight float64
	
	for i, p := range points {
		w := weights[i]
		sumX += p.X * w
		sumY += p.Y * w
		sumWeight += w
	}

	if sumWeight == 0 {
		sumWeight = 1
	}

	return Point2D{X: sumX / sumWeight, Y: sumY / sumWeight}
}

// weiszfeldIteration performs one iteration of the Weiszfeld algorithm
func weiszfeldIteration(points []Point2D, weights []float64, current Point2D) (Point2D, error) {
	var numeratorX, numeratorY, denominator float64
	
	for i, p := range points {
		distance := euclideanDistance(current, p)
		
		// Handle case where current point coincides with one of the input points
		if distance < 1e-15 {
			// Use a small perturbation to avoid division by zero
			distance = 1e-15
		}
		
		weight := weights[i] / distance
		numeratorX += weight * p.X
		numeratorY += weight * p.Y
		denominator += weight
	}

	if denominator == 0 {
		return current, errors.New("division by zero in Weiszfeld algorithm")
	}

	return Point2D{X: numeratorX / denominator, Y: numeratorY / denominator}, nil
}

// euclideanDistance calculates the Euclidean distance between two points
func euclideanDistance(p1, p2 Point2D) float64 {
	dx := p1.X - p2.X
	dy := p1.Y - p2.Y
	return math.Sqrt(dx*dx + dy*dy)
}