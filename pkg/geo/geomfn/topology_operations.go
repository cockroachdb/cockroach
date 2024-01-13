// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geomfn

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"

	"github.com/twpayne/go-geom"
)

const (
	// EPSILON is the smallest value that can be represented by a float32.
	EPSILON = math.SmallestNonzeroFloat32
)

func GeometricMedian(g geo.Geometry, tolerance float32, max_iter int, fail_if_not_converged bool) (geo.Geometry, error) {
	// 
	if tolerance < 0 {
		return geo.Geometry{}, pgerror.Newf(pgcode.InvalidParameterValue, "tolerance must be positive")
	} else if max_iter < 0 {
		return geo.Geometry{}, pgerror.Newf(pgcode.InvalidParameterValue, "max iterations must be positive")
	}

	t, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}

	switch typ := t.(type) {
	case *geom.Point:
		return geo.MakeGeometryFromGeomT(t)
	case *geom.MultiPoint:
		mp, _ := t.(*geom.MultiPoint)
		if mp.NumPoints() == 0 {
			return geo.MakeGeometryFromLayoutAndPointCoords(geom.XYZ, []float64{})
		}
		
		points, err := transformPoints4D(mp)
		if err != nil {
			return geo.Geometry{}, err
		}

		median := initialPoint3D(points)
		iter := updatePoint(median, points, max_iter, tolerance)
		if fail_if_not_converged && iter >= max_iter {
			return geo.Geometry{}, pgerror.Newf(pgcode.InvalidParameterValue, "failed to converge within %.1E after %d iterations", tolerance, max_iter)
		}

		return geo.MakeGeometryFromLayoutAndPointCoords(median.Layout(), median.FlatCoords())
	default:
		return geo.Geometry{}, pgerror.Newf(pgcode.InvalidParameterValue, "unsupported geometry type: %T", typ)
	}

}

// Boundary returns the boundary of a given Geometry.
func Boundary(g geo.Geometry) (geo.Geometry, error) {
	// follow PostGIS behavior
	if g.Empty() {
		return g, nil
	}
	boundaryEWKB, err := geos.Boundary(g.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(boundaryEWKB)
}

// Centroid returns the Centroid of a given Geometry.
func Centroid(g geo.Geometry) (geo.Geometry, error) {
	centroidEWKB, err := geos.Centroid(g.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(centroidEWKB)
}

// MinimumBoundingCircle returns minimum bounding circle of an EWKB
func MinimumBoundingCircle(g geo.Geometry) (geo.Geometry, geo.Geometry, float64, error) {
	if BoundingBoxHasInfiniteCoordinates(g) || BoundingBoxHasNaNCoordinates(g) {
		return geo.Geometry{}, geo.Geometry{}, 0, pgerror.Newf(pgcode.InvalidParameterValue, "value out of range: overflow")
	}

	polygonEWKB, centroidEWKB, radius, err := geos.MinimumBoundingCircle(g.EWKB())
	if err != nil {
		return geo.Geometry{}, geo.Geometry{}, 0, err
	}

	polygon, err := geo.ParseGeometryFromEWKB(polygonEWKB)
	if err != nil {
		return geo.Geometry{}, geo.Geometry{}, 0, err
	}

	centroid, err := geo.ParseGeometryFromEWKB(centroidEWKB)
	if err != nil {
		return geo.Geometry{}, geo.Geometry{}, 0, err
	}

	return polygon, centroid, radius, nil
}

// ClipByRect clips a given Geometry by the given BoundingBox.
func ClipByRect(g geo.Geometry, b geo.CartesianBoundingBox) (geo.Geometry, error) {
	if g.Empty() {
		return g, nil
	}
	clipByRectEWKB, err := geos.ClipByRect(g.EWKB(), b.LoX, b.LoY, b.HiX, b.HiY)
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(clipByRectEWKB)
}

// ConvexHull returns the convex hull of a given Geometry.
func ConvexHull(g geo.Geometry) (geo.Geometry, error) {
	convexHullEWKB, err := geos.ConvexHull(g.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(convexHullEWKB)
}

// Difference returns the difference between two given Geometries.
func Difference(a, b geo.Geometry) (geo.Geometry, error) {
	// follow PostGIS behavior
	if a.Empty() || b.Empty() {
		return a, nil
	}
	if a.SRID() != b.SRID() {
		return geo.Geometry{}, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	diffEWKB, err := geos.Difference(a.EWKB(), b.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(diffEWKB)
}

// SimplifyGEOS returns a simplified Geometry with GEOS.
func SimplifyGEOS(g geo.Geometry, tolerance float64) (geo.Geometry, error) {
	if math.IsNaN(tolerance) || g.ShapeType2D() == geopb.ShapeType_Point || g.ShapeType2D() == geopb.ShapeType_MultiPoint {
		return g, nil
	}
	simplifiedEWKB, err := geos.Simplify(g.EWKB(), tolerance)
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(simplifiedEWKB)
}

// SimplifyPreserveTopology returns a simplified Geometry with topology preserved.
func SimplifyPreserveTopology(g geo.Geometry, tolerance float64) (geo.Geometry, error) {
	simplifiedEWKB, err := geos.TopologyPreserveSimplify(g.EWKB(), tolerance)
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(simplifiedEWKB)
}

// PointOnSurface returns the PointOnSurface of a given Geometry.
func PointOnSurface(g geo.Geometry) (geo.Geometry, error) {
	pointOnSurfaceEWKB, err := geos.PointOnSurface(g.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(pointOnSurfaceEWKB)
}

// Intersection returns the geometries of intersection between A and B.
func Intersection(a geo.Geometry, b geo.Geometry) (geo.Geometry, error) {
	if a.SRID() != b.SRID() {
		return geo.Geometry{}, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	// Match PostGIS.
	if a.Empty() {
		return a, nil
	}
	if b.Empty() {
		return b, nil
	}
	retEWKB, err := geos.Intersection(a.EWKB(), b.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(retEWKB)
}

// UnaryUnion returns the geometry of union between input geometry components
func UnaryUnion(g geo.Geometry) (geo.Geometry, error) {
	retEWKB, err := geos.UnaryUnion(g.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(retEWKB)
}

// Union returns the geometries of union between A and B.
func Union(a geo.Geometry, b geo.Geometry) (geo.Geometry, error) {
	if a.SRID() != b.SRID() {
		return geo.Geometry{}, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	retEWKB, err := geos.Union(a.EWKB(), b.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(retEWKB)
}

// SymDifference returns the geometries of symmetric difference between A and B.
func SymDifference(a geo.Geometry, b geo.Geometry) (geo.Geometry, error) {
	if a.SRID() != b.SRID() {
		return geo.Geometry{}, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	retEWKB, err := geos.SymDifference(a.EWKB(), b.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(retEWKB)
}

// SharedPaths Returns a geometry collection containing paths shared by the two input geometries.
func SharedPaths(a geo.Geometry, b geo.Geometry) (geo.Geometry, error) {
	if a.SRID() != b.SRID() {
		return geo.Geometry{}, geo.NewMismatchingSRIDsError(a.SpatialObject(), b.SpatialObject())
	}
	paths, err := geos.SharedPaths(a.EWKB(), b.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	gm, err := geo.ParseGeometryFromEWKB(paths)
	if err != nil {
		return geo.Geometry{}, err
	}
	return gm, nil
}

// MinimumRotatedRectangle Returns a minimum rotated rectangle enclosing a geometry
func MinimumRotatedRectangle(g geo.Geometry) (geo.Geometry, error) {
	paths, err := geos.MinimumRotatedRectangle(g.EWKB())
	if err != nil {
		return geo.Geometry{}, err
	}
	gm, err := geo.ParseGeometryFromEWKB(paths)
	if err != nil {
		return geo.Geometry{}, err
	}
	return gm, nil
}

// BoundingBoxHasInfiniteCoordinates checks if the bounding box of a Geometry
// has an infinite coordinate.
func BoundingBoxHasInfiniteCoordinates(g geo.Geometry) bool {
	boundingBox := g.BoundingBoxRef()
	if boundingBox == nil {
		return false
	}
	// Don't use `:= range []float64{...}` to avoid memory allocation.
	isInf := func(ord float64) bool {
		return math.IsInf(ord, 0)
	}
	if isInf(boundingBox.LoX) || isInf(boundingBox.LoY) || isInf(boundingBox.HiX) || isInf(boundingBox.HiY) {
		return true
	}
	return false
}

// BoundingBoxHasNaNCoordinates checks if the bounding box of a Geometry
// has a NaN coordinate.
func BoundingBoxHasNaNCoordinates(g geo.Geometry) bool {
	boundingBox := g.BoundingBoxRef()
	if boundingBox == nil {
		return false
	}
	// Don't use `:= range []float64{...}` to avoid memory allocation.
	isNaN := func(ord float64) bool {
		return math.IsNaN(ord)
	}
	if isNaN(boundingBox.LoX) || isNaN(boundingBox.LoY) || isNaN(boundingBox.HiX) || isNaN(boundingBox.HiY) {
		return true
	}
	return false
}

func initialPoint3D(points []*geom.Point) *geom.Point {
	mass := 0.0
	guess := geom.NewPointFlat(geom.XYZ, []float64{0, 0, 0})
	for _, p := range points {
		x := p.X() * p.M()
		y := p.Y() * p.M()
		z := p.Z() * p.M()
		_ = guess.MustSetCoords(geom.Coord([]float64{x, y, z}))
		mass += p.M()
	}

	return geom.NewPointFlat(geom.XYZ, []float64{guess.X() / mass, guess.Y() / mass, guess.Z() / mass})
}

func updatePoint(median *geom.Point, points []*geom.Point, max_iter int, tolerance float32) int {
	iter := 0
	hit := false
	distances := make([]float64, len(points))
	wdCurr := weightedDistances3D(median, points, distances)
	
	for ; iter < max_iter; iter++ {
		next := geom.NewPointFlat(geom.XYZ, []float64{0, 0, 0})
		denom := 0.0
		for i, point := range points {
			if distances[i] > EPSILON {
				x := point.X() * point.M()
				y := point.Y() * point.M()
				z := point.Z() * point.M()
				_ = next.MustSetCoords(geom.Coord([]float64{x, y, z}))
				denom += 1.0 / distances[i]
			} else {
				hit = true
			}

			if denom > EPSILON {
				x := next.X() / denom
				y := next.Y() / denom
				z := next.Z() / denom
				_ = next.MustSetCoords(geom.Coord([]float64{x, y, z}))

				if hit {
					dx := 0.0
					dy := 0.0
					dz := 0.0
					hit = false
					for j, p := range points {
						if distances[j] > EPSILON {
							dx += (p.X() - median.X()) * p.M()
							dy += (p.Y() - median.Y()) * p.M()
							dz += (p.Z() - median.Z()) * p.M()
						}
					}

					dsqrt := math.Sqrt(dx*dx + dy*dy + dz*dz)
					if dsqrt > EPSILON {
						rInv := math.Max(0, 1.0 / dsqrt)
						x := (1.0 - rInv) * next.X() + rInv*median.X()
						y := (1.0 - rInv) * next.Y() + rInv*median.Y()
						z := (1.0 - rInv) * next.Z() + rInv*median.Z()
						_ = next.MustSetCoords(geom.Coord([]float64{x, y, z}))
					}
				}

				wdNext := weightedDistances3D(next, points, distances)
				delta := wdCurr - wdNext
				if delta < float64(tolerance) {
					break
				}
				_ = median.MustSetCoords(geom.Coord([]float64{next.X(), next.Y(), next.Z()}))
				wdCurr = wdNext
			}
		}
	}	

	return iter
}

func weightedDistances3D(p *geom.Point, points []*geom.Point, distances []float64) float64 {
	weight := 0.0
	for i, point := range points {
		dist := euclideanDistance3D(p, point)
		distances[i] = dist / point.M()
		weight += dist * point.M()
	}
	return weight
}

func euclideanDistance3D(p1 *geom.Point, p2 *geom.Point) float64 {
	dx := p1.X() - p2.X()
	dy := p1.Y() - p2.Y()
	dz := p1.Z() - p2.Z()
	return math.Sqrt(dx*dx + dy*dy + dz*dz)
}

func transformPoints4D(mp *geom.MultiPoint) ([]*geom.Point, error) {
	points := make([]*geom.Point, mp.NumPoints())
	n := 0
	for i := 0; i < mp.NumPoints(); i++ {
		p := mp.Point(i)
		if p.Layout() == geom.XYM || p.Layout() == geom.XYZM {
			if p.M() < 0 {
				return nil, pgerror.Newf(pgcode.InvalidParameterValue, "Geometric median input contains points with negative weights (POINT(%g %g %g %g))." + 
					"Implementation can't guarantee global minimum convergence.", p.X(), p.Y(), p.Z(), p.M())
			} else if p.M() <= EPSILON {
				continue
			}
			points[n] = geom.NewPointFlat(geom.XYZM, []float64{p.X(), p.Y(), p.Z(), p.M()})
		} else {
			points[n] = geom.NewPointFlat(geom.XYZM, []float64{p.X(), p.Y(), p.Z(), 1.0})
		}
		n++
	}

	return points[:n], nil
}
